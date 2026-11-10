package com.bank.risk.etl;

import com.bank.risk.etl.function.RiskScoringFunction;
import com.bank.risk.etl.model.TransactionEvent;
import com.bank.risk.etl.parser.TransactionEventParser;
import com.bank.risk.etl.sink.DorisStreamLoadSink;
import com.bank.risk.etl.sink.HudiChangelogSink;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;

/**
 * 作业二：Kafka -&gt; 清洗 -&gt; 风控计算 -&gt; 双写。
 *
 * <p>分流逻辑（对应银行场景的两类诉求）：
 * <ul>
 *   <li>原始变更日志 -&gt; Hudi 等价存储：合规审计、历史回溯，要求全量留存。</li>
 *   <li>清洗后事件 -&gt; 按用户 keyBy 做窗口风控计算 -&gt; Doris：低延迟实时查询。</li>
 *   <li>解析失败记录 -&gt; Kafka DLQ Topic：不阻断主流，可单独回放。</li>
 * </ul>
 */
public final class StreamEtlJob {

    private StreamEtlJob() {
    }

    public static void main(String[] args) throws Exception {
        EtlOptions options = EtlOptions.from(args);
        options.validate();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(options.getInt("parallelism"));
        env.enableCheckpointing(options.getLong("checkpoint-interval-ms"));
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(options.get("kafka-bootstrap"))
                .setTopics(options.get("kafka-topic"))
                .setGroupId(options.get("kafka-group"))
                .setStartingOffsets(startingOffsets(options.get("kafka-startup-mode")))
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        SingleOutputStreamOperator<TransactionEvent> parsed = env
                .fromSource(source, WatermarkStrategy.noWatermarks(), "kafka-cdc-source")
                .uid("kafka-cdc-source")
                .process(new TransactionEventParser())
                .uid("parse-cdc-json")
                .name("parse-and-clean-cdc-json");

        // 1) 坏记录进 DLQ Topic，避免脏数据拖垮主链路
        KafkaSink<String> dlqSink = KafkaSink.<String>builder()
                .setBootstrapServers(options.get("kafka-bootstrap"))
                .setRecordSerializer(KafkaRecordSerializationSchema.<String>builder()
                        .setTopic(options.get("dlq-topic"))
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build())
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();
        parsed.getSideOutput(TransactionEventParser.DLQ_TAG)
                .sinkTo(dlqSink)
                .uid("dlq-to-kafka")
                .name("dead-letter-to-kafka");

        // 2) 原始变更日志全量留存（Hudi 等价实现）
        // RichSinkFunction 属于旧版 SinkFunction API，Flink 1.20 需用 addSink（sinkTo 只接受 Sink V1/V2）
        parsed.addSink(new HudiChangelogSink(
                        options.get("hudi-base-path"), options.getInt("hudi-flush-every")))
                .uid("hudi-changelog-sink")
                .name("raw-changelog-to-history");

        // 3) 按用户维度做窗口风控计算后写入 Doris
        parsed.keyBy(TransactionEvent::getUserId)
                .process(new RiskScoringFunction(
                        options.getLong("risk-window-ms"),
                        options.getInt("risk-high-score"),
                        options.getInt("risk-medium-score"),
                        options.getLong("state-ttl-hours")))
                .uid("risk-scoring")
                .name("risk-scoring-by-user")
                .addSink(new DorisStreamLoadSink(
                        options.get("doris-fe-host"),
                        options.getInt("doris-http-port"),
                        options.get("doris-database"),
                        options.get("doris-table"),
                        options.get("doris-user"),
                        options.get("doris-password"),
                        options.getInt("doris-batch-size"),
                        options.getLong("doris-flush-interval-ms")))
                .uid("doris-risk-sink")
                .name("risk-result-to-doris");

        env.execute(options.get("job-name"));
    }

    private static OffsetsInitializer startingOffsets(String mode) {
        switch (mode) {
            case "latest":
                return OffsetsInitializer.latest();
            case "committed":
                return OffsetsInitializer.committedOffsets(OffsetResetStrategy.LATEST);
            case "earliest":
            default:
                return OffsetsInitializer.earliest();
        }
    }
}
