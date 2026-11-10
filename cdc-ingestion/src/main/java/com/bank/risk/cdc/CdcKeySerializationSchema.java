package com.bank.risk.cdc;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.serialization.SerializationSchema;

import java.nio.charset.StandardCharsets;

/**
 * Kafka 消息 Key = 交易流水号（删除事件取 before 镜像）。
 *
 * <p>同一笔交易的所有变更（INSERT / UPDATE / DELETE）落到同一分区，
 * 保证下游按 key 消费时的顺序性，这是风控链路“状态不回退”的前提。
 */
public class CdcKeySerializationSchema implements SerializationSchema<String> {

    private static final long serialVersionUID = 1L;
    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Override
    public byte[] serialize(String element) {
        if (element == null) {
            return new byte[0];
        }
        try {
            JsonNode root = MAPPER.readTree(element);
            JsonNode row = root.path("after");
            if (row.isMissingNode() || row.isNull()) {
                row = root.path("before");
            }
            String key = row.path("transaction_id").asText("");
            if (key.isEmpty()) {
                key = root.path("db").asText("") + "." + root.path("table").asText("")
                        + ":" + root.path("ts_ms").asText("");
            }
            return key.getBytes(StandardCharsets.UTF_8);
        } catch (Exception e) {
            // 解析失败不应阻断采集，退化为整条消息作为 key
            return element.getBytes(StandardCharsets.UTF_8);
        }
    }
}
