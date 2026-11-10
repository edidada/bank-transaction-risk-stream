package com.bank.risk.etl.sink;

import com.bank.risk.etl.model.TransactionEvent;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.UUID;

/**
 * 历史存储 Sink：把原始 CDC 变更日志按天分区写成 NDJSON。
 *
 * <p>这是模拟工程对 Hudi 的等价实现：保留 {@code op / before / after} 完整变更镜像，
 * 等价于 Hudi MOR 表开启 {@code changelog.enabled} 后的 changelog 语义，可供审计与回放。
 * 之所以不引入 {@code hudi-flink1.20-bundle}：该 bundle 体积大且与 CDC Connector 存在
 * Scala / Parquet 依赖冲突风险；本地模拟用文件系统即可，生产切换到对象存储时
 * 把 base-path 换成 {@code s3a://...} 并引入对应 Hadoop/S3 依赖即可（见部署文档）。
 */
public class HudiChangelogSink extends RichSinkFunction<TransactionEvent> {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(HudiChangelogSink.class);
    private static final ZoneId BANK_ZONE = ZoneId.of("Asia/Shanghai");
    private static final DateTimeFormatter DAY = DateTimeFormatter.ofPattern("yyyy-MM-dd");

    private final String basePath;
    private final int flushEvery;

    private transient BufferedWriter writer;
    private transient int sinceFlush;

    public HudiChangelogSink(String basePath, int flushEvery) {
        this.basePath = basePath;
        this.flushEvery = Math.max(1, flushEvery);
    }

    @Override
    public void open(Configuration parameters) throws IOException {
        int subtask = getRuntimeContext().getIndexOfThisSubtask();
        Path file = resolvePath(basePath, System.currentTimeMillis(), subtask,
                UUID.randomUUID().toString().replace("-", "").substring(0, 8));
        Files.createDirectories(file.getParent());
        writer = Files.newBufferedWriter(file, StandardCharsets.UTF_8,
                StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        LOG.info("HudiChangelogSink 写入文件: {}", file);
    }

    @Override
    public void invoke(TransactionEvent value, Context context) throws IOException {
        if (value == null || value.getRaw() == null) {
            return;
        }
        writer.write(value.getRaw());
        writer.newLine();
        if (++sinceFlush >= flushEvery) {
            writer.flush();
            sinceFlush = 0;
        }
    }

    /** 按事件日期（银行时区）生成分区路径：{@code <base>/dt=yyyy-MM-dd/part-<subtask>-<fileId>.ndjson}。 */
    public static Path resolvePath(String basePath, long eventTime, int subtask, String fileId) {
        String date = partitionOf(eventTime);
        return toPath(basePath).resolve("dt=" + date)
                .resolve("part-" + subtask + "-" + fileId + ".ndjson");
    }

    public static String partitionOf(long eventTime) {
        return Instant.ofEpochMilli(eventTime).atZone(BANK_ZONE).format(DAY);
    }

    /** 支持 {@code file://} 与普通本地路径；Windows 下兼容 {@code file:///D:/...} 写法。 */
    public static Path toPath(String basePath) {
        String path = basePath;
        if (path.startsWith("file://")) {
            path = path.substring("file://".length());
            if (path.matches("^/[A-Za-z]:.*")) {
                path = path.substring(1);
            }
        }
        return Paths.get(path);
    }

    @Override
    public void close() throws IOException {
        if (writer != null) {
            writer.flush();
            writer.close();
        }
    }
}
