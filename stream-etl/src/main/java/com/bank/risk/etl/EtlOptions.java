package com.bank.risk.etl;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * stream-etl 作业配置，解析规则与 cdc-ingestion 的 {@code JobOptions} 一致：
 * 命令行参数 &gt; 环境变量 &gt; 默认值。环境变量名如 {@code KAFKA_TOPIC}、{@code DORIS_FE_HOST}。
 */
public final class EtlOptions {

    private static final Map<String, String> DEFAULTS;

    static {
        Map<String, String> defaults = new LinkedHashMap<>();
        defaults.put("kafka-bootstrap", "kafka:9092");
        defaults.put("kafka-topic", "bank-cdc-events");
        defaults.put("kafka-group", "bank-risk-etl");
        defaults.put("dlq-topic", "bank-cdc-dlq");
        defaults.put("kafka-startup-mode", "earliest");
        defaults.put("doris-fe-host", "doris-fe");
        defaults.put("doris-http-port", "8030");
        defaults.put("doris-database", "risk");
        defaults.put("doris-table", "risk_event_detail");
        defaults.put("doris-user", "root");
        defaults.put("doris-password", "");
        defaults.put("doris-batch-size", "200");
        defaults.put("doris-flush-interval-ms", "5000");
        defaults.put("hudi-base-path", "file:///data/hudi/bank_transaction_changelog");
        defaults.put("hudi-flush-every", "500");
        defaults.put("risk-window-ms", "600000");
        defaults.put("risk-high-score", "60");
        defaults.put("risk-medium-score", "30");
        defaults.put("parallelism", "2");
        defaults.put("checkpoint-interval-ms", "30000");
        defaults.put("state-ttl-hours", "2");
        defaults.put("job-name", "stream-etl");
        DEFAULTS = Collections.unmodifiableMap(defaults);
    }

    private final Map<String, String> values;

    private EtlOptions(Map<String, String> values) {
        this.values = Collections.unmodifiableMap(values);
    }

    public static EtlOptions from(String[] args) {
        Map<String, String> parsed = new LinkedHashMap<>();
        if (args != null) {
            for (int i = 0; i < args.length; i++) {
                String arg = args[i];
                if (arg == null || !arg.startsWith("--")) {
                    continue;
                }
                String body = arg.substring(2);
                int eq = body.indexOf('=');
                String key;
                String value;
                if (eq > 0) {
                    key = body.substring(0, eq);
                    value = body.substring(eq + 1);
                } else {
                    key = body;
                    value = (i + 1 < args.length && args[i + 1] != null && !args[i + 1].startsWith("--"))
                            ? args[++i] : "true";
                }
                parsed.put(key, value);
            }
        }

        Map<String, String> resolved = new LinkedHashMap<>(DEFAULTS);
        for (Map.Entry<String, String> entry : DEFAULTS.entrySet()) {
            String env = System.getenv(entry.getKey().replace('-', '_').toUpperCase());
            if (env != null && !env.isEmpty()) {
                resolved.put(entry.getKey(), env);
            }
        }
        resolved.putAll(parsed);
        return new EtlOptions(resolved);
    }

    public String get(String key) {
        return values.get(key);
    }

    public int getInt(String key) {
        return Integer.parseInt(values.get(key));
    }

    public long getLong(String key) {
        return Long.parseLong(values.get(key));
    }

    public Map<String, String> asMap() {
        return values;
    }

    public void validate() {
        require("kafka-bootstrap");
        require("kafka-topic");
        require("doris-fe-host");
        require("doris-database");
        require("doris-table");
        require("hudi-base-path");
        requireNonNegative("doris-http-port");
        requireNonNegative("parallelism");
        requireNonNegative("doris-batch-size");
        if (getInt("doris-batch-size") <= 0) {
            throw new IllegalArgumentException("doris-batch-size 必须大于 0");
        }
        if (getInt("risk-high-score") <= getInt("risk-medium-score")) {
            throw new IllegalArgumentException("risk-high-score 必须大于 risk-medium-score");
        }
        String mode = get("kafka-startup-mode");
        if (!"earliest".equals(mode) && !"latest".equals(mode) && !"committed".equals(mode)) {
            throw new IllegalArgumentException("kafka-startup-mode 仅支持 earliest/latest/committed");
        }
    }

    private void require(String key) {
        String value = values.get(key);
        if (value == null || value.trim().isEmpty()) {
            throw new IllegalArgumentException("缺少必填配置: " + key);
        }
    }

    private void requireNonNegative(String key) {
        if (getInt(key) <= 0) {
            throw new IllegalArgumentException(key + " 必须为正整数");
        }
    }
}
