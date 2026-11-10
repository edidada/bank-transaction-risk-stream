package com.bank.risk.cdc;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * cdc-ingestion 作业配置。
 *
 * <p>优先级：命令行参数（{@code --key value} 或 {@code --key=value}） &gt; 环境变量 &gt; 默认值。
 * 环境变量名由键名转换而来：{@code mysql-host -> MYSQL_HOST}。
 *
 * <p>把配置解析做成不依赖 Flink 的纯 Java 类，便于在单元测试中覆盖。
 */
public final class JobOptions {

    private static final Map<String, String> DEFAULTS;

    static {
        Map<String, String> defaults = new LinkedHashMap<>();
        defaults.put("mysql-host", "mysql");
        defaults.put("mysql-port", "3306");
        defaults.put("mysql-user", "cdc");
        defaults.put("mysql-password", "cdc123456");
        defaults.put("mysql-database", "bank");
        defaults.put("mysql-table", "bank.transaction");
        defaults.put("mysql-server-id", "5401-5404");
        defaults.put("cdc-startup-mode", "initial");
        defaults.put("kafka-bootstrap", "kafka:9092");
        defaults.put("kafka-topic", "bank-cdc-events");
        defaults.put("parallelism", "1");
        defaults.put("checkpoint-interval-ms", "30000");
        defaults.put("job-name", "cdc-ingestion");
        DEFAULTS = Collections.unmodifiableMap(defaults);
    }

    private final Map<String, String> values;

    private JobOptions(Map<String, String> values) {
        this.values = Collections.unmodifiableMap(values);
    }

    public static JobOptions from(String[] args) {
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
            String env = System.getenv(toEnvName(entry.getKey()));
            if (env != null && !env.isEmpty()) {
                resolved.put(entry.getKey(), env);
            }
        }
        resolved.putAll(parsed);
        return new JobOptions(resolved);
    }

    private static String toEnvName(String key) {
        return key.replace('-', '_').replace('.', '_').toUpperCase();
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
        require("mysql-host");
        require("mysql-user");
        require("mysql-database");
        require("mysql-table");
        require("kafka-bootstrap");
        require("kafka-topic");
        if (getInt("mysql-port") <= 0) {
            throw new IllegalArgumentException("mysql-port 必须为正整数");
        }
        if (getInt("parallelism") <= 0) {
            throw new IllegalArgumentException("parallelism 必须为正整数");
        }
        if (getLong("checkpoint-interval-ms") <= 0) {
            throw new IllegalArgumentException("checkpoint-interval-ms 必须为正整数");
        }
        String mode = get("cdc-startup-mode");
        if (!"initial".equals(mode) && !"latest".equals(mode)
                && !"earliest".equals(mode) && !"snapshot".equals(mode)) {
            throw new IllegalArgumentException("cdc-startup-mode 仅支持 initial/latest/earliest/snapshot，实际为: " + mode);
        }
    }

    private void require(String key) {
        String value = values.get(key);
        if (value == null || value.trim().isEmpty()) {
            throw new IllegalArgumentException("缺少必填配置: " + key);
        }
    }
}
