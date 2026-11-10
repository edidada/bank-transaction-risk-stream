package com.bank.risk.sim;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 交易数据生成器配置。优先级：命令行参数 &gt; 环境变量 &gt; 默认值。
 * 环境变量名如 {@code JDBC_URL}、{@code MYSQL_USER}、{@code TPS}。
 */
public final class GeneratorOptions {

    private static final Map<String, String> DEFAULTS;

    static {
        Map<String, String> defaults = new LinkedHashMap<>();
        defaults.put("jdbc-url", "jdbc:mysql://localhost:3306/bank?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=Asia/Shanghai");
        defaults.put("mysql-user", "root");
        defaults.put("mysql-password", "root");
        defaults.put("tps", "20");
        defaults.put("duration-seconds", "0");
        defaults.put("seed", "20261110");
        defaults.put("update-ratio", "0.15");
        defaults.put("delete-ratio", "0.02");
        defaults.put("init-schema", "true");
        DEFAULTS = Collections.unmodifiableMap(defaults);
    }

    private final Map<String, String> values;

    private GeneratorOptions(Map<String, String> values) {
        this.values = Collections.unmodifiableMap(values);
    }

    public static GeneratorOptions from(String[] args) {
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
        return new GeneratorOptions(resolved);
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

    public double getDouble(String key) {
        return Double.parseDouble(values.get(key));
    }

    public boolean getBoolean(String key) {
        return Boolean.parseBoolean(values.get(key));
    }

    public void validate() {
        require("jdbc-url");
        require("mysql-user");
        if (getInt("tps") <= 0 || getInt("tps") > 10000) {
            throw new IllegalArgumentException("tps 必须在 1..10000 之间");
        }
        if (getLong("duration-seconds") < 0) {
            throw new IllegalArgumentException("duration-seconds 不能为负数（0 表示持续运行）");
        }
        if (getDouble("update-ratio") < 0 || getDouble("update-ratio") > 1) {
            throw new IllegalArgumentException("update-ratio 必须在 0..1 之间");
        }
        if (getDouble("delete-ratio") < 0 || getDouble("delete-ratio") > 1) {
            throw new IllegalArgumentException("delete-ratio 必须在 0..1 之间");
        }
        if (getDouble("update-ratio") + getDouble("delete-ratio") > 1) {
            throw new IllegalArgumentException("update-ratio 与 delete-ratio 之和不能大于 1");
        }
    }

    private void require(String key) {
        String value = values.get(key);
        if (value == null || value.trim().isEmpty()) {
            throw new IllegalArgumentException("缺少必填配置: " + key);
        }
    }
}
