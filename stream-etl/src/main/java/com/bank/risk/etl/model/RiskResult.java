package com.bank.risk.etl.model;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;
import java.math.BigDecimal;

/**
 * 风控结果，一行对应 Doris 表 {@code risk.risk_event_detail} 的一行。
 *
 * <p>字段名通过 {@link JsonProperty} 显式声明为下划线风格，
 * 这样 Doris Stream Load 以 {@code format=json&read_json_by_line=true} 导入时无需再指定 columns。
 */
public class RiskResult implements Serializable {

    private static final long serialVersionUID = 1L;

    public static final String LEVEL_HIGH = "HIGH";
    public static final String LEVEL_MEDIUM = "MEDIUM";
    public static final String LEVEL_LOW = "LOW";

    @JsonProperty("transaction_id")
    private String transactionId;
    @JsonProperty("user_id")
    private String userId;
    @JsonProperty("event_time")
    private long eventTime;
    @JsonProperty("processed_at")
    private long processedAt;
    @JsonProperty("op")
    private String op;
    @JsonProperty("amount")
    private BigDecimal amount;
    @JsonProperty("city")
    private String city;
    @JsonProperty("merchant_id")
    private String merchantId;
    @JsonProperty("score")
    private int score;
    @JsonProperty("level")
    private String level;
    @JsonProperty("window_count")
    private int windowCount;
    @JsonProperty("window_amount")
    private BigDecimal windowAmount;
    /** 命中规则说明，多个之间以 | 分隔，便于在 Doris 中直接检索。 */
    @JsonProperty("reasons")
    private String reasons;

    public String getTransactionId() {
        return transactionId;
    }

    public void setTransactionId(String transactionId) {
        this.transactionId = transactionId;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public long getEventTime() {
        return eventTime;
    }

    public void setEventTime(long eventTime) {
        this.eventTime = eventTime;
    }

    public long getProcessedAt() {
        return processedAt;
    }

    public void setProcessedAt(long processedAt) {
        this.processedAt = processedAt;
    }

    public String getOp() {
        return op;
    }

    public void setOp(String op) {
        this.op = op;
    }

    public BigDecimal getAmount() {
        return amount;
    }

    public void setAmount(BigDecimal amount) {
        this.amount = amount;
    }

    public String getCity() {
        return city;
    }

    public void setCity(String city) {
        this.city = city;
    }

    public String getMerchantId() {
        return merchantId;
    }

    public void setMerchantId(String merchantId) {
        this.merchantId = merchantId;
    }

    public int getScore() {
        return score;
    }

    public void setScore(int score) {
        this.score = score;
    }

    public String getLevel() {
        return level;
    }

    public void setLevel(String level) {
        this.level = level;
    }

    public int getWindowCount() {
        return windowCount;
    }

    public void setWindowCount(int windowCount) {
        this.windowCount = windowCount;
    }

    public BigDecimal getWindowAmount() {
        return windowAmount;
    }

    public void setWindowAmount(BigDecimal windowAmount) {
        this.windowAmount = windowAmount;
    }

    public String getReasons() {
        return reasons;
    }

    public void setReasons(String reasons) {
        this.reasons = reasons;
    }
}
