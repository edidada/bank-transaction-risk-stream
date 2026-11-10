package com.bank.risk.etl.model;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.Objects;

/**
 * 从 Kafka 中的统一 CDC JSON 解析出的交易事件，是 ETL 内部的标准数据模型。
 *
 * <p>字段与 MySQL {@code bank.transaction} 表列一一对应；
 * {@code event_time} 在源表中定义为 BIGINT（epoch millis），避免 Debezium 时间类型转换带来的歧义。
 */
public class TransactionEvent implements Serializable {

    private static final long serialVersionUID = 1L;

    private String op;
    private String db;
    private String table;
    private long tsMs;
    private String transactionId;
    private String userId;
    private BigDecimal amount;
    private String currency;
    private String status;
    private String channel;
    private String merchantId;
    private String city;
    private String cardNo;
    private long eventTime;
    /** 原始 CDC JSON，写入 Hudi 变更日志时原样保留（含 before/after 镜像）。 */
    private String raw;

    public String getOp() {
        return op;
    }

    public void setOp(String op) {
        this.op = op;
    }

    public String getDb() {
        return db;
    }

    public void setDb(String db) {
        this.db = db;
    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public long getTsMs() {
        return tsMs;
    }

    public void setTsMs(long tsMs) {
        this.tsMs = tsMs;
    }

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

    public BigDecimal getAmount() {
        return amount;
    }

    public void setAmount(BigDecimal amount) {
        this.amount = amount;
    }

    public String getCurrency() {
        return currency;
    }

    public void setCurrency(String currency) {
        this.currency = currency;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public String getChannel() {
        return channel;
    }

    public void setChannel(String channel) {
        this.channel = channel;
    }

    public String getMerchantId() {
        return merchantId;
    }

    public void setMerchantId(String merchantId) {
        this.merchantId = merchantId;
    }

    public String getCity() {
        return city;
    }

    public void setCity(String city) {
        this.city = city;
    }

    public String getCardNo() {
        return cardNo;
    }

    public void setCardNo(String cardNo) {
        this.cardNo = cardNo;
    }

    public long getEventTime() {
        return eventTime;
    }

    public void setEventTime(long eventTime) {
        this.eventTime = eventTime;
    }

    public String getRaw() {
        return raw;
    }

    public void setRaw(String raw) {
        this.raw = raw;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof TransactionEvent)) {
            return false;
        }
        return Objects.equals(transactionId, ((TransactionEvent) other).transactionId);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(transactionId);
    }

    @Override
    public String toString() {
        return "TransactionEvent{" + "transactionId='" + transactionId + '\''
                + ", userId='" + userId + '\'' + ", amount=" + amount + ", op='" + op + '\'' + '}';
    }
}
