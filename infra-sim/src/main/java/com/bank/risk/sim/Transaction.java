package com.bank.risk.sim;

import java.math.BigDecimal;

/** 一条待写入 MySQL 的模拟交易记录。 */
public class Transaction {

    private final String transactionId;
    private final String userId;
    private final BigDecimal amount;
    private final String currency;
    private final String status;
    private final String channel;
    private final String merchantId;
    private final String city;
    private final String cardNo;
    private final long eventTime;

    public Transaction(String transactionId, String userId, BigDecimal amount, String currency,
                       String status, String channel, String merchantId, String city,
                       String cardNo, long eventTime) {
        this.transactionId = transactionId;
        this.userId = userId;
        this.amount = amount;
        this.currency = currency;
        this.status = status;
        this.channel = channel;
        this.merchantId = merchantId;
        this.city = city;
        this.cardNo = cardNo;
        this.eventTime = eventTime;
    }

    public String getTransactionId() {
        return transactionId;
    }

    public String getUserId() {
        return userId;
    }

    public BigDecimal getAmount() {
        return amount;
    }

    public String getCurrency() {
        return currency;
    }

    public String getStatus() {
        return status;
    }

    public String getChannel() {
        return channel;
    }

    public String getMerchantId() {
        return merchantId;
    }

    public String getCity() {
        return city;
    }

    public String getCardNo() {
        return cardNo;
    }

    public long getEventTime() {
        return eventTime;
    }
}
