package com.bank.risk.etl.rule;

import java.io.Serializable;
import java.math.BigDecimal;

/** 某一用户在滑动窗口内的聚合视图，作为规则引擎的输入。 */
public class RiskWindowSnapshot implements Serializable {

    private static final long serialVersionUID = 1L;

    private final int count;
    private final BigDecimal totalAmount;
    private final String previousCity;
    private final long previousEventTime;
    private final long windowMs;

    public RiskWindowSnapshot(int count, BigDecimal totalAmount,
                              String previousCity, long previousEventTime, long windowMs) {
        this.count = count;
        this.totalAmount = totalAmount;
        this.previousCity = previousCity;
        this.previousEventTime = previousEventTime;
        this.windowMs = windowMs;
    }

    public int getCount() {
        return count;
    }

    public BigDecimal getTotalAmount() {
        return totalAmount;
    }

    public String getPreviousCity() {
        return previousCity;
    }

    public long getPreviousEventTime() {
        return previousEventTime;
    }

    public long getWindowMs() {
        return windowMs;
    }
}
