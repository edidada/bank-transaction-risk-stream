package com.bank.risk.etl.rule;

import com.bank.risk.etl.model.RiskResult;
import com.bank.risk.etl.model.TransactionEvent;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

/**
 * 单用户滑动窗口状态（纯 Java，可脱离 Flink 单测），由 Flink ValueState 持有。
 *
 * <p>以事件时间为基准做窗口驱逐；用 {@code maxSeenEventTime} 防止乱序事件让窗口回退。
 * 超过窗口的旧记录会被移除，状态大小与窗口长度成正比，而不是全量历史。
 */
public class UserRiskWindow implements Serializable {

    private static final long serialVersionUID = 1L;

    private final long windowMs;
    private final List<Long> eventTimes = new ArrayList<>();
    private final List<BigDecimal> amounts = new ArrayList<>();

    private String lastCity;
    private long lastEventTime;
    private long maxSeenEventTime;

    public UserRiskWindow(long windowMs) {
        if (windowMs <= 0) {
            throw new IllegalArgumentException("windowMs 必须大于 0");
        }
        this.windowMs = windowMs;
    }

    public RiskResult evaluate(TransactionEvent event, RiskRules rules) {
        long now = Math.max(maxSeenEventTime, event.getEventTime());
        maxSeenEventTime = now;
        String previousCity = lastCity;
        long previousEventTime = lastEventTime;

        // 先把本笔计入窗口，再驱逐过期数据，最后基于完整窗口做规则判定
        eventTimes.add(now);
        amounts.add(event.getAmount() == null ? BigDecimal.ZERO : event.getAmount());
        evictExpired(now);

        RiskWindowSnapshot snapshot = new RiskWindowSnapshot(
                eventTimes.size(), sum(amounts), previousCity, previousEventTime, windowMs);
        RiskResult result = rules.evaluate(event, snapshot);

        lastCity = event.getCity();
        lastEventTime = now;
        return result;
    }

    private void evictExpired(long now) {
        long cutoff = now - windowMs;
        int expired = 0;
        while (expired < eventTimes.size() && eventTimes.get(expired) < cutoff) {
            expired++;
        }
        if (expired > 0) {
            eventTimes.subList(0, expired).clear();
            amounts.subList(0, expired).clear();
        }
    }

    private static BigDecimal sum(List<BigDecimal> values) {
        BigDecimal total = BigDecimal.ZERO;
        for (BigDecimal value : values) {
            total = total.add(value);
        }
        return total;
    }

    /** 当前窗口内的交易笔数，供监控与测试使用。 */
    public int size() {
        return eventTimes.size();
    }
}
