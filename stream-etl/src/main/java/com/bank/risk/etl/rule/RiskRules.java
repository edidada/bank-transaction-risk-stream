package com.bank.risk.etl.rule;

import com.bank.risk.etl.model.RiskResult;
import com.bank.risk.etl.model.TransactionEvent;

import java.io.Serializable;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * 风控规则引擎（纯函数，无 Flink 依赖，可直接单测）。
 *
 * <p>规则与分值：
 * <table border="1">
 *   <caption>规则表</caption>
 *   <tr><th>规则</th><th>条件</th><th>分值</th></tr>
 *   <tr><td>LARGE_AMOUNT</td><td>单笔金额 &ge; 50000</td><td>40</td></tr>
 *   <tr><td>HIGH_FREQUENCY</td><td>窗口内交易笔数 &ge; 5</td><td>25</td></tr>
 *   <tr><td>WINDOW_AMOUNT_SPIKE</td><td>窗口内累计金额 &ge; 100000</td><td>20</td></tr>
 *   <tr><td>ODD_HOUR</td><td>交易发生在 01:00-05:00（Asia/Shanghai）</td><td>10</td></tr>
 *   <tr><td>CITY_HOPPING</td><td>短时间内出现跨城市交易</td><td>15</td></tr>
 *   <tr><td>BLACKLIST_MERCHANT</td><td>命中风险商户名单</td><td>30</td></tr>
 * </table>
 *
 * <p>分值上限不做截断，便于按分数排序；等级由阈值判定。
 */
public class RiskRules implements Serializable {

    private static final long serialVersionUID = 1L;

    public static final BigDecimal LARGE_AMOUNT = new BigDecimal("50000");
    public static final BigDecimal WINDOW_AMOUNT_SPIKE = new BigDecimal("100000");
    public static final int HIGH_FREQUENCY_COUNT = 5;
    public static final long CITY_HOP_MAX_INTERVAL_MS = 60 * 60 * 1000L;

    private static final int ODD_HOUR_START = 1;
    private static final int ODD_HOUR_END = 5;
    private static final ZoneId BANK_ZONE = ZoneId.of("Asia/Shanghai");

    private static final Set<String> BLACKLIST_MERCHANTS = Collections.unmodifiableSet(
            new HashSet<>(Arrays.asList("M-BLACK-001", "M-BLACK-002")));

    private final int highScore;
    private final int mediumScore;

    public RiskRules(int highScore, int mediumScore) {
        if (highScore <= mediumScore) {
            throw new IllegalArgumentException("highScore 必须大于 mediumScore");
        }
        this.highScore = highScore;
        this.mediumScore = mediumScore;
    }

    public RiskResult evaluate(TransactionEvent event, RiskWindowSnapshot window) {
        List<String> reasons = new ArrayList<>();
        int score = 0;

        if (event.getAmount().compareTo(LARGE_AMOUNT) >= 0) {
            score += 40;
            reasons.add("LARGE_AMOUNT");
        }
        if (window.getCount() >= HIGH_FREQUENCY_COUNT) {
            score += 25;
            reasons.add("HIGH_FREQUENCY");
        }
        if (window.getTotalAmount().compareTo(WINDOW_AMOUNT_SPIKE) >= 0) {
            score += 20;
            reasons.add("WINDOW_AMOUNT_SPIKE");
        }
        if (isOddHour(event.getEventTime())) {
            score += 10;
            reasons.add("ODD_HOUR");
        }
        if (isCityHopping(event, window)) {
            score += 15;
            reasons.add("CITY_HOPPING");
        }
        if (BLACKLIST_MERCHANTS.contains(event.getMerchantId())) {
            score += 30;
            reasons.add("BLACKLIST_MERCHANT");
        }

        RiskResult result = new RiskResult();
        result.setTransactionId(event.getTransactionId());
        result.setUserId(event.getUserId());
        result.setEventTime(event.getEventTime());
        result.setProcessedAt(System.currentTimeMillis());
        result.setOp(event.getOp());
        result.setAmount(event.getAmount());
        result.setCity(event.getCity());
        result.setMerchantId(event.getMerchantId());
        result.setScore(score);
        result.setLevel(levelOf(score));
        result.setWindowCount(window.getCount());
        result.setWindowAmount(window.getTotalAmount());
        result.setReasons(reasons.isEmpty() ? "NONE" : String.join("|", reasons));
        return result;
    }

    private String levelOf(int score) {
        if (score >= highScore) {
            return RiskResult.LEVEL_HIGH;
        }
        if (score >= mediumScore) {
            return RiskResult.LEVEL_MEDIUM;
        }
        return RiskResult.LEVEL_LOW;
    }

    private static boolean isOddHour(long eventTime) {
        ZonedDateTime time = Instant.ofEpochMilli(eventTime).atZone(BANK_ZONE);
        return time.getHour() >= ODD_HOUR_START && time.getHour() < ODD_HOUR_END;
    }

    private static boolean isCityHopping(TransactionEvent event, RiskWindowSnapshot window) {
        String previousCity = window.getPreviousCity();
        if (previousCity == null || previousCity.isEmpty()
                || event.getCity() == null || event.getCity().isEmpty()
                || previousCity.equals(event.getCity())) {
            return false;
        }
        long gap = event.getEventTime() - window.getPreviousEventTime();
        return gap >= 0 && gap <= CITY_HOP_MAX_INTERVAL_MS;
    }

    public static Set<String> blacklistMerchants() {
        return BLACKLIST_MERCHANTS;
    }
}
