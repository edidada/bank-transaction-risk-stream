package com.bank.risk.sim;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Random;

/**
 * 可复现的模拟交易生成器（纯 Java，给定 seed 输出确定，便于单元测试）。
 *
 * <p>刻意注入了几类会被风控命中的样本，方便端到端演示：
 * 约 3% 大额交易、约 2% 黑名单商户、多城市随机分布（可能触发跨城规则）。
 */
public class TransactionFactory {

    private static final String[] USERS = {
            "U1001", "U1002", "U1003", "U1004", "U1005", "U1006", "U1007", "U1008"
    };
    private static final String[] CITIES = {
            "Beijing", "Shanghai", "Guangzhou", "Shenzhen", "Hangzhou", "Chengdu"
    };
    private static final String[] CHANNELS = {"MOBILE", "WEB", "POS", "ATM"};
    private static final String[] MERCHANTS = {
            "M-001", "M-002", "M-003", "M-004", "M-005",
            "M-006", "M-007", "M-008", "M-009", "M-010"
    };
    private static final String[] STATUSES = {"INIT", "SUCCESS", "SUCCESS", "SUCCESS", "FAILED"};

    private final Random random;
    private long sequence;

    public TransactionFactory(long seed) {
        this.random = new Random(seed);
    }

    /** @param eventTime 事件发生时间（epoch millis），由调用方传入，保持本类无时间依赖。 */
    public Transaction next(long eventTime) {
        String transactionId = String.format("TX%012d", ++sequence);
        String userId = pick(USERS);
        BigDecimal amount = randomAmount();
        String merchantId = random.nextDouble() < 0.02 ? "M-BLACK-001" : pick(MERCHANTS);
        String cardNo = "622202" + String.format("%010d", random.nextLong() & 0x7FFFFFFFFFL);

        return new Transaction(
                transactionId,
                userId,
                amount,
                "CNY",
                pick(STATUSES),
                pick(CHANNELS),
                merchantId,
                pick(CITIES),
                cardNo,
                eventTime);
    }

    public long generatedCount() {
        return sequence;
    }

    private BigDecimal randomAmount() {
        double value;
        if (random.nextDouble() < 0.03) {
            // 大额样本：50000 ~ 200000
            value = 50_000 + random.nextDouble() * 150_000;
        } else {
            // 常规样本：1 ~ 5000
            value = 1 + random.nextDouble() * 4_999;
        }
        return BigDecimal.valueOf(value).setScale(2, RoundingMode.HALF_UP);
    }

    private <T> T pick(T[] values) {
        return values[random.nextInt(values.length)];
    }
}
