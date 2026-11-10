package com.bank.risk.etl.rule;

import com.bank.risk.etl.model.RiskResult;
import com.bank.risk.etl.model.TransactionEvent;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class UserRiskWindowTest {

    private static final long WINDOW_MS = 10 * 60 * 1000L;
    private static final long BASE = 1730000000000L;
    private static final RiskRules RULES = new RiskRules(60, 30);

    @Test
    void shouldAccumulateEventsInsideWindow() {
        UserRiskWindow window = new UserRiskWindow(WINDOW_MS);

        RiskResult last = null;
        for (int i = 0; i < 5; i++) {
            last = window.evaluate(event(BASE + i * 60_000L, "100.00", "Beijing"), RULES);
        }

        assertEquals(5, last.getWindowCount());
        assertEquals(new BigDecimal("500.00"), last.getWindowAmount());
        assertTrue(last.getReasons().contains("HIGH_FREQUENCY"));
        assertEquals(5, window.size());
    }

    @Test
    void shouldEvictExpiredEvents() {
        UserRiskWindow window = new UserRiskWindow(WINDOW_MS);

        window.evaluate(event(BASE, "100.00", "Beijing"), RULES);
        window.evaluate(event(BASE + 60_000L, "100.00", "Beijing"), RULES);
        // 第三笔足够晚，使窗口下界越过第二笔（窗口边界为闭区间）
        RiskResult result = window.evaluate(event(BASE + WINDOW_MS + 120_000L, "100.00", "Beijing"), RULES);

        // 前两笔已超过 10 分钟窗口，只剩本笔
        assertEquals(1, result.getWindowCount());
        assertEquals(new BigDecimal("100.00"), result.getWindowAmount());
        assertEquals(1, window.size());
    }

    @Test
    void shouldNotShrinkWindowOnOutOfOrderEvent() {
        UserRiskWindow window = new UserRiskWindow(WINDOW_MS);

        window.evaluate(event(BASE + 120_000L, "100.00", "Beijing"), RULES);
        // 乱序到达的旧事件：仍以已见到的最大事件时间为准，不应让窗口回退
        RiskResult result = window.evaluate(event(BASE, "50.00", "Beijing"), RULES);

        assertEquals(2, result.getWindowCount());
        assertEquals(new BigDecimal("150.00"), result.getWindowAmount());
        assertEquals(2, window.size());
    }

    @Test
    void shouldDetectCityHoppingAcrossConsecutiveEvents() {
        UserRiskWindow window = new UserRiskWindow(WINDOW_MS);

        window.evaluate(event(BASE, "100.00", "Beijing"), RULES);
        RiskResult result = window.evaluate(event(BASE + 60_000L, "100.00", "Shanghai"), RULES);

        assertTrue(result.getReasons().contains("CITY_HOPPING"));
    }

    @Test
    void shouldRejectNonPositiveWindow() {
        try {
            new UserRiskWindow(0);
        } catch (IllegalArgumentException expected) {
            assertTrue(expected.getMessage().contains("windowMs"));
            return;
        }
        throw new AssertionError("应抛出 IllegalArgumentException");
    }

    private static TransactionEvent event(long eventTime, String amount, String city) {
        TransactionEvent event = new TransactionEvent();
        event.setOp("c");
        event.setTransactionId("TX-" + eventTime);
        event.setUserId("U1001");
        event.setAmount(new BigDecimal(amount));
        event.setCity(city);
        event.setMerchantId("M-001");
        event.setEventTime(eventTime);
        return event;
    }
}
