package com.bank.risk.etl.rule;

import com.bank.risk.etl.model.RiskResult;
import com.bank.risk.etl.model.TransactionEvent;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.ZoneId;
import java.time.ZonedDateTime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RiskRulesTest {

    private static final RiskRules RULES = new RiskRules(60, 30);
    private static final long NORMAL_TIME = ZonedDateTime.of(2026, 11, 10, 14, 30, 0, 0,
            ZoneId.of("Asia/Shanghai")).toInstant().toEpochMilli();
    private static final long ODD_HOUR_TIME = ZonedDateTime.of(2026, 11, 10, 3, 15, 0, 0,
            ZoneId.of("Asia/Shanghai")).toInstant().toEpochMilli();

    @Test
    void shouldReturnLowForNormalTransaction() {
        TransactionEvent event = event("100.00", "Beijing", "M-001", NORMAL_TIME);

        RiskResult result = RULES.evaluate(event, snapshot(1, "100.00", null, 0));

        assertEquals(RiskResult.LEVEL_LOW, result.getLevel());
        assertEquals(0, result.getScore());
        assertEquals("NONE", result.getReasons());
        assertEquals(new BigDecimal("100.00"), result.getWindowAmount());
        assertEquals(1, result.getWindowCount());
    }

    @Test
    void shouldFlagLargeAmount() {
        RiskResult result = RULES.evaluate(
                event("50000.00", "Beijing", "M-001", NORMAL_TIME),
                snapshot(1, "50000.00", null, 0));

        assertEquals(40, result.getScore());
        assertEquals(RiskResult.LEVEL_MEDIUM, result.getLevel());
        assertTrue(result.getReasons().contains("LARGE_AMOUNT"));
    }

    @Test
    void shouldEscalateToHighWhenMultipleRulesHit() {
        RiskResult result = RULES.evaluate(
                event("60000.00", "Beijing", "M-001", NORMAL_TIME),
                snapshot(5, "260000.00", "Beijing", NORMAL_TIME - 1000));

        assertEquals(RiskResult.LEVEL_HIGH, result.getLevel());
        assertTrue(result.getScore() >= 60);
        assertTrue(result.getReasons().contains("LARGE_AMOUNT"));
        assertTrue(result.getReasons().contains("HIGH_FREQUENCY"));
        assertTrue(result.getReasons().contains("WINDOW_AMOUNT_SPIKE"));
    }

    @Test
    void shouldFlagOddHourTransaction() {
        RiskResult result = RULES.evaluate(
                event("100.00", "Beijing", "M-001", ODD_HOUR_TIME),
                snapshot(1, "100.00", null, 0));

        assertEquals(10, result.getScore());
        assertTrue(result.getReasons().contains("ODD_HOUR"));
    }

    @Test
    void shouldFlagBlacklistMerchant() {
        RiskResult result = RULES.evaluate(
                event("100.00", "Beijing", "M-BLACK-001", NORMAL_TIME),
                snapshot(1, "100.00", null, 0));

        assertEquals(30, result.getScore());
        assertTrue(result.getReasons().contains("BLACKLIST_MERCHANT"));
    }

    @Test
    void shouldFlagCityHoppingWithinOneHour() {
        RiskResult result = RULES.evaluate(
                event("100.00", "Shanghai", "M-001", NORMAL_TIME),
                snapshot(2, "200.00", "Beijing", NORMAL_TIME - 10 * 60 * 1000));

        assertTrue(result.getReasons().contains("CITY_HOPPING"));
    }

    @Test
    void shouldNotFlagCityHoppingWhenTooFarApart() {
        RiskResult result = RULES.evaluate(
                event("100.00", "Shanghai", "M-001", NORMAL_TIME),
                snapshot(2, "200.00", "Beijing", NORMAL_TIME - 2 * 60 * 60 * 1000));

        assertTrue(!result.getReasons().contains("CITY_HOPPING"));
    }

    @Test
    void shouldRejectInvertedThresholds() {
        assertThrows(IllegalArgumentException.class, () -> new RiskRules(20, 30));
    }

    private static RiskWindowSnapshot snapshot(int count, String total,
                                               String previousCity, long previousEventTime) {
        return new RiskWindowSnapshot(count, new BigDecimal(total),
                previousCity, previousEventTime, 600000L);
    }

    private static TransactionEvent event(String amount, String city, String merchant, long eventTime) {
        TransactionEvent event = new TransactionEvent();
        event.setOp("c");
        event.setTransactionId("TX001");
        event.setUserId("U1001");
        event.setAmount(new BigDecimal(amount));
        event.setCity(city);
        event.setMerchantId(merchant);
        event.setEventTime(eventTime);
        return event;
    }
}
