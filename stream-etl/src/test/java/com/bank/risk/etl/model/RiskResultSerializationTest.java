package com.bank.risk.etl.model;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RiskResultSerializationTest {

    @Test
    void shouldSerializeWithSnakeCaseColumnsForDorisStreamLoad() throws Exception {
        RiskResult result = new RiskResult();
        result.setTransactionId("TX001");
        result.setUserId("U1001");
        result.setEventTime(1730000000000L);
        result.setProcessedAt(1730000000500L);
        result.setOp("c");
        result.setAmount(new BigDecimal("12800.00"));
        result.setCity("Beijing");
        result.setMerchantId("M-001");
        result.setScore(65);
        result.setLevel(RiskResult.LEVEL_HIGH);
        result.setWindowCount(5);
        result.setWindowAmount(new BigDecimal("128000.00"));
        result.setReasons("LARGE_AMOUNT|HIGH_FREQUENCY");

        JsonNode json = new ObjectMapper().readTree(new ObjectMapper().writeValueAsString(result));

        assertEquals("TX001", json.get("transaction_id").asText());
        assertEquals("U1001", json.get("user_id").asText());
        assertEquals(65, json.get("score").asInt());
        assertEquals("HIGH", json.get("level").asText());
        assertEquals(5, json.get("window_count").asInt());
        // JSON 中 DECIMAL 以数值写出，scale 可能被归一化，用数值比较而非字符串
        assertEquals(0, new BigDecimal(json.get("window_amount").asText())
                .compareTo(new BigDecimal("128000.00")));
        assertEquals("LARGE_AMOUNT|HIGH_FREQUENCY", json.get("reasons").asText());
        // 不能出现 camelCase 字段，否则 Doris 无法按列名匹配
        assertFalse(json.has("transactionId"));
        assertFalse(json.has("windowCount"));
        assertTrue(json.has("processed_at"));
    }
}
