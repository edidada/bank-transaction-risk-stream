package com.bank.risk.etl.parser;

import com.bank.risk.etl.model.TransactionEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TransactionEventParserTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Test
    void shouldParseInsertEvent() throws Exception {
        String raw = cdc("c", row("TX001", "U1001", "12800.00", "SUCCESS", "Beijing", "M-001"), null);

        TransactionEvent event = TransactionEventParser.parseOrThrow(raw);

        assertEquals("c", event.getOp());
        assertEquals("bank", event.getDb());
        assertEquals("transaction", event.getTable());
        assertEquals("TX001", event.getTransactionId());
        assertEquals("U1001", event.getUserId());
        assertEquals(new BigDecimal("12800.00"), event.getAmount());
        assertEquals("Beijing", event.getCity());
        assertEquals(1730000001000L, event.getEventTime());
        assertEquals("622202******1234", event.getCardNo());
        assertEquals(raw, event.getRaw());
    }

    @Test
    void shouldUseBeforeImageForDeleteEvent() throws Exception {
        String raw = cdc("d", null, row("TX001", "U1001", "12800.00", "SUCCESS", "Beijing", "M-001"));

        TransactionEvent event = TransactionEventParser.parseOrThrow(raw);

        assertEquals("d", event.getOp());
        assertEquals("TX001", event.getTransactionId());
        assertNotNull(event.getAmount());
    }

    @Test
    void shouldUseTsMsWhenEventTimeMissing() throws Exception {
        ObjectNode after = row("TX001", "U1001", "10.00", "SUCCESS", "Shanghai", "M-001");
        after.remove("event_time");
        String raw = cdc("c", after, null);

        assertEquals(1730000000000L, TransactionEventParser.parseOrThrow(raw).getEventTime());
    }

    @Test
    void shouldReturnNullWhenNoRowImage() throws Exception {
        String raw = cdc("c", null, null);

        assertNull(TransactionEventParser.parseOrThrow(raw));
    }

    @Test
    void shouldRejectMissingUserId() {
        String raw = cdc("c", row("TX001", "", "10.00", "SUCCESS", "Beijing", "M-001"), null);

        IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
                () -> TransactionEventParser.parseOrThrow(raw));
        assertTrue(error.getMessage().contains("user_id"));
    }

    @Test
    void shouldRejectNegativeAmount() {
        String raw = cdc("c", row("TX001", "U1001", "-1", "SUCCESS", "Beijing", "M-001"), null);

        IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
                () -> TransactionEventParser.parseOrThrow(raw));
        assertTrue(error.getMessage().contains("负数"));
    }

    @Test
    void shouldRejectNonNumericAmount() {
        String raw = cdc("c", row("TX001", "U1001", "abc", "SUCCESS", "Beijing", "M-001"), null);

        assertThrows(IllegalArgumentException.class, () -> TransactionEventParser.parseOrThrow(raw));
    }

    @Test
    void shouldRejectMalformedJson() {
        assertThrows(Exception.class, () -> TransactionEventParser.parseOrThrow("not-a-json"));
        assertThrows(IllegalArgumentException.class, () -> TransactionEventParser.parseOrThrow("  "));
    }

    @Test
    void shouldMaskCardNumber() {
        assertEquals("622202******1234", TransactionEventParser.maskCardNo("6222020000001234"));
        assertEquals("6222", TransactionEventParser.maskCardNo("6222"));
        assertEquals("", TransactionEventParser.maskCardNo(null));
    }

    private static ObjectNode row(String txId, String userId, String amount,
                                  String status, String city, String merchantId) {
        ObjectNode row = MAPPER.createObjectNode();
        row.put("transaction_id", txId);
        row.put("user_id", userId);
        row.put("amount", amount);
        row.put("currency", "CNY");
        row.put("status", status);
        row.put("channel", "MOBILE");
        row.put("merchant_id", merchantId);
        row.put("city", city);
        row.put("card_no", "6222020000001234");
        row.put("event_time", 1730000001000L);
        return row;
    }

    private static String cdc(String op, ObjectNode after, ObjectNode before) {
        ObjectNode root = MAPPER.createObjectNode();
        root.put("op", op);
        root.put("db", "bank");
        root.put("table", "transaction");
        root.put("ts_ms", 1730000000000L);
        if (before == null) {
            root.putNull("before");
        } else {
            root.set("before", before);
        }
        if (after == null) {
            root.putNull("after");
        } else {
            root.set("after", after);
        }
        return root.toString();
    }
}
