package com.bank.risk.cdc;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CdcEventDeserializerTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static final Schema ROW_SCHEMA = SchemaBuilder.struct()
            .name("bank.transaction.Value")
            .optional()
            .field("transaction_id", Schema.STRING_SCHEMA)
            .field("user_id", Schema.STRING_SCHEMA)
            .field("amount", Schema.OPTIONAL_STRING_SCHEMA)
            .field("status", Schema.STRING_SCHEMA)
            .build();

    private static final Schema SOURCE_SCHEMA = SchemaBuilder.struct()
            .name("io.debezium.connector.mysql.Source")
            .field("db", Schema.STRING_SCHEMA)
            .field("table", Schema.STRING_SCHEMA)
            .field("ts_ms", Schema.OPTIONAL_INT64_SCHEMA)
            .build();

    private static final Schema ENVELOPE_SCHEMA = SchemaBuilder.struct()
            .name("bank.transaction.Envelope")
            .field("op", Schema.STRING_SCHEMA)
            .field("before", ROW_SCHEMA)
            .field("after", ROW_SCHEMA)
            .field("source", SOURCE_SCHEMA)
            .field("ts_ms", Schema.OPTIONAL_INT64_SCHEMA)
            .build();

    @Test
    void shouldSerializeInsertEventToUnifiedJson() throws Exception {
        Struct after = row("TX202611100001", "U1001", "12800.00", "SUCCESS");
        SourceRecord record = envelope("c", null, after);

        List<String> collected = collect(record);

        assertEquals(1, collected.size());
        JsonNode event = MAPPER.readTree(collected.get(0));
        assertEquals("c", event.get("op").asText());
        assertEquals("bank", event.get("db").asText());
        assertEquals("transaction", event.get("table").asText());
        assertEquals(1730000000000L, event.get("ts_ms").asLong());
        assertTrue(event.get("before").isNull());
        assertEquals("TX202611100001", event.get("after").get("transaction_id").asText());
        assertEquals("12800.00", event.get("after").get("amount").asText());
    }

    @Test
    void shouldKeepBeforeImageForDeleteEvent() throws Exception {
        Struct before = row("TX202611100001", "U1001", "12800.00", "SUCCESS");
        SourceRecord record = envelope("d", before, null);

        String json = collect(record).get(0);

        JsonNode event = MAPPER.readTree(json);
        assertEquals("d", event.get("op").asText());
        assertEquals("TX202611100001", event.get("before").get("transaction_id").asText());
        assertTrue(event.get("after").isNull());
        // 删除事件的 Kafka Key 必须取 before 镜像，保证与之前的变更同分区
        assertEquals("TX202611100001",
                new String(new CdcKeySerializationSchema().serialize(json), StandardCharsets.UTF_8));
    }

    @Test
    void shouldIgnoreTombstoneRecord() throws Exception {
        SourceRecord record = new SourceRecord(
                Collections.emptyMap(), Collections.emptyMap(), "bank.transaction", ENVELOPE_SCHEMA, null);

        assertTrue(collect(record).isEmpty());
    }

    @Test
    void shouldExposeStringProducedType() {
        assertEquals(Types.STRING, new CdcEventDeserializer().getProducedType());
    }

    @Test
    void shouldUseTransactionIdAsKafkaKey() throws Exception {
        SourceRecord record = envelope("u",
                row("TX202611100001", "U1001", "12800.00", "INIT"),
                row("TX202611100001", "U1001", "12800.00", "SUCCESS"));

        String json = collect(record).get(0);

        assertEquals("TX202611100001",
                new String(new CdcKeySerializationSchema().serialize(json), StandardCharsets.UTF_8));
    }

    private static Struct row(String txId, String userId, String amount, String status) {
        return new Struct(ROW_SCHEMA)
                .put("transaction_id", txId)
                .put("user_id", userId)
                .put("amount", amount)
                .put("status", status);
    }

    private static SourceRecord envelope(String op, Struct before, Struct after) {
        Struct source = new Struct(SOURCE_SCHEMA)
                .put("db", "bank")
                .put("table", "transaction")
                .put("ts_ms", 1730000000000L);
        Struct payload = new Struct(ENVELOPE_SCHEMA)
                .put("op", op)
                .put("before", before)
                .put("after", after)
                .put("source", source)
                .put("ts_ms", 1730000000001L);
        return new SourceRecord(
                Collections.emptyMap(), Collections.emptyMap(), "bank.transaction", ENVELOPE_SCHEMA, payload);
    }

    private static List<String> collect(SourceRecord record) throws Exception {
        List<String> out = new ArrayList<>();
        Collector<String> collector = new Collector<String>() {
            @Override
            public void collect(String value) {
                out.add(value);
            }

            @Override
            public void close() {
            }
        };
        new CdcEventDeserializer().deserialize(record, collector);
        return out;
    }
}
