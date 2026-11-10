package com.bank.risk.etl;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class EtlOptionsTest {

    @Test
    void shouldUseDefaultsAndPassValidation() {
        EtlOptions options = EtlOptions.from(new String[0]);

        assertEquals("bank-cdc-events", options.get("kafka-topic"));
        assertEquals("risk.risk_event_detail", options.get("doris-database") + "." + options.get("doris-table"));
        assertEquals(600000L, options.getLong("risk-window-ms"));
        assertEquals(2, options.getInt("parallelism"));
        options.validate();
    }

    @Test
    void shouldOverrideWithArguments() {
        EtlOptions options = EtlOptions.from(new String[]{
                "--kafka-bootstrap", "kafka-1:9092,kafka-2:9092",
                "--doris-fe-host=doris-fe-0",
                "--risk-high-score", "80"
        });

        assertEquals("kafka-1:9092,kafka-2:9092", options.get("kafka-bootstrap"));
        assertEquals("doris-fe-0", options.get("doris-fe-host"));
        assertEquals(80, options.getInt("risk-high-score"));
        options.validate();
    }

    @Test
    void shouldRejectInvertedScoreThresholds() {
        EtlOptions options = EtlOptions.from(new String[]{
                "--risk-high-score", "20", "--risk-medium-score", "30"
        });

        IllegalArgumentException error = assertThrows(IllegalArgumentException.class, options::validate);
        assertTrue(error.getMessage().contains("risk-high-score"));
    }

    @Test
    void shouldRejectUnsupportedKafkaStartupMode() {
        EtlOptions options = EtlOptions.from(new String[]{"--kafka-startup-mode", "middle"});

        assertThrows(IllegalArgumentException.class, options::validate);
    }
}
