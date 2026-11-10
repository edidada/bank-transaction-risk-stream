package com.bank.risk.cdc;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class JobOptionsTest {

    @Test
    void shouldUseDefaultsAndPassValidation() {
        JobOptions options = JobOptions.from(new String[0]);

        assertEquals("mysql", options.get("mysql-host"));
        assertEquals(3306, options.getInt("mysql-port"));
        assertEquals("bank.transaction", options.get("mysql-table"));
        assertEquals("bank-cdc-events", options.get("kafka-topic"));
        assertEquals("initial", options.get("cdc-startup-mode"));
        options.validate();
    }

    @Test
    void shouldLetArgumentsOverrideDefaults() {
        JobOptions options = JobOptions.from(new String[]{
                "--mysql-host", "10.0.0.9",
                "--mysql-port", "3307",
                "--kafka-topic", "risk-cdc",
                "--parallelism", "2"
        });

        assertEquals("10.0.0.9", options.get("mysql-host"));
        assertEquals(3307, options.getInt("mysql-port"));
        assertEquals("risk-cdc", options.get("kafka-topic"));
        assertEquals(2, options.getInt("parallelism"));
        // 未显式覆盖的项仍取默认值
        assertEquals("cdc-ingestion", options.get("job-name"));
        options.validate();
    }

    @Test
    void shouldSupportEqualsSyntax() {
        JobOptions options = JobOptions.from(new String[]{
                "--mysql-host=db-primary",
                "--cdc-startup-mode=latest"
        });

        assertEquals("db-primary", options.get("mysql-host"));
        assertEquals("latest", options.get("cdc-startup-mode"));
        options.validate();
    }

    @Test
    void shouldRejectUnknownStartupMode() {
        JobOptions options = JobOptions.from(new String[]{"--cdc-startup-mode", "whatever"});

        IllegalArgumentException error = assertThrows(IllegalArgumentException.class, options::validate);
        assertTrue(error.getMessage().contains("cdc-startup-mode"));
    }

    @Test
    void shouldRejectInvalidPort() {
        JobOptions options = JobOptions.from(new String[]{"--mysql-port", "not-a-number"});

        assertThrows(NumberFormatException.class, options::validate);
    }

    @Test
    void shouldRejectBlankRequiredValue() {
        JobOptions options = JobOptions.from(new String[]{"--kafka-bootstrap", "   "});

        IllegalArgumentException error = assertThrows(IllegalArgumentException.class, options::validate);
        assertTrue(error.getMessage().contains("kafka-bootstrap"));
    }
}
