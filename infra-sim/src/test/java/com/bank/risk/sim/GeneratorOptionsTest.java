package com.bank.risk.sim;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class GeneratorOptionsTest {

    @Test
    void shouldUseDefaultsAndPassValidation() {
        GeneratorOptions options = GeneratorOptions.from(new String[0]);

        assertEquals(20, options.getInt("tps"));
        assertEquals(0L, options.getLong("duration-seconds"));
        assertTrue(options.getBoolean("init-schema"));
        options.validate();
    }

    @Test
    void shouldOverrideWithArguments() {
        GeneratorOptions options = GeneratorOptions.from(new String[]{
                "--tps", "50",
                "--duration-seconds=120",
                "--seed", "1"
        });

        assertEquals(50, options.getInt("tps"));
        assertEquals(120L, options.getLong("duration-seconds"));
        assertEquals(1L, options.getLong("seed"));
        options.validate();
    }

    @Test
    void shouldRejectInvalidRatios() {
        GeneratorOptions tooLarge = GeneratorOptions.from(new String[]{"--update-ratio", "1.5"});
        assertThrows(IllegalArgumentException.class, tooLarge::validate);

        GeneratorOptions sumTooLarge = GeneratorOptions.from(new String[]{
                "--update-ratio", "0.8", "--delete-ratio", "0.5"
        });
        assertThrows(IllegalArgumentException.class, sumTooLarge::validate);
    }

    @Test
    void shouldRejectInvalidTps() {
        GeneratorOptions options = GeneratorOptions.from(new String[]{"--tps", "0"});
        assertThrows(IllegalArgumentException.class, options::validate);
    }

    @Test
    void shouldToggleBooleanFlag() {
        GeneratorOptions options = GeneratorOptions.from(new String[]{"--init-schema", "false"});
        assertFalse(options.getBoolean("init-schema"));
    }

    @Test
    void shouldStripDatabaseFromJdbcUrl() {
        assertEquals("jdbc:mysql://localhost:3306/?useSSL=false&serverTimezone=Asia/Shanghai",
                TransactionDataGenerator.stripDatabase(
                        "jdbc:mysql://localhost:3306/bank?useSSL=false&serverTimezone=Asia/Shanghai"));
        assertEquals("jdbc:mysql://localhost:3306/",
                TransactionDataGenerator.stripDatabase("jdbc:mysql://localhost:3306/bank"));
    }
}
