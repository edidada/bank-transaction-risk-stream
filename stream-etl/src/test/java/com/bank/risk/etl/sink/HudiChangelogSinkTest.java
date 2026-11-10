package com.bank.risk.etl.sink;

import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.time.Instant;
import java.time.ZoneId;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class HudiChangelogSinkTest {

    @Test
    void shouldBuildDailyPartitionPath() {
        long eventTime = 1730000000000L;
        String expectedDay = Instant.ofEpochMilli(eventTime)
                .atZone(ZoneId.of("Asia/Shanghai")).toLocalDate().toString();

        Path path = HudiChangelogSink.resolvePath("file:///data/hudi/changelog", eventTime, 3, "abc12345");
        String normalized = path.toString().replace('\\', '/');

        assertTrue(normalized.endsWith("dt=" + expectedDay + "/part-3-abc12345.ndjson"), normalized);
        assertEquals(expectedDay, HudiChangelogSink.partitionOf(eventTime));
    }

    @Test
    void shouldSupportFileSchemeAndPlainPath() {
        assertTrue(HudiChangelogSink.toPath("file:///data/hudi")
                .toString().replace('\\', '/').endsWith("data/hudi"));
        assertTrue(HudiChangelogSink.toPath("/data/hudi")
                .toString().replace('\\', '/').endsWith("data/hudi"));
    }

    @Test
    void shouldSupportWindowsDriveLetterUnderFileScheme() {
        Path path = HudiChangelogSink.toPath("file:///D:/data/hudi");
        assertTrue(path.toString().contains("data"));
    }
}
