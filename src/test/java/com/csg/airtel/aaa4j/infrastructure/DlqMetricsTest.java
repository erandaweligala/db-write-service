package com.csg.airtel.aaa4j.infrastructure;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.net.ConnectException;
import java.sql.SQLNonTransientException;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class DlqMetricsTest {

    private SimpleMeterRegistry registry;
    private DlqMetrics dlqMetrics;

    @BeforeEach
    void setUp() {
        registry = new SimpleMeterRegistry();
        dlqMetrics = new DlqMetrics(registry);
    }

    private double dlqCount(String channel, String reason) {
        Counter c = registry.find(DlqMetrics.METRIC_DLQ)
                .tag("channel", channel)
                .tag("reason", reason)
                .counter();
        return c == null ? -1.0 : c.count();
    }

    private double droppedCount(String channel, String reason) {
        Counter c = registry.find(DlqMetrics.METRIC_DROPPED)
                .tag("channel", channel)
                .tag("reason", reason)
                .counter();
        return c == null ? -1.0 : c.count();
    }

    // -----------------------------------------------------------------------
    // DLQ disposition
    // -----------------------------------------------------------------------

    @Test
    @DisplayName("recordDlqEvent increments a channel+reason tagged counter")
    void recordDlqEvent_incrementsTaggedCounter() {
        dlqMetrics.recordDlqEvent("DC-DR", DlqMetrics.Reason.DESERIALIZATION);
        dlqMetrics.recordDlqEvent("DC-DR", DlqMetrics.Reason.DESERIALIZATION);

        assertEquals(2.0, dlqCount("DC-DR", "deserialization"));
        assertEquals(2L, dlqMetrics.getDlqTotal());
        assertEquals(0L, dlqMetrics.getDroppedTotal());
    }

    @Test
    @DisplayName("distinct channel/reason pairs get distinct counters")
    void recordDlqEvent_distinctPairs() {
        dlqMetrics.recordDlqEvent("DC-DR", DlqMetrics.Reason.DB_PERMANENT);
        dlqMetrics.recordDlqEvent("DR-DC", DlqMetrics.Reason.DB_PERMANENT);
        dlqMetrics.recordDlqEvent("DC-DR", DlqMetrics.Reason.DESERIALIZATION);

        assertEquals(1.0, dlqCount("DC-DR", "db_permanent"));
        assertEquals(1.0, dlqCount("DR-DC", "db_permanent"));
        assertEquals(1.0, dlqCount("DC-DR", "deserialization"));
        assertEquals(3L, dlqMetrics.getDlqTotal());
    }

    // -----------------------------------------------------------------------
    // Throwable classification
    // -----------------------------------------------------------------------

    @Test
    @DisplayName("transient throwable classifies as db_transient_exhausted")
    void recordDlqEvent_transientThrowable() {
        dlqMetrics.recordDlqEvent("scheduler", new ConnectException("connection refused"));
        assertEquals(1.0, dlqCount("scheduler", "db_transient_exhausted"));
    }

    @Test
    @DisplayName("permanent throwable classifies as db_permanent")
    void recordDlqEvent_permanentThrowable() {
        dlqMetrics.recordDlqEvent("scheduler",
                new SQLNonTransientException("ORA-00001: unique constraint violated"));
        assertEquals(1.0, dlqCount("scheduler", "db_permanent"));
    }

    @Test
    @DisplayName("null throwable classifies as unknown")
    void classify_nullThrowable() {
        assertEquals(DlqMetrics.Reason.UNKNOWN, DlqMetrics.classify(null));
    }

    // -----------------------------------------------------------------------
    // Dropped disposition
    // -----------------------------------------------------------------------

    @Test
    @DisplayName("recordDroppedEvent uses the dropped metric, not the DLQ metric")
    void recordDroppedEvent_usesDroppedMetric() {
        dlqMetrics.recordDroppedEvent("ums-mysql-dc", DlqMetrics.Reason.DB_PERMANENT);

        assertEquals(1.0, droppedCount("ums-mysql-dc", "db_permanent"));
        assertEquals(1L, dlqMetrics.getDroppedTotal());
        assertEquals(0L, dlqMetrics.getDlqTotal());
    }

    // -----------------------------------------------------------------------
    // Null / blank guards
    // -----------------------------------------------------------------------

    @Test
    @DisplayName("null channel and null reason fall back to 'unknown'")
    void nullInputs_fallBackToUnknown() {
        dlqMetrics.recordDlqEvent(null, (DlqMetrics.Reason) null);
        assertEquals(1.0, dlqCount("unknown", "unknown"));
    }

    @Test
    @DisplayName("blank channel falls back to 'unknown'")
    void blankChannel_fallsBackToUnknown() {
        dlqMetrics.recordDlqEvent("   ", DlqMetrics.Reason.MISSING_HEADERS);
        assertEquals(1.0, dlqCount("unknown", "missing_headers"));
    }

    // -----------------------------------------------------------------------
    // Snapshot shape
    // -----------------------------------------------------------------------

    @Test
    @DisplayName("snapshot reports totals and per-channel/reason breakdown")
    @SuppressWarnings("unchecked")
    void snapshot_reportsBreakdown() {
        dlqMetrics.recordDlqEvent("DC-DR", DlqMetrics.Reason.DB_PERMANENT);
        dlqMetrics.recordDlqEvent("DC-DR", DlqMetrics.Reason.DB_PERMANENT);
        dlqMetrics.recordDlqEvent("DC-DR", DlqMetrics.Reason.DESERIALIZATION);
        dlqMetrics.recordDroppedEvent("ums-mysql-dr", DlqMetrics.Reason.DB_TRANSIENT_EXHAUSTED);

        Map<String, Object> snap = dlqMetrics.snapshot();

        assertEquals(3L, snap.get("dlqTotal"));
        assertEquals(1L, snap.get("droppedTotal"));

        Map<String, Object> dlq = (Map<String, Object>) snap.get("dlq");
        Map<String, Long> dcDr = (Map<String, Long>) dlq.get("DC-DR");
        assertEquals(2L, dcDr.get("db_permanent"));
        assertEquals(1L, dcDr.get("deserialization"));

        // Highest count is listed first within a channel.
        assertEquals("db_permanent", dcDr.keySet().iterator().next());

        Map<String, Object> dropped = (Map<String, Object>) snap.get("dropped");
        Map<String, Long> ums = (Map<String, Long>) dropped.get("ums-mysql-dr");
        assertEquals(1L, ums.get("db_transient_exhausted"));
    }
}
