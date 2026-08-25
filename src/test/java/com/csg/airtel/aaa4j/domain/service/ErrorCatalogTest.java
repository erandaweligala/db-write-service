package com.csg.airtel.aaa4j.domain.service;

import com.csg.airtel.aaa4j.application.config.ConnectivityMonitoringConfig;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class ErrorCatalogTest {

    private static final String SERVICE = "db-write-service";

    private MeterRegistry registry;
    private ErrorCatalog catalog;

    @BeforeEach
    void setUp() {
        registry = new SimpleMeterRegistry();
        ConnectivityMonitoringConfig config = mock(ConnectivityMonitoringConfig.class);
        when(config.serviceName()).thenReturn(SERVICE);
        catalog = new ErrorCatalog(registry, config);
    }

    @Test
    void oneFaultSeenManyTimesIsOneRowWithAnOccurrenceCount() {
        // Same duplicate-key rejection three times, each carrying a different row id.
        for (int i = 0; i < 3; i++) {
            catalog.record(new SQLException("ORA-00001: unique constraint (AAA.PK_SESSION) violated on id " + i),
                    "SQLException", "repository", "oracle");
        }

        List<ErrorCatalog.ErrorSummary> rows = catalog.snapshot();
        assertEquals(1, rows.size(), "the same fault must not fan out into several rows");

        ErrorCatalog.ErrorSummary row = rows.get(0);
        assertEquals("SQLException", row.error());
        assertEquals("ORA-00001", row.code());
        assertEquals("unique constraint (AAA.PK_SESSION) violated on id #", row.reason());
        assertEquals(3L, row.occurrences());
        assertEquals(3L, catalog.totalOccurrences());
    }

    @Test
    void theUnredactedMessageAndThrowingFrameAreKept() {
        catalog.record(new SQLException("ORA-00001: unique constraint violated on id 88213"),
                "SQLException", "repository", "oracle");

        ErrorCatalog.ErrorSummary row = catalog.snapshot().get(0);
        // The reason is normalised, but one real example is always retrievable.
        assertTrue(row.sampleMessage().contains("88213"), "sample must keep the real text");
        assertTrue(row.origin().startsWith("ErrorCatalogTest."), "origin must name the throwing frame");
        assertTrue(row.firstSeen() > 0 && row.lastSeen() >= row.firstSeen());
    }

    @Test
    void occurrencesAreExportedToPrometheus() {
        catalog.record(new SQLException("ORA-12541: TNS:no listener"), "SQLException", "repository", "oracle");
        catalog.record(new SQLException("ORA-12541: TNS:no listener"), "SQLException", "repository", "oracle");

        Counter counter = registry.find("application_error_occurrences")
                .tags(Tags.of(
                        "service", SERVICE,
                        "error", "SQLException",
                        "code", "ORA-12541",
                        "reason", "TNS:no listener",
                        "layer", "repository",
                        "source", "oracle"))
                .counter();
        assertNotNull(counter, "every catalog row must have a matching Prometheus series");
        assertEquals(2.0, counter.count());
    }

    @Test
    void theLoudestErrorIsAlwaysTheFirstRow() {
        for (int i = 0; i < 2; i++) {
            catalog.record(new RuntimeException("Connection reset by peer"), "RuntimeException", "repository", "oracle");
        }
        for (int i = 0; i < 7; i++) {
            catalog.record(new SQLException("ORA-12541: TNS:no listener"), "SQLException", "repository", "oracle");
        }

        List<ErrorCatalog.ErrorSummary> rows = catalog.snapshot();
        assertEquals(2, rows.size());
        assertEquals(7L, rows.get(0).occurrences());
        assertEquals("ORA-12541", rows.get(0).code());
        assertEquals(2L, rows.get(1).occurrences());
        assertEquals(ErrorSignatures.NO_CODE, rows.get(1).code(), "an absent code must say so, not be blank");
    }

    @Test
    void distinctFaultsFromOneExceptionClassAreNotConflated() {
        // The whole point: SQLException alone is not an identity.
        catalog.record(new SQLException("ORA-00001: unique constraint violated"), "SQLException", "repository", "oracle");
        catalog.record(new SQLException("ORA-12541: TNS:no listener"), "SQLException", "repository", "oracle");

        List<ErrorCatalog.ErrorSummary> rows = catalog.snapshot();
        assertEquals(2, rows.size());
        assertNotEquals(rows.get(0).code(), rows.get(1).code());
    }

    @Test
    void attributionToLayerAndSourceIsPreserved() {
        catalog.record(new RuntimeException("boom"), "RuntimeException", "repository", "oracle");
        catalog.record(new RuntimeException("boom"), "RuntimeException", "consumer", "kafka");
        assertEquals(2, catalog.snapshot().size());
    }

    @Test
    void cardinalityIsCappedAndTheTotalStaysExact() {
        int over = ErrorCatalog.MAX_SIGNATURES + 250;
        for (int i = 0; i < over; i++) {
            catalog.record(new RuntimeException("failure kind zz" + i + " qq" + i),
                    "RuntimeException" + i, "service", "internal");
        }

        assertTrue(catalog.atCapacity());
        assertTrue(catalog.distinctSignatures() <= ErrorCatalog.MAX_SIGNATURES + 1,
                "distinct signatures must never exceed the ceiling plus the overflow row");
        assertEquals((long) over, catalog.totalOccurrences(), "overflow must lose detail, never counts");

        long summed = catalog.snapshot().stream().mapToLong(ErrorCatalog.ErrorSummary::occurrences).sum();
        assertEquals(catalog.totalOccurrences(), summed, "rows must always add up to the total");

        long overflowRow = catalog.snapshot().stream()
                .filter(r -> ErrorCatalog.OVERFLOW.equals(r.error()))
                .mapToLong(ErrorCatalog.ErrorSummary::occurrences)
                .sum();
        assertTrue(overflowRow > 0, "the excess must be visible in the overflow row");
    }

    @Test
    void recordingIsNullSafe() {
        catalog.record(null, "RuntimeException", "service", "internal");
        assertEquals(0L, catalog.totalOccurrences());

        catalog.record(new RuntimeException((String) null), "RuntimeException", null, null);
        ErrorCatalog.ErrorSummary row = catalog.snapshot().get(0);
        assertEquals(1L, catalog.totalOccurrences());
        assertEquals(ErrorSignatures.NO_REASON, row.reason());
        assertEquals(ErrorSignatures.NO_CODE, row.code());
        assertEquals("unknown", row.layer());
        assertEquals("unknown", row.source());
    }

    @Test
    void freshCatalogIsEmptyRatherThanUndefined() {
        assertEquals(0L, catalog.totalOccurrences());
        assertEquals(0, catalog.distinctSignatures());
        assertFalse(catalog.atCapacity());
        assertTrue(catalog.snapshot().isEmpty());
    }
}
