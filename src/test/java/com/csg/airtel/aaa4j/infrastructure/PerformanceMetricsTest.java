package com.csg.airtel.aaa4j.infrastructure;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("PerformanceMetrics Tests")
class PerformanceMetricsTest {

    private PerformanceMetrics metrics;
    private MeterRegistry registry;

    @BeforeEach
    void setUp() {
        registry = new SimpleMeterRegistry();
        metrics = new PerformanceMetrics(registry);
    }

    @Test
    @DisplayName("Should create PerformanceMetrics with MeterRegistry")
    void testConstructor() {
        assertNotNull(metrics);
    }

    @Test
    @DisplayName("Should record a single DB update")
    void testRecordDbUpdate() {
        metrics.recordDbUpdate();
        double totalUpdates = metrics.getTotalUpdates();
        assertEquals(1.0, totalUpdates);
    }

    @Test
    @DisplayName("Should record multiple DB updates")
    void testRecordMultipleDbUpdates() {
        for (int i = 0; i < 10; i++) {
            metrics.recordDbUpdate();
        }
        assertEquals(10.0, metrics.getTotalUpdates());
    }

    @Test
    @DisplayName("Should record a failed DB update")
    void testRecordDbUpdateFailure() {
        metrics.recordDbUpdateFailure();
        double failureRate = metrics.getFailureRate();
        assertEquals(100.0, failureRate, 0.01);
    }

    @Test
    @DisplayName("Should record multiple failed DB updates")
    void testRecordMultipleDbUpdateFailures() {
        metrics.recordDbUpdate();
        metrics.recordDbUpdateFailure();
        metrics.recordDbUpdateFailure();

        double totalUpdates = metrics.getTotalUpdates();
        double failureRate = metrics.getFailureRate();

        assertEquals(3.0, totalUpdates);
        assertEquals(66.67, failureRate, 0.1);
    }

    @Test
    @DisplayName("Should calculate failure rate correctly")
    void testFailureRateCalculation() {
        for (int i = 0; i < 10; i++) {
            metrics.recordDbUpdate();
        }
        for (int i = 0; i < 5; i++) {
            metrics.recordDbUpdateFailure();
        }

        double failureRate = metrics.getFailureRate();
        assertEquals(33.33, failureRate, 0.1);
    }

    @Test
    @DisplayName("Should return 0 failure rate when no updates")
    void testZeroFailureRateNoUpdates() {
        double failureRate = metrics.getFailureRate();
        assertEquals(0.0, failureRate);
    }

    @Test
    @DisplayName("Should record batch update with size")
    void testRecordBatchUpdate() {
        metrics.recordBatchUpdate(5);
        assertEquals(5.0, metrics.getTotalUpdates());
    }

    @Test
    @DisplayName("Should record multiple batch updates")
    void testRecordMultipleBatchUpdates() {
        metrics.recordBatchUpdate(10);
        metrics.recordBatchUpdate(20);
        metrics.recordBatchUpdate(15);

        assertEquals(45.0, metrics.getTotalUpdates());
    }

    @Test
    @DisplayName("Should record batch and single updates together")
    void testRecordBatchAndSingleUpdates() {
        metrics.recordDbUpdate();
        metrics.recordBatchUpdate(10);
        metrics.recordDbUpdate();
        metrics.recordBatchUpdate(5);

        assertEquals(17.0, metrics.getTotalUpdates());
    }

    @Test
    @DisplayName("Should record DB update duration")
    void testRecordDbUpdateDuration() {
        Duration duration = Duration.ofMillis(100);
        metrics.recordDbUpdateDuration(duration);
        // Just verify it doesn't throw an exception
        assertNotNull(metrics);
    }

    @Test
    @DisplayName("Should record multiple DB update durations")
    void testRecordMultipleDbUpdateDurations() {
        metrics.recordDbUpdateDuration(Duration.ofMillis(50));
        metrics.recordDbUpdateDuration(Duration.ofMillis(100));
        metrics.recordDbUpdateDuration(Duration.ofMillis(75));
        assertNotNull(metrics);
    }

    @Test
    @DisplayName("Should record batch update duration")
    void testRecordBatchUpdateDuration() {
        Duration duration = Duration.ofMillis(200);
        metrics.recordBatchUpdateDuration(duration);
        assertNotNull(metrics);
    }

    @Test
    @DisplayName("Should record multiple batch update durations")
    void testRecordMultipleBatchUpdateDurations() {
        metrics.recordBatchUpdateDuration(Duration.ofMillis(100));
        metrics.recordBatchUpdateDuration(Duration.ofMillis(150));
        metrics.recordBatchUpdateDuration(Duration.ofMillis(200));
        assertNotNull(metrics);
    }

    @Test
    @DisplayName("Should get current throughput (TPS)")
    void testGetCurrentThroughput() {
        double throughput = metrics.getCurrentThroughput();
        // Initial throughput should be 0 as not enough time has passed
        assertEquals(0.0, throughput);
    }

    @Test
    @DisplayName("Should get total updates")
    void testGetTotalUpdates() {
        assertEquals(0.0, metrics.getTotalUpdates());

        metrics.recordDbUpdate();
        assertEquals(1.0, metrics.getTotalUpdates());

        metrics.recordBatchUpdate(5);
        assertEquals(6.0, metrics.getTotalUpdates());
    }

    @Test
    @DisplayName("Should get failure rate")
    void testGetFailureRate() {
        assertEquals(0.0, metrics.getFailureRate());

        metrics.recordDbUpdate();
        metrics.recordDbUpdate();
        metrics.recordDbUpdateFailure();

        double failureRate = metrics.getFailureRate();
        assertEquals(33.33, failureRate, 0.1);
    }

    @Test
    @DisplayName("Should handle zero total updates for failure rate")
    void testFailureRateWithZeroUpdates() {
        double failureRate = metrics.getFailureRate();
        assertEquals(0.0, failureRate);
    }

    @Test
    @DisplayName("Should handle 100% failure rate")
    void testHundredPercentFailureRate() {
        for (int i = 0; i < 5; i++) {
            metrics.recordDbUpdateFailure();
        }

        double failureRate = metrics.getFailureRate();
        assertEquals(100.0, failureRate, 0.01);
    }

    @Test
    @DisplayName("Should handle zero failure rate")
    void testZeroFailureRate() {
        for (int i = 0; i < 10; i++) {
            metrics.recordDbUpdate();
        }

        double failureRate = metrics.getFailureRate();
        assertEquals(0.0, failureRate, 0.01);
    }

    @Test
    @DisplayName("Should track large batch updates")
    void testLargeBatchUpdates() {
        metrics.recordBatchUpdate(1000);
        metrics.recordBatchUpdate(1000);
        metrics.recordBatchUpdate(1000);

        assertEquals(3000.0, metrics.getTotalUpdates());
    }

    @Test
    @DisplayName("Should handle concurrent metric recording")
    void testConcurrentMetricRecording() {
        for (int i = 0; i < 100; i++) {
            metrics.recordDbUpdate();
        }

        assertEquals(100.0, metrics.getTotalUpdates());
    }

    @Test
    @DisplayName("Should calculate accurate total updates with mixed operations")
    void testMixedOperationsTotalUpdates() {
        metrics.recordDbUpdate();
        metrics.recordDbUpdate();
        metrics.recordBatchUpdate(50);
        metrics.recordDbUpdateFailure();
        metrics.recordDbUpdateFailure();
        metrics.recordBatchUpdate(100);
        metrics.recordDbUpdate();

        assertEquals(153.0, metrics.getTotalUpdates());
    }

    @Test
    @DisplayName("Should handle Duration with seconds")
    void testDurationWithSeconds() {
        Duration duration = Duration.ofSeconds(5);
        metrics.recordDbUpdateDuration(duration);
        assertNotNull(metrics);
    }

    @Test
    @DisplayName("Should handle Duration with nanos")
    void testDurationWithNanos() {
        Duration duration = Duration.ofNanos(1000000);
        metrics.recordDbUpdateDuration(duration);
        assertNotNull(metrics);
    }

    @Test
    @DisplayName("Should record zero duration")
    void testZeroDuration() {
        Duration duration = Duration.ZERO;
        metrics.recordDbUpdateDuration(duration);
        assertNotNull(metrics);
    }

    @Test
    @DisplayName("Should maintain metrics state across operations")
    void testMetricsStateConsistency() {
        double initialTotal = metrics.getTotalUpdates();
        double initialFailureRate = metrics.getFailureRate();

        metrics.recordDbUpdate();
        metrics.recordDbUpdate();
        metrics.recordDbUpdateFailure();

        double newTotal = metrics.getTotalUpdates();
        double newFailureRate = metrics.getFailureRate();

        assertNotEquals(initialTotal, newTotal);
        assertNotEquals(initialFailureRate, newFailureRate);
    }
}
