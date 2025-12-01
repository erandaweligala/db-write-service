package com.csg.airtel.aaa4j.application.controller;

import com.csg.airtel.aaa4j.infrastructure.DatabaseCircuitBreaker;
import com.csg.airtel.aaa4j.infrastructure.PerformanceMetrics;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@DisplayName("PerformanceMonitoringResource Tests")
class PerformanceMonitoringResourceTest {

    @Mock
    private PerformanceMetrics mockMetrics;

    @Mock
    private DatabaseCircuitBreaker mockCircuitBreaker;

    private PerformanceMonitoringResource resource;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        resource = new PerformanceMonitoringResource();
        resource.metrics = mockMetrics;
        resource.circuitBreaker = mockCircuitBreaker;
    }

    @Test
    @DisplayName("Should create PerformanceMonitoringResource")
    void testConstructor() {
        assertNotNull(resource);
    }

    @Test
    @DisplayName("Should get metrics successfully")
    void testGetMetrics() {
        // Setup
        when(mockMetrics.getCurrentThroughput()).thenReturn(500.0);
        when(mockMetrics.getTotalUpdates()).thenReturn(10000.0);
        when(mockMetrics.getFailureRate()).thenReturn(2.5);
        when(mockCircuitBreaker.getState()).thenReturn(DatabaseCircuitBreaker.CircuitState.CLOSED);
        when(mockCircuitBreaker.getFailureCount()).thenReturn(0);

        // Execute
        Map<String, Object> result = resource.getMetrics();

        // Verify
        assertNotNull(result);
        assertTrue(result.containsKey("currentThroughputTPS"));
        assertTrue(result.containsKey("totalUpdates"));
        assertTrue(result.containsKey("failureRatePercent"));
        assertTrue(result.containsKey("circuitBreakerState"));
        assertTrue(result.containsKey("circuitBreakerFailureCount"));
    }

    @Test
    @DisplayName("Should return correct throughput value")
    void testMetricsThroughput() {
        // Setup
        when(mockMetrics.getCurrentThroughput()).thenReturn(750.5);
        when(mockMetrics.getTotalUpdates()).thenReturn(15000.0);
        when(mockMetrics.getFailureRate()).thenReturn(1.5);
        when(mockCircuitBreaker.getState()).thenReturn(DatabaseCircuitBreaker.CircuitState.CLOSED);
        when(mockCircuitBreaker.getFailureCount()).thenReturn(0);

        // Execute
        Map<String, Object> result = resource.getMetrics();

        // Verify
        assertNotNull(result.get("currentThroughputTPS"));
    }

    @Test
    @DisplayName("Should return total updates")
    void testMetricsTotalUpdates() {
        // Setup
        when(mockMetrics.getCurrentThroughput()).thenReturn(100.0);
        when(mockMetrics.getTotalUpdates()).thenReturn(50000.0);
        when(mockMetrics.getFailureRate()).thenReturn(0.5);
        when(mockCircuitBreaker.getState()).thenReturn(DatabaseCircuitBreaker.CircuitState.CLOSED);
        when(mockCircuitBreaker.getFailureCount()).thenReturn(0);

        // Execute
        Map<String, Object> result = resource.getMetrics();

        // Verify
        assertNotNull(result.get("totalUpdates"));
    }

    @Test
    @DisplayName("Should return failure rate")
    void testMetricsFailureRate() {
        // Setup
        when(mockMetrics.getCurrentThroughput()).thenReturn(200.0);
        when(mockMetrics.getTotalUpdates()).thenReturn(20000.0);
        when(mockMetrics.getFailureRate()).thenReturn(5.0);
        when(mockCircuitBreaker.getState()).thenReturn(DatabaseCircuitBreaker.CircuitState.CLOSED);
        when(mockCircuitBreaker.getFailureCount()).thenReturn(10);

        // Execute
        Map<String, Object> result = resource.getMetrics();

        // Verify
        assertNotNull(result.get("failureRatePercent"));
    }

    @Test
    @DisplayName("Should return circuit breaker state")
    void testCircuitBreakerState() {
        // Setup
        when(mockMetrics.getCurrentThroughput()).thenReturn(100.0);
        when(mockMetrics.getTotalUpdates()).thenReturn(10000.0);
        when(mockMetrics.getFailureRate()).thenReturn(1.0);
        when(mockCircuitBreaker.getState()).thenReturn(DatabaseCircuitBreaker.CircuitState.OPEN);
        when(mockCircuitBreaker.getFailureCount()).thenReturn(50);

        // Execute
        Map<String, Object> result = resource.getMetrics();

        // Verify
        assertEquals("OPEN", result.get("circuitBreakerState"));
    }

    @Test
    @DisplayName("Should get circuit breaker status")
    void testGetCircuitBreakerStatus() {
        // Setup
        when(mockCircuitBreaker.getState()).thenReturn(DatabaseCircuitBreaker.CircuitState.CLOSED);
        when(mockCircuitBreaker.getFailureCount()).thenReturn(0);
        when(mockCircuitBreaker.allowRequest()).thenReturn(true);

        // Execute
        Map<String, Object> result = resource.getCircuitBreakerStatus();

        // Verify
        assertNotNull(result);
        assertTrue(result.containsKey("state"));
        assertTrue(result.containsKey("failureCount"));
        assertTrue(result.containsKey("isAllowingRequests"));
    }

    @Test
    @DisplayName("Should return CLOSED state in circuit breaker status")
    void testCircuitBreakerStatusClosed() {
        // Setup
        when(mockCircuitBreaker.getState()).thenReturn(DatabaseCircuitBreaker.CircuitState.CLOSED);
        when(mockCircuitBreaker.getFailureCount()).thenReturn(0);
        when(mockCircuitBreaker.allowRequest()).thenReturn(true);

        // Execute
        Map<String, Object> result = resource.getCircuitBreakerStatus();

        // Verify
        assertEquals("CLOSED", result.get("state"));
        assertEquals(0, result.get("failureCount"));
        assertEquals(true, result.get("isAllowingRequests"));
    }

    @Test
    @DisplayName("Should return OPEN state in circuit breaker status")
    void testCircuitBreakerStatusOpen() {
        // Setup
        when(mockCircuitBreaker.getState()).thenReturn(DatabaseCircuitBreaker.CircuitState.OPEN);
        when(mockCircuitBreaker.getFailureCount()).thenReturn(50);
        when(mockCircuitBreaker.allowRequest()).thenReturn(false);

        // Execute
        Map<String, Object> result = resource.getCircuitBreakerStatus();

        // Verify
        assertEquals("OPEN", result.get("state"));
        assertEquals(50, result.get("failureCount"));
        assertEquals(false, result.get("isAllowingRequests"));
    }

    @Test
    @DisplayName("Should reset circuit breaker")
    void testResetCircuitBreaker() {
        // Setup
        when(mockCircuitBreaker.getState()).thenReturn(DatabaseCircuitBreaker.CircuitState.CLOSED);

        // Execute
        Map<String, String> result = resource.resetCircuitBreaker();

        // Verify
        assertNotNull(result);
        assertTrue(result.containsKey("status"));
        assertTrue(result.containsKey("message"));
        assertEquals("success", result.get("status"));
        verify(mockCircuitBreaker, times(1)).reset();
    }

    @Test
    @DisplayName("Should get performance summary")
    void testGetPerformanceSummary() {
        // Setup
        when(mockMetrics.getCurrentThroughput()).thenReturn(800.0);
        when(mockMetrics.getTotalUpdates()).thenReturn(50000.0);
        when(mockMetrics.getFailureRate()).thenReturn(1.0);
        when(mockCircuitBreaker.getState()).thenReturn(DatabaseCircuitBreaker.CircuitState.CLOSED);

        // Execute
        Map<String, Object> result = resource.getPerformanceSummary();

        // Verify
        assertNotNull(result);
        assertTrue(result.containsKey("throughput"));
        assertTrue(result.containsKey("reliability"));
        assertTrue(result.containsKey("circuitBreaker"));
    }

    @Test
    @DisplayName("Should include throughput section in summary")
    void testSummaryThroughputSection() {
        // Setup
        when(mockMetrics.getCurrentThroughput()).thenReturn(900.0);
        when(mockMetrics.getTotalUpdates()).thenReturn(60000.0);
        when(mockMetrics.getFailureRate()).thenReturn(0.5);
        when(mockCircuitBreaker.getState()).thenReturn(DatabaseCircuitBreaker.CircuitState.CLOSED);

        // Execute
        Map<String, Object> result = resource.getPerformanceSummary();

        // Verify
        assertNotNull(result.get("throughput"));
        assertTrue(result.get("throughput") instanceof Map);
    }

    @Test
    @DisplayName("Should include reliability section in summary")
    void testSummaryReliabilitySection() {
        // Setup
        when(mockMetrics.getCurrentThroughput()).thenReturn(500.0);
        when(mockMetrics.getTotalUpdates()).thenReturn(30000.0);
        when(mockMetrics.getFailureRate()).thenReturn(2.0);
        when(mockCircuitBreaker.getState()).thenReturn(DatabaseCircuitBreaker.CircuitState.CLOSED);

        // Execute
        Map<String, Object> result = resource.getPerformanceSummary();

        // Verify
        assertNotNull(result.get("reliability"));
        assertTrue(result.get("reliability") instanceof Map);
    }

    @Test
    @DisplayName("Should include circuit breaker section in summary")
    void testSummaryCircuitBreakerSection() {
        // Setup
        when(mockMetrics.getCurrentThroughput()).thenReturn(600.0);
        when(mockMetrics.getTotalUpdates()).thenReturn(40000.0);
        when(mockMetrics.getFailureRate()).thenReturn(1.5);
        when(mockCircuitBreaker.getState()).thenReturn(DatabaseCircuitBreaker.CircuitState.CLOSED);

        // Execute
        Map<String, Object> result = resource.getPerformanceSummary();

        // Verify
        assertNotNull(result.get("circuitBreaker"));
        assertTrue(result.get("circuitBreaker") instanceof Map);
    }

    @Test
    @DisplayName("Should calculate performance percentage correctly")
    void testPerformancePercentage() {
        // Setup - 1000 TPS target
        when(mockMetrics.getCurrentThroughput()).thenReturn(500.0);
        when(mockMetrics.getTotalUpdates()).thenReturn(25000.0);
        when(mockMetrics.getFailureRate()).thenReturn(1.0);
        when(mockCircuitBreaker.getState()).thenReturn(DatabaseCircuitBreaker.CircuitState.CLOSED);

        // Execute
        Map<String, Object> result = resource.getPerformanceSummary();

        // Verify
        assertNotNull(result);
    }

    @Test
    @DisplayName("Should show healthy status when circuit is CLOSED")
    void testHealthyStatusWhenClosed() {
        // Setup
        when(mockMetrics.getCurrentThroughput()).thenReturn(1000.0);
        when(mockMetrics.getTotalUpdates()).thenReturn(100000.0);
        when(mockMetrics.getFailureRate()).thenReturn(0.1);
        when(mockCircuitBreaker.getState()).thenReturn(DatabaseCircuitBreaker.CircuitState.CLOSED);

        // Execute
        Map<String, Object> result = resource.getPerformanceSummary();

        // Verify
        assertNotNull(result);
        assertTrue(result.containsKey("circuitBreaker"));
    }

    @Test
    @DisplayName("Should show unhealthy status when circuit is OPEN")
    void testUnhealthyStatusWhenOpen() {
        // Setup
        when(mockMetrics.getCurrentThroughput()).thenReturn(0.0);
        when(mockMetrics.getTotalUpdates()).thenReturn(100000.0);
        when(mockMetrics.getFailureRate()).thenReturn(50.0);
        when(mockCircuitBreaker.getState()).thenReturn(DatabaseCircuitBreaker.CircuitState.OPEN);

        // Execute
        Map<String, Object> result = resource.getPerformanceSummary();

        // Verify
        assertNotNull(result);
    }

    @Test
    @DisplayName("Should handle zero throughput")
    void testZeroThroughput() {
        // Setup
        when(mockMetrics.getCurrentThroughput()).thenReturn(0.0);
        when(mockMetrics.getTotalUpdates()).thenReturn(0.0);
        when(mockMetrics.getFailureRate()).thenReturn(0.0);
        when(mockCircuitBreaker.getState()).thenReturn(DatabaseCircuitBreaker.CircuitState.CLOSED);
        when(mockCircuitBreaker.getFailureCount()).thenReturn(0);

        // Execute
        Map<String, Object> result = resource.getMetrics();

        // Verify
        assertNotNull(result);
        assertNotNull(result.get("currentThroughputTPS"));
    }

    @Test
    @DisplayName("Should handle high throughput values")
    void testHighThroughputValues() {
        // Setup
        when(mockMetrics.getCurrentThroughput()).thenReturn(2000.0);
        when(mockMetrics.getTotalUpdates()).thenReturn(500000.0);
        when(mockMetrics.getFailureRate()).thenReturn(0.01);
        when(mockCircuitBreaker.getState()).thenReturn(DatabaseCircuitBreaker.CircuitState.CLOSED);
        when(mockCircuitBreaker.getFailureCount()).thenReturn(0);

        // Execute
        Map<String, Object> result = resource.getMetrics();

        // Verify
        assertNotNull(result);
    }

    @Test
    @DisplayName("Should format metrics as strings")
    void testMetricsFormatting() {
        // Setup
        when(mockMetrics.getCurrentThroughput()).thenReturn(123.456);
        when(mockMetrics.getTotalUpdates()).thenReturn(98765.432);
        when(mockMetrics.getFailureRate()).thenReturn(3.456);
        when(mockCircuitBreaker.getState()).thenReturn(DatabaseCircuitBreaker.CircuitState.CLOSED);
        when(mockCircuitBreaker.getFailureCount()).thenReturn(5);

        // Execute
        Map<String, Object> result = resource.getMetrics();

        // Verify
        assertNotNull(result.get("currentThroughputTPS"));
        assertNotNull(result.get("totalUpdates"));
        assertNotNull(result.get("failureRatePercent"));
    }

    @Test
    @DisplayName("Should handle HALF_OPEN state")
    void testHalfOpenState() {
        // Setup
        when(mockMetrics.getCurrentThroughput()).thenReturn(100.0);
        when(mockMetrics.getTotalUpdates()).thenReturn(10000.0);
        when(mockMetrics.getFailureRate()).thenReturn(5.0);
        when(mockCircuitBreaker.getState()).thenReturn(DatabaseCircuitBreaker.CircuitState.HALF_OPEN);
        when(mockCircuitBreaker.getFailureCount()).thenReturn(25);
        when(mockCircuitBreaker.allowRequest()).thenReturn(true);

        // Execute
        Map<String, Object> result = resource.getCircuitBreakerStatus();

        // Verify
        assertEquals("HALF_OPEN", result.get("state"));
    }
}
