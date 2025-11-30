package com.csg.airtel.aaa4j.application.controller;

import com.csg.airtel.aaa4j.infrastructure.DatabaseCircuitBreaker;
import com.csg.airtel.aaa4j.infrastructure.PerformanceMetrics;
import jakarta.inject.Inject;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import org.jboss.logging.Logger;

import java.util.HashMap;
import java.util.Map;

/**
 * REST endpoints for monitoring performance and circuit breaker status
 * Critical for validating 1000+ TPS throughput in production
 */
@Path("/api/monitoring")
public class PerformanceMonitoringResource {
    private static final Logger log = Logger.getLogger(PerformanceMonitoringResource.class);

    @Inject
    PerformanceMetrics metrics;

    @Inject
    DatabaseCircuitBreaker circuitBreaker;

    /**
     * Get current performance metrics
     * Returns throughput, total updates, and failure rate
     */
    @GET
    @Path("/metrics")
    @Produces(MediaType.APPLICATION_JSON)
    public Map<String, Object> getMetrics() {
        Map<String, Object> metricsMap = new HashMap<>();

        metricsMap.put("currentThroughputTPS", String.format("%.2f", metrics.getCurrentThroughput()));
        metricsMap.put("totalUpdates", String.format("%.0f", metrics.getTotalUpdates()));
        metricsMap.put("failureRatePercent", String.format("%.2f", metrics.getFailureRate()));
        metricsMap.put("circuitBreakerState", circuitBreaker.getState().toString());
        metricsMap.put("circuitBreakerFailureCount", circuitBreaker.getFailureCount());

        log.debugf("Metrics requested: TPS=%.2f, Total=%.0f, FailureRate=%.2f%%",
                metrics.getCurrentThroughput(),
                metrics.getTotalUpdates(),
                metrics.getFailureRate());

        return metricsMap;
    }

    /**
     * Get circuit breaker status
     */
    @GET
    @Path("/circuit-breaker")
    @Produces(MediaType.APPLICATION_JSON)
    public Map<String, Object> getCircuitBreakerStatus() {
        Map<String, Object> status = new HashMap<>();

        status.put("state", circuitBreaker.getState().toString());
        status.put("failureCount", circuitBreaker.getFailureCount());
        status.put("isAllowingRequests", circuitBreaker.allowRequest());

        return status;
    }

    /**
     * Reset circuit breaker (admin operation)
     */
    @POST
    @Path("/circuit-breaker/reset")
    @Produces(MediaType.APPLICATION_JSON)
    public Map<String, String> resetCircuitBreaker() {
        circuitBreaker.reset();
        log.info("Circuit breaker manually reset via API");

        Map<String, String> response = new HashMap<>();
        response.put("status", "success");
        response.put("message", "Circuit breaker has been reset to CLOSED state");

        return response;
    }

    /**
     * Get performance summary
     */
    @GET
    @Path("/summary")
    @Produces(MediaType.APPLICATION_JSON)
    public Map<String, Object> getPerformanceSummary() {
        Map<String, Object> summary = new HashMap<>();

        double currentTps = metrics.getCurrentThroughput();
        double totalUpdates = metrics.getTotalUpdates();
        double failureRate = metrics.getFailureRate();

        summary.put("throughput", Map.of(
                "currentTPS", String.format("%.2f", currentTps),
                "target", "1000",
                "performancePercent", String.format("%.1f", (currentTps / 1000.0) * 100.0)
        ));

        summary.put("reliability", Map.of(
                "totalUpdates", String.format("%.0f", totalUpdates),
                "failureRate", String.format("%.2f%%", failureRate),
                "successRate", String.format("%.2f%%", 100.0 - failureRate)
        ));

        summary.put("circuitBreaker", Map.of(
                "state", circuitBreaker.getState().toString(),
                "healthy", circuitBreaker.getState() == DatabaseCircuitBreaker.CircuitState.CLOSED
        ));

        return summary;
    }
}
