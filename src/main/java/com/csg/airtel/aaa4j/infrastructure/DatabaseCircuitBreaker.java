package com.csg.airtel.aaa4j.infrastructure;

import com.csg.airtel.aaa4j.application.common.LoggingUtil;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Circuit Breaker pattern implementation for database operations
 * Prevents cascading failures when database is under stress
 * Critical for maintaining stability at 1000+ TPS
 */
@ApplicationScoped
public class DatabaseCircuitBreaker {
    private static final Logger log = Logger.getLogger(DatabaseCircuitBreaker.class);

    // Circuit breaker configuration
    private static final int FAILURE_THRESHOLD = 50;  // Open circuit after 50 failures
    private static final Duration TIMEOUT = Duration.ofSeconds(30);  // Stay open for 30 seconds
    private static final int SUCCESS_THRESHOLD = 10;  // Require 10 successes to close

    private final AtomicInteger failureCount = new AtomicInteger(0);
    private final AtomicInteger successCount = new AtomicInteger(0);
    private final AtomicReference<CircuitState> state = new AtomicReference<>(CircuitState.CLOSED);
    private final AtomicReference<Instant> lastOpenTime = new AtomicReference<>(Instant.MIN);

    @Inject
    MeterRegistry meterRegistry;

    private Counter transitionsToOpen;
    private Counter transitionsToHalfOpen;
    private Counter transitionsToClosed;

    @PostConstruct
    void registerMetrics() {
        // Gauge that Prometheus can alert on (>= 1 means service is degraded).
        //   0 = CLOSED (healthy), 1 = HALF_OPEN (probing), 2 = OPEN (rejecting)
        Gauge.builder("db.circuit.breaker.state", state, s -> s.get().ordinal())
                .description("Database circuit breaker state (0=CLOSED, 1=OPEN, 2=HALF_OPEN per CircuitState ordinal)")
                .register(meterRegistry);

        Gauge.builder("db.circuit.breaker.failure.count", failureCount, AtomicInteger::get)
                .description("Current consecutive failure count tracked by the breaker")
                .register(meterRegistry);

        transitionsToOpen = Counter.builder("db.circuit.breaker.transitions")
                .description("Circuit breaker state transitions")
                .tag("to", "OPEN")
                .register(meterRegistry);
        transitionsToHalfOpen = Counter.builder("db.circuit.breaker.transitions")
                .description("Circuit breaker state transitions")
                .tag("to", "HALF_OPEN")
                .register(meterRegistry);
        transitionsToClosed = Counter.builder("db.circuit.breaker.transitions")
                .description("Circuit breaker state transitions")
                .tag("to", "CLOSED")
                .register(meterRegistry);
    }

    /**
     * Check if the circuit breaker allows the operation
     */
    public boolean allowRequest() {
        CircuitState currentState = state.get();

        switch (currentState) {
            case CLOSED:
                return true;

            case OPEN:
                // Check if timeout has elapsed
                if (Duration.between(lastOpenTime.get(), Instant.now()).compareTo(TIMEOUT) > 0) {
                    LoggingUtil.logInfo(log, "allowRequest", "Circuit breaker transitioning from OPEN to HALF_OPEN");
                    state.set(CircuitState.HALF_OPEN);
                    successCount.set(0);
                    if (transitionsToHalfOpen != null) transitionsToHalfOpen.increment();
                    return true;
                }
                return false;

            case HALF_OPEN:
                return true;

            default:
                return false;
        }
    }

    /**
     * Record a successful operation
     */
    public void recordSuccess() {
        CircuitState currentState = state.get();

        if (currentState == CircuitState.HALF_OPEN) {
            int successes = successCount.incrementAndGet();
            if (successes >= SUCCESS_THRESHOLD) {
                LoggingUtil.logInfo(log, "recordSuccess", "Circuit breaker transitioning from HALF_OPEN to CLOSED");
                state.set(CircuitState.CLOSED);
                failureCount.set(0);
                successCount.set(0);
                if (transitionsToClosed != null) transitionsToClosed.increment();
            }
        } else if (currentState == CircuitState.CLOSED) {
            // Reset failure count on success
            failureCount.set(0);
        }
    }

    /**
     * Record a failed operation
     */
    public void recordFailure() {
        CircuitState currentState = state.get();

        if (currentState == CircuitState.HALF_OPEN) {
            LoggingUtil.logWarn(log, "recordFailure", "Circuit breaker transitioning from HALF_OPEN to OPEN due to failure");
            state.set(CircuitState.OPEN);
            lastOpenTime.set(Instant.now());
            successCount.set(0);
            if (transitionsToOpen != null) transitionsToOpen.increment();
        } else if (currentState == CircuitState.CLOSED) {
            int failures = failureCount.incrementAndGet();
            if (failures >= FAILURE_THRESHOLD) {
                LoggingUtil.logError(log, "recordFailure", null, "Circuit breaker OPENING after %d failures", failures);
                state.set(CircuitState.OPEN);
                lastOpenTime.set(Instant.now());
                if (transitionsToOpen != null) transitionsToOpen.increment();
            }
        }
    }

    /**
     * Get current circuit state
     */
    public CircuitState getState() {
        return state.get();
    }

    /**
     * Get current failure count
     */
    public int getFailureCount() {
        return failureCount.get();
    }

    /**
     * Manually reset the circuit breaker
     */
    public void reset() {
        LoggingUtil.logInfo(log, "reset", "Circuit breaker manually reset to CLOSED");
        state.set(CircuitState.CLOSED);
        failureCount.set(0);
        successCount.set(0);
    }

    /**
     * Circuit breaker states
     */
    public enum CircuitState {
        CLOSED,      // Normal operation
        OPEN,        // Circuit is open, rejecting requests
        HALF_OPEN    // Testing if circuit can be closed
    }
}
