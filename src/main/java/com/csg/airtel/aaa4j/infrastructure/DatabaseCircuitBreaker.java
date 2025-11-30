package com.csg.airtel.aaa4j.infrastructure;

import jakarta.enterprise.context.ApplicationScoped;
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
                    log.info("Circuit breaker transitioning from OPEN to HALF_OPEN");
                    state.set(CircuitState.HALF_OPEN);
                    successCount.set(0);
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
                log.info("Circuit breaker transitioning from HALF_OPEN to CLOSED");
                state.set(CircuitState.CLOSED);
                failureCount.set(0);
                successCount.set(0);
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
            log.warn("Circuit breaker transitioning from HALF_OPEN to OPEN due to failure");
            state.set(CircuitState.OPEN);
            lastOpenTime.set(Instant.now());
            successCount.set(0);
        } else if (currentState == CircuitState.CLOSED) {
            int failures = failureCount.incrementAndGet();
            if (failures >= FAILURE_THRESHOLD) {
                log.errorf("Circuit breaker OPENING after %d failures", failures);
                state.set(CircuitState.OPEN);
                lastOpenTime.set(Instant.now());
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
        log.info("Circuit breaker manually reset to CLOSED");
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
