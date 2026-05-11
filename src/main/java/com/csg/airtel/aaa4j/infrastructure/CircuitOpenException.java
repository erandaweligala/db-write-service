package com.csg.airtel.aaa4j.infrastructure;

/**
 * Thrown when {@link DatabaseCircuitBreaker} is OPEN and a DB call is short-circuited.
 * Treated as a transient failure so the in-app retry loop will back off and try again
 * once the breaker transitions to HALF_OPEN / CLOSED.
 */
public class CircuitOpenException extends RuntimeException {
    public CircuitOpenException(String message) {
        super(message);
    }
}
