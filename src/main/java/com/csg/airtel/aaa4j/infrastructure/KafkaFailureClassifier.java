package com.csg.airtel.aaa4j.infrastructure;

import java.net.ConnectException;
import java.net.SocketException;
import java.sql.SQLNonTransientException;
import java.sql.SQLRecoverableException;
import java.sql.SQLTimeoutException;
import java.sql.SQLTransientException;
import java.util.concurrent.TimeoutException;
import java.util.function.Predicate;

/**
 * Classifies DB write failures as transient (retryable / DB-down) vs permanent (poison message).
 *
 * Transient: connection / timeout / pool exhaustion / reactive client closed — keep retrying,
 *            eventually the broker / DB recovers and the write succeeds.
 *
 * Permanent: constraint violation, malformed payload, syntax error, unknown column — retrying
 *            will never help. These go straight to the dead-letter topic for manual triage.
 *
 * The reactive Oracle / Vert.x driver does not always surface JDBC exception types; we therefore
 * also inspect the message and the cause chain.
 */
public final class KafkaFailureClassifier {

    private KafkaFailureClassifier() {}

    public static final Predicate<Throwable> IS_TRANSIENT = KafkaFailureClassifier::isTransient;

    public static boolean isTransient(Throwable t) {
        for (Throwable c = t; c != null; c = c.getCause()) {
            if (isTransientType(c)) {
                return true;
            }
            if (matchesTransientMessage(c.getMessage())) {
                return true;
            }
            if (c == c.getCause()) {
                break;
            }
        }
        return false;
    }

    private static boolean isTransientType(Throwable c) {
        if (c instanceof CircuitOpenException) return true;
        if (c instanceof SQLTransientException) return true;
        if (c instanceof SQLRecoverableException) return true;
        if (c instanceof SQLTimeoutException) return true;
        if (c instanceof TimeoutException) return true;
        if (c instanceof ConnectException) return true;
        if (c instanceof SocketException) return true;
        // Permanent JDBC errors — explicitly NOT transient.
        if (c instanceof SQLNonTransientException) return false;
        return false;
    }

    private static boolean matchesTransientMessage(String msg) {
        if (msg == null) return false;
        String m = msg.toLowerCase();
        // Vert.x SQL client / Oracle reactive client transient signals.
        // Also matches the DBWriteRepository's own breaker rejection so the executor
        // retries with backoff until the breaker transitions back to CLOSED.
        return m.contains("circuit breaker is open")
                || m.contains("connection is closed")
                || m.contains("connection closed")
                || m.contains("connection reset")
                || m.contains("connection refused")
                || m.contains("pool closed")
                || m.contains("pool is closed")
                || m.contains("no more connections")
                || m.contains("acquisition timeout")
                || m.contains("connection attempt failed")
                || m.contains("timeout")
                || m.contains("ora-12519")  // listener: no handler for service
                || m.contains("ora-12541")  // no listener
                || m.contains("ora-12537")  // connection closed
                || m.contains("ora-17008")  // closed connection
                || m.contains("ora-03113")  // end-of-file on comm channel
                || m.contains("ora-03114")  // not connected
                || m.contains("ora-01089")  // immediate shutdown in progress
                || m.contains("ora-00060"); // deadlock — retry will likely succeed
    }
}
