package com.csg.airtel.aaa4j.domain.service;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.ConnectException;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.util.concurrent.TimeoutException;

import javax.net.ssl.SSLHandshakeException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ConnectivityFailureClassifierTest {

    @Test
    void classifiesRefusedConnections() {
        assertEquals(ConnectivityFailureReason.CONNECTION_REFUSED,
                ConnectivityFailureClassifier.classify(new ConnectException("Connection refused: localhost/127.0.0.1:6379")));
        // No message at all — the JDK type alone is enough.
        assertEquals(ConnectivityFailureReason.CONNECTION_REFUSED,
                ConnectivityFailureClassifier.classify(new ConnectException()));
        // Oracle listener down
        assertEquals(ConnectivityFailureReason.CONNECTION_REFUSED,
                ConnectivityFailureClassifier.classify(new RuntimeException("ORA-12541: TNS:no listener")));
    }

    @Test
    void classifiesTimeouts() {
        assertEquals(ConnectivityFailureReason.CONNECTION_TIMEOUT,
                ConnectivityFailureClassifier.classify(new TimeoutException("Timeout[3000] ms")));
        assertEquals(ConnectivityFailureReason.CONNECTION_TIMEOUT,
                ConnectivityFailureClassifier.classify(new SocketTimeoutException("Read timed out")));
        assertEquals(ConnectivityFailureReason.CONNECTION_TIMEOUT,
                ConnectivityFailureClassifier.classify(
                        new RuntimeException("Expiring 4 record(s) for accounting-dc-0: 30000 ms has passed since batch creation")));
        assertEquals(ConnectivityFailureReason.CONNECTION_TIMEOUT,
                ConnectivityFailureClassifier.classify(new RuntimeException("ORA-12170: TNS:Connect timeout occurred")));
    }

    @Test
    void classifiesPoolStarvationAheadOfTimeout() {
        // Vert.x pool messages mention both waiting and timeouts; pool exhaustion is the real cause.
        assertEquals(ConnectivityFailureReason.POOL_EXHAUSTED,
                ConnectivityFailureClassifier.classify(
                        new RuntimeException("Timeout waiting for a connection: connection pool reached max wait queue size of 500")));
        assertEquals(ConnectivityFailureReason.POOL_EXHAUSTED,
                ConnectivityFailureClassifier.classify(new RuntimeException("Max waiting handlers reached")));
    }

    @Test
    void classifiesDroppedConnections() {
        assertEquals(ConnectivityFailureReason.CONNECTION_CLOSED,
                ConnectivityFailureClassifier.classify(new IOException("Connection reset by peer")));
        assertEquals(ConnectivityFailureReason.CONNECTION_CLOSED,
                ConnectivityFailureClassifier.classify(new RuntimeException("ORA-03113: end-of-file on communication channel")));
    }

    @Test
    void classifiesUnreachableHostsAndCredentialsAndTls() {
        assertEquals(ConnectivityFailureReason.HOST_UNREACHABLE,
                ConnectivityFailureClassifier.classify(new UnknownHostException("redis-cluster-headless")));
        assertEquals(ConnectivityFailureReason.AUTHENTICATION_FAILED,
                ConnectivityFailureClassifier.classify(new RuntimeException("WRONGPASS invalid username-password pair")));
        assertEquals(ConnectivityFailureReason.AUTHENTICATION_FAILED,
                ConnectivityFailureClassifier.classify(new RuntimeException("ORA-01017: invalid credential")));
        assertEquals(ConnectivityFailureReason.TLS_FAILURE,
                ConnectivityFailureClassifier.classify(new SSLHandshakeException("PKIX path building failed")));
    }

    @Test
    void classifiesKafkaClusterReachability() {
        assertEquals(ConnectivityFailureReason.BROKER_UNAVAILABLE,
                ConnectivityFailureClassifier.classify(new RuntimeException("Topic accounting-dc not present in metadata after 60000 ms")));
        assertEquals(ConnectivityFailureReason.BROKER_UNAVAILABLE,
                ConnectivityFailureClassifier.classify(new RuntimeException("No resolvable bootstrap urls given in bootstrap.servers")));
    }

    @Test
    void classifiesElasticsearchAndHttpTransportFaults() {
        // Every node in the REST client's pool has been blacklisted.
        assertEquals(ConnectivityFailureReason.SERVICE_UNAVAILABLE,
                ConnectivityFailureClassifier.classify(new IOException("No living connections")));
        assertEquals(ConnectivityFailureReason.SERVICE_UNAVAILABLE,
                ConnectivityFailureClassifier.classify(new RuntimeException("master_not_discovered_exception")));
        // The async client wraps its own wait this way rather than in a SocketTimeoutException.
        assertEquals(ConnectivityFailureReason.CONNECTION_TIMEOUT,
                ConnectivityFailureClassifier.classify(
                        new RuntimeException("listener timeout after waiting for [30000] ms")));
        assertEquals(ConnectivityFailureReason.CONNECTION_CLOSED,
                ConnectivityFailureClassifier.classify(new IllegalStateException("Connection pool shut down")));
        assertEquals(ConnectivityFailureReason.POOL_EXHAUSTED,
                ConnectivityFailureClassifier.classify(
                        new RuntimeException("Timeout waiting for connection from pool")));
    }

    @Test
    void walksTheCauseChain() {
        Throwable root = new ConnectException("Connection refused");
        Throwable wrapped = new IllegalStateException("could not load session",
                new RuntimeException("cache lookup failed", root));

        assertEquals(ConnectivityFailureReason.CONNECTION_REFUSED,
                ConnectivityFailureClassifier.classify(wrapped));
    }

    @Test
    void selfReferentialCauseDoesNotLoop() {
        RuntimeException selfCausing = new RuntimeException("boom") {
            @Override
            public synchronized Throwable getCause() {
                return this;
            }
        };
        assertEquals(ConnectivityFailureReason.APPLICATION_ERROR,
                ConnectivityFailureClassifier.classify(selfCausing));
    }

    @Test
    void applicationErrorsAreNotConnectivityFailures() {
        assertEquals(ConnectivityFailureReason.APPLICATION_ERROR,
                ConnectivityFailureClassifier.classify(new IllegalArgumentException("bucket id must not be null")));
        assertEquals(ConnectivityFailureReason.APPLICATION_ERROR,
                ConnectivityFailureClassifier.classify(new NullPointerException()));
        assertEquals(ConnectivityFailureReason.APPLICATION_ERROR,
                ConnectivityFailureClassifier.classify(null));
        // A business message that merely contains the word "timeout" must not read as a transport fault.
        assertEquals(ConnectivityFailureReason.APPLICATION_ERROR,
                ConnectivityFailureClassifier.classify(new IllegalStateException("Session-Timeout attribute is not numeric")));

        assertFalse(ConnectivityFailureReason.APPLICATION_ERROR.isConnectivityFailure());
        assertTrue(ConnectivityFailureReason.CONNECTION_TIMEOUT.isConnectivityFailure());
    }
}
