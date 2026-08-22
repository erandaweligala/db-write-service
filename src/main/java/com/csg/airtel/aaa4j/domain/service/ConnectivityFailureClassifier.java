package com.csg.airtel.aaa4j.domain.service;

import java.io.IOException;
import java.net.ConnectException;
import java.net.NoRouteToHostException;
import java.net.PortUnreachableException;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.nio.channels.ClosedChannelException;
import java.util.Locale;
import java.util.Map;

import javax.net.ssl.SSLException;

/**
 * Decides whether a {@link Throwable} raised against an infrastructure dependency —
 * a database, Redis, Kafka, Elasticsearch, or an HTTP service — is a connectivity
 * problem, and if so which kind.
 *
 * <p>Classification walks the cause chain and, for each link, tries three cheap
 * strategies in order:</p>
 * <ol>
 *   <li>exact match on the exception's simple name — covers vendor types
 *       (Kafka, Vert.x, SmallRye, Apache HttpClient) without compiling against them;</li>
 *   <li>token scan of the exception message — covers the generic wrappers those
 *       stacks love ({@code NoStackTraceThrowable}, {@code SQLException}, ORA codes);</li>
 *   <li>JDK type hierarchy — the socket/IO exceptions everything eventually bottoms out in.</li>
 * </ol>
 *
 * <p>Anything unmatched is {@link ConnectivityFailureReason#APPLICATION_ERROR}: counted
 * against the dependency but never treated as an outage.</p>
 */
final class ConnectivityFailureClassifier {

    /** Hard cap on cause-chain walking; mirrors {@code ExceptionMetricsService}. */
    private static final int MAX_CAUSE_DEPTH = 16;

    /** Exception simple name -> reason. Vendor types are matched by name to avoid hard imports. */
    private static final Map<String, ConnectivityFailureReason> BY_SIMPLE_NAME = Map.ofEntries(
            // Timeouts — java.util.concurrent, Kafka, Netty, Vert.x, SmallRye Fault Tolerance
            Map.entry("TimeoutException", ConnectivityFailureReason.CONNECTION_TIMEOUT),
            Map.entry("SocketTimeoutException", ConnectivityFailureReason.CONNECTION_TIMEOUT),
            Map.entry("NoStackTraceTimeoutException", ConnectivityFailureReason.CONNECTION_TIMEOUT),
            Map.entry("ConnectTimeoutException", ConnectivityFailureReason.CONNECTION_TIMEOUT),
            Map.entry("ReadTimeoutException", ConnectivityFailureReason.CONNECTION_TIMEOUT),
            Map.entry("RequestTimeoutException", ConnectivityFailureReason.CONNECTION_TIMEOUT),
            // Dropped / reset connections
            Map.entry("NetworkException", ConnectivityFailureReason.CONNECTION_CLOSED),
            Map.entry("DisconnectException", ConnectivityFailureReason.CONNECTION_CLOSED),
            Map.entry("ClosedConnectionException", ConnectivityFailureReason.CONNECTION_CLOSED),
            Map.entry("ConnectionClosedException", ConnectivityFailureReason.CONNECTION_CLOSED),
            Map.entry("NoHttpResponseException", ConnectivityFailureReason.CONNECTION_CLOSED),
            // Pool starvation — Vert.x SQL/Redis pools
            Map.entry("ConnectionPoolTooBusyException", ConnectivityFailureReason.POOL_EXHAUSTED),
            Map.entry("PoolClosedException", ConnectivityFailureReason.POOL_EXHAUSTED),
            Map.entry("ConnectionPoolTimeoutException", ConnectivityFailureReason.POOL_EXHAUSTED),
            // Kafka cluster reachability
            Map.entry("BrokerNotAvailableException", ConnectivityFailureReason.BROKER_UNAVAILABLE),
            Map.entry("CoordinatorNotAvailableException", ConnectivityFailureReason.BROKER_UNAVAILABLE),
            Map.entry("LeaderNotAvailableException", ConnectivityFailureReason.BROKER_UNAVAILABLE),
            Map.entry("NotLeaderOrFollowerException", ConnectivityFailureReason.BROKER_UNAVAILABLE),
            // Credentials
            Map.entry("AuthenticationException", ConnectivityFailureReason.AUTHENTICATION_FAILED),
            Map.entry("SaslAuthenticationException", ConnectivityFailureReason.AUTHENTICATION_FAILED),
            Map.entry("AuthorizationException", ConnectivityFailureReason.AUTHENTICATION_FAILED),
            Map.entry("ClusterAuthorizationException", ConnectivityFailureReason.AUTHENTICATION_FAILED),
            Map.entry("TopicAuthorizationException", ConnectivityFailureReason.AUTHENTICATION_FAILED),
            // Reached but unusable
            Map.entry("CircuitBreakerOpenException", ConnectivityFailureReason.SERVICE_UNAVAILABLE),
            Map.entry("SQLRecoverableException", ConnectivityFailureReason.SERVICE_UNAVAILABLE),
            Map.entry("SQLTransientConnectionException", ConnectivityFailureReason.SERVICE_UNAVAILABLE),
            Map.entry("SQLNonTransientConnectionException", ConnectivityFailureReason.SERVICE_UNAVAILABLE));

    /**
     * Message tokens scanned in order — first hit wins, so specific phrases are listed
     * before broad ones. All tokens are lowercase; the message is lowercased once per link.
     */
    private static final MessageRule[] MESSAGE_RULES = {
            // Pool starvation first: these messages also mention "timeout"/"connection".
            rule("pool exhausted", ConnectivityFailureReason.POOL_EXHAUSTED),
            rule("connection pool reached max wait queue size", ConnectivityFailureReason.POOL_EXHAUSTED),
            rule("max waiting handlers", ConnectivityFailureReason.POOL_EXHAUSTED),
            rule("waiting queue is full", ConnectivityFailureReason.POOL_EXHAUSTED),
            rule("no more connections", ConnectivityFailureReason.POOL_EXHAUSTED),
            rule("failed to get a connection", ConnectivityFailureReason.POOL_EXHAUSTED),
            rule("timeout waiting for a connection", ConnectivityFailureReason.POOL_EXHAUSTED),
            rule("timeout waiting for connection from pool", ConnectivityFailureReason.POOL_EXHAUSTED),
            // Credentials
            rule("ora-01017", ConnectivityFailureReason.AUTHENTICATION_FAILED),
            rule("ora-28000", ConnectivityFailureReason.AUTHENTICATION_FAILED),
            rule("ora-28001", ConnectivityFailureReason.AUTHENTICATION_FAILED),
            rule("noauth", ConnectivityFailureReason.AUTHENTICATION_FAILED),
            rule("wrongpass", ConnectivityFailureReason.AUTHENTICATION_FAILED),
            rule("authentication failed", ConnectivityFailureReason.AUTHENTICATION_FAILED),
            rule("invalid username", ConnectivityFailureReason.AUTHENTICATION_FAILED),
            rule("invalid password", ConnectivityFailureReason.AUTHENTICATION_FAILED),
            rule("not authorized", ConnectivityFailureReason.AUTHENTICATION_FAILED),
            // TLS
            rule("handshake", ConnectivityFailureReason.TLS_FAILURE),
            rule("certificate", ConnectivityFailureReason.TLS_FAILURE),
            rule("sslexception", ConnectivityFailureReason.TLS_FAILURE),
            // Nothing listening
            rule("connection refused", ConnectivityFailureReason.CONNECTION_REFUSED),
            rule("ora-12541", ConnectivityFailureReason.CONNECTION_REFUSED),
            // Name resolution / routing
            rule("unknown host", ConnectivityFailureReason.HOST_UNREACHABLE),
            rule("no route to host", ConnectivityFailureReason.HOST_UNREACHABLE),
            rule("network is unreachable", ConnectivityFailureReason.HOST_UNREACHABLE),
            rule("name or service not known", ConnectivityFailureReason.HOST_UNREACHABLE),
            rule("temporary failure in name resolution", ConnectivityFailureReason.HOST_UNREACHABLE),
            rule("unresolved address", ConnectivityFailureReason.HOST_UNREACHABLE),
            rule("nodename nor servname", ConnectivityFailureReason.HOST_UNREACHABLE),
            rule("ora-12545", ConnectivityFailureReason.HOST_UNREACHABLE),
            // Dropped mid-flight
            rule("connection reset", ConnectivityFailureReason.CONNECTION_CLOSED),
            rule("broken pipe", ConnectivityFailureReason.CONNECTION_CLOSED),
            rule("connection closed", ConnectivityFailureReason.CONNECTION_CLOSED),
            rule("connection is closed", ConnectivityFailureReason.CONNECTION_CLOSED),
            rule("connection was closed", ConnectivityFailureReason.CONNECTION_CLOSED),
            rule("socket closed", ConnectivityFailureReason.CONNECTION_CLOSED),
            rule("closed channel", ConnectivityFailureReason.CONNECTION_CLOSED),
            rule("connection pool shut down", ConnectivityFailureReason.CONNECTION_CLOSED),
            rule("end-of-file", ConnectivityFailureReason.CONNECTION_CLOSED),
            rule("ora-03113", ConnectivityFailureReason.CONNECTION_CLOSED),
            rule("ora-03114", ConnectivityFailureReason.CONNECTION_CLOSED),
            rule("ora-03135", ConnectivityFailureReason.CONNECTION_CLOSED),
            // Kafka cluster reachability
            rule("no brokers available", ConnectivityFailureReason.BROKER_UNAVAILABLE),
            rule("broker not available", ConnectivityFailureReason.BROKER_UNAVAILABLE),
            rule("not present in metadata", ConnectivityFailureReason.BROKER_UNAVAILABLE),
            rule("coordinator is not available", ConnectivityFailureReason.BROKER_UNAVAILABLE),
            rule("no resolvable bootstrap urls", ConnectivityFailureReason.BROKER_UNAVAILABLE),
            rule("failed to construct kafka", ConnectivityFailureReason.BROKER_UNAVAILABLE),
            // Timeouts — deliberately phrase-bound so an application message that merely
            // contains the word "timeout" (e.g. a Session-Timeout attribute) is not swept up.
            rule("timed out", ConnectivityFailureReason.CONNECTION_TIMEOUT),
            rule("timeout waiting", ConnectivityFailureReason.CONNECTION_TIMEOUT),
            rule("connect timeout", ConnectivityFailureReason.CONNECTION_TIMEOUT),
            rule("read timeout", ConnectivityFailureReason.CONNECTION_TIMEOUT),
            rule("listener timeout after waiting for", ConnectivityFailureReason.CONNECTION_TIMEOUT),
            rule("request timeout", ConnectivityFailureReason.CONNECTION_TIMEOUT),
            rule("ms has passed since batch creation", ConnectivityFailureReason.CONNECTION_TIMEOUT),
            rule("ora-12170", ConnectivityFailureReason.CONNECTION_TIMEOUT),
            // Reached but unusable
            rule("ora-12514", ConnectivityFailureReason.SERVICE_UNAVAILABLE),
            rule("ora-12505", ConnectivityFailureReason.SERVICE_UNAVAILABLE),
            rule("ora-01033", ConnectivityFailureReason.SERVICE_UNAVAILABLE),
            rule("ora-01034", ConnectivityFailureReason.SERVICE_UNAVAILABLE),
            rule("ora-27101", ConnectivityFailureReason.SERVICE_UNAVAILABLE),
            rule("clusterdown", ConnectivityFailureReason.SERVICE_UNAVAILABLE),
            rule("masterdown", ConnectivityFailureReason.SERVICE_UNAVAILABLE),
            rule("loading dataset in memory", ConnectivityFailureReason.SERVICE_UNAVAILABLE),
            rule("circuit breaker", ConnectivityFailureReason.SERVICE_UNAVAILABLE),
            rule("service unavailable", ConnectivityFailureReason.SERVICE_UNAVAILABLE),
            // Elasticsearch: every node in the REST client's pool is blacklisted, or the
            // cluster itself is not serving requests.
            rule("no living connections", ConnectivityFailureReason.SERVICE_UNAVAILABLE),
            rule("master_not_discovered_exception", ConnectivityFailureReason.SERVICE_UNAVAILABLE),
            rule("cluster_block_exception", ConnectivityFailureReason.SERVICE_UNAVAILABLE),
            rule("es_rejected_execution_exception", ConnectivityFailureReason.SERVICE_UNAVAILABLE)
    };

    private ConnectivityFailureClassifier() {
        // Utility class
    }

    /**
     * Classifies a failure by walking its cause chain and returning the first
     * connectivity reason found, or {@link ConnectivityFailureReason#APPLICATION_ERROR}
     * when no link looks like a transport problem.
     *
     * @param throwable the failure to classify; {@code null} yields {@code APPLICATION_ERROR}
     * @return the classified reason, never {@code null}
     */
    static ConnectivityFailureReason classify(Throwable throwable) {
        Throwable current = throwable;
        for (int depth = 0; current != null && depth < MAX_CAUSE_DEPTH; depth++) {
            ConnectivityFailureReason reason = classifyOne(current);
            if (reason.isConnectivityFailure()) {
                return reason;
            }
            Throwable cause = current.getCause();
            current = cause == current ? null : cause;
        }
        return ConnectivityFailureReason.APPLICATION_ERROR;
    }

    private static ConnectivityFailureReason classifyOne(Throwable throwable) {
        ConnectivityFailureReason byName = BY_SIMPLE_NAME.get(throwable.getClass().getSimpleName());
        if (byName != null) {
            return byName;
        }
        ConnectivityFailureReason byMessage = classifyMessage(throwable.getMessage());
        if (byMessage.isConnectivityFailure()) {
            return byMessage;
        }
        return classifyByHierarchy(throwable);
    }

    private static ConnectivityFailureReason classifyMessage(String message) {
        if (message == null || message.isEmpty()) {
            return ConnectivityFailureReason.APPLICATION_ERROR;
        }
        String lower = message.toLowerCase(Locale.ROOT);
        for (MessageRule rule : MESSAGE_RULES) {
            if (lower.contains(rule.token())) {
                return rule.reason();
            }
        }
        return ConnectivityFailureReason.APPLICATION_ERROR;
    }

    private static ConnectivityFailureReason classifyByHierarchy(Throwable throwable) {
        if (throwable instanceof ConnectException) {
            return ConnectivityFailureReason.CONNECTION_REFUSED;
        }
        if (throwable instanceof UnknownHostException
                || throwable instanceof NoRouteToHostException
                || throwable instanceof PortUnreachableException) {
            return ConnectivityFailureReason.HOST_UNREACHABLE;
        }
        if (throwable instanceof SocketTimeoutException) {
            return ConnectivityFailureReason.CONNECTION_TIMEOUT;
        }
        if (throwable instanceof SSLException) {
            return ConnectivityFailureReason.TLS_FAILURE;
        }
        if (throwable instanceof ClosedChannelException || throwable instanceof SocketException) {
            return ConnectivityFailureReason.CONNECTION_CLOSED;
        }
        // Any remaining IOException against a dependency is, by definition, a transport failure.
        if (throwable instanceof IOException) {
            return ConnectivityFailureReason.SERVICE_UNAVAILABLE;
        }
        return ConnectivityFailureReason.APPLICATION_ERROR;
    }

    private static MessageRule rule(String token, ConnectivityFailureReason reason) {
        return new MessageRule(token, reason);
    }

    /** One lowercase message token and the reason it implies. */
    private record MessageRule(String token, ConnectivityFailureReason reason) {}
}
