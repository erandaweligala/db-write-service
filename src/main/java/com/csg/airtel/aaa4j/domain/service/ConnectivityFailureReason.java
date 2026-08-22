package com.csg.airtel.aaa4j.domain.service;

/**
 * Why a call to an external dependency (database, Redis, Kafka, Elasticsearch, an HTTP
 * service) failed.
 *
 * <p>Every value except {@link #APPLICATION_ERROR} denotes a transport/connectivity
 * problem — the dependency could not be reached, authenticated against, or kept
 * connected. {@code APPLICATION_ERROR} is the catch-all for failures that reached
 * the dependency and came back as a business/logic error (bad SQL, wrong key type,
 * mapping failure, a rejected document); those are counted but never flip a
 * dependency to DOWN.</p>
 *
 * <p>The label is what appears as the {@code reason} Prometheus tag, so it is kept
 * lowercase and snake_case for Grafana legends.</p>
 */
public enum ConnectivityFailureReason {

    /** TCP connect rejected by the host — process not listening on the port. */
    CONNECTION_REFUSED("connection_refused"),

    /** Connect, read, or command timed out before the dependency answered. */
    CONNECTION_TIMEOUT("connection_timeout"),

    /** An established connection was reset, closed, or dropped mid-flight. */
    CONNECTION_CLOSED("connection_closed"),

    /** DNS resolution failed or the host/network is unroutable. */
    HOST_UNREACHABLE("host_unreachable"),

    /** No connection could be borrowed — pool exhausted or wait queue full. */
    POOL_EXHAUSTED("pool_exhausted"),

    /** Credentials rejected (ORA-01017, Redis NOAUTH/WRONGPASS, Kafka SASL). */
    AUTHENTICATION_FAILED("authentication_failed"),

    /** TLS/SSL handshake or certificate validation failure. */
    TLS_FAILURE("tls_failure"),

    /** Kafka broker/coordinator not available or metadata unavailable. */
    BROKER_UNAVAILABLE("broker_unavailable"),

    /** Reached but unusable — listener down, instance starting, cluster down, circuit open. */
    SERVICE_UNAVAILABLE("service_unavailable"),

    /** Not a connectivity problem; the dependency answered with an error. */
    APPLICATION_ERROR("application_error");

    private final String label;

    ConnectivityFailureReason(String label) {
        this.label = label;
    }

    public String label() {
        return label;
    }

    /** {@code true} for every reason that indicates the dependency itself is unreachable/unusable. */
    public boolean isConnectivityFailure() {
        return this != APPLICATION_ERROR;
    }
}
