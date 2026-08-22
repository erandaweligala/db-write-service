package com.csg.airtel.aaa4j.application.config;

import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;

/**
 * Configuration for dependency connectivity monitoring (Oracle, Redis, Kafka).
 *
 * <p>Drives the health probes and the up/down state machine whose metrics are
 * scraped by Prometheus and rendered in Grafana.</p>
 */
@ConfigMapping(prefix = "connectivity")
public interface ConnectivityMonitoringConfig {

    /**
     * Value of the {@code service} tag on every {@code dependency_*} metric. Every AAA
     * service exports the same metric names, so this is what keeps their series — and
     * therefore their dashboards and alerts — apart in a shared Prometheus.
     * Default: db-write-service
     */
    @WithDefault("db-write-service")
    String serviceName();

    /**
     * Master switch for connectivity monitoring. When false, failures are still
     * classified and counted but no active probe runs.
     * Default: true
     */
    @WithDefault("true")
    boolean enabled();

    /**
     * Number of consecutive connectivity failures (from live traffic or probes)
     * before a dependency is marked DOWN. Guards against single-blip flapping.
     * Default: 3
     */
    @WithDefault("3")
    int failureThreshold();

    /**
     * Timeout applied to each health probe, in milliseconds. A probe that does not
     * answer within this budget counts as a connectivity failure.
     * Default: 2000ms
     */
    @WithDefault("2000")
    long probeTimeoutMs();

    /**
     * Whether the Oracle probe ({@code SELECT 1 FROM DUAL}) runs.
     * Default: true
     */
    @WithDefault("true")
    boolean probeDatabase();

    /**
     * Whether the Redis probe ({@code PING}) runs.
     * Default: true
     */
    @WithDefault("true")
    boolean probeRedis();

    /**
     * Whether the Kafka probe (AdminClient {@code describeCluster}) runs.
     * Default: true
     */
    @WithDefault("true")
    boolean probeKafka();

    /**
     * Probe interval as a Quarkus scheduler {@code every} expression. Declared here for
     * documentation and validation; the scheduler reads the raw
     * {@code connectivity.probe-interval} property directly.
     * Default: 15s
     */
    @WithDefault("15s")
    String probeInterval();
}
