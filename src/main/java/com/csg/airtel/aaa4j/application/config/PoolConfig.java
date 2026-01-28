package com.csg.airtel.aaa4j.application.config;

import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;

/**
 * Configuration for Oracle database connection pool.
 * Optimized for 1000 TPS handling with configurable pool parameters.
 */
@ConfigMapping(prefix = "pool")
public interface PoolConfig {

    /**
     * Maximum number of connections in the pool.
     * For 1000 TPS with average query time of 50ms, need ~50 connections.
     * Default: 50
     */
    @WithDefault("50")
    int maxSize();

    /**
     * Maximum time to wait for a connection from the pool.
     * Default: 5000ms (5 seconds)
     */
    @WithDefault("5000")
    int connectionTimeout();

    /**
     * Time after which an idle connection is closed.
     * Default: 600000ms (10 minutes)
     */
    @WithDefault("600000")
    int idleTimeout();

    /**
     * Maximum lifetime of a connection in the pool.
     * Default: 1800000ms (30 minutes)
     */
    @WithDefault("1800000")
    int maxLifetime();

    /**
     * Number of connections to be acquired at a time when pool needs more.
     * Default: 4
     */
    @WithDefault("4")
    int acquireIncrement();

    /**
     * Maximum number of statements to cache per connection.
     * Caching prepared statements improves performance for repeated queries.
     * Default: 256
     */
    @WithDefault("256")
    int preparedStatementCacheMaxSize();

    /**
     * Enable pipelining to send multiple queries on same connection.
     * Improves throughput for high TPS scenarios.
     * Default: true
     */
    @WithDefault("true")
    boolean pipeliningEnabled();

    /**
     * Maximum number of queries that can be pipelined.
     * Default: 256
     */
    @WithDefault("256")
    int pipeliningLimit();

    /**
     * Event loop size for async operations.
     * Should match vertx event-loops-pool-size for optimal performance.
     * Default: 32
     */
    @WithDefault("32")
    int eventLoopSize();

    /**
     * TCP keep alive to detect dead connections.
     * Default: true
     */
    @WithDefault("true")
    boolean tcpKeepAlive();

    /**
     * TCP no delay for reduced latency.
     * Default: true
     */
    @WithDefault("true")
    boolean tcpNoDelay();

    /**
     * Enable pool cleaner to remove idle connections.
     * Default: true
     */
    @WithDefault("true")
    boolean poolCleanerEnabled();

    /**
     * Interval between pool cleaner runs in milliseconds.
     * Default: 60000ms (1 minute)
     */
    @WithDefault("60000")
    int poolCleanerInterval();
}
