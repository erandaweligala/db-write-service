package com.csg.airtel.aaa4j.application.config;

import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;

/**
 * Configuration for MySQL database connection pool.
 * Mirror of {@link PoolConfig} (Oracle) but bound to the {@code mysql-pool} prefix,
 * allowing each datasource to be tuned independently.
 */
@ConfigMapping(prefix = "mysql-pool")
public interface MySQLPoolConfig {

    @WithDefault("50")
    int maxSize();

    @WithDefault("5000")
    int connectionTimeout();

    @WithDefault("600000")
    int idleTimeout();

    @WithDefault("1800000")
    int maxLifetime();

    @WithDefault("4")
    int acquireIncrement();

    @WithDefault("256")
    int preparedStatementCacheMaxSize();

    @WithDefault("true")
    boolean pipeliningEnabled();

    @WithDefault("256")
    int pipeliningLimit();

    @WithDefault("32")
    int eventLoopSize();

    @WithDefault("true")
    boolean tcpKeepAlive();

    @WithDefault("true")
    boolean tcpNoDelay();

    @WithDefault("true")
    boolean poolCleanerEnabled();

    @WithDefault("60000")
    int poolCleanerInterval();
}