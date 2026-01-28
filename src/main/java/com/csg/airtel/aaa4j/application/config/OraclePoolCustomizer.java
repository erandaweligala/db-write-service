package com.csg.airtel.aaa4j.application.config;

import io.quarkus.reactive.oracle.client.OraclePoolCreator;
import io.vertx.oracleclient.OracleBuilder;
import io.vertx.oracleclient.OracleConnectOptions;
import io.vertx.sqlclient.Pool;
import io.vertx.sqlclient.PoolOptions;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.jboss.logging.Logger;

import java.util.concurrent.TimeUnit;

/**
 * Customizes the Oracle connection
 * Applies configuration from PoolConfig to tune pool behavior.
 */
@Singleton
public class OraclePoolCustomizer implements OraclePoolCreator {

    private static final Logger log = Logger.getLogger(OraclePoolCustomizer.class);

    private final PoolConfig poolConfig;

    private Pool corePool;

    @Inject
    public OraclePoolCustomizer(PoolConfig poolConfig) {
        this.poolConfig = poolConfig;
    }

    @Override
    public Pool create(Input input) {

        // Get Quarkus-configured connect options as base
        OracleConnectOptions connectOptions = input.oracleConnectOptions();

        PoolOptions poolOptions = new PoolOptions()
                .setMaxSize(poolConfig.maxSize())
                .setIdleTimeout(poolConfig.idleTimeout())
                .setIdleTimeoutUnit(TimeUnit.MILLISECONDS)
                .setMaxLifetime(poolConfig.maxLifetime())
                .setMaxLifetimeUnit(TimeUnit.MILLISECONDS)
                .setConnectionTimeout(poolConfig.connectionTimeout())
                .setConnectionTimeoutUnit(TimeUnit.MILLISECONDS)
                .setPoolCleanerPeriod(poolConfig.poolCleanerInterval())
                .setEventLoopSize(poolConfig.eventLoopSize())
                .setShared(true)
                .setName("oracle-pool");


        connectOptions
                .setTcpKeepAlive(poolConfig.tcpKeepAlive())
                .setTcpNoDelay(poolConfig.tcpNoDelay());


        // Apply prepared statement cache size
        connectOptions.setPreparedStatementCacheMaxSize(poolConfig.preparedStatementCacheMaxSize());

        log.infof("Oracle pool '%s' configured: maxSize=%d, connectionTimeout=%dms, idleTimeout=%dms, " +
                        "maxLifetime=%dms, eventLoopSize=%d, pipelining=%s, tcpKeepAlive=%s, tcpNoDelay=%s",
                poolOptions.getName(),
                poolConfig.maxSize(),
                poolConfig.connectionTimeout(),
                poolConfig.idleTimeout(),
                poolConfig.maxLifetime(),
                poolConfig.eventLoopSize(),
                poolConfig.pipeliningEnabled(),
                poolConfig.tcpKeepAlive(),
                poolConfig.tcpNoDelay());

        this.corePool = OracleBuilder.pool()
                .with(poolOptions)
                .connectingTo(connectOptions)
                .using(input.vertx())
                .build();
        return corePool;
    }

    @Produces
    @Singleton
    io.vertx.mutiny.sqlclient.Pool mutinyPool() {
        return io.vertx.mutiny.sqlclient.Pool.newInstance(corePool);
    }
}
