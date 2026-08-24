package com.csg.airtel.aaa4j.application.config;

import com.csg.airtel.aaa4j.application.common.LoggingUtil;
import io.quarkus.reactive.oracle.client.OraclePoolCreator;
import io.vertx.oracleclient.OracleBuilder;
import io.vertx.oracleclient.OracleConnectOptions;
import io.vertx.sqlclient.Pool;
import io.vertx.sqlclient.PoolOptions;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.jboss.logging.Logger;

import java.util.concurrent.TimeUnit;

/**
 * Customizes the Oracle connection pool.
 * Applies configuration from {@link PoolConfig} to tune pool behavior.
 *
 * <p><b>This class only builds the pool — it must not expose it as a bean.</b>
 * Quarkus already registers {@code io.vertx.sqlclient.Pool} and
 * {@code io.vertx.mutiny.sqlclient.Pool} beans that wrap whatever
 * {@link #create(Input)} returns, and it calls {@code create} lazily, when the
 * datasource is first injected. A local {@code @Produces} method returning
 * {@code Pool.newInstance(field)} overrode those default beans and was invoked
 * before {@code create} had ever run, so the field was still {@code null} and
 * {@code newInstance(null)} handed every consumer a {@code null} pool — surfacing
 * as {@code NullPointerException: Cannot invoke "io.vertx.mutiny.sqlclient.Pool
 * .withTransaction(...)" because "this.pool" is null} on the first Kafka event.
 * Inject {@code io.vertx.mutiny.sqlclient.Pool} directly and let Quarkus produce it.
 */
@Singleton
public class OraclePoolCustomizer implements OraclePoolCreator {

    private static final Logger log = Logger.getLogger(OraclePoolCustomizer.class);

    private final PoolConfig poolConfig;

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

        LoggingUtil.logInfo(log, "create",
                "Oracle pool '%s' configured: maxSize=%d, connectionTimeout=%dms, idleTimeout=%dms, " +
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

        return OracleBuilder.pool()
                .with(poolOptions)
                .connectingTo(connectOptions)
                .using(input.vertx())
                .build();
    }
}
