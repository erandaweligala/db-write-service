package com.csg.airtel.aaa4j.application.config;

import com.csg.airtel.aaa4j.application.common.LoggingUtil;
import io.quarkus.reactive.mysql.client.MySQLPoolCreator;
import io.vertx.mysqlclient.MySQLConnectOptions;
import io.vertx.mysqlclient.MySQLPool;
import io.vertx.sqlclient.Pool;
import io.vertx.sqlclient.PoolOptions;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import org.jboss.logging.Logger;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Customises the Vert.x MySQL connection pool.
 * <p>
 * Produces a {@link io.vertx.mutiny.sqlclient.Pool} bean qualified with
 * {@code @Named("mysql")} so it can be injected alongside the Oracle pool
 * without ambiguity.
 * <p>
 * Mirrors {@link OraclePoolCustomizer} but targets the named datasource
 * {@code quarkus.datasource.mysql.*} and reads pool tuning from
 * {@link MySQLPoolConfig} (prefix {@code mysql-pool}).
 */
@Singleton
public class MySQLPoolCustomizer implements MySQLPoolCreator {

    private static final Logger log = Logger.getLogger(MySQLPoolCustomizer.class);

    private final MySQLPoolConfig poolConfig;

    /** Kept so the Mutiny producer below can wrap it. */
    private Pool corePool;

    @Inject
    public MySQLPoolCustomizer(MySQLPoolConfig poolConfig) {
        this.poolConfig = poolConfig;
    }

    // -----------------------------------------------------------------------
    // MySQLPoolCreator — called by Quarkus during boot
    // -----------------------------------------------------------------------

    @Override
    public Pool create(Input input) {

        List<MySQLConnectOptions> connectOptionsList = input.mySQLConnectOptionsList();
        connectOptionsList.forEach(opts -> {
            opts.setTcpKeepAlive(poolConfig.tcpKeepAlive());
            opts.setTcpNoDelay(poolConfig.tcpNoDelay());
            opts.setPreparedStatementCacheMaxSize(poolConfig.preparedStatementCacheMaxSize());
        });

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
                .setName("mysql-pool");

        LoggingUtil.logInfo(log, "create",
                "MySQL pool '%s' configured: hosts=%d, maxSize=%d, connectionTimeout=%dms, " +
                        "idleTimeout=%dms, maxLifetime=%dms, eventLoopSize=%d, tcpKeepAlive=%s, tcpNoDelay=%s",
                poolOptions.getName(),
                connectOptionsList.size(),
                poolConfig.maxSize(),
                poolConfig.connectionTimeout(),
                poolConfig.idleTimeout(),
                poolConfig.maxLifetime(),
                poolConfig.eventLoopSize(),
                poolConfig.tcpKeepAlive(),
                poolConfig.tcpNoDelay());

        // ✅ MySQLPool.pool() accepts List<MySQLConnectOptions> for multi-host support
        this.corePool = MySQLPool.pool(
                input.vertx(),
                connectOptionsList,
                poolOptions);

        return corePool;
    }

    // -----------------------------------------------------------------------
    // Produce a Mutiny wrapper so repositories can inject it by name
    // -----------------------------------------------------------------------

    /**
     * Exposes the MySQL pool as a Mutiny {@link io.vertx.mutiny.sqlclient.Pool}
     * qualified with {@code @Named("mysql")}.
     *
     * <p>Injection site example:
     * <pre>
     *   {@literal @}Inject {@literal @}Named("mysql")
     *   io.vertx.mutiny.sqlclient.Pool mysqlPool;
     * </pre>
     */
    @Produces
    @Singleton
    @MySQLClient          // ← replaces @Named("mysql")
    io.vertx.mutiny.sqlclient.Pool mutinyMySQLPool() {
        return io.vertx.mutiny.sqlclient.Pool.newInstance(corePool);
    }
}