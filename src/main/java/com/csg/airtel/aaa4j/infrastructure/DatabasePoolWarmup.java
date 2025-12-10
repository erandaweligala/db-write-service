package com.csg.airtel.aaa4j.infrastructure;

import io.quarkus.runtime.StartupEvent;
import io.smallrye.mutiny.Uni;
import io.vertx.mutiny.sqlclient.Pool;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

import java.util.ArrayList;
import java.util.List;

/**
 * Database Connection Pool Warm-up
 * Pre-creates connections on application startup to avoid cold-start latency
 * Critical for achieving consistent 1000+ TPS from startup
 */
//@ApplicationScoped
public class DatabasePoolWarmup {
    private static final Logger log = Logger.getLogger(DatabasePoolWarmup.class);

    private static final int WARMUP_CONNECTIONS = 50;  // Warm up 50 connections
    private static final String WARMUP_QUERY = "SELECT 1 FROM DUAL";  // Oracle warmup query


    private final Pool dbClient;
    @Inject
    public DatabasePoolWarmup(Pool dbClient) {
        this.dbClient = dbClient;
    }

    void onStart(StartupEvent ev) {
        log.info("Starting database connection pool warm-up...");
        long startTime = System.currentTimeMillis();

        // Execute warmup queries in parallel
        List<Uni<Void>> warmupQueries = new ArrayList<>(WARMUP_CONNECTIONS);

        for (int i = 0; i < WARMUP_CONNECTIONS; i++) {
            Uni<Void> warmupQuery = dbClient.query(WARMUP_QUERY)
                    .execute()
                    .replaceWithVoid()
                    .onFailure().invoke(throwable ->
                            log.warnf("Warmup query failed: %s", throwable.getMessage()));
            warmupQueries.add(warmupQuery);
        }

        // Wait for all warmup queries to complete
        Uni.combine().all().unis(warmupQueries)
                .discardItems()
                .subscribe()
                .with(
                        success -> {
                            long duration = System.currentTimeMillis() - startTime;
                            log.infof("Database pool warm-up completed successfully in %d ms. " +
                                    "Pre-created %d connections.", duration, WARMUP_CONNECTIONS);
                        },
                        failure -> {
                            long duration = System.currentTimeMillis() - startTime;
                            log.errorf(failure, "Database pool warm-up encountered errors after %d ms", duration);
                        }
                );
    }
}
