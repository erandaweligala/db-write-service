package com.csg.airtel.aaa4j.domain.service;

import com.csg.airtel.aaa4j.external.repository.DBWriteRepository;
import com.csg.airtel.aaa4j.infrastructure.DatabaseCircuitBreaker;
import com.csg.airtel.aaa4j.infrastructure.PerformanceMetrics;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;

@ApplicationScoped
public class DBOperationsService {

    private static final Logger log = Logger.getLogger(DBWriteRepository.class);

    final DatabaseCircuitBreaker circuitBreaker;
    final PerformanceMetrics metrics;

    @Inject
    DBWriteRepository dbWriteRepository;

    @Inject
    public DBOperationsService( DatabaseCircuitBreaker circuitBreaker, PerformanceMetrics metrics) {
        this.circuitBreaker = circuitBreaker;
        this.metrics = metrics;

    }

    public Uni<Integer> createOrDelete(String tableName,
                                       Map<String, Object> rowValues,
                                       String eventType,
                                       Map<String, Object> whereConditions) {
        // Circuit breaker check
        if (!circuitBreaker.allowRequest()) {
            log.warn("Circuit breaker is OPEN, rejecting database update request");
            metrics.recordDbUpdateFailure();
            return Uni.createFrom().failure(new RuntimeException("Circuit breaker is OPEN"));
        }

        if (log.isDebugEnabled()) {
            log.debugf("Operation=%s, table=%s", eventType, tableName);
        }

        Instant startTime = Instant.now();

        if (eventType.equalsIgnoreCase("DELETE")){
            if (whereConditions.isEmpty()) {
                log.warn("Delete operation rejected: WHERE conditions are required");
                metrics.recordDbUpdateFailure();
                return Uni.createFrom().failure(new IllegalArgumentException("WHERE conditions required"));
            }

            return dbWriteRepository.executeDelete(tableName, whereConditions)
                    .onItem().invoke(rowCount -> {
                        // Record success metrics
                        Duration duration = Duration.between(startTime, Instant.now());
                        metrics.recordDbUpdate();
                        metrics.recordDbUpdateDuration(duration);
                        circuitBreaker.recordSuccess();

                        if (log.isDebugEnabled()) {
                            log.debugf("Deleted %s: %d rows in %d ms", tableName, rowCount, duration.toMillis());
                        }
                    })
                    .onFailure().invoke(throwable -> {
                        // Record failure metrics
                        metrics.recordDbUpdateFailure();
                        circuitBreaker.recordFailure();
                        log.errorf(throwable, "Delete failed: %s", tableName);
                    });
        }


        return dbWriteRepository.executeInsert(tableName, rowValues)
                .onItem().invoke(rowCount -> {
                    // Record success metrics
                    Duration duration = Duration.between(startTime, Instant.now());
                    metrics.recordDbUpdate();
                    metrics.recordDbUpdateDuration(duration);
                    circuitBreaker.recordSuccess();

                    if (log.isDebugEnabled()) {
                        log.debugf("Inserted %s: %d rows in %d ms", tableName, rowCount, duration.toMillis());
                    }
                })
                .onFailure().invoke(throwable -> {
                    // Record failure metrics
                    metrics.recordDbUpdateFailure();
                    circuitBreaker.recordFailure();
                    log.errorf(throwable, "Insert failed: %s", tableName);
                });
    }



}
