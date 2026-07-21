package com.csg.airtel.aaa4j.external.repository;

import com.csg.airtel.aaa4j.application.common.LoggingUtil;
import com.csg.airtel.aaa4j.domain.service.ExceptionMetricsService;
import com.csg.airtel.aaa4j.infrastructure.DatabaseCircuitBreaker;
import com.csg.airtel.aaa4j.infrastructure.PerformanceMetrics;
import io.smallrye.mutiny.Uni;
import io.vertx.mutiny.sqlclient.Pool;
import io.vertx.mutiny.sqlclient.SqlClient;
import io.vertx.mutiny.sqlclient.SqlResult;
import io.vertx.mutiny.sqlclient.Tuple;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Pattern;

@ApplicationScoped
public class DBWriteRepository {

    private static final Logger log = Logger.getLogger(DBWriteRepository.class);

    private static final Pattern TIMESTAMP_PATTERN = Pattern.compile("\\d{4}-\\d{2}-\\d{2}[T ]\\d{2}:\\d{2}:\\d{2}.*");
    private static final DateTimeFormatter ISO_FORMATTER = DateTimeFormatter.ISO_LOCAL_DATE_TIME;

    final Pool client;
    final DatabaseCircuitBreaker circuitBreaker;
    final PerformanceMetrics metrics;
    final ExceptionMetricsService exceptionMetrics;

    @Inject
    public DBWriteRepository(Pool client,
                             DatabaseCircuitBreaker circuitBreaker,
                             PerformanceMetrics metrics,
                             ExceptionMetricsService exceptionMetrics) {
        this.client = client;
        this.circuitBreaker = circuitBreaker;
        this.metrics = metrics;
        this.exceptionMetrics = exceptionMetrics;
    }

    // -----------------------------------------------------------------------
    // UPDATE
    // -----------------------------------------------------------------------

    public Uni<Integer> update(String tableName,
                               Map<String, Object> columnValues,
                               Map<String, Object> whereConditions) {
        return update(client, tableName, columnValues, whereConditions);
    }

    public Uni<Integer> update(SqlClient sqlClient,
                               String tableName,
                               Map<String, Object> columnValues,
                               Map<String, Object> whereConditions) {

        if (!circuitBreaker.allowRequest()) {
            LoggingUtil.logWarn(log, "update", "Circuit breaker is OPEN, rejecting database update request");
            metrics.recordDbUpdateFailure();
            return Uni.createFrom().failure(new RuntimeException("Circuit breaker is OPEN"));
        }

        if (whereConditions == null || whereConditions.isEmpty()) {
            LoggingUtil.logWarn(log, "update", "Update operation rejected: WHERE conditions are required");
            metrics.recordDbUpdateFailure();
            return Uni.createFrom().failure(new IllegalArgumentException("WHERE conditions required"));
        }

        if (log.isDebugEnabled()) {
            log.debugf("Update: table=%s, cols=%d, conditions=%d",
                    tableName, columnValues.size(), whereConditions.size());
        }

        long startNanos = System.nanoTime();

        return executeUpdate(sqlClient, tableName, columnValues, whereConditions)
                .onItem().invoke(rowCount -> {
                    long elapsedNanos = System.nanoTime() - startNanos;
                    metrics.recordDbUpdate();
                    metrics.recordDbUpdateDuration(Duration.ofNanos(elapsedNanos));
                    circuitBreaker.recordSuccess();
                    if (log.isDebugEnabled()) {
                        log.debugf("Updated %s: %d rows in %d ms", tableName, rowCount, elapsedNanos / 1_000_000L);
                    }
                })
                .onFailure().invoke(throwable -> {
                    metrics.recordDbUpdateFailure();
                    circuitBreaker.recordFailure();
                    exceptionMetrics.recordException(throwable,
                            ExceptionMetricsService.Layer.REPOSITORY,
                            ExceptionMetricsService.Source.ORACLE);
                    LoggingUtil.logError(log, "update", throwable, "Update failed: %s", tableName);
                });
    }

    private Uni<Integer> executeUpdate(SqlClient sqlClient,
                                       String tableName,
                                       Map<String, Object> columnValues,
                                       Map<String, Object> whereConditions) {

        int setCount = columnValues.size();
        int whereCount = whereConditions.size();
        int totalParams = setCount + whereCount;

        StringBuilder sql = new StringBuilder(64 + tableName.length() + (totalParams * 10));
        sql.append("UPDATE ").append(tableName).append(" SET ");

        Object[] values = new Object[totalParams];
        int idx = 0;

        Iterator<Map.Entry<String, Object>> setIter = columnValues.entrySet().iterator();
        while (setIter.hasNext()) {
            Map.Entry<String, Object> entry = setIter.next();
            sql.append(entry.getKey()).append(" = ?");
            values[idx++] = convertValue(entry.getValue());
            if (setIter.hasNext()) sql.append(", ");
        }

        sql.append(" WHERE ");
        Iterator<Map.Entry<String, Object>> whereIter = whereConditions.entrySet().iterator();
        while (whereIter.hasNext()) {
            Map.Entry<String, Object> entry = whereIter.next();
            sql.append(entry.getKey()).append(" = ?");
            values[idx++] = entry.getValue();
            if (whereIter.hasNext()) sql.append(" AND ");
        }

        String sqlStr = sql.toString();
        if (log.isDebugEnabled()) {
            log.debugf("SQL: %s", sqlStr);
        }

        return sqlClient.preparedQuery(sqlStr)
                .execute(Tuple.from(values))
                .map(SqlResult::rowCount)
                .onFailure().invoke(t -> LoggingUtil.logError(log, "executeUpdate", t, "Update query failed on table: %s", tableName));
    }


    public Uni<Integer> executeInsert(String tableName,
                                      Map<String, Object> columnValues) {
        return executeInsert(client, tableName, columnValues);
    }

    public Uni<Integer> executeInsert(SqlClient sqlClient,
                                      String tableName,
                                      Map<String, Object> columnValues) {


        if (!circuitBreaker.allowRequest()) {
            LoggingUtil.logWarn(log, "executeInsert", "Circuit breaker is OPEN, rejecting database insert request");
            metrics.recordDbUpdateFailure();
            return Uni.createFrom().failure(new RuntimeException("Circuit breaker is OPEN"));
        }

        if (columnValues == null || columnValues.isEmpty()) {
            LoggingUtil.logWarn(log, "executeInsert", "Insert operation rejected: column values are required");
            return Uni.createFrom().failure(new IllegalArgumentException("Column values required"));
        }

        long startNanos = System.nanoTime();

        int columnCount = columnValues.size();

        StringBuilder sql = new StringBuilder(64 + tableName.length() + (columnCount * 10));
        sql.append("INSERT INTO ").append(tableName).append(" (");

        Object[] values = new Object[columnCount];
        int idx = 0;

        Iterator<Map.Entry<String, Object>> colIter = columnValues.entrySet().iterator();
        while (colIter.hasNext()) {
            Map.Entry<String, Object> entry = colIter.next();
            sql.append(entry.getKey());
            values[idx++] = convertValue(entry.getValue());
            if (colIter.hasNext()) sql.append(", ");
        }

        sql.append(") VALUES (");
        for (int i = 0; i < columnCount; i++) {
            sql.append("?");
            if (i < columnCount - 1) sql.append(", ");
        }
        sql.append(")");

        String sqlStr = sql.toString();
        if (log.isDebugEnabled()) {
            log.debugf("SQL: %s", sqlStr);
        }

        return sqlClient.preparedQuery(sqlStr)
                .execute(Tuple.from(values))
                .map(SqlResult::rowCount)
                .onFailure(DBWriteRepository::isUniqueConstraintViolation)
                .recoverWithItem(throwable -> {
                    LoggingUtil.logDebug(log, "executeInsert", "Duplicate record skipped for table=%s (unique constraint): %s",
                            tableName, throwable.getMessage());
                    return 0;
                })
                .onItem().invoke(rowCount -> {
                    long elapsedNanos = System.nanoTime() - startNanos;
                    metrics.recordDbUpdate();
                    metrics.recordDbUpdateDuration(Duration.ofNanos(elapsedNanos));
                    circuitBreaker.recordSuccess();
                    if (log.isDebugEnabled()) {
                        log.debugf("Inserted into %s: %d rows in %d ms", tableName, rowCount, elapsedNanos / 1_000_000L);
                    }
                })
                .onFailure().invoke(throwable -> {
                    // Only non-duplicate failures reach here
                    metrics.recordDbUpdateFailure();
                    circuitBreaker.recordFailure();
                    exceptionMetrics.recordException(throwable,
                            ExceptionMetricsService.Layer.REPOSITORY,
                            ExceptionMetricsService.Source.ORACLE);
                    LoggingUtil.logError(log, "executeInsert", throwable, "Insert failed on table: %s", tableName);
                });
    }


    public Uni<Integer> executeDelete(String tableName,
                                      Map<String, Object> whereConditions) {
        return executeDelete(client, tableName, whereConditions);
    }

    public Uni<Integer> executeDelete(SqlClient sqlClient,
                                      String tableName,
                                      Map<String, Object> whereConditions) {

        if (!circuitBreaker.allowRequest()) {
            LoggingUtil.logWarn(log, "executeDelete", "Circuit breaker is OPEN, rejecting database delete request");
            metrics.recordDbUpdateFailure();
            return Uni.createFrom().failure(new RuntimeException("Circuit breaker is OPEN"));
        }

        if (whereConditions == null || whereConditions.isEmpty()) {
            LoggingUtil.logWarn(log, "executeDelete", "Delete operation rejected: WHERE conditions are required");
            return Uni.createFrom().failure(new IllegalArgumentException("WHERE conditions required for DELETE"));
        }

        long startNanos = System.nanoTime();

        int whereCount = whereConditions.size();

        StringBuilder sql = new StringBuilder(32 + tableName.length() + (whereCount * 10));
        sql.append("DELETE FROM ").append(tableName).append(" WHERE ");

        Object[] values = new Object[whereCount];
        int idx = 0;

        Iterator<Map.Entry<String, Object>> whereIter = whereConditions.entrySet().iterator();
        while (whereIter.hasNext()) {
            Map.Entry<String, Object> entry = whereIter.next();
            sql.append(entry.getKey()).append(" = ?");
            values[idx++] = entry.getValue();
            if (whereIter.hasNext()) sql.append(" AND ");
        }

        String sqlStr = sql.toString();
        if (log.isDebugEnabled()) {
            log.debugf("SQL: %s", sqlStr);
        }

        return sqlClient.preparedQuery(sqlStr)
                .execute(Tuple.from(values))
                .map(SqlResult::rowCount)
                .onItem().invoke(rowCount -> {
                    long elapsedNanos = System.nanoTime() - startNanos;
                    metrics.recordDbUpdate();
                    metrics.recordDbUpdateDuration(Duration.ofNanos(elapsedNanos));
                    circuitBreaker.recordSuccess();
                    if (log.isDebugEnabled()) {
                        log.debugf("Deleted from %s: %d rows in %d ms", tableName, rowCount, elapsedNanos / 1_000_000L);
                    }
                })
                .onFailure().invoke(throwable -> {
                    metrics.recordDbUpdateFailure();
                    circuitBreaker.recordFailure();
                    exceptionMetrics.recordException(throwable,
                            ExceptionMetricsService.Layer.REPOSITORY,
                            ExceptionMetricsService.Source.ORACLE);
                    LoggingUtil.logError(log, "executeDelete", throwable, "Delete failed on table: %s", tableName);
                });
    }

    public Uni<Integer> executeNativeQuery(String sql, List<Object> params) {
        return executeNativeQuery(client, sql, params);
    }

    public Uni<Integer> executeNativeQuery(SqlClient sqlClient,
                                           String sql,
                                           List<Object> params) {

        if (!circuitBreaker.allowRequest()) {
            LoggingUtil.logWarn(log, "executeNativeQuery",
                    " Circuit breaker OPEN — rejecting native query");
            metrics.recordDbUpdateFailure();
            return Uni.createFrom().failure(new RuntimeException("Circuit breaker is OPEN"));
        }

        if (sql == null || sql.isBlank()) {
            LoggingUtil.logWarn(log, "executeNativeQuery",
                    " Native query rejected — SQL is null or blank");
            return Uni.createFrom().failure(
                    new IllegalArgumentException("nativeQuery SQL must not be blank"));
        }

        // params may be null or empty — some queries have no bind variables
        List<Object> safeParams = (params != null) ? params : java.util.Collections.emptyList();

        // Run each param through convertValue so date strings become LocalDateTime/LocalDate
        // consistently with INSERT/UPDATE/DELETE behaviour
        Object[] boundValues = safeParams.stream()
                .map(this::convertValue)
                .toArray();

        if (log.isDebugEnabled()) {
            log.debugf(" Native query: params=%d, sql=%s", boundValues.length, sql);
        }

        Instant startTime = Instant.now();

        return sqlClient.preparedQuery(sql)
                .execute(boundValues.length > 0 ? Tuple.from(boundValues) : Tuple.tuple())
                .map(SqlResult::rowCount)
                .onItem().invoke(rowCount -> {
                    Duration duration = Duration.between(startTime, Instant.now());
                    metrics.recordDbUpdate();
                    metrics.recordDbUpdateDuration(duration);
                    circuitBreaker.recordSuccess();
                    if (log.isDebugEnabled()) {
                        log.debugf(" Native query affected %d rows in %d ms",
                                Optional.ofNullable(rowCount), duration.toMillis());
                    }
                })
                .onFailure().invoke(t -> {
                    metrics.recordDbUpdateFailure();
                    circuitBreaker.recordFailure();
                    exceptionMetrics.recordException(t,
                            ExceptionMetricsService.Layer.REPOSITORY,
                            ExceptionMetricsService.Source.ORACLE);
                    LoggingUtil.logError(log, "executeNativeQuery", t,
                            "Native query failed");
                });
    }

    /**
     * Returns true if the throwable represents an Oracle unique constraint
     * violation (ORA-00001). Used to make inserts idempotent.
     */
    private static boolean isUniqueConstraintViolation(Throwable t) {
        if (t == null) return false;
        String msg = t.getMessage();
        return msg != null && (msg.contains("ORA-00001") || msg.contains("unique constraint"));
    }

    @SuppressWarnings("java:S1066")
    private Object convertValue(Object value) {
        if (value == null) {
            return null;
        }

        if (value instanceof String strValue && !strValue.isEmpty()) {
            if (strValue.length() >= 19 && TIMESTAMP_PATTERN.matcher(strValue).matches()) {
                try {
                    String isoFormat = strValue.replace(' ', 'T');
                    return LocalDateTime.parse(isoFormat, ISO_FORMATTER);
                } catch (DateTimeParseException e) {
                    if (log.isDebugEnabled()) {
                        log.debugf("Failed to parse timestamp string: %s", strValue);
                    }
                    return value;
                }
            }
        }

        return value;
    }
}
