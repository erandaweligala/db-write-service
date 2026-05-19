package com.csg.airtel.aaa4j.external.repository;

import com.csg.airtel.aaa4j.application.common.LoggingUtil;
import com.csg.airtel.aaa4j.application.config.MySQLClient;
import com.csg.airtel.aaa4j.infrastructure.DatabaseCircuitBreaker;
import com.csg.airtel.aaa4j.infrastructure.PerformanceMetrics;
import io.smallrye.mutiny.Uni;
import io.vertx.mutiny.sqlclient.Pool;
import io.vertx.mutiny.sqlclient.SqlClient;
import io.vertx.mutiny.sqlclient.SqlResult;
import io.vertx.mutiny.sqlclient.Tuple;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import org.jboss.logging.Logger;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.*;
import java.util.regex.Pattern;

/**
 * Reactive MySQL write repository for INSERT / UPDATE / DELETE operations.
 *
 * <p><b>Key differences from {@link DBWriteRepository} (Oracle):</b>
 * <ul>
 *   <li>Pool injected as {@code @Named("mysql")} — avoids ambiguity with the
 *       Oracle pool running in the same application context.</li>
 *   <li>Unique-constraint detection uses MySQL error code 1062
 *       ("Duplicate entry") instead of ORA-00001, and walks the full cause
 *       chain because the Vert.x MySQL client sometimes wraps
 *       {@code MySQLException} inside a generic {@code VertxException}.</li>
 *   <li>{@link #convertValue} maps String timestamps to {@link LocalDateTime}
 *       and date-only strings to {@link LocalDate}. It deliberately does NOT
 *       convert {@code "true"/"false"} strings to Boolean — VARCHAR columns
 *       like {@code action_log.status} must stay as strings; only actual Java
 *       {@code Boolean} objects (from JSON {@code true/false} literals) are
 *       passed through for {@code TINYINT(1)} columns like
 *       {@code actions.is_main_action}.</li>
 * </ul>
 *
 * <p><b>Confirmed compatible tables (snake_case column names):</b>
 * <ul>
 *   <li>{@code action_log} — id(AUTO_INCREMENT), activity_id, activity,
 *       status(VARCHAR), status_description, user, detailed_request,
 *       created_at(DATETIME), updated_at(DATETIME), url, action_id(INT),
 *       activity_type</li>
 *   <li>{@code actions} — id(AUTO_INCREMENT), name, description,
 *       is_main_action(TINYINT/boolean), type, component_id, main_action_id</li>
 *   <li>{@code menus} — id(AUTO_INCREMENT), name, display_name, parent_id</li>
 *   <li>Join tables — {@code action_to_tenants}(action_id, tenant_id),
 *       {@code menu_to_tenants}(menu_id, tenant_id)</li>
 * </ul>
 */
@ApplicationScoped
public class MySQLWriteRepository {

    private static final Logger log = Logger.getLogger(MySQLWriteRepository.class);

    // Matches "2024-01-15T10:30:00", "2024-01-15 10:30:00", "2024-01-15T10:30:00.123", etc.
    private static final Pattern TIMESTAMP_PATTERN =
            Pattern.compile("\\d{4}-\\d{2}-\\d{2}[T ]\\d{2}:\\d{2}:\\d{2}.*");

    // Anchored — matches exactly "2024-01-15" and nothing longer
    private static final Pattern DATE_ONLY_PATTERN =
            Pattern.compile("^\\d{4}-\\d{2}-\\d{2}$");

    private static final DateTimeFormatter ISO_FORMATTER  = DateTimeFormatter.ISO_LOCAL_DATE_TIME;
    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ISO_LOCAL_DATE;

    final Pool client;
    final DatabaseCircuitBreaker circuitBreaker;
    final PerformanceMetrics metrics;

    @Inject
    public MySQLWriteRepository(@MySQLClient Pool client,
                                DatabaseCircuitBreaker circuitBreaker,
                                PerformanceMetrics metrics) {
        this.client = client;
        this.circuitBreaker = circuitBreaker;
        this.metrics = metrics;
    }

    // =========================================================================
    // UPDATE
    // =========================================================================

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
            LoggingUtil.logWarn(log, "update",
                    "[MySQL] Circuit breaker OPEN — rejecting update on table: %s", tableName);
            metrics.recordDbUpdateFailure();
            return Uni.createFrom().failure(new RuntimeException("Circuit breaker is OPEN"));
        }

        if (whereConditions == null || whereConditions.isEmpty()) {
            LoggingUtil.logWarn(log, "update",
                    "[MySQL] Update rejected — WHERE conditions required for table: %s", tableName);
            metrics.recordDbUpdateFailure();
            return Uni.createFrom().failure(
                    new IllegalArgumentException("WHERE conditions required"));
        }

        if (columnValues == null || columnValues.isEmpty()) {
            LoggingUtil.logWarn(log, "update",
                    "[MySQL] Update rejected — no SET values provided for table: %s", tableName);
            metrics.recordDbUpdateFailure();
            return Uni.createFrom().failure(
                    new IllegalArgumentException("Column values required for UPDATE"));
        }

        if (log.isDebugEnabled()) {
            log.debugf("[MySQL] Update: table=%s, setCols=%d, whereCols=%d",
                    tableName, columnValues.size(), whereConditions.size());
        }

        Instant startTime = Instant.now();

        return executeUpdate(sqlClient, tableName, columnValues, whereConditions)
                .onItem().invoke(rowCount -> {
                    Duration duration = Duration.between(startTime, Instant.now());
                    metrics.recordDbUpdate();
                    metrics.recordDbUpdateDuration(duration);
                    circuitBreaker.recordSuccess();
                    if (log.isDebugEnabled()) {
                        log.debugf("[MySQL] Updated %s: %d rows in %d ms",
                                tableName, rowCount, duration.toMillis());
                    }
                })
                .onFailure().invoke(t -> {
                    metrics.recordDbUpdateFailure();
                    circuitBreaker.recordFailure();
                    LoggingUtil.logError(log, "update", t,
                            "[MySQL] Update failed on table: %s", tableName);
                });
    }

    private Uni<Integer> executeUpdate(SqlClient sqlClient,
                                       String tableName,
                                       Map<String, Object> columnValues,
                                       Map<String, Object> whereConditions) {

        int setCount   = columnValues.size();
        int whereCount = whereConditions.size();
        int total      = setCount + whereCount;

        StringBuilder sql = new StringBuilder(64 + tableName.length() + (total * 16));
        sql.append("UPDATE ").append(tableName).append(" SET ");

        Object[] values = new Object[total];
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
            values[idx++] = convertValue(entry.getValue());
            if (whereIter.hasNext()) sql.append(" AND ");
        }

        String sqlStr = sql.toString();
        if (log.isDebugEnabled()) log.debugf("[MySQL] SQL: %s", sqlStr);

        return sqlClient.preparedQuery(sqlStr)
                .execute(Tuple.from(values))
                .map(SqlResult::rowCount)
                .onFailure().invoke(t -> LoggingUtil.logError(log, "executeUpdate", t,
                        "[MySQL] Update query failed on table: %s", tableName));
    }

    // =========================================================================
    // INSERT
    // =========================================================================

    public Uni<Integer> executeInsert(String tableName,
                                      Map<String, Object> columnValues) {
        return executeInsert(client, tableName, columnValues);
    }

    public Uni<Integer> executeInsert(SqlClient sqlClient,
                                      String tableName,
                                      Map<String, Object> columnValues) {

        if (!circuitBreaker.allowRequest()) {
            LoggingUtil.logWarn(log, "executeInsert",
                    "[MySQL] Circuit breaker OPEN — rejecting insert on table: %s", tableName);
            metrics.recordDbUpdateFailure();
            return Uni.createFrom().failure(new RuntimeException("Circuit breaker is OPEN"));
        }

        if (columnValues == null || columnValues.isEmpty()) {
            LoggingUtil.logWarn(log, "executeInsert",
                    "[MySQL] Insert rejected — column values required for table: %s", tableName);
            return Uni.createFrom().failure(
                    new IllegalArgumentException("Column values required"));
        }

        Instant startTime = Instant.now();
        int columnCount   = columnValues.size();

        // Build column list and placeholder list separately for clarity
        StringBuilder cols = new StringBuilder(32 + (columnCount * 16));
        StringBuilder placeholders = new StringBuilder(16 + (columnCount * 3));
        Object[] values = new Object[columnCount];
        int idx = 0;

        Iterator<Map.Entry<String, Object>> colIter = columnValues.entrySet().iterator();
        while (colIter.hasNext()) {
            Map.Entry<String, Object> entry = colIter.next();
            cols.append(entry.getKey());
            placeholders.append("?");
            values[idx++] = convertValue(entry.getValue());
            if (colIter.hasNext()) {
                cols.append(", ");
                placeholders.append(", ");
            }
        }

        String sqlStr = "INSERT INTO " + tableName +
                " (" + cols + ") VALUES (" + placeholders + ")";

        if (log.isDebugEnabled()) log.debugf("[MySQL] SQL: %s", sqlStr);

        return sqlClient.preparedQuery(sqlStr)
                .execute(Tuple.from(values))
                .map(SqlResult::rowCount)
                // Silently skip MySQL error 1062 (duplicate entry) — idempotent behaviour
                .onFailure(MySQLWriteRepository::isUniqueConstraintViolation)
                .recoverWithItem(t -> {
                    LoggingUtil.logDebug(log, "executeInsert",
                            "[MySQL] Duplicate record skipped for table=%s: %s",
                            tableName, t.getMessage());
                    return 0;
                })
                .onItem().invoke(rowCount -> {
                    Duration duration = Duration.between(startTime, Instant.now());
                    metrics.recordDbUpdate();
                    metrics.recordDbUpdateDuration(duration);
                    circuitBreaker.recordSuccess();
                    if (log.isDebugEnabled()) {
                        log.debugf("[MySQL] Inserted into %s: %d rows in %d ms",
                                tableName, rowCount, duration.toMillis());
                    }
                })
                .onFailure().invoke(t -> {
                    // Only non-duplicate failures reach here
                    metrics.recordDbUpdateFailure();
                    circuitBreaker.recordFailure();
                    LoggingUtil.logError(log, "executeInsert", t,
                            "[MySQL] Insert failed on table: %s", tableName);
                });
    }

    // =========================================================================
    // DELETE
    // =========================================================================

    public Uni<Integer> executeDelete(String tableName,
                                      Map<String, Object> whereConditions) {
        return executeDelete(client, tableName, whereConditions);
    }

    public Uni<Integer> executeDelete(SqlClient sqlClient,
                                      String tableName,
                                      Map<String, Object> whereConditions) {

        if (!circuitBreaker.allowRequest()) {
            LoggingUtil.logWarn(log, "executeDelete",
                    "[MySQL] Circuit breaker OPEN — rejecting delete on table: %s", tableName);
            metrics.recordDbUpdateFailure();
            return Uni.createFrom().failure(new RuntimeException("Circuit breaker is OPEN"));
        }

        if (whereConditions == null || whereConditions.isEmpty()) {
            LoggingUtil.logWarn(log, "executeDelete",
                    "[MySQL] Delete rejected — WHERE conditions required for table: %s", tableName);
            return Uni.createFrom().failure(
                    new IllegalArgumentException("WHERE conditions required for DELETE"));
        }

        Instant startTime = Instant.now();
        int whereCount    = whereConditions.size();

        StringBuilder sql = new StringBuilder(32 + tableName.length() + (whereCount * 16));
        sql.append("DELETE FROM ").append(tableName).append(" WHERE ");

        Object[] values = new Object[whereCount];
        int idx = 0;

        Iterator<Map.Entry<String, Object>> whereIter = whereConditions.entrySet().iterator();
        while (whereIter.hasNext()) {
            Map.Entry<String, Object> entry = whereIter.next();
            sql.append(entry.getKey()).append(" = ?");
            values[idx++] = convertValue(entry.getValue());
            if (whereIter.hasNext()) sql.append(" AND ");
        }

        String sqlStr = sql.toString();
        if (log.isDebugEnabled()) log.debugf("[MySQL] SQL: %s", sqlStr);

        return sqlClient.preparedQuery(sqlStr)
                .execute(Tuple.from(values))
                .map(SqlResult::rowCount)
                .onItem().invoke(rowCount -> {
                    Duration duration = Duration.between(startTime, Instant.now());
                    metrics.recordDbUpdate();
                    metrics.recordDbUpdateDuration(duration);
                    circuitBreaker.recordSuccess();
                    if (log.isDebugEnabled()) {
                        log.debugf("[MySQL] Deleted from %s: %d rows in %d ms",
                                tableName, rowCount, duration.toMillis());
                    }
                })
                .onFailure().invoke(t -> {
                    metrics.recordDbUpdateFailure();
                    circuitBreaker.recordFailure();
                    LoggingUtil.logError(log, "executeDelete", t,
                            "[MySQL] Delete failed on table: %s", tableName);
                });
    }


    // =========================================================================
    // NATIVE QUERY
    // =========================================================================

    /**
     * Executes an arbitrary parameterised SQL statement as a prepared statement.
     *
     * <p>Use this for queries that cannot be expressed with the structured
     * {@code columnValues}/{@code whereConditions} maps — typically:
     * <ul>
     *   <li>JOIN-based bulk UPDATEs (e.g. expire users via a roles subquery)</li>
     *   <li>Upserts using {@code INSERT ... ON DUPLICATE KEY UPDATE}</li>
     *   <li>Conditional updates using MySQL functions
     *       ({@code TIMESTAMPDIFF}, {@code CURDATE}, {@code STR_TO_DATE}, etc.)</li>
     * </ul>
     *
     * <p><b>Security contract:</b> {@code sql} must contain only {@code ?}
     * placeholders — never string-interpolated values. The caller (UMS) is
     * responsible for ensuring this. The db-write-service binds {@code params}
     * positionally via a prepared statement, so no SQL injection is possible as
     * long as the contract is respected.
     *
     * <p>Example — expire inactive users:
     * <pre>
     * sql:
     *   UPDATE users JOIN user_to_roles ON users.id = user_to_roles.user_id
     *   SET users.status_id = ?
     *   WHERE users.status_id = ?
     *   AND TIMESTAMPDIFF(day,
     *       STR_TO_DATE(SUBSTRING_INDEX(users.last_login_datetime,\'.\', 1),\'%Y-%m-%d %H:%i:%s\'),
     *       CURDATE()) > ?
     *   AND user_to_roles.role_id IN (SELECT id FROM roles WHERE tenant_id = ?)
     *
     * params: [2, 1, 90, 5]
     * </pre>
     *
     * @param sql    full SQL with {@code ?} placeholders only — no interpolated values
     * @param params bind values in the same positional order as the {@code ?} placeholders
     * @return number of rows affected
     */
    public Uni<Integer> executeNativeQuery(String sql, List<Object> params) {
        return executeNativeQuery(client, sql, params);
    }

    public Uni<Integer> executeNativeQuery(SqlClient sqlClient,
                                           String sql,
                                           List<Object> params) {

        if (!circuitBreaker.allowRequest()) {
            LoggingUtil.logWarn(log, "executeNativeQuery",
                    "[MySQL] Circuit breaker OPEN — rejecting native query");
            metrics.recordDbUpdateFailure();
            return Uni.createFrom().failure(new RuntimeException("Circuit breaker is OPEN"));
        }

        if (sql == null || sql.isBlank()) {
            LoggingUtil.logWarn(log, "executeNativeQuery",
                    "[MySQL] Native query rejected — SQL is null or blank");
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
            log.debugf("[MySQL] Native query: params=%d, sql=%s", boundValues.length, sql);
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
                        log.debugf("[MySQL] Native query affected %d rows in %d ms",
                                Optional.ofNullable(rowCount), duration.toMillis());
                    }
                })
                .onFailure().invoke(t -> {
                    metrics.recordDbUpdateFailure();
                    circuitBreaker.recordFailure();
                    LoggingUtil.logError(log, "executeNativeQuery", t,
                            "[MySQL] Native query failed");
                });
    }

    // =========================================================================
    // Helpers
    // =========================================================================

    /**
     * Walks the full cause chain looking for MySQL error code 1062
     * ("Duplicate entry"). Walking is necessary because the Vert.x MySQL
     * client sometimes wraps {@code MySQLException} inside a
     * {@code VertxException}.
     */
    private static boolean isUniqueConstraintViolation(Throwable t) {
        Throwable current = t;
        while (current != null) {
            String msg = current.getMessage();
            if (msg != null && (msg.contains("1062") || msg.contains("Duplicate entry"))) {
                return true;
            }
            current = current.getCause();
        }
        return false;
    }

    /**
     * Converts values to the Java types the Vert.x MySQL reactive client
     * requires for correct JDBC bind-variable mapping.
     *
     * <p><b>Rules applied in order:</b>
     * <ol>
     *   <li>{@code null} → {@code null}</li>
     *   <li>Non-String types (Integer, Long, Boolean, LocalDateTime, …) →
     *       passed through unchanged. Jackson already produces the right
     *       Java type when deserialising the Kafka JSON payload:
     *       JSON {@code true/false} → Java {@code Boolean} (correct for
     *       {@code actions.is_main_action TINYINT(1)}),
     *       JSON numbers → Integer/Long (correct for id, action_id, parent_id).</li>
     *   <li>String length ≥ 19 matching TIMESTAMP_PATTERN →
     *       {@link LocalDateTime}. Handles both ISO ({@code "2024-01-15T10:30:00"})
     *       and SQL ({@code "2024-01-15 10:30:00"}) formats.
     *       Targets: {@code action_log.created_at}, {@code action_log.updated_at}.</li>
     *   <li>String length == 10 matching DATE_ONLY_PATTERN →
     *       {@link LocalDate}. MySQL {@code DATE} columns reject LocalDateTime.
     *       Targets: any DATE column without a time component.</li>
     *   <li>All other strings → unchanged (VARCHAR, TEXT, ENUM columns).
     *       String {@code "true"/"false"} is intentionally NOT converted —
     *       {@code action_log.status} is VARCHAR(255) and must stay as a string.</li>
     * </ol>
     */
    @SuppressWarnings("java:S1066")
    private Object convertValue(Object value) {
        if (value == null) return null;

        // Non-string: Integer, Long, Boolean, BigDecimal, LocalDateTime, etc.
        // Jackson already produced the right type from JSON — pass through.
        if (!(value instanceof String)) return value;

        String strValue = (String) value;
        if (strValue.isEmpty()) return strValue;

        int len = strValue.length();

        // DATETIME / TIMESTAMP — targets: created_at, updated_at in action_log
        if (len >= 19 && TIMESTAMP_PATTERN.matcher(strValue).matches()) {
            try {
                return LocalDateTime.parse(strValue.replace(' ', 'T'), ISO_FORMATTER);
            } catch (DateTimeParseException e) {
                if (log.isDebugEnabled()) {
                    log.debugf("[MySQL] Could not parse as DATETIME, trying DATE: %s", strValue);
                }
                // Fall through to DATE check
            }
        }

        // DATE only — anchored pattern prevents datetime strings from matching
        if (len == 10 && DATE_ONLY_PATTERN.matcher(strValue).matches()) {
            try {
                return LocalDate.parse(strValue, DATE_FORMATTER);
            } catch (DateTimeParseException e) {
                if (log.isDebugEnabled()) {
                    log.debugf("[MySQL] Could not parse as DATE, leaving as string: %s", strValue);
                }
            }
        }

        // VARCHAR / TEXT / ENUM — leave as-is
        // This covers: action_log.status, action_log.activity, actions.type,
        // actions.name, menus.name, menus.display_name, etc.
        return strValue;
    }
}