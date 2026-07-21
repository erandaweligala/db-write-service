package com.csg.airtel.aaa4j.domain.service;

import com.csg.airtel.aaa4j.application.common.LoggingUtil;
import com.csg.airtel.aaa4j.domain.model.DBWriteRequestUMS;
import com.csg.airtel.aaa4j.external.repository.DBWriteRepository;
import io.smallrye.mutiny.Uni;
import io.vertx.mutiny.sqlclient.Pool;
import io.vertx.mutiny.sqlclient.SqlClient;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

/**
 * Orchestrates MySQL write operations received from Kafka.
 *
 * <h3>Design: single dispatch method, always transactional</h3>
 *
 * <p>The previous design had three near-identical switch statements:
 * <ul>
 *   <li>{@code processSingleWrite(DBWriteRequestMySQL)} — pool level</li>
 *   <li>{@code processSingleWrite(SqlClient, DBWriteRequest)} — connection level</li>
 *   <li>{@code processSingleWriteWithCount(SqlClient, DBWriteRequest)} — connection level + count</li>
 * </ul>
 * Any new {@code eventType} had to be added in all three places. Forgetting one
 * caused a silent skip via the {@code default} branch — the worst kind of bug.
 *
 * <p>Now there is exactly <b>one</b> dispatch method:
 * {@link #dispatch(SqlClient, DBWriteRequestUMS)} — always takes a {@link SqlClient},
 * always returns the row count. Every execution path goes through
 * {@link Pool#withTransaction}, so:
 * <ul>
 *   <li>Single writes: transaction opens, one statement executes, commits.</li>
 *   <li>Parent + related writes: all share the same transaction — if the parent
 *       is a duplicate (0 rows), related writes are skipped and the transaction
 *       commits cleanly with no work done.</li>
 * </ul>
 * The overhead of a single-statement transaction in MySQL is one extra round
 * trip (BEGIN / COMMIT). At 1000 TPS this is negligible compared to the
 * correctness guarantee and the elimination of duplicate code.
 *
 * <p><b>Supported eventType values (case-insensitive):</b><br>
 * {@code CREATE / INSERT / BULK_CREATE} → INSERT<br>
 * {@code UPDATE / UPDATE_EVENT / BULK_UPDATE} → UPDATE<br>
 * {@code DELETE} → DELETE
 */
@ApplicationScoped
public class UMSDbWriteService {

    private static final Logger log = Logger.getLogger(UMSDbWriteService.class);

    final DBWriteRepository dbWriteRepository;
    final Pool pool;

    @Inject
    public UMSDbWriteService(DBWriteRepository dbWriteRepository,
                             Pool pool) {
        this.dbWriteRepository = dbWriteRepository;
        this.pool = pool;
    }

    // =========================================================================
    // Entry points (called by MySQLWriteConsumer)
    // =========================================================================

    public Uni<Void> processDbWriteRequest(DBWriteRequestUMS request) {
        return processEvent(request);
    }

    public Uni<Void> processEvent(DBWriteRequestUMS request) {

        if (request == null) {
            LoggingUtil.logWarn(log, "processEvent",
                    "[MySQL] Received null DBWriteRequest — skipping");
            return Uni.createFrom().voidItem();
        }

        String eventType = request.getEventType();
        String tableName = request.getTableName();

        if (eventType == null || eventType.isBlank()) {
            LoggingUtil.logWarn(log, "processEvent",
                    "[MySQL] Null/blank eventType for user: %s, table: %s — skipping",
                    request.getUserName(), tableName);
            return Uni.createFrom().voidItem();
        }

        // NATIVE_QUERY carries its own SQL — tableName is not required
        boolean isNativeQuery = "NATIVE_QUERY".equalsIgnoreCase(eventType);
        if (!isNativeQuery && (tableName == null || tableName.isBlank())) {
            LoggingUtil.logWarn(log, "processEvent",
                    "[MySQL] Null/blank tableName for user: %s, eventType: %s — skipping",
                    request.getUserName(), eventType);
            return Uni.createFrom().voidItem();
        }

        LoggingUtil.logDebug(log, "processEvent",
                "[MySQL] Processing eventType=%s for user=%s on table=%s",
                eventType, request.getUserName(), tableName);

        boolean hasRelatedWrites =
                request.getRelatedWrites() != null && !request.getRelatedWrites().isEmpty();

        return pool.withTransaction(conn ->
                dispatch(conn, request)
                        .chain(rowCount -> {
                            if (!hasRelatedWrites) {
                                // Single write — nothing more to do
                                return Uni.createFrom().voidItem();
                            }
                            if (rowCount > 0) {
                                // Parent succeeded — process children in same transaction
                                return processRelatedWrites(conn, request);
                            }
                            // Parent was a duplicate — skip children, commit empty transaction
                            LoggingUtil.logDebug(log, "processEvent",
                                    "[MySQL] Parent insert duplicate (0 rows) — " +
                                            "skipping %d related writes for user=%s, table=%s",
                                    request.getRelatedWrites().size(),
                                    request.getUserName(), tableName);
                            return Uni.createFrom().voidItem();
                        })
        );
    }

    // =========================================================================
    // Single dispatch — THE only switch statement in this class
    // =========================================================================

    /**
     * Dispatches a single {@link DBWriteRequestUMS} to the correct repository
     * method and returns the affected row count.
     *
     * <p>This is the <b>single source of truth</b> for eventType routing.
     * All execution paths (standalone writes and related writes) call this
     * method — there is no duplicated switch elsewhere in this class.
     *
     * <p>Return values:
     * <ul>
     *   <li>{@code > 0} — rows were affected (insert/update/delete succeeded)</li>
     *   <li>{@code 0} — duplicate insert was silently skipped</li>
     * </ul>
     */
    private Uni<Integer> dispatch(SqlClient conn, DBWriteRequestUMS request) {
        return switch (request.getEventType().toUpperCase()) {

            case "CREATE", "BULK_CREATE", "INSERT" ->
                    dbWriteRepository.executeInsert(
                            conn,
                            request.getTableName(),
                            request.getColumnValues()
                    );

            case "UPDATE", "BULK_UPDATE", "UPDATE_EVENT" ->
                    dbWriteRepository.update(
                            conn,
                            request.getTableName(),
                            request.getColumnValues(),
                            request.getWhereConditions()
                    );

            case "DELETE" ->
                    dbWriteRepository.executeDelete(
                            conn,
                            request.getTableName(),
                            request.getWhereConditions()
                    );

            // ------------------------------------------------------------------
            // NATIVE_QUERY — for complex DML that cannot be expressed with the
            // structured columnValues/whereConditions maps.
            // Examples: JOIN-based bulk UPDATEs, upserts, queries using MySQL
            // date functions (TIMESTAMPDIFF, CURDATE, STR_TO_DATE, etc.)
            //
            // Contract: nativeQuery must use ? placeholders only.
            //           queryParams must be in the same positional order.
            // ------------------------------------------------------------------
            case "NATIVE_QUERY" -> {
                if (request.getNativeQuery() == null || request.getNativeQuery().isBlank()) {
                    LoggingUtil.logWarn(log, "dispatch",
                            "[MySQL] NATIVE_QUERY received but nativeQuery field is " +
                                    "null/blank for user: %s — skipping", request.getUserName());
                    yield Uni.createFrom().item(0);
                }
                yield dbWriteRepository.executeNativeQuery(
                        conn,
                        request.getNativeQuery(),
                        request.getQueryParams()
                );
            }

            default -> {
                LoggingUtil.logWarn(log, "dispatch",
                        "[MySQL] Unknown eventType: '%s' for user: %s, table: %s — skipping",
                        request.getEventType(), request.getUserName(), request.getTableName());
                yield Uni.createFrom().item(0);
            }
        };
    }

    // =========================================================================
    // Related writes — always runs inside the caller's open transaction
    // =========================================================================

    /**
     * Executes each related write sequentially using the same {@link SqlClient}
     * (and therefore the same transaction) as the parent write.
     *
     * <p>Each related write goes through {@link #dispatch} — the same routing
     * logic, no special cases. A related write that is itself a duplicate is
     * silently skipped; other failures propagate up and roll back the entire
     * transaction including the parent.
     */
    private Uni<Void> processRelatedWrites(SqlClient conn, DBWriteRequestUMS request) {
        LoggingUtil.logDebug(log, "processRelatedWrites",
                "[MySQL] Processing %d related writes for user=%s, parentTable=%s",
                request.getRelatedWrites().size(),
                request.getUserName(),
                request.getTableName());

        Uni<Void> chain = Uni.createFrom().voidItem();
        for (DBWriteRequestUMS related : request.getRelatedWrites()) {
            chain = chain.chain(() -> {
                LoggingUtil.logDebug(log, "processRelatedWrites",
                        "[MySQL] Related write: eventType=%s, table=%s",
                        related.getEventType(), related.getTableName());
                return dispatch(conn, related).replaceWithVoid();
            });
        }
        return chain;
    }
}