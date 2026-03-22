package com.csg.airtel.aaa4j.domain.service;

import com.csg.airtel.aaa4j.application.aspect.LogDomainService;
import com.csg.airtel.aaa4j.application.common.LoggingUtil;
import com.csg.airtel.aaa4j.domain.model.DBWriteRequest;
import com.csg.airtel.aaa4j.external.repository.DBWriteRepository;
import io.smallrye.mutiny.Uni;
import io.vertx.mutiny.sqlclient.Pool;
import io.vertx.mutiny.sqlclient.SqlClient;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;


@ApplicationScoped
public class DBWriteService {

    private static final Logger log = Logger.getLogger(DBWriteService.class);
    final DBWriteRepository dbWriteRepository;
    final DBOperationsService dbOperationsService;
    final Pool pool;

    @Inject
    public DBWriteService(DBWriteRepository dbWriteRepository, DBOperationsService dbOperationsService, Pool pool) {
        this.dbWriteRepository = dbWriteRepository;
        this.dbOperationsService = dbOperationsService;
        this.pool = pool;
    }

    @LogDomainService
    public Uni<Void> processDbWriteRequest(DBWriteRequest dbWriteRequest) {
        return processEvent(dbWriteRequest);
    }


    @LogDomainService
    public Uni<Void> processEvent(DBWriteRequest request) {

        if (request == null) {
            LoggingUtil.logWarn(log, "processEvent", "Received null DBWriteRequest — skipping");
            return Uni.createFrom().voidItem();
        }

        String eventType = request.getEventType();

        if (eventType == null || eventType.isBlank()) {
            LoggingUtil.logWarn(log, "processEvent",
                    "Received request with null/blank eventType for user: %s, table: %s — skipping",
                    request.getUserName(), request.getTableName());
            return Uni.createFrom().voidItem();
        }

        LoggingUtil.logDebug(log, "processEvent", "Processing eventType=%s for user=%s on table=%s",
                eventType, request.getUserName(), request.getTableName());

        boolean hasRelatedWrites = request.getRelatedWrites() != null && !request.getRelatedWrites().isEmpty();

        if (hasRelatedWrites) {
            return pool.withTransaction(conn ->
                    processSingleWriteWithCount(conn, request)
                            .chain(rowCount -> {
                                if (rowCount > 0) {
                                    return processRelatedWrites(conn, request);
                                } else {
                                    LoggingUtil.logDebug(log, "processEvent",
                                            "Parent insert was duplicate (0 rows affected), " +
                                            "skipping related writes for user=%s, table=%s",
                                            request.getUserName(), request.getTableName());
                                    return Uni.createFrom().voidItem();
                                }
                            })
            );
        }

        return processSingleWrite(request);
    }

    private Uni<Void> processSingleWrite(DBWriteRequest request) {
        return switch (request.getEventType().toUpperCase()) {
            case "CREATE", "BULK_CREATE" ->
                    dbWriteRepository.executeInsert(
                            request.getTableName(),
                            request.getColumnValues()
                    ).replaceWithVoid();

            case "UPDATE", "BULK_UPDATE","UPDATE_EVENT" ->
                    dbWriteRepository.update(
                            request.getTableName(),
                            request.getColumnValues(),
                            request.getWhereConditions()
                    ).replaceWithVoid();

            case "DELETE" ->
                    dbWriteRepository.executeDelete(
                            request.getTableName(),
                            request.getWhereConditions()
                    ).replaceWithVoid();

            default -> {
                LoggingUtil.logWarn(log, "processSingleWrite", "Unknown eventType: '%s' for user: %s — skipping",
                        request.getEventType(), request.getUserName());
                yield Uni.createFrom().voidItem();
            }
        };
    }

    private Uni<Void> processSingleWrite(SqlClient conn, DBWriteRequest request) {
        return switch (request.getEventType().toUpperCase()) {
            case "CREATE", "BULK_CREATE" ->
                    dbWriteRepository.executeInsert(
                            conn,
                            request.getTableName(),
                            request.getColumnValues()
                    ).replaceWithVoid();

            case "UPDATE", "BULK_UPDATE" ->
                    dbWriteRepository.update(
                            conn,
                            request.getTableName(),
                            request.getColumnValues(),
                            request.getWhereConditions()
                    ).replaceWithVoid();

            case "DELETE" ->
                    dbWriteRepository.executeDelete(
                            conn,
                            request.getTableName(),
                            request.getWhereConditions()
                    ).replaceWithVoid();

            default -> {
                LoggingUtil.logWarn(log, "processSingleWrite", "Unknown eventType: '%s' for user: %s — skipping",
                        request.getEventType(), request.getUserName());
                yield Uni.createFrom().voidItem();
            }
        };
    }

    /**
     * Same as processSingleWrite(SqlClient, DBWriteRequest) but preserves the
     * row count so the caller can decide whether to proceed with related writes.
     * A return value of 0 means the row was a duplicate and was skipped.
     */
    private Uni<Integer> processSingleWriteWithCount(SqlClient conn, DBWriteRequest request) {
        return switch (request.getEventType().toUpperCase()) {
            case "CREATE", "BULK_CREATE" ->
                    dbWriteRepository.executeInsert(
                            conn,
                            request.getTableName(),
                            request.getColumnValues()
                    );

            case "UPDATE", "BULK_UPDATE" ->
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

            default -> {
                LoggingUtil.logWarn(log, "processSingleWriteWithCount", "Unknown eventType: '%s' for user: %s — skipping",
                        request.getEventType(), request.getUserName());
                yield Uni.createFrom().item(0);
            }
        };
    }

    private Uni<Void> processRelatedWrites(DBWriteRequest request) {
        if (request.getRelatedWrites() == null || request.getRelatedWrites().isEmpty()) {
            return Uni.createFrom().voidItem();
        }

        LoggingUtil.logDebug(log, "processRelatedWrites", "Processing %d related writes for user=%s",
                request.getRelatedWrites().size(), request.getUserName());

        Uni<Void> chain = Uni.createFrom().voidItem();
        for (DBWriteRequest related : request.getRelatedWrites()) {
            chain = chain.chain(() -> {
                LoggingUtil.logDebug(log, "processRelatedWrites",
                        "Processing related write: eventType=%s, table=%s, user=%s",
                        related.getEventType(), related.getTableName(), related.getUserName());
                return processSingleWrite(related);
            });
        }
        return chain;
    }

    private Uni<Void> processRelatedWrites(SqlClient conn, DBWriteRequest request) {
        if (request.getRelatedWrites() == null || request.getRelatedWrites().isEmpty()) {
            return Uni.createFrom().voidItem();
        }

        LoggingUtil.logDebug(log, "processRelatedWrites", "Processing %d related writes for user=%s",
                request.getRelatedWrites().size(), request.getUserName());

        Uni<Void> chain = Uni.createFrom().voidItem();
        for (DBWriteRequest related : request.getRelatedWrites()) {
            chain = chain.chain(() -> {
                LoggingUtil.logDebug(log, "processRelatedWrites",
                        "Processing related write: eventType=%s, table=%s, user=%s",
                        related.getEventType(), related.getTableName(), related.getUserName());
                return processSingleWrite(conn, related);
            });
        }
        return chain;
    }
}