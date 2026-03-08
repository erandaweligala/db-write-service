package com.csg.airtel.aaa4j.domain.service;

import com.csg.airtel.aaa4j.application.aspect.LogDomainService;
import com.csg.airtel.aaa4j.application.listner.SyncEventProducer;
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
    final SyncEventProducer syncEventProducer;
    final Pool pool;

    @Inject
    public DBWriteService(DBWriteRepository dbWriteRepository,
                          DBOperationsService dbOperationsService,
                          SyncEventProducer syncEventProducer,
                          Pool pool) {
        this.dbWriteRepository = dbWriteRepository;
        this.dbOperationsService = dbOperationsService;
        this.syncEventProducer = syncEventProducer;
        this.pool = pool;
    }

    @LogDomainService
    public Uni<Void> processDbWriteRequest(DBWriteRequest dbWriteRequest) {
        return processDbWriteRequestAndSync(dbWriteRequest, true);
    }

    /**
     * Process a DB write request. If publishSync is true, the event will be
     * published to the remote site after successful local application.
     * Set publishSync=false for events received FROM the remote site.
     */
    Uni<Void> processDbWriteRequestAndSync(DBWriteRequest dbWriteRequest, boolean publishSync) {
        if ("UPDATE_EVENT".equalsIgnoreCase(dbWriteRequest.getEventType())) {
            return dbWriteRepository.update(
                            dbWriteRequest.getTableName(),
                            dbWriteRequest.getColumnValues(),
                            dbWriteRequest.getWhereConditions()
                    ).replaceWithVoid()
                    .chain(() -> processRelatedWrites(dbWriteRequest))
                    .chain(() -> publishSync
                            ? syncEventProducer.publishForSync(dbWriteRequest)
                            : Uni.createFrom().voidItem());
        }
        return Uni.createFrom().voidItem();
    }

    @LogDomainService
    public Uni<Void> processEvent(DBWriteRequest request) {
        return processEventAndSync(request, true);
    }

    /**
     * Process an event with optional sync publishing.
     * Called by SyncEventConsumer with publishSync=false to avoid re-publishing.
     */
    public Uni<Void> processEventAndSync(DBWriteRequest request, boolean publishSync) {

        if (request == null) {
            log.warn("Received null DBWriteRequest — skipping");
            return Uni.createFrom().voidItem();
        }

        String eventType = request.getEventType();

        if (eventType == null || eventType.isBlank()) {
            log.warnf("Received request with null/blank eventType for user: %s, table: %s — skipping",
                    request.getUserName(), request.getTableName());
            return Uni.createFrom().voidItem();
        }

        log.infof("Processing eventType=%s for user=%s on table=%s",
                eventType, request.getUserName(), request.getTableName());

        boolean hasRelatedWrites = request.getRelatedWrites() != null && !request.getRelatedWrites().isEmpty();

        if (hasRelatedWrites) {
            return pool.withTransaction(conn ->
                    processSingleWrite(conn, request)
                            .chain(() -> processRelatedWrites(conn, request))
            ).chain(() -> publishSync
                    ? syncEventProducer.publishForSync(request)
                    : Uni.createFrom().voidItem());
        }

        return processSingleWrite(request)
                .chain(() -> publishSync
                        ? syncEventProducer.publishForSync(request)
                        : Uni.createFrom().voidItem());
    }

    private Uni<Void> processSingleWrite(DBWriteRequest request) {
        return switch (request.getEventType().toUpperCase()) {
            case "CREATE", "BULK_CREATE" ->
                    dbWriteRepository.executeInsert(
                            request.getTableName(),
                            request.getColumnValues()
                    ).replaceWithVoid();

            case "UPDATE", "BULK_UPDATE" ->
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
                log.warnf("Unknown eventType: '%s' for user: %s — skipping",
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
                log.warnf("Unknown eventType: '%s' for user: %s — skipping",
                        request.getEventType(), request.getUserName());
                yield Uni.createFrom().voidItem();
            }
        };
    }

    private Uni<Void> processRelatedWrites(DBWriteRequest request) {
        if (request.getRelatedWrites() == null || request.getRelatedWrites().isEmpty()) {
            return Uni.createFrom().voidItem();
        }

        log.infof("Processing %d related writes for user=%s",
                request.getRelatedWrites().size(), request.getUserName());

        // Chain each related write sequentially — order matters (e.g. DELETE mac then INSERT mac)
        Uni<Void> chain = Uni.createFrom().voidItem();
        for (DBWriteRequest related : request.getRelatedWrites()) {
            chain = chain.chain(() -> {
                log.infof("Processing related write: eventType=%s, table=%s, user=%s",
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

        log.infof("Processing %d related writes for user=%s",
                request.getRelatedWrites().size(), request.getUserName());

        // Chain each related write sequentially within the same transaction connection
        Uni<Void> chain = Uni.createFrom().voidItem();
        for (DBWriteRequest related : request.getRelatedWrites()) {
            chain = chain.chain(() -> {
                log.infof("Processing related write: eventType=%s, table=%s, user=%s",
                        related.getEventType(), related.getTableName(), related.getUserName());
                return processSingleWrite(conn, related);
            });
        }
        return chain;
    }


}
