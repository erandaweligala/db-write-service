package com.csg.airtel.aaa4j.domain.service;

import com.csg.airtel.aaa4j.application.aspect.LogDomainService;
import com.csg.airtel.aaa4j.domain.model.DBWriteRequest;
import com.csg.airtel.aaa4j.external.repository.DBWriteRepository;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.transaction.Transactional;
import org.jboss.logging.Logger;


@ApplicationScoped
public class DBWriteService {

    private static final Logger log = Logger.getLogger(DBWriteService.class);
    final DBWriteRepository dbWriteRepository;
    final DBOperationsService dbOperationsService;
    @Inject
    public DBWriteService(DBWriteRepository dbWriteRepository, DBOperationsService dbOperationsService) {
        this.dbWriteRepository = dbWriteRepository;
        this.dbOperationsService = dbOperationsService;
    }

    @LogDomainService
    public Uni<Void> processDbWriteRequest(DBWriteRequest dbWriteRequest) {
        if ("UPDATE_EVENT".equalsIgnoreCase(dbWriteRequest.getEventType())) {
            return dbWriteRepository.update(
                            dbWriteRequest.getTableName(),
                            dbWriteRequest.getColumnValues(),
                            dbWriteRequest.getWhereConditions()
                    ).replaceWithVoid()
                    .chain(() -> processRelatedWrites(dbWriteRequest)); // ADD THIS
        }
        return Uni.createFrom().voidItem();
    }


    @LogDomainService
    public Uni<Void> processEvent(DBWriteRequest request) {

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

       //todo  processSingleWrite = success but processRelatedWrites fail i need to rollback insert
        return processSingleWrite(request)
                .chain(() -> processRelatedWrites(request));
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


}
