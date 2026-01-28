package com.csg.airtel.aaa4j.domain.service;

import com.csg.airtel.aaa4j.application.aspect.LogDomainService;
import com.csg.airtel.aaa4j.domain.model.DBWriteRequest;
import com.csg.airtel.aaa4j.external.repository.DBWriteRepository;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;


@ApplicationScoped
public class DBWriteService {

    private static final Logger log = Logger.getLogger(DBWriteRepository.class);
    final DBWriteRepository dbWriteRepository;
    final DBOperationsService dbOperationsService;
    @Inject
    public DBWriteService(DBWriteRepository dbWriteRepository, DBOperationsService dbOperationsService) {
        this.dbWriteRepository = dbWriteRepository;
        this.dbOperationsService = dbOperationsService;
    }

    @LogDomainService
    public Uni<Void> processDbWriteRequest(DBWriteRequest dbWriteRequest) {
        if("UPDATE_EVENT".equalsIgnoreCase(dbWriteRequest.getEventType())) {
            return dbWriteRepository.update(
                    dbWriteRequest.getTableName(),
                    dbWriteRequest.getColumnValues(),
                    dbWriteRequest.getWhereConditions()
            ).replaceWithVoid();
        }
        return Uni.createFrom().voidItem();
    }

    @LogDomainService
    public Uni<Void> processEvent(DBWriteRequest dbWriteRequestGeneric) {

        switch (dbWriteRequestGeneric.getEventType()) {
            case "CREATE" :
            case "BULK_CREATE" :
                dbOperationsService.createOrDelete(
                        dbWriteRequestGeneric.getTableName(),
                        dbWriteRequestGeneric.getColumnValues(),
                        dbWriteRequestGeneric.getEventType(),
                        null);
                break;

            case "UPDATE" :
            case "BULK_UPDATE" :
                    dbWriteRepository.update(
                            dbWriteRequestGeneric.getTableName(),
                            dbWriteRequestGeneric.getColumnValues(),
                            dbWriteRequestGeneric.getWhereConditions());
                    break;

            case "DELETE" :
                dbOperationsService.createOrDelete(
                        dbWriteRequestGeneric.getTableName(),
                        dbWriteRequestGeneric.getColumnValues(),
                        dbWriteRequestGeneric.getEventType(),
                        dbWriteRequestGeneric.getWhereConditions());
                break;
            default:
                log.warn("Invalid event type");
        }

        return Uni.createFrom().voidItem();
    }


}
