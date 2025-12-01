package com.csg.airtel.aaa4j.domain.service;

import com.csg.airtel.aaa4j.application.aspect.LogDomainService;
import com.csg.airtel.aaa4j.domain.model.DBWriteRequest;
import com.csg.airtel.aaa4j.external.repository.DBWriteRepository;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import java.util.ArrayList;
import java.util.List;


@ApplicationScoped
public class DBWriteService {


    final DBWriteRepository dbWriteRepository;
    @Inject
    public DBWriteService(DBWriteRepository dbWriteRepository) {
        this.dbWriteRepository = dbWriteRepository;
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


}
