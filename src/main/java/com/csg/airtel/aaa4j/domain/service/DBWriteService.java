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

    /**
     * Process multiple DB write requests in a single batch transaction
     * Optimized for high throughput (1000+ TPS)
     *
     * @param requests List of DB write requests
     * @return Uni<Void> completing when all updates are done
     */
    @LogDomainService
    public Uni<Void> processBatchDbWriteRequests(List<DBWriteRequest> requests) {
        if (requests == null || requests.isEmpty()) {
            return Uni.createFrom().voidItem();
        }

        List<DBWriteRepository.UpdateOperation> operations = new ArrayList<>(requests.size());

        for (DBWriteRequest request : requests) {
            if ("UPDATE_EVENT".equalsIgnoreCase(request.getEventType())) {
                operations.add(new DBWriteRepository.UpdateOperation(
                        request.getTableName(),
                        request.getColumnValues(),
                        request.getWhereConditions()
                ));
            }
        }

        if (operations.isEmpty()) {
            return Uni.createFrom().voidItem();
        }

        return dbWriteRepository.batchUpdate(operations).replaceWithVoid();
    }

}
