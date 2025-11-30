package com.csg.airtel.aaa4j.application.controller;

import com.csg.airtel.aaa4j.domain.model.DBWriteRequest;
import com.csg.airtel.aaa4j.domain.service.DBWriteService;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.*;
import jakarta.ws.rs.core.MediaType;


import java.util.HashMap;
import java.util.Map;


@Path("/debug")
@ApplicationScoped
public class RedisDebugResource {
    @Inject
    DBWriteService dbWriteService;

    @POST
    @Path("/db-create-update")
    @Produces(MediaType.TEXT_PLAIN)
    @Consumes(MediaType.APPLICATION_JSON)
    public Uni<Map<String, Object>> interimUpdate(DBWriteRequest request) {

        return dbWriteService
                .processDbWriteRequest(request)
                .onItem().transform(result -> {
                    Map<String, Object> res = new HashMap<>();
                    res.put("accounting_result", result);
                    return res;
                });
    }
}