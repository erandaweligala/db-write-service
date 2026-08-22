package com.csg.airtel.aaa4j.application.controller;

import com.csg.airtel.aaa4j.domain.service.ConnectivityMonitoringService;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Read-only view of database, Redis, and Kafka connectivity as tracked by
 * {@link ConnectivityMonitoringService}.
 *
 * <p>Grafana reads the same state from Prometheus ({@code dependency_up} and friends);
 * this endpoint is for humans and for probes that want a single yes/no answer without
 * parsing the metrics scrape.</p>
 */
@Path("/monitoring/connectivity")
@ApplicationScoped
public class ConnectivityResource {

    private final ConnectivityMonitoringService connectivityMonitoringService;

    public ConnectivityResource(ConnectivityMonitoringService connectivityMonitoringService) {
        this.connectivityMonitoringService = connectivityMonitoringService;
    }

    /**
     * Current connectivity state of every tracked dependency.
     * Returns 503 when at least one dependency is down, so uptime checks can key off the status code.
     */
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public Response status() {
        boolean allUp = connectivityMonitoringService.allUp();
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("status", allUp ? "UP" : "DOWN");
        body.put("dependencies", connectivityMonitoringService.snapshot());
        return Response
                .status(allUp ? Response.Status.OK : Response.Status.SERVICE_UNAVAILABLE)
                .entity(body)
                .build();
    }
}
