package com.csg.airtel.aaa4j.application.controller;

import com.csg.airtel.aaa4j.scripts.BulkInsertScript;
import com.csg.airtel.aaa4j.scripts.BulkInsertScript.BulkInsertResult;
import io.smallrye.mutiny.Uni;
import jakarta.inject.Inject;
import jakarta.ws.rs.*;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import org.jboss.logging.Logger;

import java.util.Map;

/**
 * REST API for executing bulk database inserts.
 *
 * Endpoints:
 * - POST /api/bulk-insert           - Run bulk insert with default 1M records
 * - POST /api/bulk-insert/custom    - Run bulk insert with custom parameters
 * - POST /api/bulk-insert/setup     - Create the target table
 * - DELETE /api/bulk-insert/cleanup - Truncate the target table
 * - GET /api/bulk-insert/count      - Get current record count
 */
@Path("/api/bulk-insert")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class BulkInsertResource {

    private static final Logger log = Logger.getLogger(BulkInsertResource.class);
    private static final String DEFAULT_TABLE = "BULK_TEST_DATA";

    private final BulkInsertScript bulkInsertScript;

    @Inject
    public BulkInsertResource(BulkInsertScript bulkInsertScript) {
        this.bulkInsertScript = bulkInsertScript;
    }

    /**
     * Execute bulk insert with default parameters (1,000,000 records)
     *
     * POST /api/bulk-insert
     * Body: { "tableName": "BULK_TEST_DATA" } (optional)
     */
    @POST
    public Uni<Response> executeBulkInsert(BulkInsertRequest request) {
        String tableName = request != null && request.tableName() != null
                ? request.tableName()
                : DEFAULT_TABLE;

        log.infof("Starting bulk insert via API: table=%s", tableName);

        return bulkInsertScript.executeBulkInsert(tableName)
                .map(result -> Response.ok(toResponse(result)).build())
                .onFailure().recoverWithItem(e -> {
                    log.errorf(e, "Bulk insert failed");
                    return Response.serverError()
                            .entity(Map.of("error", e.getMessage()))
                            .build();
                });
    }

    /**
     * Execute bulk insert with custom parameters
     *
     * POST /api/bulk-insert/custom
     * Body: {
     *   "tableName": "MY_TABLE",
     *   "totalRecords": 500000,
     *   "batchSize": 500,
     *   "useSingleRowInsert": false
     * }
     */
    @POST
    @Path("/custom")
    public Uni<Response> executeBulkInsertCustom(CustomBulkInsertRequest request) {
        String tableName = request.tableName() != null ? request.tableName() : DEFAULT_TABLE;
        int totalRecords = request.totalRecords() > 0 ? request.totalRecords() : 1_000_000;
        int batchSize = request.batchSize() > 0 ? request.batchSize() : 1000;
        boolean useSingleRow = request.useSingleRowInsert();

        log.infof("Starting custom bulk insert via API: table=%s, records=%d, batchSize=%d, singleRow=%s",
                tableName, totalRecords, batchSize, useSingleRow);

        Uni<BulkInsertResult> insertUni = useSingleRow
                ? bulkInsertScript.executeBulkInsertSingleRow(tableName, totalRecords, batchSize)
                : bulkInsertScript.executeBulkInsert(tableName, totalRecords, batchSize);

        return insertUni
                .map(result -> Response.ok(toResponse(result)).build())
                .onFailure().recoverWithItem(e -> {
                    log.errorf(e, "Custom bulk insert failed");
                    return Response.serverError()
                            .entity(Map.of("error", e.getMessage()))
                            .build();
                });
    }

    /**
     * Create the target table
     *
     * POST /api/bulk-insert/setup?tableName=MY_TABLE
     */
    @POST
    @Path("/setup")
    public Uni<Response> setupTable(@QueryParam("tableName") @DefaultValue(DEFAULT_TABLE) String tableName) {
        log.infof("Setting up table: %s", tableName);

        return bulkInsertScript.createTableIfNotExists(tableName)
                .map(v -> Response.ok(Map.of(
                        "message", "Table setup completed",
                        "tableName", tableName
                )).build())
                .onFailure().recoverWithItem(e -> {
                    log.errorf(e, "Table setup failed");
                    return Response.serverError()
                            .entity(Map.of("error", e.getMessage()))
                            .build();
                });
    }

    /**
     * Truncate the target table
     *
     * DELETE /api/bulk-insert/cleanup?tableName=MY_TABLE
     */
    @DELETE
    @Path("/cleanup")
    public Uni<Response> cleanupTable(@QueryParam("tableName") @DefaultValue(DEFAULT_TABLE) String tableName) {
        log.infof("Cleaning up table: %s", tableName);

        return bulkInsertScript.truncateTable(tableName)
                .map(v -> Response.ok(Map.of(
                        "message", "Table truncated",
                        "tableName", tableName
                )).build())
                .onFailure().recoverWithItem(e -> {
                    log.errorf(e, "Table cleanup failed");
                    return Response.serverError()
                            .entity(Map.of("error", e.getMessage()))
                            .build();
                });
    }

    /**
     * Get current record count
     *
     * GET /api/bulk-insert/count?tableName=MY_TABLE
     */
    @GET
    @Path("/count")
    public Uni<Response> getRecordCount(@QueryParam("tableName") @DefaultValue(DEFAULT_TABLE) String tableName) {
        return bulkInsertScript.getRecordCount(tableName)
                .map(count -> Response.ok(Map.of(
                        "tableName", tableName,
                        "recordCount", count
                )).build())
                .onFailure().recoverWithItem(e -> {
                    log.errorf(e, "Failed to get record count");
                    return Response.serverError()
                            .entity(Map.of("error", e.getMessage()))
                            .build();
                });
    }

    private Map<String, Object> toResponse(BulkInsertResult result) {
        return Map.of(
                "tableName", result.tableName(),
                "totalRequested", result.totalRequested(),
                "totalInserted", result.totalInserted(),
                "totalFailed", result.totalFailed(),
                "durationMs", result.duration().toMillis(),
                "durationFormatted", formatDuration(result.duration()),
                "recordsPerSecond", Math.round(result.recordsPerSecond())
        );
    }

    private String formatDuration(java.time.Duration duration) {
        long hours = duration.toHours();
        long minutes = duration.toMinutesPart();
        long seconds = duration.toSecondsPart();

        if (hours > 0) {
            return String.format("%dh %dm %ds", hours, minutes, seconds);
        } else if (minutes > 0) {
            return String.format("%dm %ds", minutes, seconds);
        } else {
            return String.format("%.1fs", duration.toMillis() / 1000.0);
        }
    }

    // Request DTOs
    public record BulkInsertRequest(String tableName) {}

    public record CustomBulkInsertRequest(
            String tableName,
            int totalRecords,
            int batchSize,
            boolean useSingleRowInsert
    ) {}
}
