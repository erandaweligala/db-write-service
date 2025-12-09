package com.csg.airtel.aaa4j.application.controller;

import com.csg.airtel.aaa4j.infrastructure.CsvExportUtil.CsvExportResult;
import com.csg.airtel.aaa4j.scripts.BulkInsertScript;
import com.csg.airtel.aaa4j.scripts.BulkInsertScript.BulkInsertResult;
import com.csg.airtel.aaa4j.scripts.BulkInsertScript.BulkInsertWithExportResult;
import io.smallrye.mutiny.Uni;
import jakarta.inject.Inject;
import jakarta.ws.rs.*;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import org.jboss.logging.Logger;

import java.util.Map;

/**
 * REST API for bulk insert operations with the USER schema.
 *
 * Schema Fields:
 * USER_ID, BANDWIDTH, BILLING, BILLING_ACCOUNT_REF, CIRCUIT_ID, CONCURRENCY,
 * CONTACT_EMAIL, CONTACT_NAME, CONTACT_NUMBER, CREATED_DATE, CUSTOM_TIMEOUT,
 * CYCLE_DATE, ENCRYPTION_METHOD, GROUP_ID, IDLE_TIMEOUT, IP_ALLOCATION, IP_POOL_NAME,
 * IPV4, IPV6, MAC_ADDRESS, NAS_PORT_TYPE, PASSWORD, REMOTE_ID, REQUEST_ID,
 * SESSION_TIMEOUT, STATUS, UPDATED_DATE, USER_NAME, VLAN_ID, NAS_IP_ADDRESS, SUBSCRIPTION
 *
 * Constraints:
 * - USER_ID: Primary Key
 * - USER_NAME: Unique Key
 * - REQUEST_ID: Unique Key
 * - MAC_ADDRESS: Unique
 * - STATUS: CHECK constraint (ACTIVE, SUSPENDED, INACTIVE)
 * - SUBSCRIPTION: CHECK constraint (PREPAID, POSTPAID, HYBRID)
 *
 * Password Distribution:
 * - MAC-based: 30%
 * - PAP: 30%
 * - CHAP: 40%
 */
@Path("/api/bulk-insert")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class BulkInsertResource {

    private static final Logger log = Logger.getLogger(BulkInsertResource.class);
    private static final String DEFAULT_TABLE = "USER_DATA";

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
     * Create the target table with USER schema
     *
     * POST /api/bulk-insert/setup?tableName=MY_TABLE
     *
     * Creates table with columns:
     * USER_ID (PK), BANDWIDTH, BILLING, BILLING_ACCOUNT_REF, CIRCUIT_ID, CONCURRENCY,
     * CONTACT_EMAIL, CONTACT_NAME, CONTACT_NUMBER, CREATED_DATE, CUSTOM_TIMEOUT,
     * CYCLE_DATE, ENCRYPTION_METHOD, GROUP_ID, IDLE_TIMEOUT, IP_ALLOCATION, IP_POOL_NAME,
     * IPV4, IPV6, MAC_ADDRESS (UNIQUE), NAS_PORT_TYPE, PASSWORD, REMOTE_ID, REQUEST_ID (UNIQUE),
     * SESSION_TIMEOUT, STATUS, UPDATED_DATE, USER_NAME (UNIQUE), VLAN_ID, NAS_IP_ADDRESS, SUBSCRIPTION
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
     * Create indexes for the target table
     *
     * POST /api/bulk-insert/indexes?tableName=MY_TABLE
     *
     * Creates indexes on:
     * - STATUS
     * - SUBSCRIPTION
     * - NAS_PORT_TYPE
     */
    @POST
    @Path("/indexes")
    public Uni<Response> createIndexes(@QueryParam("tableName") @DefaultValue(DEFAULT_TABLE) String tableName) {
        log.infof("Creating indexes for table: %s", tableName);

        return bulkInsertScript.createIndexes(tableName)
                .map(v -> Response.ok(Map.of(
                        "message", "Indexes created successfully",
                        "tableName", tableName
                )).build())
                .onFailure().recoverWithItem(e -> {
                    log.errorf(e, "Index creation failed");
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

    /**
     * Export existing data to CSV file with MD5 hashed CHAP passwords.
     *
     * GET /api/bulk-insert/export?tableName=MY_TABLE&outputDir=/tmp/exports
     *
     * CHAP passwords (those starting with "CHAP:") will have their values
     * hashed using MD5 before being written to the CSV file.
     * MAC and PAP passwords remain unchanged.
     */
    @GET
    @Path("/export")
    public Uni<Response> exportToCsv(
            @QueryParam("tableName") @DefaultValue(DEFAULT_TABLE) String tableName,
            @QueryParam("outputDir") String outputDir) {
        log.infof("Starting CSV export: table=%s, outputDir=%s", tableName, outputDir);

        return bulkInsertScript.exportToCsv(tableName, outputDir)
                .map(result -> Response.ok(toExportResponse(result)).build())
                .onFailure().recoverWithItem(e -> {
                    log.errorf(e, "CSV export failed");
                    return Response.serverError()
                            .entity(Map.of("error", e.getMessage()))
                            .build();
                });
    }

    /**
     * Execute bulk insert and then export data to CSV file.
     * CHAP passwords are hashed with MD5 in the exported CSV.
     *
     * POST /api/bulk-insert/with-export
     * Body: {
     *   "tableName": "MY_TABLE",
     *   "totalRecords": 100000,
     *   "batchSize": 1000,
     *   "outputDir": "/tmp/exports"
     * }
     */
    @POST
    @Path("/with-export")
    public Uni<Response> executeBulkInsertWithExport(BulkInsertWithExportRequest request) {
        String tableName = request.tableName() != null ? request.tableName() : DEFAULT_TABLE;
        int totalRecords = request.totalRecords() > 0 ? request.totalRecords() : 1_000_000;
        int batchSize = request.batchSize() > 0 ? request.batchSize() : 1000;
        String outputDir = request.outputDir();

        log.infof("Starting bulk insert with CSV export: table=%s, records=%d, batchSize=%d, outputDir=%s",
                tableName, totalRecords, batchSize, outputDir);

        return bulkInsertScript.executeBulkInsertWithCsvExport(tableName, totalRecords, batchSize, outputDir)
                .map(result -> Response.ok(toCombinedResponse(result)).build())
                .onFailure().recoverWithItem(e -> {
                    log.errorf(e, "Bulk insert with export failed");
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

    private Map<String, Object> toExportResponse(CsvExportResult result) {
        return Map.of(
                "filePath", result.filePath(),
                "recordCount", result.recordCount(),
                "durationMs", result.durationMs(),
                "recordsPerSecond", Math.round(result.recordsPerSecond())
        );
    }

    private Map<String, Object> toCombinedResponse(BulkInsertWithExportResult result) {
        return Map.of(
                "insert", toResponse(result.insertResult()),
                "export", toExportResponse(result.exportResult())
        );
    }

    // Request DTOs
    public record BulkInsertRequest(String tableName) {}

    public record CustomBulkInsertRequest(
            String tableName,
            int totalRecords,
            int batchSize,
            boolean useSingleRowInsert
    ) {}

    public record BulkInsertWithExportRequest(
            String tableName,
            int totalRecords,
            int batchSize,
            String outputDir
    ) {}
}
