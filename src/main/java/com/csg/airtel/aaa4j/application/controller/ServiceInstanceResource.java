package com.csg.airtel.aaa4j.application.controller;

import com.csg.airtel.aaa4j.scripts.ServiceInstanceDataGenerator;
import com.csg.airtel.aaa4j.scripts.ServiceInstanceDataGenerator.GenerationResult;
import io.smallrye.mutiny.Uni;
import jakarta.inject.Inject;
import jakarta.ws.rs.*;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import org.jboss.logging.Logger;

import java.util.Map;

/**
 * REST API for SERVICE_INSTANCE and BUCKET_INSTANCE data generation.
 *
 * This controller provides endpoints to:
 * - Generate test data for SERVICE_INSTANCE and BUCKET_INSTANCE tables
 * - Each username from AAA_USER gets 3 SERVICE_INSTANCE records
 * - Each SERVICE_INSTANCE gets 2-5 BUCKET_INSTANCE records (one-to-many relationship)
 *
 * Data Generation Rules:
 * - SERVICE_INSTANCE.USERNAME = AAA_USER.USER_NAME
 * - SERVICE_INSTANCE.SERVICE_START_DATE: 5% future, 40% today, 55% yesterday or earlier
 * - SERVICE_INSTANCE.EXPIRY_DATE: 50% before today, 50% after today
 * - SERVICE_INSTANCE.PLAN_ID: Random from predefined list
 * - BUCKET_INSTANCE.INITIAL_BALANCE: > 9,999,999,999
 * - BUCKET_INSTANCE.TIME_WINDOW: 00-08, 00-24, 00-18, or 18-24
 * - BUCKET_INSTANCE.IS_UNLIMITED: 0 or 1
 */
@Path("/api/service-instance")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class ServiceInstanceResource {

    private static final Logger log = Logger.getLogger(ServiceInstanceResource.class);

    private final ServiceInstanceDataGenerator dataGenerator;

    @Inject
    public ServiceInstanceResource(ServiceInstanceDataGenerator dataGenerator) {
        this.dataGenerator = dataGenerator;
    }

    /**
     * Generate SERVICE_INSTANCE and BUCKET_INSTANCE data for all users in AAA_USER.
     *
     * POST /api/service-instance/generate
     *
     * This will:
     * 1. Fetch all usernames from AAA_USER table
     * 2. Create 3 SERVICE_INSTANCE records per username
     * 3. Create 2-5 BUCKET_INSTANCE records per SERVICE_INSTANCE
     *
     * Response:
     * {
     *   "serviceInstancesCreated": 300,
     *   "bucketInstancesCreated": 1050,
     *   "failed": 0,
     *   "durationMs": 5432,
     *   "durationFormatted": "5s"
     * }
     */
    @POST
    @Path("/generate")
    public Uni<Response> generateData() {
        log.info("Starting SERVICE_INSTANCE and BUCKET_INSTANCE data generation via API");

        return dataGenerator.generateData()
                .map(result -> Response.ok(toResponse(result)).build())
                .onFailure().recoverWithItem(e -> {
                    log.errorf(e, "Data generation failed: %s", e.getMessage());
                    return Response.serverError()
                            .entity(Map.of(
                                    "error", e.getMessage(),
                                    "message", "Failed to generate data. Ensure AAA_USER table exists and is populated."
                            ))
                            .build();
                });
    }

    /**
     * Get information about the data generation process.
     *
     * GET /api/service-instance/info
     *
     * Returns:
     * - Number of users in AAA_USER
     * - Expected number of SERVICE_INSTANCE records (users * 3)
     * - Expected number of BUCKET_INSTANCE records (services * 2-5)
     */
    @GET
    @Path("/info")
    public Uni<Response> getInfo() {
        return dataGenerator.generateData()
                .map(result -> Response.ok(Map.of(
                        "description", "SERVICE_INSTANCE and BUCKET_INSTANCE Data Generator",
                        "servicesPerUser", 3,
                        "bucketsPerService", "2-5 (random)",
                        "serviceStartDateDistribution", Map.of(
                                "future", "5%",
                                "today", "40%",
                                "past", "55%"
                        ),
                        "expiryDateDistribution", Map.of(
                                "expired", "50%",
                                "valid", "50%"
                        ),
                        "planIds", "100COMBO182-192",
                        "bucketInitialBalance", "> 9,999,999,999",
                        "timeWindows", "00-08, 00-24, 00-18, 18-24"
                )).build())
                .onFailure().recoverWithItem(e ->
                    Response.ok(Map.of(
                            "description", "SERVICE_INSTANCE and BUCKET_INSTANCE Data Generator",
                            "status", "Not yet executed"
                    )).build()
                );
    }

    private Map<String, Object> toResponse(GenerationResult result) {
        return Map.of(
                "serviceInstancesCreated", result.serviceInstancesCreated(),
                "bucketInstancesCreated", result.bucketInstancesCreated(),
                "failed", result.failed(),
                "durationMs", result.duration().toMillis(),
                "durationFormatted", formatDuration(result.duration())
        );
    }

    private String formatDuration(java.time.Duration duration) {
        long minutes = duration.toMinutes();
        long seconds = duration.toSecondsPart();

        if (minutes > 0) {
            return String.format("%dm %ds", minutes, seconds);
        } else {
            return String.format("%.1fs", duration.toMillis() / 1000.0);
        }
    }
}
