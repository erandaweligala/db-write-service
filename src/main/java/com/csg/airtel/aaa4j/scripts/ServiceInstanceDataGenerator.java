package com.csg.airtel.aaa4j.scripts;

import io.quarkus.redis.datasource.ReactiveRedisDataSource;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.vertx.core.json.JsonArray;
import io.vertx.mutiny.sqlclient.Pool;
import io.vertx.mutiny.sqlclient.Row;
import io.vertx.mutiny.sqlclient.Tuple;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Data Generator for SERVICE_INSTANCE table only.
 *
 * Features:
 * - Fetches usernames from AAA_USER table
 * - Creates 2 SERVICE_INSTANCE records per username
 * - Generates realistic test data with specified distributions
 * - Batch processing for optimal performance using executeBatch to avoid Oracle bind variable limits
 * - Progress reporting and error handling
 */
@ApplicationScoped
public class ServiceInstanceDataGenerator {

    private static final Logger log = Logger.getLogger(ServiceInstanceDataGenerator.class);
    @Inject
     ReactiveRedisDataSource reactiveRedisDataSource;
    // Configuration constants - OPTIMIZED FOR HIGH THROUGHPUT
    private static final int SERVICES_PER_USER = 2;
    private static final int BATCH_SIZE = 5000; // Increased from 1000 for better throughput
    private static final int PROGRESS_INTERVAL = 20000; // Less frequent logging
    private static final int CONCURRENT_BATCHES = 4; // Increased from 1 for parallel processing

    // SERVICE_INSTANCE constants
    private static final String[] PLAN_IDS = {
        "100COMBO182", "100COMBO183", "100COMBO184", "100COMBO185",
        "100COMBO187", "100COMBO188", "100COMBO189", "100COMBO190",
        "100COMBO191", "100COMBO192"
    };

    private static final String[] PLAN_TYPES = {"PREPAID", "POSTPAID", "HYBRID"};
    private static final String[] STATUSES = {"Active", "Suspend", "Inactive"};
    private static final String[] BILLING_TYPES = {"MONTHLY", "QUARTERLY", "YEARLY", "USAGE_BASED"};

    private final Pool client;
    private final Random random = new Random();

    @Inject
    public ServiceInstanceDataGenerator(Pool client) {
        this.client = client;
    }

    /**
     * Main execution method - generates data for all users
     */

    public Uni<GenerationResult> generateData() {
        log.info("Starting SERVICE_INSTANCE data generation");
        Instant startTime = Instant.now();

        return fetchUsernames()
                .chain(usernames -> {
                    log.infof("Found %d users in AAA_USER table", usernames.size());
                    log.infof("Will create %d service instances (%d per user)",
                            usernames.size() * SERVICES_PER_USER, SERVICES_PER_USER);

                    return generateServiceInstances(usernames, startTime);
                });
    }

    /**
     * Fetch all usernames from AAA_USER table
     */
    //to
    private Uni<List<String>> fetchUsernames() {
        log.info("Fetching usernames from AAA_USER table...");

        return getUserData()
                .onItem()
                .transformToUni(usernames -> {
                    if (usernames == null || usernames.isEmpty()) {
                        return client.query("SELECT USER_NAME FROM AAA_USER ORDER BY USER_NAME")
                                .execute()
                                .onItem()
                                .transformToUni(rows -> {
                                    List<String> usernameList = new ArrayList<>();
                                    for (Row row : rows) {
                                        usernameList.add(row.getString("USER_NAME"));
                                    }
                                    return storeUserData(usernameList)
                                            .onFailure()
                                            .invoke(throwable -> {
                                                log.warnf("Unable to store usernames to cache");
                                            })
                                            .replaceWith(usernameList);
                                })
                                .onFailure()
                                .invoke(e -> log.errorf(e, "Failed to fetch usernames from AAA_USER: %s", e.getMessage()));
                    } else {
                        log.infof("load data from cache");
                        return Uni.createFrom().item(usernames);
                    }
                });
    }

    /**
     * Generate SERVICE_INSTANCE records for all users - OPTIMIZED
     */
    private Uni<GenerationResult> generateServiceInstances(List<String> usernames, Instant startTime) {
        AtomicInteger serviceCount = new AtomicInteger(0);
        AtomicInteger failedCount = new AtomicInteger(0);
        AtomicLong serviceIdCounter = new AtomicLong(System.currentTimeMillis() % 1000000);

        int totalServices = usernames.size() * SERVICES_PER_USER;
        int totalBatches = (totalServices + BATCH_SIZE - 1) / BATCH_SIZE;

        log.infof("Processing %d service instances in %d batches with %d concurrent workers",
                totalServices, totalBatches, CONCURRENT_BATCHES);

        // Stream processing: generate records on-the-fly instead of pre-creating all
        return Multi.createFrom().iterable(usernames)
                .onItem().transformToMulti(username ->
                    Multi.createFrom().range(0, SERVICES_PER_USER)
                        .map(i -> createServiceInstanceRecord(username, serviceIdCounter.incrementAndGet()))
                )
                .merge()
                .group().intoLists().of(BATCH_SIZE)
                .capDemandsTo(CONCURRENT_BATCHES)
                .onItem().transformToUniAndMerge(batch ->
                    insertServiceInstanceBatch(batch)
                        .onItem().invoke(() -> {
                            int services = serviceCount.addAndGet(batch.size());

                            if (services % PROGRESS_INTERVAL == 0 || services == totalServices) {
                                Duration elapsed = Duration.between(startTime, Instant.now());
                                double rps = services * 1000.0 / Math.max(1, elapsed.toMillis());
                                log.infof("Progress: %d/%d services (%.1f%%) | %.0f svc/s",
                                        services, totalServices, (services * 100.0 / totalServices), rps);
                            }
                        })
                        .onFailure().invoke(e -> {
                            failedCount.addAndGet(batch.size());
                            log.errorf(e, "Batch insert failed: %s", e.getMessage());
                        })
                        .onFailure().recoverWithItem(Collections.emptyList())
                )
                .collect().asList()
                .map(results -> {
                    Duration totalDuration = Duration.between(startTime, Instant.now());
                    return new GenerationResult(
                            serviceCount.get(),
                            failedCount.get(),
                            totalDuration
                    );
                })
                .onItem().invoke(result ->
                    log.infof("Data generation completed: %s", result)
                );
    }

    /**
     * Create a SERVICE_INSTANCE record with generated data
     */
    private ServiceInstanceRecord createServiceInstanceRecord(String username, long serviceId) {
        LocalDateTime now = LocalDateTime.now();
        LocalDateTime serviceStartDate = generateServiceStartDate();

        // Generate plan details
        String planId = PLAN_IDS[random.nextInt(PLAN_IDS.length)];
        String planName = "Plan " + planId;
        String planType = PLAN_TYPES[random.nextInt(PLAN_TYPES.length)];

        // Generate dates
        LocalDateTime expiryDate = generateExpiryDate(serviceStartDate);
        LocalDateTime cycleStartDate = serviceStartDate.withDayOfMonth(1);
        LocalDateTime cycleEndDate = cycleStartDate.plusMonths(1).minusDays(1);
        LocalDateTime nextCycleStartDate = cycleStartDate.plusMonths(1);

        return new ServiceInstanceRecord(
                serviceId,
                now,                                          // CREATED_AT
                expiryDate,                                   // EXPIRY_DATE
                1,                            // IS_GROUP (0 or 1)
                nextCycleStartDate,                           // NEXT_CYCLE_START_DATE
                planId,                                       // PLAN_ID
                planName,                                     // PLAN_NAME
                planType,                                     // PLAN_TYPE
                random.nextInt(2),                            // RECURRING_FLAG (0 or 1)
                "REQ-" + UUID.randomUUID().toString().replace("-", "").substring(0, 20), // REQUEST_ID
                cycleEndDate,                                 // CYCLE_END_DATE
                cycleStartDate,                               // CYCLE_START_DATE
                serviceStartDate,                             // SERVICE_START_DATE
                STATUSES[random.nextInt(STATUSES.length)],    // STATUS
                now,                                          // UPDATED_AT
                username,                                     // USERNAME
                BILLING_TYPES[random.nextInt(BILLING_TYPES.length)], // BILLING
                random.nextInt(28) + 1                        // CYCLE_DATE (1-28)
        );
    }

    /**
     * Generate SERVICE_START_DATE based on distribution:
     * - 5% future dates
     * - 40% today
     * - 55% yesterday or earlier
     */
    private LocalDateTime generateServiceStartDate() {
        int choice = random.nextInt(100);
        LocalDateTime now = LocalDateTime.now();

        if (choice < 5) {
            // 5% future (1-30 days ahead)
            return now.plusDays(random.nextInt(30) + 1);
        } else if (choice < 45) {
            // 40% today
            return now.minusHours(random.nextInt(24));
        } else {
            // 55% past (1-365 days ago)
            return now.minusDays(random.nextInt(365) + 1);
        }
    }

    /**
     * Generate EXPIRY_DATE - 50% before today, 50% after today
     */
    private LocalDateTime generateExpiryDate(LocalDateTime serviceStartDate) {
        if (random.nextBoolean()) {
            // 50% expired (before today)
            return serviceStartDate.plusDays(random.nextInt(90) + 1);
        } else {
            // 50% valid (future date)
            return serviceStartDate.plusDays(random.nextInt(365) + 90);
        }
    }

    /**
     * Insert a batch of SERVICE_INSTANCE records - OPTIMIZED
     * Uses executeBatch with individual INSERT statements to avoid Oracle INSERT ALL bind variable limitations
     */
    private Uni<List<Long>> insertServiceInstanceBatch(List<ServiceInstanceRecord> batch) {
        String sql = "INSERT INTO SERVICE_INSTANCE " +
                "(ID, CREATED_AT, EXPIRY_DATE, IS_GROUP, NEXT_CYCLE_START_DATE, " +
                "PLAN_ID, PLAN_NAME, PLAN_TYPE, RECURRING_FLAG, REQUEST_ID, " +
                "CYCLE_END_DATE, CYCLE_START_DATE, SERVICE_START_DATE, STATUS, UPDATED_AT, " +
                "USERNAME, BILLING, CYCLE_DATE) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

        List<Tuple> tuples = new ArrayList<>(batch.size());
        List<Long> serviceIds = new ArrayList<>(batch.size());

        for (ServiceInstanceRecord record : batch) {
            List<Object> values = new ArrayList<>(18);
            values.add(record.id);
            values.add(record.createdAt);
            values.add(record.expiryDate);
            values.add(record.isGroup);
            values.add(record.nextCycleStartDate);
            values.add(record.planId);
            values.add(record.planName);
            values.add(record.planType);
            values.add(record.recurringFlag);
            values.add(record.requestId);
            values.add(record.cycleEndDate);
            values.add(record.cycleStartDate);
            values.add(record.serviceStartDate);
            values.add(record.status);
            values.add(record.updatedAt);
            values.add(record.username);
            values.add(record.billing);
            values.add(record.cycleDate);

            tuples.add(Tuple.from(values));
            serviceIds.add(record.id);
        }

        return client.preparedQuery(sql)
                .executeBatch(tuples)
                .map(result -> serviceIds);

    }

    // Record classes for data structures
    private record ServiceInstanceRecord(
            long id,
            LocalDateTime createdAt,
            LocalDateTime expiryDate,
            int isGroup,
            LocalDateTime nextCycleStartDate,
            String planId,
            String planName,
            String planType,
            int recurringFlag,
            String requestId,
            LocalDateTime cycleEndDate,
            LocalDateTime cycleStartDate,
            LocalDateTime serviceStartDate,
            String status,
            LocalDateTime updatedAt,
            String username,
            String billing,
            int cycleDate
    ) {}

    /**
     * Result record for data generation
     */
    public record GenerationResult(
            int serviceInstancesCreated,
            int failed,
            Duration duration
    ) {
        @Override
        public String toString() {
            return String.format(
                    "GenerationResult{services=%d, failed=%d, duration=%s}",
                    serviceInstancesCreated, failed, formatDuration(duration)
            );
        }

        private static String formatDuration(Duration duration) {
            long minutes = duration.toMinutes();
            long seconds = duration.toSecondsPart();
            return String.format("%dm %ds", minutes, seconds);
        }
    }

    public Uni<Void> storeUserData(List<String> userNames) {
        final long startTime = log.isDebugEnabled() ? System.currentTimeMillis() : 0;
        if (log.isDebugEnabled()) {
            log.debugf("Storing %d usernames in cache", userNames.size());
        }

        // Convert List to JsonArray for proper Redis storage
        JsonArray jsonArray = new JsonArray(userNames);

        return reactiveRedisDataSource.value(String.class)
                .set("usernames", jsonArray.encode())
                .invoke(() -> {
                    if (log.isDebugEnabled()) {
                        log.debugf("Stored %d usernames in cache in %d ms",
                                userNames.size(),
                                (System.currentTimeMillis() - startTime));
                    }
                })
                .onFailure()
                .invoke(e -> log.errorf(e, "Failed to store usernames in cache: %s", e.getMessage()));
    }
    public Uni<List<String>> getUserData() {
        final long startTime = log.isDebugEnabled() ? System.currentTimeMillis() : 0;
        log.info("Retrieving user data from cache");

        return reactiveRedisDataSource.value(String.class)
                .get("usernames")
                .onItem().transform(value -> {
                    if (value == null) {
                        log.info("No user data found in cache, will fetch from database");
                        return Collections.<String>emptyList(); // Explicit type
                    }

                    try {
                        JsonArray jsonArray = new JsonArray(value);
                        List<String> userNames = new ArrayList<>();

                        for (int i = 0; i < jsonArray.size(); i++) {
                            userNames.add(jsonArray.getString(i));
                        }

                        if (log.isDebugEnabled()) {
                            log.debugf("Retrieved %d usernames from cache in %d ms",
                                    userNames.size(), (System.currentTimeMillis() - startTime));
                        }
                        log.infof("User data retrieved from cache: %d usernames", userNames.size());
                        return userNames;

                    } catch (Exception e) {
                        log.errorf(e, "Failed to parse cached user data: %s", e.getMessage());
                        return Collections.<String>emptyList(); // Explicit type
                    }
                })
                .onFailure().recoverWithItem(Collections.<String>emptyList()); // Explicit type
    }
}
