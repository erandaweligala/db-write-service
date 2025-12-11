package com.csg.airtel.aaa4j.scripts;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
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
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Data Generator for SERVICE_INSTANCE and BUCKET_INSTANCE tables.
 *
 * Features:
 * - Fetches usernames from AAA_USER table
 * - Creates 3 SERVICE_INSTANCE records per username
 * - Creates multiple BUCKET_INSTANCE records per SERVICE_INSTANCE (one-to-many relationship)
 * - Generates realistic test data with specified distributions
 * - Batch processing for optimal performance
 * - Progress reporting and error handling
 */
@ApplicationScoped
public class ServiceInstanceDataGenerator {

    private static final Logger log = Logger.getLogger(ServiceInstanceDataGenerator.class);

    // Configuration constants
    private static final int SERVICES_PER_USER = 3;
    private static final int BATCH_SIZE = 400;
    private static final int BUCKET_BATCH_SIZE = 1000; // Larger batch size for bucket inserts
    private static final int PROGRESS_INTERVAL = 500;
    private static final int CONCURRENT_BATCHES = 1;
    private static final int BUCKET_CONCURRENT_BATCHES = 5; // Higher concurrency for bucket inserts

    // SERVICE_INSTANCE constants
    private static final String[] PLAN_IDS = {
        "100COMBO182", "100COMBO183", "100COMBO184", "100COMBO185",
        "100COMBO187", "100COMBO188", "100COMBO189", "100COMBO190",
        "100COMBO191", "100COMBO192"
    };

    private static final String[] PLAN_TYPES = {"PREPAID", "POSTPAID", "HYBRID"};
    private static final String[] STATUSES = {"ACTIVE", "SUSPENDED", "INACTIVE"};
    private static final String[] BILLING_TYPES = {"MONTHLY", "QUARTERLY", "YEARLY", "USAGE_BASED"};

    // BUCKET_INSTANCE constants
    private static final String[] TIME_WINDOWS = {"00-08", "00-24", "00-18", "18-24"};
    private static final String[] CONSUMTION_LIMIT = {"1", "7", "30"};
    private static final String[] BUCKET_TYPES = {"DATA", "COMBO"};
    private static final String[] RULES = {"100Mbps", "200Mbps", "300Kbps", "1Gbps", "100kbps"};

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
        log.info("Starting SERVICE_INSTANCE and BUCKET_INSTANCE data generation");
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
//SELECT USER_NAME FROM AAA_USER ORDER BY USER_NAME FETCH FIRST 10 ROWS ONLY
        return client.query("SELECT USER_NAME FROM AAA_USER ORDER BY USER_NAME FETCH FIRST 1000 ROWS ONLY")
                .execute()
                .map(rows -> {
                    List<String> usernames = new ArrayList<>();
                    for (Row row : rows) {
                        usernames.add(row.getString("USER_NAME"));
                    }
                    return usernames;
                })
                .onFailure().invoke(e ->
                    log.errorf(e, "Failed to fetch usernames from AAA_USER: %s", e.getMessage())
                );
    }

    /**
     * Generate SERVICE_INSTANCE records for all users
     */
    private Uni<GenerationResult> generateServiceInstances(List<String> usernames, Instant startTime) {
        AtomicInteger serviceCount = new AtomicInteger(0);
        AtomicInteger bucketCount = new AtomicInteger(0);
        AtomicInteger failedCount = new AtomicInteger(0);
        AtomicLong serviceIdCounter = new AtomicLong(System.currentTimeMillis() % 1000000);

        // Create list of service instance records to insert
        List<ServiceInstanceRecord> serviceRecords = new ArrayList<>();
        for (String username : usernames) {
            for (int i = 0; i < SERVICES_PER_USER; i++) {
                serviceRecords.add(createServiceInstanceRecord(username, serviceIdCounter.incrementAndGet()));
            }
        }

        int totalServices = serviceRecords.size();
        int totalBatches = (totalServices + BATCH_SIZE - 1) / BATCH_SIZE;

        log.infof("Processing %d service instances in %d batches", totalServices, totalBatches);

        return Multi.createFrom().iterable(serviceRecords)
                .group().intoLists().of(BATCH_SIZE)
                .capDemandsTo(CONCURRENT_BATCHES)
                .onItem().transformToUniAndMerge(batch ->
                    insertServiceInstanceBatch(batch)
                        .chain(serviceIds -> insertBucketInstancesForServices(batch, serviceIds))
                        .onItem().invoke(buckets -> {
                            int services = serviceCount.addAndGet(batch.size());
                            int totalBuckets = bucketCount.addAndGet(buckets);

                            if (services % PROGRESS_INTERVAL == 0 || services == totalServices) {
                                Duration elapsed = Duration.between(startTime, Instant.now());
                                double rps = services * 1000.0 / Math.max(1, elapsed.toMillis());
                                log.infof("Progress: %d/%d services (%.1f%%) | %d buckets | %.0f svc/s",
                                        services, totalServices, (services * 100.0 / totalServices),
                                        totalBuckets, rps);
                            }
                        })
                        .onFailure().invoke(e -> {
                            failedCount.addAndGet(batch.size());
                            log.errorf(e, "Batch insert failed: %s", e.getMessage());
                        })
                        .onFailure().recoverWithItem(0)
                )
                .collect().asList()
                .map(results -> {
                    Duration totalDuration = Duration.between(startTime, Instant.now());
                    return new GenerationResult(
                            serviceCount.get(),
                            bucketCount.get(),
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
     * Insert a batch of SERVICE_INSTANCE records
     */
    private Uni<List<Long>> insertServiceInstanceBatch(List<ServiceInstanceRecord> batch) {
        StringBuilder sql = new StringBuilder("INSERT ALL ");

        String columns = "(ID, CREATED_AT, EXPIRY_DATE, IS_GROUP, NEXT_CYCLE_START_DATE, " +
                "PLAN_ID, PLAN_NAME, PLAN_TYPE, RECURRING_FLAG, REQUEST_ID, " +
                "CYCLE_END_DATE, CYCLE_START_DATE, SERVICE_START_DATE, STATUS, UPDATED_AT, " +
                "USERNAME, BILLING, CYCLE_DATE)";

        String placeholders = "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

        List<Object> values = new ArrayList<>();
        List<Long> serviceIds = new ArrayList<>();

        for (ServiceInstanceRecord record : batch) {
            sql.append("INTO SERVICE_INSTANCE ").append(columns)
               .append(" VALUES ").append(placeholders).append(" ");

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

            serviceIds.add(record.id);
        }

        sql.append("SELECT * FROM DUAL");

        return client.preparedQuery(sql.toString())
                .execute(Tuple.from(values))
                .map(result -> serviceIds)
                .onFailure().retry().atMost(2);
    }

    /**
     * Insert BUCKET_INSTANCE records for each SERVICE_INSTANCE
     */
    private Uni<Integer> insertBucketInstancesForServices(
            List<ServiceInstanceRecord> serviceRecords,
            List<Long> serviceIds) {

        List<BucketInstanceRecord> bucketRecords = new ArrayList<>();
        AtomicLong serviceIdCounter = new AtomicLong(System.currentTimeMillis() % 1000000);
        for (int i = 0; i < serviceRecords.size(); i++) {
            long serviceId = serviceIds.get(i);
            // Generate 2-5 bucket instances per service
            int bucketCount = random.nextInt(4) + 2;

            for (int j = 0; j < bucketCount; j++) {
                bucketRecords.add(createBucketInstanceRecord(serviceId, j + 1,serviceIdCounter.incrementAndGet()));
            }
        }

        if (bucketRecords.isEmpty()) {
            return Uni.createFrom().item(0);
        }

        return insertBucketInstanceBatch(bucketRecords);
    }

    /**
     * Create a BUCKET_INSTANCE record with generated data
     */
    private BucketInstanceRecord createBucketInstanceRecord(long serviceId, int priority,long id) {
        String bucketId = "BUCKET-" + serviceId + "-" + priority;

        LocalDateTime now = LocalDateTime.now();
        LocalDateTime expiration = now.plusDays(random.nextInt(365) + 30);

        int isUnlimited = random.nextInt(10) < 2 ? 1 : 0; // 20% unlimited

        // If unlimited = 1, set initialBalance and currentBalance to null
        Long initialBalance = null;
        Long currentBalance = null;
        long consumptionLimit = 0L;
        long maxCarryForward = 0L;
        long totalCarryForward = 0L;
        long usage = 0L;

        if (isUnlimited == 0) {
            // Limited bucket: generate balance values
            initialBalance = 10_000_000_000L + random.nextLong(90_000_000_000L); // > 9999999999
            currentBalance = initialBalance - random.nextLong(initialBalance / 10);
            consumptionLimit = initialBalance / 10;
            maxCarryForward = initialBalance / 5;
            totalCarryForward = random.nextLong(initialBalance / 20);
            usage = random.nextLong(initialBalance / 5);
        }

        return new BucketInstanceRecord(
                id,
                bucketId,                                           // BUCKET_ID
                BUCKET_TYPES[random.nextInt(BUCKET_TYPES.length)], // BUCKET_TYPE
                random.nextInt(2),                                  // CARRY_FORWARD (0 or 1)
                random.nextInt(90) + 30,                            // CARRY_FORWARD_VALIDITY
                consumptionLimit,                                   // CONSUMPTION_LIMIT
                CONSUMTION_LIMIT[random.nextInt(TIME_WINDOWS.length)],  // CONSUMPTION_LIMIT_WINDOW
                currentBalance,                                     // CURRENT_BALANCE (NULL if unlimited)
                expiration,                                         // EXPIRATION
                initialBalance,                                     // INITIAL_BALANCE (NULL if unlimited)
                maxCarryForward,                                    // MAX_CARRY_FORWARD
                priority,                                           // PRIORITY
                RULES[random.nextInt(RULES.length)],                // RULE
                String.valueOf(serviceId),                          // SERVICE_ID (FK)
                TIME_WINDOWS[random.nextInt(TIME_WINDOWS.length)],  // TIME_WINDOW
                totalCarryForward,                                  // TOTAL_CARRY_FORWARD
                usage,                                              // USAGE
                now,                                                // UPDATED_AT
                isUnlimited                                         // IS_UNLIMITED
        );
    }

    /**
     * Insert a batch of BUCKET_INSTANCE records using optimized batch processing
     * Supports large-scale inserts (e.g., 4 million records) by:
     * - Chunking large batches into manageable sizes (BUCKET_BATCH_SIZE)
     * - Parallel processing multiple chunks concurrently
     * - Using INSERT ALL for efficient batch inserts
     */
    private Uni<Integer> insertBucketInstanceBatch(List<BucketInstanceRecord> batch) {
        if (batch.isEmpty()) {
            return Uni.createFrom().item(0);
        }

        AtomicInteger totalInserted = new AtomicInteger(0);
        AtomicInteger processedChunks = new AtomicInteger(0);
        int totalChunks = (batch.size() + BUCKET_BATCH_SIZE - 1) / BUCKET_BATCH_SIZE;

        // Split into chunks and process in parallel
        return Multi.createFrom().iterable(batch)
                .group().intoLists().of(BUCKET_BATCH_SIZE)
                .capDemandsTo(BUCKET_CONCURRENT_BATCHES)
                .onItem().transformToUniAndMerge(chunk ->
                    insertBucketChunk(chunk)
                        .onItem().invoke(count -> {
                            totalInserted.addAndGet(count);
                            int completed = processedChunks.incrementAndGet();
                            if (completed % 10 == 0 || completed == totalChunks) {
                                log.infof("Bucket insertion progress: %d/%d chunks (%d records)",
                                        completed, totalChunks, totalInserted.get());
                            }
                        })
                        .onFailure().invoke(e ->
                            log.errorf(e, "Failed to insert bucket chunk: %s", e.getMessage())
                        )
                        .onFailure().recoverWithItem(0)
                )
                .collect().asList()
                .map(results -> totalInserted.get());
    }

    /**
     * Insert a single chunk of BUCKET_INSTANCE records using INSERT ALL
     * Optimized for Oracle with parameterized batch inserts
     */
    private Uni<Integer> insertBucketChunk(List<BucketInstanceRecord> chunk) {
        if (chunk.isEmpty()) {
            return Uni.createFrom().item(0);
        }

        // For very small chunks (< 5), use individual inserts to avoid overhead
        if (chunk.size() < 5) {
            return insertBucketIndividual(chunk);
        }

        StringBuilder sql = new StringBuilder("INSERT ALL ");

        String columns = "(ID, BUCKET_ID, BUCKET_TYPE, CARRY_FORWARD, CARRY_FORWARD_VALIDITY, " +
                "CONSUMPTION_LIMIT, CONSUMPTION_LIMIT_WINDOW, CURRENT_BALANCE, EXPIRATION, " +
                "INITIAL_BALANCE, MAX_CARRY_FORWARD, PRIORITY, RULE, SERVICE_ID, TIME_WINDOW, " +
                "TOTAL_CARRY_FORWARD, USAGE, UPDATED_AT, IS_UNLIMITED)";

        String placeholders = "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

        List<Object> values = new ArrayList<>();

        for (BucketInstanceRecord record : chunk) {
            sql.append("INTO BUCKET_INSTANCE ").append(columns)
               .append(" VALUES ").append(placeholders).append(" ");

            values.add(record.id);
            values.add(record.bucketId);
            values.add(record.bucketType);
            values.add(record.carryForward);
            values.add(record.carryForwardValidity);
            values.add(record.consumptionLimit);
            values.add(record.consumptionLimitWindow);
            values.add(record.currentBalance);
            values.add(record.expiration);
            values.add(record.initialBalance);
            values.add(record.maxCarryForward);
            values.add(record.priority);
            values.add(record.rule);
            values.add(record.serviceId);
            values.add(record.timeWindow);
            values.add(record.totalCarryForward);
            values.add(record.usage);
            values.add(record.updatedAt);
            values.add(record.isUnlimited);
        }

        sql.append("SELECT * FROM DUAL");

        return client.preparedQuery(sql.toString())
                .execute(Tuple.from(values))
                .map(result -> chunk.size())
                .onFailure().retry().atMost(2);
    }

    /**
     * Fallback method for inserting small batches individually
     * Used for chunks smaller than 5 records to avoid INSERT ALL overhead
     */
    private Uni<Integer> insertBucketIndividual(List<BucketInstanceRecord> records) {
        String sql = "INSERT INTO BUCKET_INSTANCE " +
                "(ID, BUCKET_ID, BUCKET_TYPE, CARRY_FORWARD, CARRY_FORWARD_VALIDITY, " +
                "CONSUMPTION_LIMIT, CONSUMPTION_LIMIT_WINDOW, CURRENT_BALANCE, EXPIRATION, " +
                "INITIAL_BALANCE, MAX_CARRY_FORWARD, PRIORITY, RULE, SERVICE_ID, TIME_WINDOW, " +
                "TOTAL_CARRY_FORWARD, USAGE, UPDATED_AT, IS_UNLIMITED) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

        return Multi.createFrom().iterable(records)
                .onItem().transformToUniAndConcatenate(record -> {
                    List<Object> values = new ArrayList<>();
                    values.add(record.id);
                    values.add(record.bucketId);
                    values.add(record.bucketType);
                    values.add(record.carryForward);
                    values.add(record.carryForwardValidity);
                    values.add(record.consumptionLimit);
                    values.add(record.consumptionLimitWindow);
                    values.add(record.currentBalance);
                    values.add(record.expiration);
                    values.add(record.initialBalance);
                    values.add(record.maxCarryForward);
                    values.add(record.priority);
                    values.add(record.rule);
                    values.add(record.serviceId);
                    values.add(record.timeWindow);
                    values.add(record.totalCarryForward);
                    values.add(record.usage);
                    values.add(record.updatedAt);
                    values.add(record.isUnlimited);

                    return client.preparedQuery(sql)
                            .execute(Tuple.from(values))
                            .map(result -> 1);
                })
                .collect().asList()
                .map(List::size)
                .onFailure().retry().atMost(2);
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

    private record BucketInstanceRecord(
            long id,
            String bucketId,
            String bucketType,
            int carryForward,
            int carryForwardValidity,
            long consumptionLimit,
            String consumptionLimitWindow,
            Long currentBalance,
            LocalDateTime expiration,
            Long initialBalance,
            long maxCarryForward,
            int priority,
            String rule,
            String serviceId,
            String timeWindow,
            long totalCarryForward,
            long usage,
            LocalDateTime updatedAt,
            int isUnlimited
    ) {}

    /**
     * Result record for data generation
     */
    public record GenerationResult(
            int serviceInstancesCreated,
            int bucketInstancesCreated,
            int failed,
            Duration duration
    ) {
        @Override
        public String toString() {
            return String.format(
                    "GenerationResult{services=%d, buckets=%d, failed=%d, duration=%s}",
                    serviceInstancesCreated, bucketInstancesCreated, failed, formatDuration(duration)
            );
        }

        private static String formatDuration(Duration duration) {
            long minutes = duration.toMinutes();
            long seconds = duration.toSecondsPart();
            return String.format("%dm %ds", minutes, seconds);
        }
    }
}
