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
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Data Generator for BUCKET_INSTANCE table.
 *
 * Features:
 * - Fetches service IDs from SERVICE_INSTANCE table
 * - Creates BUCKET_INSTANCE records for each service
 * - Uses database sequence (BUCKET_INSTANCE_SEQ) for unique ID generation in batch operations
 * - Generates realistic test data with specified distributions
 * - Batch processing for optimal performance using executeBatch to avoid Oracle bind variable limits
 * - Progress reporting and error handling
 */
@ApplicationScoped
public class BucketInstanceDataGenerator {

    private static final Logger log = Logger.getLogger(BucketInstanceDataGenerator.class);
    @Inject
     ReactiveRedisDataSource reactiveRedisDataSource;
    // Configuration constants - OPTIMIZED FOR HIGH THROUGHPUT
    private static final int BUCKET_INSTANCE = 1;
    private static final int BATCH_SIZE = 5000; // Increased from 1000 for better throughput
    private static final int PROGRESS_INTERVAL = 20000; // Less frequent logging
    private static final int CONCURRENT_BATCHES = 5; // Increased from 1 for parallel processing
    private static final int MAX_RETRY_ATTEMPTS = 3; // Maximum retry attempts for failed batches
    private static final long INITIAL_RETRY_DELAY_MS = 1000; // Initial retry delay (1 second)
    private static final long BATCH_TIMEOUT_SECONDS = 60; // Timeout for each batch operation

    private static final String[] TIME_WINDOWS = {"00-08", "00-24", "00-18", "18-24"};
    private static final String[] CONSUMPTION_LIMIT = {"1", "7", "30"};
    private static final String[] BUCKET_TYPES = {"DATA", "COMBO"};
    private static final String[] RULES = {"100Mbps", "200Mbps", "300Kbps", "1Gbps", "100kbps"};
    private static final int[] PRIORITY = {10,20};

    private final Pool client;
    private final Random random = new Random();

    @Inject
    public BucketInstanceDataGenerator(Pool client) {
        this.client = client;
    }
    public Multi<Long> fetchAllIds() {
        return client.query("SELECT ID FROM SERVICE_INSTANCE ORDER BY ID")
                .execute()
                .onFailure()
                .invoke(e -> log.errorf(e, "Failed to fetch ID from SERVICE_INSTANCE: %s", e.getMessage()))
                .onItem().transformToUni(rows -> {
                    List<Long> idList = new ArrayList<>();
                    for (Row row : rows) {
                        idList.add(row.getLong("ID"));
                    }

                    // First store them in cache
                    return storeUserData(idList)
                            .onFailure().invoke(ex -> log.warn("Unable to store ID list to cache"))
                            .replaceWith(idList);   // return list after caching
                })
                .onItem()
                .transformToMulti(list -> Multi.createFrom().iterable(list)); // Convert List<Long> → Multi<Long>
    }
    /**
     * Main execution method - generates data for all users
     */

    public Uni<GenerationResult> generateDataBucket() {
        log.info("Starting SERVICE_INSTANCE data generation");
        Instant startTime = Instant.now();

        return fetchUsernames()
                .chain(usernames -> {
                    log.infof("Found %d users in AAA_USER table", usernames.size());
                    log.infof("Will create %d service instances (%d per user)",
                            usernames.size() * BUCKET_INSTANCE, BUCKET_INSTANCE);

                    return generateServiceInstances(usernames, startTime);
                });
    }

    /**
     * Fetch all usernames from AAA_USER table
     */
    //to
    private Uni<List<Long>> fetchUsernames() {
        log.info("Fetching IDS from SERVICE_INSTANCE table...");

        return getUserData()
                .onItem()
                .transformToUni(serviceId -> {
                    if (serviceId == null || serviceId.isEmpty()) {
                        return client.query("SELECT ID FROM SERVICE_INSTANCE ORDER BY ID")
                                .execute()
                                .onItem()
                                .transformToUni(rows -> {
                                    List<Long> usernameList = new ArrayList<>();
                                    for (Row row : rows) {
                                        usernameList.add(row.getLong("ID"));
                                    }
                                    return storeUserData(usernameList)
                                            .onFailure()
                                            .invoke(throwable -> {
                                                log.warnf("Unable to store ID to cache");
                                            })
                                            .replaceWith(usernameList);
                                })
                                .onFailure()
                                .invoke(e -> log.errorf(e, "Failed to fetch ID from SERVICE_INSTANCE: %s", e.getMessage()));
                    } else {
                        log.infof("load data from cache");
                        return Uni.createFrom().item(serviceId);
                    }
                });
    }

    /**
     * Generate BUCKET_INSTANCE records for all services - OPTIMIZED
     * Uses database sequence (BUCKET_INSTANCE_SEQ) for unique ID generation
     */
    private Uni<GenerationResult> generateServiceInstances(List<Long> serviceIds, Instant startTime) {
        AtomicInteger serviceCount = new AtomicInteger(0);
        AtomicInteger failedCount = new AtomicInteger(0);

        int totalServices = serviceIds.size() * BUCKET_INSTANCE;
        int totalBatches = (totalServices + BATCH_SIZE - 1) / BATCH_SIZE;

        log.infof("Processing %d bucket instances in %d batches with %d concurrent workers",
                totalServices, totalBatches, CONCURRENT_BATCHES);

        // Stream processing: generate records on-the-fly instead of pre-creating all
        return Multi.createFrom().iterable(serviceIds)
                .onItem().transformToMulti(serviceId ->
                    Multi.createFrom().range(0, BUCKET_INSTANCE)
                        .map(i -> serviceId) // Just pass through serviceId
                )
                .merge()
                .group().intoLists().of(BATCH_SIZE)
                .capDemandsTo(CONCURRENT_BATCHES)
                .onItem().transformToUniAndMerge(serviceIdBatch -> {
                    // Wrap the entire batch processing in retry logic with timeout
                    Uni<List<Long>> batchOperation = generateIds(serviceIdBatch.size())
                        .chain(ids -> {
                            // Create batch records with generated IDs
                            List<BucketInstanceRecord> batch = new ArrayList<>(serviceIdBatch.size());
                            for (int i = 0; i < serviceIdBatch.size(); i++) {
                                batch.add(createBucketInstanceRecord(serviceIdBatch.get(i), ids.get(i)));
                            }
                            return insertServiceInstanceBatch(batch);
                        })
                        .ifNoItem().after(Duration.ofSeconds(BATCH_TIMEOUT_SECONDS))
                        .failWith(() -> new RuntimeException(
                            String.format("Batch operation timed out after %d seconds", BATCH_TIMEOUT_SECONDS)
                        ));

                    return retryWithBackoff(
                        batchOperation,
                        MAX_RETRY_ATTEMPTS,
                        String.format("Batch insert (%d records)", serviceIdBatch.size())
                    )
                        .onItem().invoke(() -> {
                            int services = serviceCount.addAndGet(serviceIdBatch.size());

                            if (services % PROGRESS_INTERVAL == 0 || services == totalServices) {
                                Duration elapsed = Duration.between(startTime, Instant.now());
                                double rps = services * 1000.0 / Math.max(1, elapsed.toMillis());
                                log.infof("Progress: %d/%d buckets (%.1f%%) | %.0f bucket/s",
                                        services, totalServices, (services * 100.0 / totalServices), rps);
                            }
                        })
                        .onFailure().invoke(e -> {
                            failedCount.addAndGet(serviceIdBatch.size());
                            log.errorf(e, "Batch insert failed after %d retries, batch size: %d - %s",
                                    MAX_RETRY_ATTEMPTS, serviceIdBatch.size(), e.getMessage());
                        })
                        .onFailure().recoverWithItem(Collections.emptyList());
                })
                .collect().asList()
                .map(results -> {
                    Duration totalDuration = Duration.between(startTime, Instant.now());
                    int actualInserted = serviceCount.get();
                    int actualFailed = failedCount.get();

                    // Validate that all records were accounted for
                    int accountedFor = actualInserted + actualFailed;
                    if (accountedFor != totalServices) {
                        log.errorf("CRITICAL: Record count mismatch! Expected: %d, Inserted: %d, Failed: %d, Total: %d, Missing: %d",
                                totalServices, actualInserted, actualFailed, accountedFor, (totalServices - accountedFor));
                    }

                    return new GenerationResult(
                            actualInserted,
                            actualFailed,
                            totalDuration
                    );
                })
                .onItem().invoke(result -> {
                    log.infof("Data generation completed: %s", result);
                    if (result.failed() > 0) {
                        log.warnf("WARNING: %d records failed to insert after %d retry attempts",
                                result.failed(), MAX_RETRY_ATTEMPTS);
                    }
                });
    }

    private BucketInstanceRecord createBucketInstanceRecord(long serviceId, long id) {
        String bucketId = "BUCKET-" + serviceId + "-" + PRIORITY[random.nextInt(PRIORITY.length)];

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
                bucketId,
                BUCKET_TYPES[random.nextInt(BUCKET_TYPES.length)],
                random.nextInt(2),
                random.nextInt(90) + 30,
                consumptionLimit,
                CONSUMPTION_LIMIT[random.nextInt(CONSUMPTION_LIMIT.length)],
                currentBalance,
                expiration,
                initialBalance,
                maxCarryForward,
                PRIORITY[random.nextInt(PRIORITY.length)],
                RULES[random.nextInt(RULES.length)],
                String.valueOf(serviceId),
                TIME_WINDOWS[random.nextInt(TIME_WINDOWS.length)],
                totalCarryForward,
                usage,
                now,
                isUnlimited
        );
    }



    /**
     * Retry a Uni operation with exponential backoff
     */
    private <T> Uni<T> retryWithBackoff(Uni<T> operation, int maxAttempts, String context) {
        return operation
                .onFailure()
                .retry()
                .withBackOff(Duration.ofMillis(INITIAL_RETRY_DELAY_MS))
                .atMost(maxAttempts - 1) // -1 because first attempt doesn't count as retry
                .onFailure()
                .invoke(e -> log.errorf(e, "Failed after %d attempts: %s - %s",
                        maxAttempts, context, e.getMessage()));
    }

    /**
     * Insert a batch of SERVICE_INSTANCE records - OPTIMIZED
     * Uses executeBatch with individual INSERT statements to avoid Oracle INSERT ALL bind variable limitations
     */
    private Uni<List<Long>> insertServiceInstanceBatch(List<BucketInstanceRecord> batch) {
        String sql = "INSERT INTO BUCKET_INSTANCE " +
                "(ID, BUCKET_ID, BUCKET_TYPE, CARRY_FORWARD, CARRY_FORWARD_VALIDITY, " +
                "CONSUMPTION_LIMIT, CONSUMPTION_LIMIT_WINDOW, CURRENT_BALANCE, EXPIRATION, " +
                "INITIAL_BALANCE, MAX_CARRY_FORWARD, PRIORITY, RULE, SERVICE_ID, TIME_WINDOW, " +
                "TOTAL_CARRY_FORWARD, USAGE, UPDATED_AT, IS_UNLIMITED) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

        List<Tuple> tuples = new ArrayList<>(batch.size());
        List<Long> serviceIds = new ArrayList<>(batch.size());
        for (BucketInstanceRecord record : batch) {
            List<Object> values = new ArrayList<>(19);
            values.add(record.id());
            values.add(record.bucketId());
            values.add(record.bucketType());
            values.add(record.carryForward());
            values.add(record.carryForwardValidity());
            values.add(record.consumptionLimit());
            values.add(record.consumptionLimitWindow());
            values.add(record.currentBalance());
            values.add(record.expiration());
            values.add(record.initialBalance());
            values.add(record.maxCarryForward());
            values.add(record.priority());
            values.add(record.rule());
            values.add(record.serviceId());
            values.add(record.timeWindow());
            values.add(record.totalCarryForward());
            values.add(record.usage());
            values.add(record.updatedAt());
            values.add(record.isUnlimited());

            tuples.add(Tuple.from(values));
            serviceIds.add(record.id);
        }

        return client.preparedQuery(sql)
                .executeBatch(tuples)
                .map(result -> serviceIds);

    }

    // Record classes for data structures
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
     * Result record for bucket instance data generation
     */
    public record GenerationResult(
            int serviceInstancesCreated,
            int failed,
            Duration duration
    ) {
        @Override
        public String toString() {
            return String.format(
                    "GenerationResult{buckets=%d, failed=%d, duration=%s}",
                    serviceInstancesCreated, failed, formatDuration(duration)
            );
        }

        private static String formatDuration(Duration duration) {
            long minutes = duration.toMinutes();
            long seconds = duration.toSecondsPart();
            return String.format("%dm %ds", minutes, seconds);
        }
    }

    public Uni<Void> storeUserData(List<Long> ids) {
        final long startTime = log.isDebugEnabled() ? System.currentTimeMillis() : 0;
        log.infof("Storing %d ids in cache", ids.size());

        JsonArray jsonArray = new JsonArray(ids);

        return reactiveRedisDataSource.value(String.class)     // <── use String
                .set("serviceIds", jsonArray.encode())         // <── store as string
                .invoke(() -> {
                    if (log.isDebugEnabled()) {
                        log.debugf("Stored %d ids in cache in %d ms",
                                ids.size(),
                                (System.currentTimeMillis() - startTime));
                    }
                })
                .onFailure()
                .invoke(e -> log.errorf(e, "Failed to store ids in cache: %s", e.getMessage()));
    }

    public Uni<List<Long>> getUserData() {
        final long startTime = log.isDebugEnabled() ? System.currentTimeMillis() : 0;
        log.info("Retrieving user data from cache");

        return reactiveRedisDataSource.value(String.class)  // <── read as String
                .get("serviceIds")
                .onItem().transform(str -> {
                    if (str == null) {
                        log.info("No user data found in cache, will fetch from database");
                        return Collections.<Long>emptyList();
                    }

                    try {
                        JsonArray jsonArray = new JsonArray(str); // <── directly parse JSON string
                        List<Long> serviceIds = new ArrayList<>(jsonArray.size());

                        for (int i = 0; i < jsonArray.size(); i++) {
                            serviceIds.add(jsonArray.getLong(i));
                        }

                        if (log.isDebugEnabled()) {
                            log.debugf("Retrieved %d ids from cache in %d ms",
                                    serviceIds.size(), (System.currentTimeMillis() - startTime));
                        }

                        log.infof("User data retrieved from cache: %d ids", serviceIds.size());
                        return serviceIds;

                    } catch (Exception e) {
                        log.errorf(e, "Failed to parse cached user data: %s", e.getMessage());
                        return Collections.<Long>emptyList();
                    }
                })
                .onFailure().recoverWithItem(Collections.<Long>emptyList());
    }

    /**
     * Generate a batch of unique IDs from the database sequence.
     * This method fetches multiple sequence values in a single database call for efficiency.
     *
     * @param batchSize Number of IDs to generate
     * @return Uni containing a list of unique IDs
     */
    private Uni<List<Long>> generateIds(int batchSize) {
        String sql = "SELECT BUCKET_INSTANCE_SEQ.NEXTVAL FROM DUAL CONNECT BY LEVEL <= ?";
        return client.preparedQuery(sql)
                .execute(Tuple.of(batchSize))
                .onItem().transform(rows -> {
                    List<Long> ids = new ArrayList<>(batchSize);
                    for (Row row : rows) {
                        ids.add(row.getLong(0));
                    }
                    if (log.isDebugEnabled()) {
                        log.debugf("Generated %d IDs from sequence: %d to %d",
                                ids.size(),
                                ids.isEmpty() ? 0 : ids.get(0),
                                ids.isEmpty() ? 0 : ids.get(ids.size() - 1));
                    }
                    return ids;
                })
                .onFailure().invoke(e ->
                    log.errorf(e, "Failed to generate %d IDs from sequence: %s", batchSize, e.getMessage())
                );
    }

}
