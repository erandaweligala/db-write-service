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
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Bulk Insert class for BUCKET_INSTANCE table.
 *
 * Features:
 * - Fetches SERVICE_INSTANCE records from database
 * - Generates BUCKET_INSTANCE records (1:1 ratio with SERVICE_INSTANCE)
 * - Performs optimized batch inserts using executeBatch
 * - Progress reporting and error handling
 * - Configurable batch sizes and concurrency
 */
@ApplicationScoped
public class BulkInsertBucketInstance {

    private static final Logger log = Logger.getLogger(BulkInsertBucketInstance.class);

    // Configuration constants
    private static final int SERVICE_FETCH_BATCH_SIZE = 5000;
    private static final int BUCKET_BATCH_SIZE = 10000;
    private static final int PROGRESS_INTERVAL = 20000;
    private static final int CONCURRENT_BATCHES = 10;

    // BUCKET_INSTANCE constants
    private static final String[] TIME_WINDOWS = {"00-08", "00-24", "00-18", "18-24"};
    private static final String[] CONSUMPTION_LIMIT = {"1", "7", "30"};
    private static final String[] BUCKET_TYPES = {"DATA", "COMBO"};
    private static final String[] RULES = {"100Mbps", "200Mbps", "300Kbps", "1Gbps", "100kbps"};

    private final Pool client;
    private final Random random = new Random();

    @Inject
    public BulkInsertBucketInstance(Pool client) {
        this.client = client;
    }

    /**
     * Main execution method - generates BUCKET_INSTANCE records for all SERVICE_INSTANCE records
     *
     * @return BulkInsertResult containing statistics about the operation
     */
    public Uni<BulkInsertResult> executeBulkInsert() {
        log.info("Starting BUCKET_INSTANCE bulk insert from SERVICE_INSTANCE table");
        Instant startTime = Instant.now();

        return fetchServiceInstanceCount()
                .chain(totalServices -> {
                    log.infof("Found %d SERVICE_INSTANCE records", totalServices);
                    log.infof("Will create %d BUCKET_INSTANCE records (1:1 ratio)", totalServices);

                    return processBucketInserts(totalServices, startTime);
                });
    }

    /**
     * Execute bulk insert with optional filtering by status
     *
     * @param status Optional status filter (e.g., "Active", "Suspend", "Inactive")
     * @return BulkInsertResult containing statistics
     */
    public Uni<BulkInsertResult> executeBulkInsert(String status) {
        log.infof("Starting BUCKET_INSTANCE bulk insert for SERVICE_INSTANCE with status: %s", status);
        Instant startTime = Instant.now();

        return fetchServiceInstanceCountByStatus(status)
                .chain(totalServices -> {
                    log.infof("Found %d SERVICE_INSTANCE records with status '%s'", totalServices, status);
                    log.infof("Will create %d BUCKET_INSTANCE records", totalServices);

                    return processBucketInsertsWithFilter(totalServices, status, startTime);
                });
    }

    /**
     * Fetch total count of SERVICE_INSTANCE records
     */
    private Uni<Long> fetchServiceInstanceCount() {
        String sql = "SELECT COUNT(*) as total FROM SERVICE_INSTANCE";

        return client.query(sql)
                .execute()
                .map(rows -> {
                    for (Row row : rows) {
                        return row.getLong("total");
                    }
                    return 0L;
                })
                .onFailure()
                .invoke(e -> log.errorf(e, "Failed to fetch SERVICE_INSTANCE count: %s", e.getMessage()));
    }

    /**
     * Fetch count of SERVICE_INSTANCE records by status
     */
    private Uni<Long> fetchServiceInstanceCountByStatus(String status) {
        String sql = "SELECT COUNT(*) as total FROM SERVICE_INSTANCE WHERE STATUS = ?";

        return client.preparedQuery(sql)
                .execute(Tuple.of(status))
                .map(rows -> {
                    for (Row row : rows) {
                        return row.getLong("total");
                    }
                    return 0L;
                })
                .onFailure()
                .invoke(e -> log.errorf(e, "Failed to fetch SERVICE_INSTANCE count by status: %s", e.getMessage()));
    }

    /**
     * Process bucket inserts for all SERVICE_INSTANCE records
     */
    private Uni<BulkInsertResult> processBucketInserts(Long totalServices, Instant startTime) {
        if (totalServices == 0) {
            log.warn("No SERVICE_INSTANCE records found, nothing to insert");
            return Uni.createFrom().item(new BulkInsertResult(0, 0, Duration.ZERO));
        }

        AtomicInteger insertedCount = new AtomicInteger(0);
        AtomicInteger failedCount = new AtomicInteger(0);
        AtomicLong bucketIdCounter = new AtomicLong(System.currentTimeMillis() % 1000000);

        // Calculate total batches
        int totalBatches = (int) ((totalServices + SERVICE_FETCH_BATCH_SIZE - 1) / SERVICE_FETCH_BATCH_SIZE);
        log.infof("Processing %d service instances in %d batches with %d concurrent workers",
                totalServices, totalBatches, CONCURRENT_BATCHES);

        // Stream processing: fetch SERVICE_INSTANCE records in batches and process
        return Multi.createFrom().range(0, totalBatches)
                .onItem().transformToUniAndMerge(batchNum -> {
                    long offset = (long) batchNum * SERVICE_FETCH_BATCH_SIZE;
                    return fetchServiceInstanceBatch(offset, SERVICE_FETCH_BATCH_SIZE)
                            .chain(serviceRecords -> generateAndInsertBuckets(serviceRecords, bucketIdCounter))
                            .onItem().invoke(inserted -> {
                                int total = insertedCount.addAndGet(inserted);

                                if (total % PROGRESS_INTERVAL == 0 || total >= totalServices) {
                                    Duration elapsed = Duration.between(startTime, Instant.now());
                                    double rps = total * 1000.0 / Math.max(1, elapsed.toMillis());
                                    log.infof("Progress: %d/%d buckets (%.1f%%) | %.0f buckets/s",
                                            total, totalServices, (total * 100.0 / totalServices), rps);
                                }
                            })
                            .onFailure().invoke(e -> {
                                failedCount.addAndGet(SERVICE_FETCH_BATCH_SIZE);
                                log.errorf(e, "Batch insert failed: %s", e.getMessage());
                            })
                            .onFailure().recoverWithItem(0);
                }, CONCURRENT_BATCHES)
                .collect().asList()
                .map(results -> {
                    Duration totalDuration = Duration.between(startTime, Instant.now());
                    int inserted = insertedCount.get();
                    int failed = failedCount.get();

                    log.infof("Bulk insert completed: %d inserted, %d failed, duration: %s",
                            inserted, failed, formatDuration(totalDuration));

                    return new BulkInsertResult(inserted, failed, totalDuration);
                });
    }

    /**
     * Process bucket inserts with status filter
     */
    private Uni<BulkInsertResult> processBucketInsertsWithFilter(Long totalServices, String status, Instant startTime) {
        if (totalServices == 0) {
            log.warnf("No SERVICE_INSTANCE records found with status '%s', nothing to insert", status);
            return Uni.createFrom().item(new BulkInsertResult(0, 0, Duration.ZERO));
        }

        AtomicInteger insertedCount = new AtomicInteger(0);
        AtomicInteger failedCount = new AtomicInteger(0);
        AtomicLong bucketIdCounter = new AtomicLong(System.currentTimeMillis() % 1000000);

        int totalBatches = (int) ((totalServices + SERVICE_FETCH_BATCH_SIZE - 1) / SERVICE_FETCH_BATCH_SIZE);
        log.infof("Processing %d service instances in %d batches", totalServices, totalBatches);

        return Multi.createFrom().range(0, totalBatches)
                .onItem().transformToUniAndMerge(batchNum -> {
                    long offset = (long) batchNum * SERVICE_FETCH_BATCH_SIZE;
                    return fetchServiceInstanceBatchByStatus(offset, SERVICE_FETCH_BATCH_SIZE, status)
                            .chain(serviceRecords -> generateAndInsertBuckets(serviceRecords, bucketIdCounter))
                            .onItem().invoke(inserted -> {
                                int total = insertedCount.addAndGet(inserted);

                                if (total % PROGRESS_INTERVAL == 0 || total >= totalServices) {
                                    Duration elapsed = Duration.between(startTime, Instant.now());
                                    double rps = total * 1000.0 / Math.max(1, elapsed.toMillis());
                                    log.infof("Progress: %d/%d buckets (%.1f%%) | %.0f buckets/s",
                                            total, totalServices, (total * 100.0 / totalServices), rps);
                                }
                            })
                            .onFailure().invoke(e -> {
                                failedCount.addAndGet(SERVICE_FETCH_BATCH_SIZE);
                                log.errorf(e, "Batch insert failed: %s", e.getMessage());
                            })
                            .onFailure().recoverWithItem(0);
                }, CONCURRENT_BATCHES)
                .collect().asList()
                .map(results -> {
                    Duration totalDuration = Duration.between(startTime, Instant.now());
                    int inserted = insertedCount.get();
                    int failed = failedCount.get();

                    log.infof("Bulk insert completed: %d inserted, %d failed, duration: %s",
                            inserted, failed, formatDuration(totalDuration));

                    return new BulkInsertResult(inserted, failed, totalDuration);
                });
    }

    /**
     * Fetch a batch of SERVICE_INSTANCE records
     */
    private Uni<List<ServiceInstanceRecord>> fetchServiceInstanceBatch(long offset, int limit) {
        String sql = "SELECT ID, USERNAME, PLAN_ID, PLAN_TYPE, STATUS " +
                     "FROM SERVICE_INSTANCE " +
                     "ORDER BY ID " +
                     "OFFSET ? ROWS FETCH NEXT ? ROWS ONLY";

        return client.preparedQuery(sql)
                .execute(Tuple.of(offset, limit))
                .map(rows -> {
                    List<ServiceInstanceRecord> records = new ArrayList<>();
                    for (Row row : rows) {
                        records.add(new ServiceInstanceRecord(
                                row.getLong("ID"),
                                row.getString("USERNAME"),
                                row.getString("PLAN_ID"),
                                row.getString("PLAN_TYPE"),
                                row.getString("STATUS")
                        ));
                    }
                    return records;
                })
                .onFailure()
                .invoke(e -> log.errorf(e, "Failed to fetch SERVICE_INSTANCE batch at offset %d: %s",
                        offset, e.getMessage()));
    }

    /**
     * Fetch a batch of SERVICE_INSTANCE records filtered by status
     */
    private Uni<List<ServiceInstanceRecord>> fetchServiceInstanceBatchByStatus(long offset, int limit, String status) {
        String sql = "SELECT ID, USERNAME, PLAN_ID, PLAN_TYPE, STATUS " +
                     "FROM SERVICE_INSTANCE " +
                     "WHERE STATUS = ? " +
                     "ORDER BY ID " +
                     "OFFSET ? ROWS FETCH NEXT ? ROWS ONLY";

        return client.preparedQuery(sql)
                .execute(Tuple.of(status, offset, limit))
                .map(rows -> {
                    List<ServiceInstanceRecord> records = new ArrayList<>();
                    for (Row row : rows) {
                        records.add(new ServiceInstanceRecord(
                                row.getLong("ID"),
                                row.getString("USERNAME"),
                                row.getString("PLAN_ID"),
                                row.getString("PLAN_TYPE"),
                                row.getString("STATUS")
                        ));
                    }
                    return records;
                })
                .onFailure()
                .invoke(e -> log.errorf(e, "Failed to fetch SERVICE_INSTANCE batch by status at offset %d: %s",
                        offset, e.getMessage()));
    }

    /**
     * Generate and insert BUCKET_INSTANCE records for a batch of SERVICE_INSTANCE records
     */
    private Uni<Integer> generateAndInsertBuckets(List<ServiceInstanceRecord> serviceRecords,
                                                   AtomicLong bucketIdCounter) {
        if (serviceRecords.isEmpty()) {
            return Uni.createFrom().item(0);
        }

        // Generate bucket records for each service (1:1 ratio)
        List<BucketInstanceRecord> bucketRecords = new ArrayList<>(serviceRecords.size());
        for (ServiceInstanceRecord service : serviceRecords) {
            BucketInstanceRecord bucket = createBucketInstanceRecord(
                    service.id(),
                    1, // priority
                    bucketIdCounter.incrementAndGet()
            );
            bucketRecords.add(bucket);
        }

        // Insert in optimized batches
        return insertBucketBatch(bucketRecords);
    }

    /**
     * Create a BUCKET_INSTANCE record with generated data
     */
    private BucketInstanceRecord createBucketInstanceRecord(long serviceId, int priority, long id) {
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
                priority,
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
     * Insert BUCKET_INSTANCE records in batches
     */
    private Uni<Integer> insertBucketBatch(List<BucketInstanceRecord> records) {
        if (records.isEmpty()) {
            return Uni.createFrom().item(0);
        }

        // Split into chunks if the batch is too large
        if (records.size() > BUCKET_BATCH_SIZE) {
            AtomicInteger totalInserted = new AtomicInteger(0);

            return Multi.createFrom().iterable(records)
                    .group().intoLists().of(BUCKET_BATCH_SIZE)
                    .onItem().transformToUniAndConcatenate(chunk ->
                            insertBucketChunk(chunk)
                                    .onItem().invoke(totalInserted::addAndGet)
                    )
                    .collect().asList()
                    .map(results -> totalInserted.get());
        }

        return insertBucketChunk(records);
    }

    /**
     * Insert a chunk of BUCKET_INSTANCE records using executeBatch
     */
    private Uni<Integer> insertBucketChunk(List<BucketInstanceRecord> records) {
        String sql = "INSERT INTO BUCKET_INSTANCE " +
                "(ID, BUCKET_ID, BUCKET_TYPE, CARRY_FORWARD, CARRY_FORWARD_VALIDITY, " +
                "CONSUMPTION_LIMIT, CONSUMPTION_LIMIT_WINDOW, CURRENT_BALANCE, EXPIRATION, " +
                "INITIAL_BALANCE, MAX_CARRY_FORWARD, PRIORITY, RULE, SERVICE_ID, TIME_WINDOW, " +
                "TOTAL_CARRY_FORWARD, USAGE, UPDATED_AT, IS_UNLIMITED) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

        List<Tuple> tuples = new ArrayList<>(records.size());

        for (BucketInstanceRecord record : records) {
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
        }

        return client.preparedQuery(sql)
                .executeBatch(tuples)
                .map(result -> records.size())
                .onFailure()
                .invoke(e -> log.errorf(e, "Failed to insert bucket chunk: %s", e.getMessage()));
    }

    /**
     * Format duration for logging
     */
    private String formatDuration(Duration duration) {
        long hours = duration.toHours();
        long minutes = duration.toMinutesPart();
        long seconds = duration.toSecondsPart();

        if (hours > 0) {
            return String.format("%dh %dm %ds", hours, minutes, seconds);
        } else if (minutes > 0) {
            return String.format("%dm %ds", minutes, seconds);
        } else {
            return String.format("%ds", seconds);
        }
    }

    // Record classes
    private record ServiceInstanceRecord(
            long id,
            String username,
            String planId,
            String planType,
            String status
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
     * Result record for bulk insert operations
     */
    public record BulkInsertResult(
            int inserted,
            int failed,
            Duration duration
    ) {
        @Override
        public String toString() {
            return String.format(
                    "BulkInsertResult{inserted=%d, failed=%d, duration=%s}",
                    inserted, failed, formatDuration(duration)
            );
        }

        private static String formatDuration(Duration duration) {
            long minutes = duration.toMinutes();
            long seconds = duration.toSecondsPart();
            return String.format("%dm %ds", minutes, seconds);
        }
    }
}
