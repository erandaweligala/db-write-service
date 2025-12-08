package com.csg.airtel.aaa4j.scripts;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.vertx.mutiny.sqlclient.Pool;
import io.vertx.mutiny.sqlclient.Row;
import io.vertx.mutiny.sqlclient.SqlResult;
import io.vertx.mutiny.sqlclient.Tuple;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Bulk Insert Script for inserting 1,000,000 records into the database.
 *
 * Features:
 * - Batch processing for optimal performance (configurable batch size)
 * - Progress reporting every N records
 * - Performance metrics (records/second, elapsed time)
 * - Configurable table and column structure
 * - Error handling and retry logic
 *
 * Usage:
 * - Inject this bean and call executeBulkInsert()
 * - Or use via REST endpoint at /api/bulk-insert
 */
@ApplicationScoped
public class BulkInsertScript {

    private static final Logger log = Logger.getLogger(BulkInsertScript.class);

    // Configuration constants
    private static final int TOTAL_RECORDS = 1_000_000;
    private static final int BATCH_SIZE = 1000;           // Records per batch
    private static final int PROGRESS_INTERVAL = 10_000;  // Log progress every N records
    private static final int CONCURRENT_BATCHES = 10;     // Number of concurrent batch executions

    private final Pool client;

    @Inject
    public BulkInsertScript(Pool client) {
        this.client = client;
    }

    /**
     * Execute bulk insert of 1,000,000 records
     *
     * @param tableName The target table name
     * @return Uni containing the result summary
     */
    public Uni<BulkInsertResult> executeBulkInsert(String tableName) {
        return executeBulkInsert(tableName, TOTAL_RECORDS, BATCH_SIZE);
    }

    /**
     * Execute bulk insert with custom parameters
     *
     * @param tableName The target table name
     * @param totalRecords Total number of records to insert
     * @param batchSize Number of records per batch
     * @return Uni containing the result summary
     */
    public Uni<BulkInsertResult> executeBulkInsert(String tableName, int totalRecords, int batchSize) {
        log.infof("Starting bulk insert: table=%s, totalRecords=%d, batchSize=%d",
                tableName, totalRecords, batchSize);

        Instant startTime = Instant.now();
        AtomicInteger insertedCount = new AtomicInteger(0);
        AtomicInteger failedCount = new AtomicInteger(0);
        AtomicLong lastProgressTime = new AtomicLong(startTime.toEpochMilli());
        AtomicInteger lastProgressCount = new AtomicInteger(0);

        int totalBatches = (totalRecords + batchSize - 1) / batchSize;

        return Multi.createFrom().range(0, totalBatches)
                .onItem().transformToUniAndMerge(batchIndex -> {
                    int startIndex = batchIndex * batchSize;
                    int endIndex = Math.min(startIndex + batchSize, totalRecords);
                    int currentBatchSize = endIndex - startIndex;

                    return insertBatch(tableName, startIndex, currentBatchSize)
                            .onItem().invoke(rowCount -> {
                                int total = insertedCount.addAndGet(rowCount);

                                // Log progress at intervals
                                if (total - lastProgressCount.get() >= PROGRESS_INTERVAL) {
                                    long currentTime = System.currentTimeMillis();
                                    long elapsed = currentTime - lastProgressTime.get();
                                    int recordsSinceLastLog = total - lastProgressCount.get();
                                    double rps = elapsed > 0 ? (recordsSinceLastLog * 1000.0 / elapsed) : 0;

                                    Duration totalElapsed = Duration.between(startTime, Instant.now());
                                    double overallRps = total * 1000.0 / totalElapsed.toMillis();
                                    double percentComplete = (total * 100.0) / totalRecords;

                                    log.infof("Progress: %d/%d (%.1f%%) | Current: %.0f rec/s | Overall: %.0f rec/s | Elapsed: %s",
                                            total, totalRecords, percentComplete, rps, overallRps,
                                            formatDuration(totalElapsed));

                                    lastProgressTime.set(currentTime);
                                    lastProgressCount.set(total);
                                }
                            })
                            .onFailure().invoke(e -> {
                                failedCount.addAndGet(currentBatchSize);
                                log.errorf(e, "Batch %d failed: %s", batchIndex, e.getMessage());
                            })
                            .onFailure().recoverWithItem(0);
                }, CONCURRENT_BATCHES)
                .collect().asList()
                .map(results -> {
                    Duration totalDuration = Duration.between(startTime, Instant.now());
                    int totalInserted = insertedCount.get();
                    int totalFailed = failedCount.get();
                    double recordsPerSecond = totalInserted * 1000.0 / totalDuration.toMillis();

                    BulkInsertResult result = new BulkInsertResult(
                            tableName,
                            totalRecords,
                            totalInserted,
                            totalFailed,
                            totalDuration,
                            recordsPerSecond
                    );

                    log.infof("Bulk insert completed: %s", result);
                    return result;
                });
    }

    /**
     * Insert a batch of records using multi-row INSERT statement
     */
    private Uni<Integer> insertBatch(String tableName, int startIndex, int batchSize) {
        // Build multi-row INSERT statement for Oracle
        // INSERT ALL INTO table VALUES (...) INTO table VALUES (...) SELECT * FROM DUAL
        StringBuilder sql = new StringBuilder();
        sql.append("INSERT ALL ");

        List<Object> values = new ArrayList<>(batchSize * 6);

        for (int i = 0; i < batchSize; i++) {
            int recordId = startIndex + i + 1;
            sql.append("INTO ").append(tableName)
               .append(" (ID, USER_NAME, EMAIL, STATUS, AMOUNT, CREATED_AT) VALUES (?, ?, ?, ?, ?, ?) ");

            // Generate sample data for each record
            values.add(recordId);                                          // ID
            values.add("user_" + recordId);                               // USER_NAME
            values.add("user" + recordId + "@example.com");               // EMAIL
            values.add(recordId % 3 == 0 ? "ACTIVE" : "PENDING");        // STATUS
            values.add(Math.random() * 10000);                           // AMOUNT (0-10000)
            values.add(LocalDateTime.now());                              // CREATED_AT
        }

        sql.append("SELECT * FROM DUAL");

        Tuple tuple = Tuple.from(values);

        return client.preparedQuery(sql.toString())
                .execute(tuple)
                .map(result -> batchSize)
                .onFailure().retry().atMost(3)
                .onFailure().recoverWithItem(0);
    }

    /**
     * Alternative method using single-row inserts with batched execution
     * This may be more compatible with some Oracle configurations
     */
    public Uni<BulkInsertResult> executeBulkInsertSingleRow(String tableName, int totalRecords, int batchSize) {
        log.infof("Starting single-row bulk insert: table=%s, totalRecords=%d, batchSize=%d",
                tableName, totalRecords, batchSize);

        Instant startTime = Instant.now();
        AtomicInteger insertedCount = new AtomicInteger(0);
        AtomicInteger failedCount = new AtomicInteger(0);

        String sql = String.format(
                "INSERT INTO %s (ID, USER_NAME, EMAIL, STATUS, AMOUNT, CREATED_AT) VALUES (?, ?, ?, ?, ?, ?)",
                tableName
        );

        return Multi.createFrom().range(0, totalRecords)
                .group().intoLists().of(batchSize)
                .onItem().transformToUniAndMerge(batch -> {
                    List<Tuple> tuples = new ArrayList<>(batch.size());

                    for (Integer index : batch) {
                        int recordId = index + 1;
                        tuples.add(Tuple.of(
                                recordId,
                                "user_" + recordId,
                                "user" + recordId + "@example.com",
                                recordId % 3 == 0 ? "ACTIVE" : "PENDING",
                                Math.random() * 10000,
                                LocalDateTime.now()
                        ));
                    }

                    return client.preparedQuery(sql)
                            .executeBatch(tuples)
                            .map(SqlResult::rowCount)
                            .onItem().invoke(rowCount -> {
                                int total = insertedCount.addAndGet(batch.size());
                                if (total % PROGRESS_INTERVAL == 0) {
                                    Duration elapsed = Duration.between(startTime, Instant.now());
                                    double rps = total * 1000.0 / elapsed.toMillis();
                                    log.infof("Progress: %d/%d (%.1f%%) | %.0f rec/s",
                                            total, totalRecords, (total * 100.0 / totalRecords), rps);
                                }
                            })
                            .onFailure().invoke(e -> {
                                failedCount.addAndGet(batch.size());
                                log.errorf(e, "Batch insert failed: %s", e.getMessage());
                            })
                            .onFailure().recoverWithItem(0);
                }, CONCURRENT_BATCHES)
                .collect().asList()
                .map(results -> {
                    Duration totalDuration = Duration.between(startTime, Instant.now());
                    int totalInserted = insertedCount.get();
                    int totalFailed = failedCount.get();
                    double recordsPerSecond = totalInserted * 1000.0 / totalDuration.toMillis();

                    return new BulkInsertResult(
                            tableName,
                            totalRecords,
                            totalInserted,
                            totalFailed,
                            totalDuration,
                            recordsPerSecond
                    );
                });
    }

    /**
     * Create the target table if it doesn't exist
     */
    public Uni<Void> createTableIfNotExists(String tableName) {
        String createTableSql = String.format("""
                BEGIN
                    EXECUTE IMMEDIATE 'CREATE TABLE %s (
                        ID NUMBER PRIMARY KEY,
                        USER_NAME VARCHAR2(100) NOT NULL,
                        EMAIL VARCHAR2(255) NOT NULL,
                        STATUS VARCHAR2(20) DEFAULT ''PENDING'',
                        AMOUNT NUMBER(15,2),
                        CREATED_AT TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )';
                EXCEPTION
                    WHEN OTHERS THEN
                        IF SQLCODE = -955 THEN NULL; -- Table already exists
                        ELSE RAISE;
                        END IF;
                END;
                """, tableName);

        return client.query(createTableSql)
                .execute()
                .replaceWithVoid()
                .onItem().invoke(() -> log.infof("Table %s created or already exists", tableName))
                .onFailure().invoke(e -> log.errorf(e, "Failed to create table %s", tableName));
    }

    /**
     * Truncate the table before inserting (optional cleanup)
     */
    public Uni<Void> truncateTable(String tableName) {
        return client.query("TRUNCATE TABLE " + tableName)
                .execute()
                .replaceWithVoid()
                .onItem().invoke(() -> log.infof("Table %s truncated", tableName))
                .onFailure().invoke(e -> log.warnf("Failed to truncate table %s: %s", tableName, e.getMessage()));
    }

    /**
     * Get the current record count in the table
     */
    public Uni<Long> getRecordCount(String tableName) {
        return client.query("SELECT COUNT(*) FROM " + tableName)
                .execute()
                .map(rows -> {
                    Row row = rows.iterator().next();
                    return row.getLong(0);
                });
    }

    private String formatDuration(Duration duration) {
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

    /**
     * Result record for bulk insert operations
     */
    public record BulkInsertResult(
            String tableName,
            int totalRequested,
            int totalInserted,
            int totalFailed,
            Duration duration,
            double recordsPerSecond
    ) {
        @Override
        public String toString() {
            return String.format(
                    "BulkInsertResult{table='%s', requested=%d, inserted=%d, failed=%d, duration=%s, rps=%.0f}",
                    tableName, totalRequested, totalInserted, totalFailed,
                    formatDuration(duration), recordsPerSecond
            );
        }

        private static String formatDuration(Duration duration) {
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
    }
}
