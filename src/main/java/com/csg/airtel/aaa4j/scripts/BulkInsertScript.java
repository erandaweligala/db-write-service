package com.csg.airtel.aaa4j.scripts;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.vertx.mutiny.sqlclient.Pool;
import io.vertx.mutiny.sqlclient.Row;
import io.vertx.mutiny.sqlclient.SqlResult;
import io.vertx.mutiny.sqlclient.Tuple;
import com.csg.airtel.aaa4j.infrastructure.CsvExportUtil;
import com.csg.airtel.aaa4j.infrastructure.CsvExportUtil.CsvExportResult;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
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

    // Data generation constants
    private static final String[] NAS_PORT_TYPES = {"Ethernet", "Wireless-802.11", "Virtual", "Async", "ISDN-Sync", "ISDN-Async-V120", "ISDN-Async-V110", "DSL"};
    private static final String[] STATUSES = {"ACTIVE", "SUSPENDED", "INACTIVE"};
    private static final String[] SUBSCRIPTIONS = {"PREPAID", "POSTPAID", "HYBRID"};
    private static final String[] ENCRYPTION_METHODS = {"WPA2", "WPA3", "AES-256", "TLS1.3", "NONE"};
    private static final String[] IP_POOL_NAMES = {"POOL_RESIDENTIAL", "POOL_BUSINESS", "POOL_ENTERPRISE", "POOL_MOBILE", "POOL_DEFAULT"};
    private static final String[] BANDWIDTHS = {"10Mbps", "50Mbps", "100Mbps", "200Mbps", "500Mbps", "1Gbps"};
    private static final String[] BILLING_TYPES = {"MONTHLY", "QUARTERLY", "YEARLY", "USAGE_BASED"};
    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    // Thread-safe sets for ensuring uniqueness
    private final Set<String> usedUserNames = ConcurrentHashMap.newKeySet();
    private final Set<String> usedRequestIds = ConcurrentHashMap.newKeySet();
    private final Set<String> usedMacAddresses = ConcurrentHashMap.newKeySet();

    private final Pool client;
    private final CsvExportUtil csvExportUtil;
    private final Random random = new Random();

    @Inject
    public BulkInsertScript(Pool client, CsvExportUtil csvExportUtil) {
        this.client = client;
        this.csvExportUtil = csvExportUtil;
    }

    /**
     * Clear uniqueness tracking sets before new bulk insert
     */
    public void resetUniquenessTracking() {
        usedUserNames.clear();
        usedRequestIds.clear();
        usedMacAddresses.clear();
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

        // Reset uniqueness tracking for new bulk insert
        resetUniquenessTracking();

        Instant startTime = Instant.now();
        AtomicInteger insertedCount = new AtomicInteger(0);
        AtomicInteger failedCount = new AtomicInteger(0);
        AtomicLong lastProgressTime = new AtomicLong(startTime.toEpochMilli());
        AtomicInteger lastProgressCount = new AtomicInteger(0);

        int totalBatches = (totalRecords + batchSize - 1) / batchSize;

        return Multi.createFrom().range(0, totalBatches)
                .capDemandsTo(CONCURRENT_BATCHES)
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
                })
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
     * Schema: USER_ID, BANDWIDTH, BILLING, BILLING_ACCOUNT_REF, CIRCUIT_ID, CONCURRENCY,
     *         CONTACT_EMAIL, CONTACT_NAME, CONTACT_NUMBER, CREATED_DATE, CUSTOM_TIMEOUT,
     *         CYCLE_DATE, ENCRYPTION_METHOD, GROUP_ID, IDLE_TIMEOUT, IP_ALLOCATION, IP_POOL_NAME,
     *         IPV4, IPV6, MAC_ADDRESS, NAS_PORT_TYPE, PASSWORD, REMOTE_ID, REQUEST_ID,
     *         SESSION_TIMEOUT, STATUS, UPDATED_DATE, USER_NAME, VLAN_ID, NAS_IP_ADDRESS, SUBSCRIPTION
     */
    private Uni<Integer> insertBatch(String tableName, int startIndex, int batchSize) {
        // Build multi-row INSERT statement for Oracle
        StringBuilder sql = new StringBuilder();
        sql.append("INSERT ALL ");

        String columns = "(USER_ID, BANDWIDTH, BILLING, BILLING_ACCOUNT_REF, CIRCUIT_ID, CONCURRENCY, " +
                "CONTACT_EMAIL, CONTACT_NAME, CONTACT_NUMBER, CREATED_DATE, CUSTOM_TIMEOUT, " +
                "CYCLE_DATE, ENCRYPTION_METHOD, GROUP_ID, IDLE_TIMEOUT, IP_ALLOCATION, IP_POOL_NAME, " +
                "IPV4, IPV6, MAC_ADDRESS, NAS_PORT_TYPE, PASSWORD, REMOTE_ID, REQUEST_ID, " +
                "SESSION_TIMEOUT, STATUS, UPDATED_DATE, USER_NAME, VLAN_ID, NAS_IP_ADDRESS, SUBSCRIPTION)";

        String placeholders = "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

        List<Object> values = new ArrayList<>(batchSize * 31);

        for (int i = 0; i < batchSize; i++) {
            int recordId = startIndex + i + 1;
            sql.append("INTO ").append(tableName).append(" ").append(columns).append(" VALUES ").append(placeholders).append(" ");

            // Generate unique values
            String userName = generateUniqueUserName(recordId);
            String requestId = generateUniqueRequestId(recordId);
            String macAddress = generateUniqueMacAddress(recordId);

            // Generate data for each record
            values.add(recordId);                                                          // USER_ID (primary key)
            values.add(BANDWIDTHS[random.nextInt(BANDWIDTHS.length)]);                    // BANDWIDTH
            values.add("3");              // BILLING
            values.add("BA-" + String.format("%010d", recordId));                         // BILLING_ACCOUNT_REF
            values.add("CKT-" + String.format("%08d", random.nextInt(100000000)));        // CIRCUIT_ID
            values.add(random.nextInt(10) + 1);                                           // CONCURRENCY (1-10)
            values.add(userName.toLowerCase() + "@telco.com");                            // CONTACT_EMAIL
            values.add(generateContactName(recordId));                                     // CONTACT_NAME
            values.add(generatePhoneNumber());                                             // CONTACT_NUMBER
            values.add(LocalDateTime.now());                                               // CREATED_DATE
            values.add(random.nextInt(3600) + 60);                                        // CUSTOM_TIMEOUT (60-3660 seconds)
            values.add(8);                                           // CYCLE_DATE (1-28)
            values.add(ENCRYPTION_METHODS[random.nextInt(ENCRYPTION_METHODS.length)]);    // ENCRYPTION_METHOD
            values.add("GRP-" + String.format("%05d", random.nextInt(10000)));            // GROUP_ID
            values.add(random.nextInt(1800) + 300);                                       // IDLE_TIMEOUT (300-2100 seconds)
            values.add(generateIPAllocation());                                            // IP_ALLOCATION
            values.add(IP_POOL_NAMES[random.nextInt(IP_POOL_NAMES.length)]);              // IP_POOL_NAME
            values.add(generateIPv4());                                                    // IPV4
            values.add(generateIPv6());                                                    // IPV6
            values.add(macAddress);                                                        // MAC_ADDRESS (unique)
            values.add(NAS_PORT_TYPES[random.nextInt(NAS_PORT_TYPES.length)]);            // NAS_PORT_TYPE
            values.add(generatePassword(macAddress));                                      // PASSWORD (30% MAC, 30% PAP, 40% CHAP)
            values.add("REM-" + UUID.randomUUID().toString().substring(0, 8));            // REMOTE_ID
            values.add(requestId);                                                         // REQUEST_ID (unique key)
            values.add(random.nextInt(86400) + 3600);                                     // SESSION_TIMEOUT (3600-90000 seconds)
            values.add(STATUSES[random.nextInt(STATUSES.length)]);                        // STATUS (ACTIVE, SUSPENDED, INACTIVE)
            values.add(LocalDateTime.now());                                               // UPDATED_DATE
            values.add(userName);                                                          // USER_NAME (unique key)
            values.add(random.nextInt(4094) + 1);                                         // VLAN_ID (1-4094)
            values.add(generateNasIpAddress());                                            // NAS_IP_ADDRESS
            values.add(SUBSCRIPTIONS[random.nextInt(SUBSCRIPTIONS.length)]);              // SUBSCRIPTION (PREPAID, POSTPAID, HYBRID)
        }

        sql.append("SELECT * FROM DUAL");

        Tuple tuple = Tuple.from(values);

        return client.preparedQuery(sql.toString())
                .execute(tuple)
                .map(result -> batchSize)
                .onFailure().retry().atMost(3)
                .onFailure().recoverWithItem(0);
    }

    // ================== Data Generation Helper Methods ==================

    /**
     * Generate unique USER_NAME
     */
    private String generateUniqueUserName(int recordId) {
        String userName = "USER_" + String.format("%08d", recordId);
        usedUserNames.add(userName);
        return userName;
    }

    /**
     * Generate unique REQUEST_ID
     */
    private String generateUniqueRequestId(int recordId) {
        String requestId = "REQ-" + UUID.randomUUID().toString().replace("-", "").substring(0, 16).toUpperCase();
        usedRequestIds.add(requestId);
        return requestId;
    }

    /**
     * Generate unique MAC_ADDRESS
     */
    private String generateUniqueMacAddress(int recordId) {
        // Generate MAC from record ID to ensure uniqueness
        String hexId = String.format("%012X", recordId);
        String macAddress = hexId.substring(0, 2) + ":" +
                           hexId.substring(2, 4) + ":" +
                           hexId.substring(4, 6) + ":" +
                           hexId.substring(6, 8) + ":" +
                           hexId.substring(8, 10) + ":" +
                           hexId.substring(10, 12);
        usedMacAddresses.add(macAddress);
        return macAddress;
    }

    /**
     * Generate PASSWORD based on distribution: MAC=30%, PAP=30%, CHAP=40%
     */

    private String generatePassword(String macAddress) {
        int choice = random.nextInt(100);
        if (choice < 30) {
            // 30% MAC-based password (use MAC address without colons)
            return "MAC:" + macAddress.replace(":", "");
        } else if (choice < 60) {
            // 30% PAP (Password Authentication Protocol)
            return "PAP:" + UUID.randomUUID().toString().substring(0, 12);
        } else {
            // 40% CHAP (Challenge-Handshake Authentication Protocol)
            // Note: CHAP passwords are hashed with MD5 during CSV export (see CsvExportUtil)
            return "CHAP:" + UUID.randomUUID().toString().substring(0, 16);
        }
    }

    /**
     * Generate random contact name
     */
    private String generateContactName(int recordId) {
        String[] firstNames = {"John", "Jane", "Robert", "Emily", "Michael", "Sarah", "David", "Lisa", "James", "Mary",
                               "William", "Patricia", "Richard", "Jennifer", "Joseph", "Linda", "Thomas", "Elizabeth"};
        String[] lastNames = {"Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis", "Rodriguez",
                              "Martinez", "Hernandez", "Lopez", "Gonzalez", "Wilson", "Anderson", "Thomas", "Taylor"};
        return firstNames[recordId % firstNames.length] + " " + lastNames[(recordId / firstNames.length) % lastNames.length];
    }

    /**
     * Generate random phone number
     */
    private String generatePhoneNumber() {
        return String.format("+1-%03d-%03d-%04d",
                random.nextInt(900) + 100,
                random.nextInt(900) + 100,
                random.nextInt(10000));
    }

    /**
     * Generate IP allocation type
     */
    private String generateIPAllocation() {
        String[] allocations = {"STATIC", "DYNAMIC", "DHCP", "PPPoE"};
        return allocations[random.nextInt(allocations.length)];
    }

    /**
     * Generate random IPv4 address (private ranges)
     */
    private String generateIPv4() {
        // Use private IP ranges
        int choice = random.nextInt(3);
        switch (choice) {
            case 0: // 10.x.x.x
                return String.format("10.%d.%d.%d", random.nextInt(256), random.nextInt(256), random.nextInt(254) + 1);
            case 1: // 172.16.x.x - 172.31.x.x
                return String.format("172.%d.%d.%d", random.nextInt(16) + 16, random.nextInt(256), random.nextInt(254) + 1);
            default: // 192.168.x.x
                return String.format("192.168.%d.%d", random.nextInt(256), random.nextInt(254) + 1);
        }
    }

    /**
     * Generate random IPv6 address
     */
    private String generateIPv6() {
        return String.format("2001:db8:%04x:%04x:%04x:%04x:%04x:%04x",
                random.nextInt(65536), random.nextInt(65536),
                random.nextInt(65536), random.nextInt(65536),
                random.nextInt(65536), random.nextInt(65536));
    }

    /**
     * Generate NAS IP address
     */
    private String generateNasIpAddress() {
        // NAS typically in specific ranges
        return String.format("10.0.%d.%d", random.nextInt(256), random.nextInt(254) + 1);
    }

    /**
     * Alternative method using single-row inserts with batched execution
     * This may be more compatible with some Oracle configurations
     */
    public Uni<BulkInsertResult> executeBulkInsertSingleRow(String tableName, int totalRecords, int batchSize) {
        log.infof("Starting single-row bulk insert: table=%s, totalRecords=%d, batchSize=%d",
                tableName, totalRecords, batchSize);

        // Reset uniqueness tracking for new bulk insert
        resetUniquenessTracking();

        Instant startTime = Instant.now();
        AtomicInteger insertedCount = new AtomicInteger(0);
        AtomicInteger failedCount = new AtomicInteger(0);

        String sql = String.format(
                "INSERT INTO %s (USER_ID, BANDWIDTH, BILLING, BILLING_ACCOUNT_REF, CIRCUIT_ID, CONCURRENCY, " +
                "CONTACT_EMAIL, CONTACT_NAME, CONTACT_NUMBER, CREATED_DATE, CUSTOM_TIMEOUT, " +
                "CYCLE_DATE, ENCRYPTION_METHOD, GROUP_ID, IDLE_TIMEOUT, IP_ALLOCATION, IP_POOL_NAME, " +
                "IPV4, IPV6, MAC_ADDRESS, NAS_PORT_TYPE, PASSWORD, REMOTE_ID, REQUEST_ID, " +
                "SESSION_TIMEOUT, STATUS, UPDATED_DATE, USER_NAME, VLAN_ID, NAS_IP_ADDRESS, SUBSCRIPTION) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                tableName
        );

        return Multi.createFrom().range(0, totalRecords)
                .group().intoLists().of(batchSize)
                .capDemandsTo(CONCURRENT_BATCHES)
                .onItem().transformToUniAndMerge(batch -> {
                    List<Tuple> tuples = new ArrayList<>(batch.size());

                    for (Integer index : batch) {
                        int recordId = index + 1;
                        String userName = generateUniqueUserName(recordId);
                        String requestId = generateUniqueRequestId(recordId);
                        String macAddress = generateUniqueMacAddress(recordId);

                        List<Object> values = new ArrayList<>(31);
                        values.add(recordId);                                                      // USER_ID
                        values.add(BANDWIDTHS[random.nextInt(BANDWIDTHS.length)]);                // BANDWIDTH
                        values.add(3);          // BILLING
                        values.add("BA-" + String.format("%010d", recordId));                     // BILLING_ACCOUNT_REF
                        values.add("CKT-" + String.format("%08d", random.nextInt(100000000)));    // CIRCUIT_ID
                        values.add(random.nextInt(10) + 1);                                       // CONCURRENCY
                        values.add(userName.toLowerCase() + "@telco.com");                        // CONTACT_EMAIL
                        values.add(generateContactName(recordId));                                 // CONTACT_NAME
                        values.add(generatePhoneNumber());                                         // CONTACT_NUMBER
                        values.add(LocalDateTime.now());                                           // CREATED_DATE
                        values.add(random.nextInt(3600) + 60);                                    // CUSTOM_TIMEOUT
                        values.add(8);                                       // CYCLE_DATE
                        values.add(ENCRYPTION_METHODS[random.nextInt(ENCRYPTION_METHODS.length)]); // ENCRYPTION_METHOD
                        values.add("GRP-" + String.format("%05d", random.nextInt(10000)));        // GROUP_ID
                        values.add(random.nextInt(1800) + 300);                                   // IDLE_TIMEOUT
                        values.add(generateIPAllocation());                                        // IP_ALLOCATION
                        values.add(IP_POOL_NAMES[random.nextInt(IP_POOL_NAMES.length)]);          // IP_POOL_NAME
                        values.add(generateIPv4());                                                // IPV4
                        values.add(generateIPv6());                                                // IPV6
                        values.add(macAddress);                                                    // MAC_ADDRESS
                        values.add(NAS_PORT_TYPES[random.nextInt(NAS_PORT_TYPES.length)]);        // NAS_PORT_TYPE
                        values.add(generatePassword(macAddress));                                  // PASSWORD
                        values.add("REM-" + UUID.randomUUID().toString().substring(0, 8));        // REMOTE_ID
                        values.add(requestId);                                                     // REQUEST_ID
                        values.add(random.nextInt(86400) + 3600);                                 // SESSION_TIMEOUT
                        values.add(STATUSES[random.nextInt(STATUSES.length)]);                    // STATUS
                        values.add(LocalDateTime.now());                                           // UPDATED_DATE
                        values.add(userName);                                                      // USER_NAME
                        values.add(random.nextInt(4094) + 1);                                     // VLAN_ID
                        values.add(generateNasIpAddress());                                        // NAS_IP_ADDRESS
                        values.add(SUBSCRIPTIONS[random.nextInt(SUBSCRIPTIONS.length)]);          // SUBSCRIPTION

                        tuples.add(Tuple.from(values));
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
                })
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
     * Execute bulk insert and then export data to CSV file.
     * CHAP passwords are hashed with MD5 in the exported CSV.
     *
     * @param tableName The target table name
     * @param totalRecords Total number of records to insert
     * @param batchSize Number of records per batch
     * @param outputDir The directory to write the CSV file (null for temp directory)
     * @return Uni containing combined result of insert and export
     */
    public Uni<BulkInsertWithExportResult> executeBulkInsertWithCsvExport(String tableName, int totalRecords,
                                                                           int batchSize, String outputDir) {
        log.infof("Starting bulk insert with CSV export: table=%s, totalRecords=%d, batchSize=%d",
                tableName, totalRecords, batchSize);

        return executeBulkInsert(tableName, totalRecords, batchSize)
                .chain(insertResult -> {
                    log.infof("Bulk insert completed, starting CSV export...");
                    return csvExportUtil.exportToCsv(tableName, outputDir)
                            .map(exportResult -> new BulkInsertWithExportResult(insertResult, exportResult));
                });
    }

    /**
     * Export existing data from table to CSV file.
     * CHAP passwords are hashed with MD5 in the exported CSV.
     *
     * @param tableName The source table name
     * @param outputDir The directory to write the CSV file (null for temp directory)
     * @return Uni containing the export result
     */
    public Uni<CsvExportResult> exportToCsv(String tableName, String outputDir) {
        return csvExportUtil.exportToCsv(tableName, outputDir);
    }

    /**
     * Create the target table if it doesn't exist
     * Schema includes all USER fields with proper constraints:
     * - USER_ID: Primary Key
     * - USER_NAME: Unique Key
     * - REQUEST_ID: Unique Key
     * - MAC_ADDRESS: Unique
     * - STATUS: CHECK constraint (ACTIVE, SUSPENDED, INACTIVE)
     * - SUBSCRIPTION: CHECK constraint (PREPAID, POSTPAID, HYBRID)
     */
    public Uni<Void> createTableIfNotExists(String tableName) {
        String createTableSql = String.format("""
                BEGIN
                    EXECUTE IMMEDIATE 'CREATE TABLE %s (
                        USER_ID NUMBER PRIMARY KEY,
                        BANDWIDTH VARCHAR2(50),
                        BILLING VARCHAR2(50),
                        BILLING_ACCOUNT_REF VARCHAR2(100),
                        CIRCUIT_ID VARCHAR2(50),
                        CONCURRENCY NUMBER(5),
                        CONTACT_EMAIL VARCHAR2(255),
                        CONTACT_NAME VARCHAR2(200),
                        CONTACT_NUMBER VARCHAR2(50),
                        CREATED_DATE TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        CUSTOM_TIMEOUT NUMBER(10),
                        CYCLE_DATE NUMBER(2),
                        ENCRYPTION_METHOD VARCHAR2(50),
                        GROUP_ID VARCHAR2(50),
                        IDLE_TIMEOUT NUMBER(10),
                        IP_ALLOCATION VARCHAR2(50),
                        IP_POOL_NAME VARCHAR2(100),
                        IPV4 VARCHAR2(15),
                        IPV6 VARCHAR2(45),
                        MAC_ADDRESS VARCHAR2(17) UNIQUE,
                        NAS_PORT_TYPE VARCHAR2(50),
                        PASSWORD VARCHAR2(255),
                        REMOTE_ID VARCHAR2(100),
                        REQUEST_ID VARCHAR2(50) UNIQUE,
                        SESSION_TIMEOUT NUMBER(10),
                        STATUS VARCHAR2(20) CHECK (STATUS IN (''ACTIVE'', ''SUSPENDED'', ''INACTIVE'')),
                        UPDATED_DATE TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        USER_NAME VARCHAR2(100) UNIQUE NOT NULL,
                        VLAN_ID NUMBER(5),
                        NAS_IP_ADDRESS VARCHAR2(15),
                        SUBSCRIPTION VARCHAR2(20) CHECK (SUBSCRIPTION IN (''PREPAID'', ''POSTPAID'', ''HYBRID''))
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
     * Create indexes for better query performance
     */
    public Uni<Void> createIndexes(String tableName) {
        String createIndexSql = String.format("""
                BEGIN
                    EXECUTE IMMEDIATE 'CREATE INDEX IDX_%s_STATUS ON %s (STATUS)';
                EXCEPTION
                    WHEN OTHERS THEN
                        IF SQLCODE = -955 THEN NULL; -- Index already exists
                        ELSE RAISE;
                        END IF;
                END;
                """, tableName, tableName);

        String createIndexSql2 = String.format("""
                BEGIN
                    EXECUTE IMMEDIATE 'CREATE INDEX IDX_%s_SUBSCRIPTION ON %s (SUBSCRIPTION)';
                EXCEPTION
                    WHEN OTHERS THEN
                        IF SQLCODE = -955 THEN NULL; -- Index already exists
                        ELSE RAISE;
                        END IF;
                END;
                """, tableName, tableName);

        String createIndexSql3 = String.format("""
                BEGIN
                    EXECUTE IMMEDIATE 'CREATE INDEX IDX_%s_NAS_PORT_TYPE ON %s (NAS_PORT_TYPE)';
                EXCEPTION
                    WHEN OTHERS THEN
                        IF SQLCODE = -955 THEN NULL; -- Index already exists
                        ELSE RAISE;
                        END IF;
                END;
                """, tableName, tableName);

        return client.query(createIndexSql)
                .execute()
                .chain(() -> client.query(createIndexSql2).execute())
                .chain(() -> client.query(createIndexSql3).execute())
                .replaceWithVoid()
                .onItem().invoke(() -> log.infof("Indexes created for table %s", tableName))
                .onFailure().invoke(e -> log.warnf("Some indexes may already exist for table %s: %s", tableName, e.getMessage()));
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

    /**
     * Combined result record for bulk insert with CSV export operations.
     */
    public record BulkInsertWithExportResult(
            BulkInsertResult insertResult,
            CsvExportResult exportResult
    ) {
        @Override
        public String toString() {
            return String.format(
                    "BulkInsertWithExportResult{insert=%s, export=%s}",
                    insertResult, exportResult
            );
        }
    }
}
