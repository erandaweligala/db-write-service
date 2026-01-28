package com.csg.airtel.aaa4j.infrastructure;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.vertx.mutiny.sqlclient.Pool;
import io.vertx.mutiny.sqlclient.Row;
import io.vertx.mutiny.sqlclient.RowSet;
import io.vertx.mutiny.sqlclient.Tuple;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Utility class for exporting database records to CSV files.
 *
 * Features:
 * - Exports USER_DATA table to CSV format
 * - Applies MD5 hashing to CHAP passwords before export
 * - Supports batch processing for large datasets
 * - Progress tracking and reporting
 */
@ApplicationScoped
public class CsvExportUtil {

    private static final Logger log = Logger.getLogger(CsvExportUtil.class);
    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd_HH-mm-ss");
    private static final DateTimeFormatter CSV_DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    private static final int BATCH_SIZE = 5000;
    private static final int PROGRESS_INTERVAL = 50000;



    private static final Pattern MD5_PATTERN =
            Pattern.compile("^[a-fA-F0-9]{32}$");

    // HEX (not MD5) → only hex chars, length != 32
    private static final Pattern HEX_PATTERN =
            Pattern.compile("^[a-fA-F0-9]+$");

    private static final String[] CSV_HEADERS = {
            "USER_ID", "BANDWIDTH", "BILLING", "BILLING_ACCOUNT_REF", "CIRCUIT_ID", "CONCURRENCY",
            "CONTACT_EMAIL", "CONTACT_NAME", "CONTACT_NUMBER", "CREATED_DATE", "CUSTOM_TIMEOUT",
            "CYCLE_DATE", "ENCRYPTION_METHOD", "GROUP_ID", "IDLE_TIMEOUT", "IP_ALLOCATION", "IP_POOL_NAME",
            "IPV4", "IPV6", "MAC_ADDRESS", "NAS_PORT_TYPE", "PASSWORD", "REMOTE_ID", "REQUEST_ID",
            "SESSION_TIMEOUT", "STATUS", "UPDATED_DATE", "USER_NAME", "VLAN_ID", "NAS_IP_ADDRESS", "SUBSCRIPTION"
    };

    private final Pool client;
    private final MD5HashUtil md5HashUtil;

    @Inject
    public CsvExportUtil(Pool client, MD5HashUtil md5HashUtil) {
        this.client = client;
        this.md5HashUtil = md5HashUtil;
    }

    /**
     * Export all records from the specified table to a CSV file.
     * CHAP passwords are hashed with MD5 before export.
     *
     * @param tableName The source table name
     * @param outputDir The directory to write the CSV file (null for temp directory)
     * @return Uni containing the result with file path and record count
     */

    // todo need to export Data from DB
    public Uni<CsvExportResult> exportToCsv(String tableName, String outputDir) {
        String timestamp = LocalDateTime.now().format(DATE_FORMATTER);
        String fileName = String.format("%s_export_%s.csv", tableName, timestamp);

        Path outputPath;
        try {
            if (outputDir != null && !outputDir.isEmpty()) {
                Path dirPath = Paths.get(outputDir);
                Files.createDirectories(dirPath);
                outputPath = dirPath.resolve(fileName);
            } else {
                Path tempDir = Files.createTempDirectory("csv_export");
                outputPath = tempDir.resolve(fileName);
            }
        } catch (IOException e) {
            log.errorf(e, "Failed to create output directory");
            return Uni.createFrom().failure(e);
        }

        log.infof("Starting CSV export: table=%s, output=%s", tableName, outputPath);

        AtomicInteger exportedCount = new AtomicInteger(0);
        long startTime = System.currentTimeMillis();

        return getRecordCount(tableName)
                .chain(totalRecords -> {
                    log.infof("Total records to export: %d", totalRecords);

                    return exportRecordsInBatches(tableName, outputPath, totalRecords, exportedCount, startTime)
                            .map(count -> {
                                long duration = System.currentTimeMillis() - startTime;
                                double recordsPerSecond = count > 0 ? (count * 1000.0 / duration) : 0;

                                CsvExportResult result = new CsvExportResult(
                                        outputPath.toString(),
                                        count,
                                        duration,
                                        recordsPerSecond
                                );

                                log.infof("CSV export completed: %s", result);
                                return result;
                            });
                });
    }

    /**
     * Export records in batches to handle large datasets efficiently.
     */
    private Uni<Integer> exportRecordsInBatches(String tableName, Path outputPath,
                                                 long totalRecords, AtomicInteger exportedCount, long startTime) {
        try {
            BufferedWriter writer = Files.newBufferedWriter(outputPath);

            // Write CSV header
            writer.write(String.join(",", CSV_HEADERS));
            writer.newLine();

            int totalBatches = (int) ((totalRecords + BATCH_SIZE - 1) / BATCH_SIZE);

            return Multi.createFrom().range(0, totalBatches)
                    .onItem().transformToUniAndConcatenate(batchIndex -> {
                        int offset = batchIndex * BATCH_SIZE;
                        return fetchBatch(tableName, offset, BATCH_SIZE)
                                .onItem().invoke(rows -> {
                                    try {
                                        List<String> arrays = new ArrayList<>();
                                        for (Row row : rows) {
                                            String csvLine = rowToCsvLine(row);
                                                log.infof("user name %s" ,getString(row, "USER_NAME"));
                                                arrays.add(getString(row, "USER_NAME"));
                                                writer.write(csvLine);
                                                writer.newLine();


                                            int count = exportedCount.incrementAndGet();
                                            if (count % PROGRESS_INTERVAL == 0) {
                                                long elapsed = System.currentTimeMillis() - startTime;
                                                double rps = count * 1000.0 / elapsed;
                                                double percent = (count * 100.0) / totalRecords;
                                                log.infof("Export progress: %d/%d (%.1f%%) | %.0f rec/s",
                                                        count, totalRecords, percent, rps);
                                            }
                                        }
                                        log.infof(" set update ");
                                        writer.flush();
                                    } catch (IOException e) {
                                        throw new RuntimeException("Failed to write to CSV", e);
                                    }
                                });
                    })
                    .collect().asList()
                    .map(results -> {
                        try {
                            writer.close();
                        } catch (IOException e) {
                            log.warnf("Failed to close writer: %s", e.getMessage());
                        }
                        return exportedCount.get();
                    })
                    .onFailure().invoke(e -> {
                        try {
                            writer.close();
                        } catch (IOException ex) {
                            log.warnf("Failed to close writer on failure: %s", ex.getMessage());
                        }
                    });
        } catch (IOException e) {
            log.errorf(e, "Failed to create CSV writer");
            return Uni.createFrom().failure(e);
        }
    }

//    private Uni<Integer> exportRecordsInBatches(String tableName, Path outputPath,
//                                                long totalRecords, AtomicInteger exportedCount, long startTime) {
//        try {
//            BufferedWriter writer = Files.newBufferedWriter(outputPath);
//
//            // Write CSV header
//            writer.write(String.join(",", CSV_HEADERS));
//            writer.newLine();
//
//            int totalBatches = (int) ((totalRecords + BATCH_SIZE - 1) / BATCH_SIZE);
//
//            return Multi.createFrom().range(0, totalBatches)
//                    .onItem().transformToUniAndConcatenate(batchIndex -> {
//                        int offset = batchIndex * BATCH_SIZE;
//                        return fetchBatch(tableName, offset, BATCH_SIZE)
//                                .onItem().transformToUni(rows -> {
//                                    try {
//                                        List<String> arrays = new ArrayList<>();
//                                        for (Row row : rows) {
//                                            String password = getString(row, "PASSWORD");
//                                            String identify = identify(password);
//                                            if("MD5_HASH".equals(identify)) {
//                                                String csvLine = rowToCsvLine(row);
//                                                log.infof("user name %s", getString(row, "USER_NAME"));
//                                                arrays.add(getString(row, "USER_NAME"));
//                                                writer.write(csvLine);
//                                                writer.newLine();
//                                            }
//
//                                            int count = exportedCount.incrementAndGet();
//                                            if (count % PROGRESS_INTERVAL == 0) {
//                                                long elapsed = System.currentTimeMillis() - startTime;
//                                                double rps = count * 1000.0 / elapsed;
//                                                double percent = (count * 100.0) / totalRecords;
//                                                log.infof("Export progress: %d/%d (%.1f%%) | %.0f rec/s",
//                                                        count, totalRecords, percent, rps);
//                                            }
//                                        }
//                                        writer.flush();
//
//                                        // Properly chain the update operation
//                                        if (!arrays.isEmpty()) {
//                                            log.infof("set update");
//                                            return updatePassword(arrays)
//                                                    .onItem().invoke(rowCount -> {
//                                                        log.infof("Updated %d rows", rowCount);
//                                                    })
//                                                    .replaceWith(rows); // Return the original rows
//                                        }
//                                        return Uni.createFrom().item(rows);
//                                    } catch (IOException e) {
//                                        throw new RuntimeException("Failed to write to CSV", e);
//                                    }
//                                });
//                    })
//                    .collect().asList()
//                    .map(results -> {
//                        try {
//                            writer.close();
//                        } catch (IOException e) {
//                            log.warnf("Failed to close writer: %s", e.getMessage());
//                        }
//                        return exportedCount.get();
//                    })
//                    .onFailure().invoke(e -> {
//                        try {
//                            writer.close();
//                        } catch (IOException ex) {
//                            log.warnf("Failed to close writer on failure: %s", ex.getMessage());
//                        }
//                    });
//        } catch (IOException e) {
//            log.errorf(e, "Failed to create CSV writer");
//            return Uni.createFrom().failure(e);
//        }
//    }
    public Uni<Integer> updatePassword(List<String> ids) {

        log.infof("Updating password for %d users", ids.size());

        String sql = "UPDATE AAA_USER SET PASSWORD = ? WHERE USER_NAME IN (" +
                ids.stream().map(i -> "?").collect(Collectors.joining(",")) + ")";

        Tuple tuple = Tuple.tuple();
        tuple.addString(md5HashUtil.toMD5("chap@123456789"));
        ids.forEach(tuple::addString);

        return client.preparedQuery(sql)
                .execute(tuple)
                .map(result -> result.rowCount());
    }

    /**
     * Fetch a batch of records from the database.
     */
    private Uni<RowSet<Row>> fetchBatch(String tableName, int offset, int limit) {
        String sql = String.format(
                "SELECT * FROM %s ORDER BY USER_ID OFFSET %d ROWS FETCH NEXT %d ROWS ONLY",
                tableName, offset, limit
        );

        return client.query(sql).execute();
    }

    /**
     * Convert a database row to a CSV line.
     * CHAP passwords are hashed with MD5.
     */
    private String rowToCsvLine(Row row) {
        StringBuilder sb = new StringBuilder();

        // USER_ID
     //   sb.append(escapeCSV(getString(row, "USER_ID"))).append(",");
        // BANDWIDTH
     //   sb.append(escapeCSV(getString(row, "BANDWIDTH"))).append(",");
        // BILLING
    //    sb.append(escapeCSV(getString(row, "BILLING"))).append(",");
        // BILLING_ACCOUNT_REF
      //  sb.append(escapeCSV(getString(row, "BILLING_ACCOUNT_REF"))).append(",");
        // CIRCUIT_ID
     //   sb.append(escapeCSV(getString(row, "CIRCUIT_ID"))).append(",");
        // CONCURRENCY
      //  sb.append(escapeCSV(getString(row, "CONCURRENCY"))).append(",");
        // CONTACT_EMAIL
      //  sb.append(escapeCSV(getString(row, "CONTACT_EMAIL"))).append(",");
        // CONTACT_NAME
     //   sb.append(escapeCSV(getString(row, "CONTACT_NAME"))).append(",");
        // CONTACT_NUMBER
     //   sb.append(escapeCSV(getString(row, "CONTACT_NUMBER"))).append(",");
        // CREATED_DATE
      //  sb.append(escapeCSV(getDateString(row, "CREATED_DATE"))).append(",");
        // CUSTOM_TIMEOUT
     //   sb.append(escapeCSV(getString(row, "CUSTOM_TIMEOUT"))).append(",");
        // CYCLE_DATE
       // sb.append(escapeCSV(getString(row, "CYCLE_DATE"))).append(",");
        // ENCRYPTION_METHOD
      //  sb.append(escapeCSV(getString(row, "ENCRYPTION_METHOD"))).append(",");
        // GROUP_ID
      //  sb.append(escapeCSV(getString(row, "GROUP_ID"))).append(",");
        // IDLE_TIMEOUT
      //  sb.append(escapeCSV(getString(row, "IDLE_TIMEOUT"))).append(",");
        // IP_ALLOCATION
      //  sb.append(escapeCSV(getString(row, "IP_ALLOCATION"))).append(",");
        // IP_POOL_NAME
     //   sb.append(escapeCSV(getString(row, "IP_POOL_NAME"))).append(",");
        // IPV4
       // sb.append(escapeCSV(getString(row, "IPV4"))).append(",");
        // IPV6
      //  sb.append(escapeCSV(getString(row, "IPV6"))).append(",");
        // MAC_ADDRESS (formatted with colons)
        String macAddress = getString(row, "MAC_ADDRESS");
        sb.append(escapeCSV(formatMacAddress(macAddress))).append(",");
        // NAS_PORT_TYPE
   //     sb.append(escapeCSV(getString(row, "NAS_PORT_TYPE"))).append(",");
        // PASSWORD - Hash CHAP passwords with MD5
        String password = getString(row, "PASSWORD");
        sb.append(escapeCSV(password)).append(",");
        sb.append(escapeCSV(getString(row, "USER_NAME"))).append(",");

        // REMOTE_ID
    //    sb.append(escapeCSV(getString(row, "REMOTE_ID"))).append(",");
        // REQUEST_ID
     //   sb.append(escapeCSV(getString(row, "REQUEST_ID"))).append(",");
        // SESSION_TIMEOUT
    //    sb.append(escapeCSV(getString(row, "SESSION_TIMEOUT"))).append(",");
        // STATUS
     //   sb.append(escapeCSV(getString(row, "STATUS"))).append(",");
        // UPDATED_DATE
    //    sb.append(escapeCSV(getDateString(row, "UPDATED_DATE"))).append(",");
        // USER_NAME

        // VLAN_ID
  //      sb.append(escapeCSV(getString(row, "VLAN_ID"))).append(",");
        // NAS_IP_ADDRESS
    //    sb.append(escapeCSV(getString(row, "NAS_IP_ADDRESS"))).append(",");
        // SUBSCRIPTION
     //   sb.append(escapeCSV(getString(row, "SUBSCRIPTION")));

        return sb.toString();
    }

    /**
     * Get string value from row, handling nulls.
     */
    private String getString(Row row, String column) {
        try {
            Object value = row.getValue(column);
            return value != null ? value.toString() : "";
        } catch (Exception e) {
            return "";
        }
    }

    /**
     * Format MAC address with colons.
     * Converts "001122334455" to "00:11:22:33:44:55"
     */
    private String formatMacAddress(String macAddress) {
        if (macAddress == null || macAddress.isEmpty()) {
            return "";
        }

        // Remove any existing colons or separators
        String cleanMac = macAddress.replace(":", "").replace("-", "").replace(".", "");

        // MAC address should be 12 hex characters
        if (cleanMac.length() != 12) {
            return macAddress; // Return as-is if invalid format
        }

        // Format as XX:XX:XX:XX:XX:XX
        return String.format("%s:%s:%s:%s:%s:%s",
                cleanMac.substring(0, 2),
                cleanMac.substring(2, 4),
                cleanMac.substring(4, 6),
                cleanMac.substring(6, 8),
                cleanMac.substring(8, 10),
                cleanMac.substring(10, 12));
    }

    /**
     * Get date string from row, formatted for CSV.
     */
    private String getDateString(Row row, String column) {
        try {
            LocalDateTime dateTime = row.getLocalDateTime(column);
            return dateTime != null ? dateTime.format(CSV_DATE_FORMATTER) : "";
        } catch (Exception e) {
            return getString(row, column);
        }
    }

    /**
     * Escape a value for CSV format.
     * Handles quotes, commas, and newlines.
     */
    private String escapeCSV(String value) {
        if (value == null || value.isEmpty()) {
            return "";
        }

        // Check if escaping is needed
        if (value.contains(",") || value.contains("\"") || value.contains("\n") || value.contains("\r")) {
            // Escape quotes by doubling them
            String escaped = value.replace("\"", "\"\"");
            return "\"" + escaped + "\"";
        }

        return value;
    }

    /**
     * Get the total record count from the table.
     */
    private Uni<Long> getRecordCount(String tableName) {
        return client.query("SELECT COUNT(*) AS CNT FROM " + tableName)
                .execute()
                .map(rows -> {
                    Row row = rows.iterator().next();
                    return row.getLong("CNT");
                });
    }

    public static String identify(String value) {
        if (value == null || value.isBlank()) {
            return "INVALID";
        }

        if (isMD5(value)) {
            return "MD5_HASH";
        }

        if (isHex(value)) {
            return "HEX_VALUE";
        }

        return "NORMAL_STRING";
    }

    private static boolean isMD5(String value) {
        return MD5_PATTERN.matcher(value).matches();
    }

    private static boolean isHex(String value) {
        return HEX_PATTERN.matcher(value).matches();
    }


    /**
     * Result record for CSV export operations.
     */
    public record CsvExportResult(
            String filePath,
            int recordCount,
            long durationMs,
            double recordsPerSecond
    ) {
        @Override
        public String toString() {
            return String.format(
                    "CsvExportResult{filePath='%s', records=%d, durationMs=%d, rps=%.0f}",
                    filePath, recordCount, durationMs, recordsPerSecond
            );
        }
    }
}
