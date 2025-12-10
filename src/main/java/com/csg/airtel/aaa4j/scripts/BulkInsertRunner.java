//package com.csg.airtel.aaa4j.scripts;
//
//import io.quarkus.runtime.Quarkus;
//import io.quarkus.runtime.QuarkusApplication;
//import io.quarkus.runtime.annotations.QuarkusMain;
//import jakarta.inject.Inject;
//import org.jboss.logging.Logger;
//
///**
// * Standalone CLI runner for bulk database inserts.
// *
// * Run with:
// *   mvn quarkus:dev -Dquarkus.args="--table=BULK_TEST_DATA --records=1000000 --batch=1000"
// *
// * Or build and run:
// *   mvn package -DskipTests
// *   java -jar target/quarkus-app/quarkus-run.jar --table=BULK_TEST_DATA --records=1000000
// *
// * Arguments:
// *   --table=TABLE_NAME    Target table name (default: BULK_TEST_DATA)
// *   --records=N           Total records to insert (default: 1,000,000)
// *   --batch=N             Batch size (default: 1000)
// *   --setup               Create table before insert
// *   --cleanup             Truncate table before insert
// *   --single-row          Use single-row insert method (slower but more compatible)
// *   --help                Show help message
// */
//@QuarkusMain
//public class BulkInsertRunner implements QuarkusApplication {
//
//    private static final Logger log = Logger.getLogger(BulkInsertRunner.class);
//
//    @Inject
//    BulkInsertScript bulkInsertScript;
//
//    @Override
//    public int run(String... args) throws Exception {
//        // Parse command line arguments
//        Config config = parseArgs(args);
//
//        if (config.showHelp) {
//            printHelp();
//            return 0;
//        }
//
//        log.info("=".repeat(60));
//        log.info("BULK INSERT SCRIPT");
//        log.info("=".repeat(60));
//        log.infof("Table:        %s", config.tableName);
//        log.infof("Records:      %,d", config.totalRecords);
//        log.infof("Batch Size:   %,d", config.batchSize);
//        log.infof("Method:       %s", config.useSingleRow ? "Single-row" : "Multi-row INSERT ALL");
//        log.info("=".repeat(60));
//
//        try {
//            // Setup table if requested
//            if (config.setupTable) {
//                log.info("Creating table if not exists...");
//                bulkInsertScript.createTableIfNotExists(config.tableName)
//                        .await().indefinitely();
//                log.info("Table setup complete.");
//            }
//
//            // Cleanup table if requested
//            if (config.cleanupTable) {
//                log.info("Truncating table...");
//                bulkInsertScript.truncateTable(config.tableName)
//                        .await().indefinitely();
//                log.info("Table truncated.");
//            }
//
//            // Get initial count
//            try {
//                Long initialCount = bulkInsertScript.getRecordCount(config.tableName)
//                        .await().indefinitely();
//                log.infof("Initial record count: %,d", initialCount);
//            } catch (Exception e) {
//                log.warn("Could not get initial count (table may not exist)");
//            }
//
//            // Execute bulk insert
//            log.info("");
//            log.info("Starting bulk insert...");
//            log.info("-".repeat(60));
//
//            BulkInsertScript.BulkInsertResult result;
//            if (config.useSingleRow) {
//                result = bulkInsertScript.executeBulkInsertSingleRow(
//                        config.tableName,
//                        config.totalRecords,
//                        config.batchSize
//                ).await().indefinitely();
//            } else {
//                result = bulkInsertScript.executeBulkInsert(
//                        config.tableName,
//                        config.totalRecords,
//                        config.batchSize
//                ).await().indefinitely();
//            }
//
//            // Print results
//            log.info("-".repeat(60));
//            log.info("");
//            log.info("=".repeat(60));
//            log.info("RESULTS");
//            log.info("=".repeat(60));
//            log.infof("Table:             %s", result.tableName());
//            log.infof("Requested:         %,d", result.totalRequested());
//            log.infof("Inserted:          %,d", result.totalInserted());
//            log.infof("Failed:            %,d", result.totalFailed());
//            log.infof("Duration:          %s", formatDuration(result.duration()));
//            log.infof("Records/Second:    %,.0f", result.recordsPerSecond());
//            log.info("=".repeat(60));
//
//            // Get final count
//            try {
//                Long finalCount = bulkInsertScript.getRecordCount(config.tableName)
//                        .await().indefinitely();
//                log.infof("Final record count: %,d", finalCount);
//            } catch (Exception e) {
//                log.warn("Could not get final count");
//            }
//
//            return result.totalFailed() > 0 ? 1 : 0;
//
//        } catch (Exception e) {
//            log.errorf(e, "Bulk insert failed: %s", e.getMessage());
//            return 1;
//        }
//    }
//
//    private Config parseArgs(String[] args) {
//        Config config = new Config();
//
//        for (String arg : args) {
//            if (arg.startsWith("--table=")) {
//                config.tableName = arg.substring("--table=".length());
//            } else if (arg.startsWith("--records=")) {
//                config.totalRecords = Integer.parseInt(arg.substring("--records=".length()));
//            } else if (arg.startsWith("--batch=")) {
//                config.batchSize = Integer.parseInt(arg.substring("--batch=".length()));
//            } else if (arg.equals("--setup")) {
//                config.setupTable = true;
//            } else if (arg.equals("--cleanup")) {
//                config.cleanupTable = true;
//            } else if (arg.equals("--single-row")) {
//                config.useSingleRow = true;
//            } else if (arg.equals("--help") || arg.equals("-h")) {
//                config.showHelp = true;
//            }
//        }
//
//        return config;
//    }
//
//    private void printHelp() {
//        System.out.println("""
//
//                BULK INSERT SCRIPT - Insert 1,000,000 records into Oracle database
//
//                Usage:
//                  java -jar quarkus-run.jar [options]
//                  mvn quarkus:dev -Dquarkus.args="[options]"
//
//                Options:
//                  --table=NAME     Target table name (default: BULK_TEST_DATA)
//                  --records=N      Total records to insert (default: 1,000,000)
//                  --batch=N        Batch size (default: 1000)
//                  --setup          Create table before insert (if not exists)
//                  --cleanup        Truncate table before insert
//                  --single-row     Use single-row insert method (slower but more compatible)
//                  --help, -h       Show this help message
//
//                Examples:
//                  # Insert 1 million records with defaults
//                  java -jar quarkus-run.jar
//
//                  # Insert 500K records with custom batch size
//                  java -jar quarkus-run.jar --records=500000 --batch=2000
//
//                  # Setup table and insert
//                  java -jar quarkus-run.jar --setup --table=MY_TABLE --records=100000
//
//                  # Clean and re-insert
//                  java -jar quarkus-run.jar --cleanup --records=1000000
//
//                Environment:
//                  Configure database connection in application.yml or via environment variables:
//                    QUARKUS_DATASOURCE_REACTIVE_URL=oracle:thin:@localhost:1522/ORCL
//                    QUARKUS_DATASOURCE_USERNAME=your_user
//                    QUARKUS_DATASOURCE_PASSWORD=your_password
//                """);
//    }
//
//    private String formatDuration(java.time.Duration duration) {
//        long hours = duration.toHours();
//        long minutes = duration.toMinutesPart();
//        long seconds = duration.toSecondsPart();
//        long millis = duration.toMillisPart();
//
//        if (hours > 0) {
//            return String.format("%dh %dm %ds", hours, minutes, seconds);
//        } else if (minutes > 0) {
//            return String.format("%dm %ds", minutes, seconds);
//        } else {
//            return String.format("%d.%03ds", seconds, millis);
//        }
//    }
//
//    private static class Config {
//        String tableName = "AAA_USER";
//        int totalRecords = 100;
//        int batchSize = 10;
//        boolean setupTable = false;
//        boolean cleanupTable = false;
//        boolean useSingleRow = false;
//        boolean showHelp = false;
//    }
//
//    public static void main(String[] args) {
//        Quarkus.run(BulkInsertRunner.class, args);
//    }
//}
