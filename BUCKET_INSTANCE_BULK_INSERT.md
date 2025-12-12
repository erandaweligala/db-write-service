# BUCKET_INSTANCE Bulk Insert

This document describes the new `BulkInsertBucketInstance` class that handles bulk insertion of BUCKET_INSTANCE records by retrieving data from the SERVICE_INSTANCE table.

## Overview

The `BulkInsertBucketInstance` class provides a separate, focused implementation for:
- Fetching SERVICE_INSTANCE records from the database
- Generating BUCKET_INSTANCE records (1:1 ratio with SERVICE_INSTANCE)
- Performing optimized batch inserts using `executeBatch`
- Progress reporting and error handling

## Features

- **1:1 Relationship**: Creates exactly 1 BUCKET_INSTANCE record for each SERVICE_INSTANCE record
- **Batch Processing**: Processes records in configurable batches for optimal performance
- **Concurrent Processing**: Uses parallel processing with configurable concurrency
- **Progress Tracking**: Real-time progress reporting with throughput metrics
- **Error Handling**: Graceful error handling with detailed logging
- **Filtering Support**: Optional filtering by SERVICE_INSTANCE status

## Configuration

The class uses the following constants for optimization:

```java
private static final int SERVICE_FETCH_BATCH_SIZE = 5000;    // Batch size for fetching SERVICE_INSTANCE
private static final int BUCKET_BATCH_SIZE = 10000;          // Batch size for inserting BUCKET_INSTANCE
private static final int PROGRESS_INTERVAL = 20000;          // Progress logging interval
private static final int CONCURRENT_BATCHES = 10;            // Number of concurrent batch workers
```

## REST API Endpoints

### 1. Bulk Insert All BUCKET_INSTANCE Records

Creates BUCKET_INSTANCE records for all SERVICE_INSTANCE records in the database.

**Endpoint**: `POST /api/bulk-insert/bucket-instances`

**Request**:
```bash
curl -X POST http://localhost:8080/api/bulk-insert/bucket-instances \
  -H "Content-Type: application/json"
```

**Response**:
```json
{
  "inserted": 100000,
  "failed": 0,
  "durationMs": 45000,
  "durationFormatted": "45.0s"
}
```

### 2. Bulk Insert BUCKET_INSTANCE by Status

Creates BUCKET_INSTANCE records only for SERVICE_INSTANCE records with a specific status.

**Endpoint**: `POST /api/bulk-insert/bucket-instances/by-status`

**Request**:
```bash
curl -X POST http://localhost:8080/api/bulk-insert/bucket-instances/by-status \
  -H "Content-Type: application/json" \
  -d '{"status": "Active"}'
```

Valid status values:
- `Active`
- `Suspend`
- `Inactive`

**Response**:
```json
{
  "inserted": 50000,
  "failed": 0,
  "durationMs": 22000,
  "durationFormatted": "22.0s"
}
```

## Generated Data

The class generates realistic BUCKET_INSTANCE data with the following characteristics:

### BUCKET_INSTANCE Fields

- **ID**: Auto-generated unique identifier
- **BUCKET_ID**: Generated as `BUCKET-{SERVICE_ID}-{PRIORITY}`
- **BUCKET_TYPE**: Randomly selected from: `DATA`, `COMBO`
- **CARRY_FORWARD**: Random 0 or 1
- **CARRY_FORWARD_VALIDITY**: Random value between 30-119 days
- **CONSUMPTION_LIMIT**: Generated based on initial balance
- **CONSUMPTION_LIMIT_WINDOW**: Randomly selected from: `1`, `7`, `30`
- **CURRENT_BALANCE**: Random value less than initial balance (NULL if unlimited)
- **EXPIRATION**: Random date 30-394 days in the future
- **INITIAL_BALANCE**: Random value > 9,999,999,999 (NULL if unlimited)
- **MAX_CARRY_FORWARD**: 20% of initial balance
- **PRIORITY**: Default value of 1
- **RULE**: Randomly selected from: `100Mbps`, `200Mbps`, `300Kbps`, `1Gbps`, `100kbps`
- **SERVICE_ID**: Foreign key to SERVICE_INSTANCE.ID
- **TIME_WINDOW**: Randomly selected from: `00-08`, `00-24`, `00-18`, `18-24`
- **TOTAL_CARRY_FORWARD**: Random value up to 5% of initial balance
- **USAGE**: Random value up to 20% of initial balance
- **UPDATED_AT**: Current timestamp
- **IS_UNLIMITED**: 20% chance of being unlimited (1), otherwise limited (0)

### Distribution

- **Unlimited Buckets**: 20% of generated buckets
- **Limited Buckets**: 80% of generated buckets with realistic balance values

## Performance

The implementation is optimized for high throughput:

- Uses `executeBatch` to minimize round trips to the database
- Concurrent batch processing with configurable workers
- Stream-based processing to reduce memory footprint
- Efficient data generation without pre-creating all records in memory

**Expected Performance**:
- ~10,000-20,000 inserts per second (depending on database configuration and network latency)

## Usage Example

### Programmatic Usage

```java
@Inject
BulkInsertBucketInstance bulkInsertBucketInstance;

public void insertBuckets() {
    // Insert for all SERVICE_INSTANCE records
    bulkInsertBucketInstance.executeBulkInsert()
        .subscribe().with(
            result -> log.infof("Inserted %d buckets in %s",
                result.inserted(), result.duration()),
            failure -> log.errorf("Failed: %s", failure.getMessage())
        );

    // Insert for specific status only
    bulkInsertBucketInstance.executeBulkInsert("Active")
        .subscribe().with(
            result -> log.infof("Inserted %d buckets for Active services",
                result.inserted()),
            failure -> log.errorf("Failed: %s", failure.getMessage())
        );
}
```

## Class Structure

### Main Class
- `BulkInsertBucketInstance` - Application-scoped service class

### Key Methods

1. **executeBulkInsert()**: Main execution method - generates BUCKET_INSTANCE for all SERVICE_INSTANCE records
2. **executeBulkInsert(String status)**: Filtered execution - generates BUCKET_INSTANCE for SERVICE_INSTANCE with specific status
3. **fetchServiceInstanceBatch()**: Fetches SERVICE_INSTANCE records in batches
4. **generateAndInsertBuckets()**: Generates and inserts BUCKET_INSTANCE records
5. **createBucketInstanceRecord()**: Creates a single BUCKET_INSTANCE record with realistic data

### Record Classes

- `ServiceInstanceRecord`: Holds SERVICE_INSTANCE data (id, username, planId, planType, status)
- `BucketInstanceRecord`: Holds BUCKET_INSTANCE data with all 19 fields
- `BulkInsertResult`: Result object containing insert statistics

## Error Handling

The implementation includes comprehensive error handling:

- Failed batches are logged but don't stop the entire process
- Failed records are counted and reported in the result
- Database connection errors are logged with detailed messages
- Invalid status filters return appropriate error responses

## Logging

The class provides detailed logging at different levels:

- **INFO**: Progress updates, completion statistics
- **DEBUG**: Batch-level details
- **ERROR**: Failure details with stack traces

Example log output:
```
INFO  Processing 100000 service instances in 20 batches with 10 concurrent workers
INFO  Progress: 20000/100000 buckets (20.0%) | 4444 buckets/s
INFO  Progress: 40000/100000 buckets (40.0%) | 4545 buckets/s
INFO  Bulk insert completed: 100000 inserted, 0 failed, duration: 22s
```

## Integration Points

The class integrates with:

- **Pool (Vert.x SQL Client)**: For reactive database operations
- **BulkInsertResource**: REST API endpoints for triggering operations
- **SERVICE_INSTANCE table**: Source of data for generating buckets
- **BUCKET_INSTANCE table**: Target table for bulk inserts

## Location

**Class File**: `src/main/java/com/csg/airtel/aaa4j/scripts/BulkInsertBucketInstance.java`

**REST Controller**: `src/main/java/com/csg/airtel/aaa4j/application/controller/BulkInsertResource.java`
- New endpoints: `/bucket-instances` and `/bucket-instances/by-status`
