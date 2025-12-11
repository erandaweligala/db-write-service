# SERVICE_INSTANCE and BUCKET_INSTANCE Data Generator

Comprehensive Java-based data generator for populating `SERVICE_INSTANCE` and `BUCKET_INSTANCE` tables with realistic test data.

## Overview

This data generator creates test data for telecom service management tables with the following relationships:

```
AAA_USER (1) ----< (3) SERVICE_INSTANCE (1) ----< (2-5) BUCKET_INSTANCE
```

- Each `USERNAME` from `AAA_USER` gets **3** `SERVICE_INSTANCE` records
- Each `SERVICE_INSTANCE` gets **2-5** `BUCKET_INSTANCE` records (random)
- All data follows specified business rules and distributions

## Table Structures

### SERVICE_INSTANCE Table

Stores service subscription instances for users.

**Columns:**
- `ID` (NUMBER) - Primary key, unique service instance identifier
- `USERNAME` (VARCHAR2) - References `AAA_USER.USER_NAME`
- `PLAN_ID` (VARCHAR2) - Service plan (100COMBO182-192)
- `PLAN_NAME` (VARCHAR2) - Descriptive plan name
- `PLAN_TYPE` (VARCHAR2) - PREPAID, POSTPAID, or HYBRID
- `STATUS` (VARCHAR2) - ACTIVE, SUSPENDED, INACTIVE, or PENDING
- `SERVICE_START_DATE` (TIMESTAMP) - Service activation date
- `EXPIRY_DATE` (TIMESTAMP) - Service expiration date
- `CYCLE_START_DATE` (TIMESTAMP) - Billing cycle start
- `CYCLE_END_DATE` (TIMESTAMP) - Billing cycle end
- `NEXT_CYCLE_START_DATE` (TIMESTAMP) - Next cycle start
- `CYCLE_DATE` (NUMBER) - Day of month for billing (1-28)
- `BILLING` (VARCHAR2) - Billing type
- `RECURRING_FLAG` (NUMBER) - 0 or 1
- `IS_GROUP` (NUMBER) - 0 or 1
- `REQUEST_ID` (VARCHAR2) - Unique request identifier
- `CREATED_AT` (TIMESTAMP) - Record creation timestamp
- `UPDATED_AT` (TIMESTAMP) - Last update timestamp

**Constraints:**
- Primary Key: `ID`
- Unique Key: `REQUEST_ID`
- Check: `IS_GROUP IN (0, 1)`
- Check: `RECURRING_FLAG IN (0, 1)`
- Check: `STATUS IN ('ACTIVE', 'SUSPENDED', 'INACTIVE', 'PENDING')`

### BUCKET_INSTANCE Table

Stores data/voice/SMS buckets for each service instance.

**Columns:**
- `ID` (NUMBER) - Auto-generated primary key (identity column)
- `BUCKET_ID` (VARCHAR2) - Unique business identifier
- `SERVICE_ID` (VARCHAR2) - References `SERVICE_INSTANCE.ID`
- `BUCKET_TYPE` (VARCHAR2) - DATA, VOICE, SMS, or COMBO
- `INITIAL_BALANCE` (NUMBER) - Starting balance (> 9,999,999,999, NULL for unlimited buckets)
- `CURRENT_BALANCE` (NUMBER) - Remaining balance (NULL for unlimited buckets)
- `USAGE` (NUMBER) - Amount consumed (0 for unlimited buckets)
- `CONSUMPTION_LIMIT` (NUMBER) - Usage limit (0 for unlimited buckets)
- `CONSUMPTION_LIMIT_WINDOW` (VARCHAR2) - Time window for limit
- `PRIORITY` (NUMBER) - Consumption priority (1 = highest)
- `RULE` (VARCHAR2) - PEAK, OFF_PEAK, ANYTIME, WEEKEND, SPECIAL
- `TIME_WINDOW` (VARCHAR2) - 00-08, 00-24, 00-18, or 18-24
- `EXPIRATION` (TIMESTAMP) - Bucket expiry date
- `CARRY_FORWARD` (NUMBER) - 0 or 1
- `CARRY_FORWARD_VALIDITY` (NUMBER) - Days valid for carry forward
- `MAX_CARRY_FORWARD` (NUMBER) - Maximum carryable amount (0 for unlimited buckets)
- `TOTAL_CARRY_FORWARD` (NUMBER) - Current carried forward amount (0 for unlimited buckets)
- `IS_UNLIMITED` (NUMBER) - 0 or 1 (20% chance of 1)
- `UPDATED_AT` (TIMESTAMP) - Last update timestamp

**Constraints:**
- Primary Key: `ID` (auto-generated)
- Unique Key: `BUCKET_ID`
- Check: `CARRY_FORWARD IN (0, 1)`
- Check: `IS_UNLIMITED IN (0, 1)`
- Check: `PRIORITY > 0`
- Check: `TIME_WINDOW IN ('00-08', '00-24', '00-18', '18-24')`

## Data Generation Rules

### SERVICE_INSTANCE Dynamic Columns

| Column | Rule | Distribution |
|--------|------|--------------|
| `SERVICE_START_DATE` | Date distribution | 5% future dates, 40% today, 55% past dates |
| `EXPIRY_DATE` | Before/After today | 50% expired (before today), 50% valid (after today) |
| `PLAN_ID` | From list | Randomly selected from: 100COMBO182, 100COMBO183, 100COMBO184, 100COMBO185, 100COMBO187, 100COMBO188, 100COMBO189, 100COMBO190, 100COMBO191, 100COMBO192 |
| `USERNAME` | From AAA_USER | Fetched from AAA_USER.USER_NAME |

### BUCKET_INSTANCE Dynamic Columns

| Column | Rule | Example Values |
|--------|------|----------------|
| `BUCKET_ID` | Unique identifier | BUCKET-{SERVICE_ID}-{PRIORITY} |
| `INITIAL_BALANCE` | Must be > 9,999,999,999 (NULL if unlimited) | Random between 10 billion and 100 billion, or NULL |
| `CURRENT_BALANCE` | Calculated from initial balance (NULL if unlimited) | INITIAL_BALANCE - usage, or NULL |
| `EXPIRATION` | Future date | 30-395 days from now |
| `PRIORITY` | Sequential per service | 1, 2, 3, 4, 5 (lower = higher priority) |
| `RULE` | Random selection | PEAK, OFF_PEAK, ANYTIME, WEEKEND, SPECIAL |
| `TIME_WINDOW` | From list | 00-08, 00-24, 00-18, 18-24 |
| `IS_UNLIMITED` | Random flag | 0 or 1 (20% chance of unlimited) |

**Important**: When `IS_UNLIMITED = 1`, the following fields are set to NULL or 0:
- `INITIAL_BALANCE` = NULL
- `CURRENT_BALANCE` = NULL
- `USAGE` = 0
- `CONSUMPTION_LIMIT` = 0
- `MAX_CARRY_FORWARD` = 0
- `TOTAL_CARRY_FORWARD` = 0

## Installation & Setup

### 1. Database Setup

Execute the SQL initialization script to create tables:

```bash
sqlplus username/password@database @src/main/resources/db/init-service-bucket-tables.sql
```

Or connect to Oracle SQL Developer and run the script manually.

### 2. Prerequisites

Ensure the following:
- ✅ Oracle database is running and accessible
- ✅ `AAA_USER` table exists and is populated with usernames
- ✅ Database connection is configured in `application.yml`
- ✅ Quarkus application dependencies are installed

### 3. Configure Database Connection

Update `src/main/resources/application.yml`:

```yaml
quarkus:
  datasource:
    reactive:
      url: oracle:thin:@localhost:1521/ORCL
    username: your_username
    password: your_password
```

## Usage

### Option 1: REST API (Recommended)

Start the Quarkus application:

```bash
./mvnw quarkus:dev
```

Generate data using the REST API:

```bash
# Generate data for all users
curl -X POST http://localhost:8080/api/service-instance/generate

# Response:
{
  "serviceInstancesCreated": 300,
  "bucketInstancesCreated": 1050,
  "failed": 0,
  "durationMs": 5432,
  "durationFormatted": "5.4s"
}
```

Get generator information:

```bash
curl http://localhost:8080/api/service-instance/info
```

### Option 2: Programmatic Usage

Inject the generator into your service:

```java
@Inject
ServiceInstanceDataGenerator dataGenerator;

public Uni<Void> generateTestData() {
    return dataGenerator.generateData()
            .onItem().invoke(result ->
                log.infof("Created %d services and %d buckets",
                    result.serviceInstancesCreated(),
                    result.bucketInstancesCreated())
            )
            .replaceWithVoid();
}
```

## API Endpoints

### POST /api/service-instance/generate

Generates SERVICE_INSTANCE and BUCKET_INSTANCE data for all users in AAA_USER.

**Response:**
```json
{
  "serviceInstancesCreated": 300,
  "bucketInstancesCreated": 1050,
  "failed": 0,
  "durationMs": 5432,
  "durationFormatted": "5.4s"
}
```

### GET /api/service-instance/info

Returns information about the data generation process.

**Response:**
```json
{
  "description": "SERVICE_INSTANCE and BUCKET_INSTANCE Data Generator",
  "servicesPerUser": 3,
  "bucketsPerService": "2-5 (random)",
  "serviceStartDateDistribution": {
    "future": "5%",
    "today": "40%",
    "past": "55%"
  },
  "expiryDateDistribution": {
    "expired": "50%",
    "valid": "50%"
  },
  "planIds": "100COMBO182-192",
  "bucketInitialBalance": "> 9,999,999,999",
  "timeWindows": "00-08, 00-24, 00-18, 18-24"
}
```

## Performance Characteristics

- **Batch Processing**: Processes data in batches of 100 records
- **Concurrent Execution**: Up to 5 batches processed concurrently
- **Progress Reporting**: Reports progress every 500 records
- **Error Handling**: Automatic retry (up to 2 retries) on failure
- **Throughput**: Approximately 100-500 records/second (varies by hardware)

### Expected Performance

For 100 users in AAA_USER:
- **SERVICE_INSTANCE**: 300 records created
- **BUCKET_INSTANCE**: ~1,000 records created (averaging 3.3 per service)
- **Duration**: 5-10 seconds (typical)

## Data Validation

After generation, validate data using these SQL queries:

```sql
-- Count records
SELECT COUNT(*) AS SERVICE_COUNT FROM SERVICE_INSTANCE;
SELECT COUNT(*) AS BUCKET_COUNT FROM BUCKET_INSTANCE;

-- Verify 3 services per user
SELECT USERNAME, COUNT(*) AS SERVICE_COUNT
FROM SERVICE_INSTANCE
GROUP BY USERNAME
ORDER BY USERNAME;

-- Verify buckets per service
SELECT SERVICE_ID, COUNT(*) AS BUCKET_COUNT
FROM BUCKET_INSTANCE
GROUP BY SERVICE_ID
ORDER BY SERVICE_ID;

-- Verify SERVICE_START_DATE distribution
SELECT
  CASE
    WHEN SERVICE_START_DATE > SYSDATE THEN 'FUTURE'
    WHEN TRUNC(SERVICE_START_DATE) = TRUNC(SYSDATE) THEN 'TODAY'
    ELSE 'PAST'
  END AS DATE_CATEGORY,
  COUNT(*) AS COUNT,
  ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM SERVICE_INSTANCE), 1) AS PERCENTAGE
FROM SERVICE_INSTANCE
GROUP BY
  CASE
    WHEN SERVICE_START_DATE > SYSDATE THEN 'FUTURE'
    WHEN TRUNC(SERVICE_START_DATE) = TRUNC(SYSDATE) THEN 'TODAY'
    ELSE 'PAST'
  END;

-- Verify EXPIRY_DATE distribution
SELECT
  CASE
    WHEN EXPIRY_DATE < SYSDATE THEN 'EXPIRED'
    ELSE 'VALID'
  END AS EXPIRY_STATUS,
  COUNT(*) AS COUNT,
  ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM SERVICE_INSTANCE), 1) AS PERCENTAGE
FROM SERVICE_INSTANCE
GROUP BY
  CASE
    WHEN EXPIRY_DATE < SYSDATE THEN 'EXPIRED'
    ELSE 'VALID'
  END;

-- Verify INITIAL_BALANCE > 9,999,999,999 for limited buckets
-- and NULL for unlimited buckets
SELECT
  COUNT(*) AS TOTAL_BUCKETS,
  SUM(CASE WHEN IS_UNLIMITED = 0 AND INITIAL_BALANCE > 9999999999 THEN 1 ELSE 0 END) AS VALID_LIMITED,
  SUM(CASE WHEN IS_UNLIMITED = 0 AND (INITIAL_BALANCE IS NULL OR INITIAL_BALANCE <= 9999999999) THEN 1 ELSE 0 END) AS INVALID_LIMITED,
  SUM(CASE WHEN IS_UNLIMITED = 1 AND INITIAL_BALANCE IS NULL THEN 1 ELSE 0 END) AS VALID_UNLIMITED,
  SUM(CASE WHEN IS_UNLIMITED = 1 AND INITIAL_BALANCE IS NOT NULL THEN 1 ELSE 0 END) AS INVALID_UNLIMITED
FROM BUCKET_INSTANCE;

-- Verify TIME_WINDOW distribution
SELECT TIME_WINDOW, COUNT(*) AS COUNT
FROM BUCKET_INSTANCE
GROUP BY TIME_WINDOW
ORDER BY TIME_WINDOW;

-- Verify IS_UNLIMITED distribution (~20% should be 1)
SELECT
  IS_UNLIMITED,
  COUNT(*) AS COUNT,
  ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM BUCKET_INSTANCE), 1) AS PERCENTAGE
FROM BUCKET_INSTANCE
GROUP BY IS_UNLIMITED
ORDER BY IS_UNLIMITED;

-- Verify unlimited buckets have NULL balances
SELECT
  IS_UNLIMITED,
  COUNT(*) AS TOTAL,
  SUM(CASE WHEN INITIAL_BALANCE IS NULL THEN 1 ELSE 0 END) AS NULL_INITIAL,
  SUM(CASE WHEN CURRENT_BALANCE IS NULL THEN 1 ELSE 0 END) AS NULL_CURRENT,
  SUM(CASE WHEN INITIAL_BALANCE IS NOT NULL THEN 1 ELSE 0 END) AS NON_NULL_INITIAL,
  SUM(CASE WHEN CURRENT_BALANCE IS NOT NULL THEN 1 ELSE 0 END) AS NON_NULL_CURRENT
FROM BUCKET_INSTANCE
GROUP BY IS_UNLIMITED
ORDER BY IS_UNLIMITED;
```

## Architecture

### Class Structure

```
ServiceInstanceDataGenerator
├── generateData() - Main entry point
├── fetchUsernames() - Fetches from AAA_USER
├── generateServiceInstances() - Creates SERVICE_INSTANCE records
│   ├── createServiceInstanceRecord() - Generates single record
│   ├── insertServiceInstanceBatch() - Batch insert
│   └── generateServiceStartDate() - Date distribution logic
└── insertBucketInstancesForServices() - Creates BUCKET_INSTANCE records
    ├── createBucketInstanceRecord() - Generates single record
    └── insertBucketInstanceBatch() - Batch insert
```

### Technology Stack

- **Framework**: Quarkus 3.26.1
- **Reactive**: SmallRye Mutiny
- **Database**: Oracle (via Vert.x Reactive Oracle Client)
- **Language**: Java 21

## Troubleshooting

### Issue: "Table does not exist" error

**Solution**: Run the SQL initialization script first:
```bash
sqlplus user/pass@db @src/main/resources/db/init-service-bucket-tables.sql
```

### Issue: "AAA_USER table not found"

**Solution**: Ensure AAA_USER table exists and has USER_NAME column:
```sql
SELECT COUNT(*) FROM AAA_USER;
```

### Issue: Slow performance

**Solutions**:
- Increase `CONCURRENT_BATCHES` in ServiceInstanceDataGenerator
- Increase database connection pool size
- Add more indexes to tables
- Check database server resources

### Issue: Foreign key constraint violation

**Solution**: Ensure usernames in AAA_USER are valid:
```sql
-- Check for invalid characters or null usernames
SELECT * FROM AAA_USER WHERE USER_NAME IS NULL OR TRIM(USER_NAME) = '';
```

## Code Files

### Core Files Created

1. **ServiceInstanceDataGenerator.java** (`src/main/java/com/csg/airtel/aaa4j/scripts/`)
   - Main data generation logic
   - Batch processing and error handling
   - Progress reporting

2. **ServiceInstanceResource.java** (`src/main/java/com/csg/airtel/aaa4j/application/controller/`)
   - REST API endpoints
   - Request/response handling

3. **init-service-bucket-tables.sql** (`src/main/resources/db/`)
   - Table creation script
   - Indexes and constraints
   - Sample validation queries

4. **SERVICE_INSTANCE_DATA_GENERATOR.md** (this file)
   - Comprehensive documentation

## Examples

### Example: Generate Data for 100 Users

Assuming AAA_USER has 100 records:

```bash
curl -X POST http://localhost:8080/api/service-instance/generate
```

**Expected Result:**
- 300 SERVICE_INSTANCE records (3 per user)
- ~1,000 BUCKET_INSTANCE records (3.3 per service average)
- Execution time: 5-10 seconds

### Example: Verify Data Distribution

```sql
-- Check SERVICE_START_DATE distribution (should be ~5% future, 40% today, 55% past)
SELECT
  CASE
    WHEN SERVICE_START_DATE > SYSDATE THEN 'FUTURE'
    WHEN TRUNC(SERVICE_START_DATE) = TRUNC(SYSDATE) THEN 'TODAY'
    ELSE 'PAST'
  END AS DATE_CATEGORY,
  COUNT(*) AS COUNT,
  ROUND(COUNT(*) * 100.0 / 300, 1) AS PERCENTAGE
FROM SERVICE_INSTANCE
GROUP BY
  CASE
    WHEN SERVICE_START_DATE > SYSDATE THEN 'FUTURE'
    WHEN TRUNC(SERVICE_START_DATE) = TRUNC(SYSDATE) THEN 'TODAY'
    ELSE 'PAST'
  END;

-- Expected output:
-- DATE_CATEGORY | COUNT | PERCENTAGE
-- FUTURE        | ~15   | ~5.0
-- TODAY         | ~120  | ~40.0
-- PAST          | ~165  | ~55.0
```

## License

This data generator is part of the db-write-service project.

## Support

For issues or questions:
1. Check the troubleshooting section above
2. Review Oracle database logs
3. Check application logs: `./mvnw quarkus:dev` output
4. Verify database connectivity and permissions

---

**Generated**: 2025-12-10
**Author**: Claude AI Assistant
**Version**: 1.0.0
