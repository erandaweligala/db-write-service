-- ============================================================================
-- Database Initialization Script for SERVICE_INSTANCE and BUCKET_INSTANCE
-- ============================================================================
-- This script creates the SERVICE_INSTANCE and BUCKET_INSTANCE tables
-- with proper constraints and indexes for optimal performance.
--
-- Usage:
--   Execute this script in your Oracle database before running the data generator
--   sqlplus username/password@database @init-service-bucket-tables.sql
-- ============================================================================

-- Drop tables if they exist (use with caution in production!)
-- DROP TABLE BUCKET_INSTANCE CASCADE CONSTRAINTS;
-- DROP TABLE SERVICE_INSTANCE CASCADE CONSTRAINTS;

-- ============================================================================
-- SERVICE_INSTANCE Table
-- ============================================================================
-- Stores service subscription instances for users
-- One user can have multiple service instances (typically 3 per user in test data)
-- ============================================================================

CREATE TABLE SERVICE_INSTANCE (
    ID                     NUMBER(19,0) NOT NULL,
    CREATED_AT             TIMESTAMP(6) NOT NULL,
    EXPIRY_DATE            TIMESTAMP(6),
    IS_GROUP               NUMBER(1,0),
    NEXT_CYCLE_START_DATE  TIMESTAMP(6),
    PLAN_ID                VARCHAR2(64 CHAR) NOT NULL,
    PLAN_NAME              VARCHAR2(64 CHAR) NOT NULL,
    PLAN_TYPE              VARCHAR2(64 CHAR) NOT NULL,
    RECURRING_FLAG         NUMBER(1,0) NOT NULL,
    REQUEST_ID             VARCHAR2(255 CHAR) NOT NULL,
    CYCLE_END_DATE         TIMESTAMP(6),
    CYCLE_START_DATE       TIMESTAMP(6),
    SERVICE_START_DATE     TIMESTAMP(6) NOT NULL,
    STATUS                 VARCHAR2(64 CHAR) NOT NULL,
    UPDATED_AT             TIMESTAMP(6) NOT NULL,
    USERNAME               VARCHAR2(64 CHAR) NOT NULL,
    BILLING                VARCHAR2(255 CHAR),
    CYCLE_DATE             NUMBER(10,0),

    -- Primary Key
    CONSTRAINT PK_SERVICE_INSTANCE PRIMARY KEY (ID),

    -- Unique Constraints
    CONSTRAINT UK_SERVICE_INSTANCE_REQUEST_ID UNIQUE (REQUEST_ID),

    -- Check Constraints
    CONSTRAINT CHK_SERVICE_INSTANCE_IS_GROUP CHECK (IS_GROUP IN (0, 1)),
    CONSTRAINT CHK_SERVICE_INSTANCE_RECURRING CHECK (RECURRING_FLAG IN (0, 1)),
    CONSTRAINT CHK_SERVICE_INSTANCE_STATUS CHECK (STATUS IN ('ACTIVE', 'SUSPENDED', 'INACTIVE', 'PENDING'))
);

-- Create indexes for better query performance
CREATE INDEX IDX_SERVICE_INSTANCE_USERNAME ON SERVICE_INSTANCE(USERNAME);
CREATE INDEX IDX_SERVICE_INSTANCE_STATUS ON SERVICE_INSTANCE(STATUS);
CREATE INDEX IDX_SERVICE_INSTANCE_PLAN_ID ON SERVICE_INSTANCE(PLAN_ID);
CREATE INDEX IDX_SERVICE_INSTANCE_START_DATE ON SERVICE_INSTANCE(SERVICE_START_DATE);
CREATE INDEX IDX_SERVICE_INSTANCE_EXPIRY_DATE ON SERVICE_INSTANCE(EXPIRY_DATE);

-- Add comments
COMMENT ON TABLE SERVICE_INSTANCE IS 'Stores service subscription instances for users';
COMMENT ON COLUMN SERVICE_INSTANCE.ID IS 'Unique service instance identifier';
COMMENT ON COLUMN SERVICE_INSTANCE.USERNAME IS 'References AAA_USER.USER_NAME';
COMMENT ON COLUMN SERVICE_INSTANCE.PLAN_ID IS 'Service plan identifier (e.g., 100COMBO182)';
COMMENT ON COLUMN SERVICE_INSTANCE.IS_GROUP IS 'Flag indicating if this is a group service (0=No, 1=Yes)';
COMMENT ON COLUMN SERVICE_INSTANCE.RECURRING_FLAG IS 'Flag indicating if service is recurring (0=No, 1=Yes)';

-- ============================================================================
-- BUCKET_INSTANCE Table
-- ============================================================================
-- Stores data/voice/SMS buckets for each service instance
-- One SERVICE_INSTANCE can have multiple BUCKET_INSTANCE records (one-to-many)
-- ============================================================================

CREATE TABLE BUCKET_INSTANCE (
    ID                          NUMBER(19,0) GENERATED ALWAYS AS IDENTITY
                                MINVALUE 1
                                MAXVALUE 9999999999999999999999999999
                                INCREMENT BY 1
                                START WITH 1
                                CACHE 20
                                NOORDER
                                NOCYCLE
                                NOKEEP
                                NOSCALE
                                NOT NULL,
    BUCKET_ID                   VARCHAR2(64 CHAR) NOT NULL,
    BUCKET_TYPE                 VARCHAR2(255 CHAR),
    CARRY_FORWARD               NUMBER(1,0) NOT NULL,
    CARRY_FORWARD_VALIDITY      NUMBER(10,0),
    CONSUMPTION_LIMIT           NUMBER(19,0),
    CONSUMPTION_LIMIT_WINDOW    VARCHAR2(255 CHAR),
    CURRENT_BALANCE             NUMBER(19,0) NOT NULL,
    EXPIRATION                  TIMESTAMP(6),
    INITIAL_BALANCE             NUMBER(19,0),
    MAX_CARRY_FORWARD           NUMBER(19,0),
    PRIORITY                    NUMBER(19,0) NOT NULL,
    RULE                        VARCHAR2(64 CHAR) NOT NULL,
    SERVICE_ID                  VARCHAR2(64 CHAR),
    TIME_WINDOW                 VARCHAR2(64 CHAR) NOT NULL,
    TOTAL_CARRY_FORWARD         NUMBER(19,0),
    USAGE                       NUMBER(19,0) NOT NULL,
    UPDATED_AT                  TIMESTAMP(6),
    IS_UNLIMITED                NUMBER(38,0) DEFAULT 0 NOT NULL,

    -- Primary Key
    CONSTRAINT PK_BUCKET_INSTANCE PRIMARY KEY (ID),

    -- Unique Constraints
    CONSTRAINT UK_BUCKET_INSTANCE_BUCKET_ID UNIQUE (BUCKET_ID),

    -- Check Constraints
    CONSTRAINT CHK_BUCKET_INSTANCE_CARRY_FWD CHECK (CARRY_FORWARD IN (0, 1)),
    CONSTRAINT CHK_BUCKET_INSTANCE_UNLIMITED CHECK (IS_UNLIMITED IN (0, 1)),
    CONSTRAINT CHK_BUCKET_INSTANCE_PRIORITY CHECK (PRIORITY > 0),
    CONSTRAINT CHK_BUCKET_INSTANCE_TIME_WINDOW CHECK (
        TIME_WINDOW IN ('00-08', '00-24', '00-18', '18-24')
    )
);

-- Create indexes for better query performance
CREATE INDEX IDX_BUCKET_INSTANCE_SERVICE_ID ON BUCKET_INSTANCE(SERVICE_ID);
CREATE INDEX IDX_BUCKET_INSTANCE_PRIORITY ON BUCKET_INSTANCE(PRIORITY);
CREATE INDEX IDX_BUCKET_INSTANCE_RULE ON BUCKET_INSTANCE(RULE);
CREATE INDEX IDX_BUCKET_INSTANCE_EXPIRATION ON BUCKET_INSTANCE(EXPIRATION);
CREATE INDEX IDX_BUCKET_INSTANCE_TIME_WINDOW ON BUCKET_INSTANCE(TIME_WINDOW);
CREATE INDEX IDX_BUCKET_INSTANCE_TYPE ON BUCKET_INSTANCE(BUCKET_TYPE);

-- Add comments
COMMENT ON TABLE BUCKET_INSTANCE IS 'Stores data/voice/SMS buckets for service instances';
COMMENT ON COLUMN BUCKET_INSTANCE.ID IS 'Auto-generated unique identifier';
COMMENT ON COLUMN BUCKET_INSTANCE.BUCKET_ID IS 'Business bucket identifier';
COMMENT ON COLUMN BUCKET_INSTANCE.SERVICE_ID IS 'References SERVICE_INSTANCE.ID';
COMMENT ON COLUMN BUCKET_INSTANCE.INITIAL_BALANCE IS 'Starting balance (> 9,999,999,999 for test data)';
COMMENT ON COLUMN BUCKET_INSTANCE.CURRENT_BALANCE IS 'Current remaining balance';
COMMENT ON COLUMN BUCKET_INSTANCE.USAGE IS 'Amount consumed from bucket';
COMMENT ON COLUMN BUCKET_INSTANCE.PRIORITY IS 'Consumption priority (lower number = higher priority)';
COMMENT ON COLUMN BUCKET_INSTANCE.TIME_WINDOW IS 'Time window for bucket usage (00-08, 00-24, 00-18, 18-24)';
COMMENT ON COLUMN BUCKET_INSTANCE.IS_UNLIMITED IS 'Flag indicating unlimited bucket (0=Limited, 1=Unlimited)';
COMMENT ON COLUMN BUCKET_INSTANCE.CARRY_FORWARD IS 'Flag indicating if unused balance carries forward (0=No, 1=Yes)';

-- ============================================================================
-- Foreign Key Relationship (Optional - uncomment if AAA_USER exists)
-- ============================================================================
-- This creates referential integrity between SERVICE_INSTANCE and AAA_USER
-- Uncomment only if AAA_USER table exists with USER_NAME as a unique key
-- ============================================================================

-- ALTER TABLE SERVICE_INSTANCE
-- ADD CONSTRAINT FK_SERVICE_INSTANCE_USER
-- FOREIGN KEY (USERNAME) REFERENCES AAA_USER(USER_NAME)
-- ON DELETE CASCADE;

-- Note: We don't add FK between BUCKET_INSTANCE.SERVICE_ID and SERVICE_INSTANCE.ID
-- because SERVICE_ID is stored as VARCHAR2 while ID is NUMBER
-- In a production system, consider changing SERVICE_ID to NUMBER(19,0) and adding:
-- ALTER TABLE BUCKET_INSTANCE
-- ADD CONSTRAINT FK_BUCKET_INSTANCE_SERVICE
-- FOREIGN KEY (SERVICE_ID) REFERENCES SERVICE_INSTANCE(ID)
-- ON DELETE CASCADE;

-- ============================================================================
-- Sample Data Validation Queries
-- ============================================================================
-- Use these queries to verify data after generation
-- ============================================================================

-- Count records
-- SELECT COUNT(*) AS SERVICE_COUNT FROM SERVICE_INSTANCE;
-- SELECT COUNT(*) AS BUCKET_COUNT FROM BUCKET_INSTANCE;

-- Check SERVICE_INSTANCE distribution
-- SELECT STATUS, COUNT(*) FROM SERVICE_INSTANCE GROUP BY STATUS;
-- SELECT PLAN_ID, COUNT(*) FROM SERVICE_INSTANCE GROUP BY PLAN_ID;

-- Check BUCKET_INSTANCE distribution
-- SELECT TIME_WINDOW, COUNT(*) FROM BUCKET_INSTANCE GROUP BY TIME_WINDOW;
-- SELECT IS_UNLIMITED, COUNT(*) FROM BUCKET_INSTANCE GROUP BY IS_UNLIMITED;

-- Verify relationship (services per user)
-- SELECT USERNAME, COUNT(*) AS SERVICE_COUNT
-- FROM SERVICE_INSTANCE
-- GROUP BY USERNAME
-- ORDER BY SERVICE_COUNT DESC;

-- Verify relationship (buckets per service)
-- SELECT SERVICE_ID, COUNT(*) AS BUCKET_COUNT
-- FROM BUCKET_INSTANCE
-- GROUP BY SERVICE_ID
-- ORDER BY BUCKET_COUNT DESC;

-- Check INITIAL_BALANCE > 9,999,999,999
-- SELECT COUNT(*) AS VALID_BALANCE_COUNT
-- FROM BUCKET_INSTANCE
-- WHERE INITIAL_BALANCE > 9999999999;

COMMIT;

-- ============================================================================
-- Script Execution Complete
-- ============================================================================
-- Tables created:
--   - SERVICE_INSTANCE (with 5 indexes)
--   - BUCKET_INSTANCE (with 6 indexes)
--
-- Next steps:
--   1. Run the data generator: POST /api/service-instance/generate
--   2. Verify data using the sample queries above
-- ============================================================================
