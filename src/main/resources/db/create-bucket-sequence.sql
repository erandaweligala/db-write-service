-- ============================================================================
-- Create Sequence for BUCKET_INSTANCE ID Generation
-- ============================================================================
-- This script creates a sequence for generating unique IDs for BUCKET_INSTANCE
-- table in batch insert operations.
--
-- Usage:
--   Execute this script in your Oracle database:
--   sqlplus username/password@database @create-bucket-sequence.sql
-- ============================================================================

-- Drop sequence if it exists (use with caution in production!)
-- DROP SEQUENCE BUCKET_INSTANCE_SEQ;

-- Create sequence for BUCKET_INSTANCE table
CREATE SEQUENCE BUCKET_INSTANCE_SEQ
    START WITH 1
    INCREMENT BY 1
    MINVALUE 1
    MAXVALUE 9999999999999999999999999999
    CACHE 1000
    NOORDER
    NOCYCLE;

COMMENT ON SEQUENCE BUCKET_INSTANCE_SEQ IS 'Sequence for generating unique IDs for BUCKET_INSTANCE table';

COMMIT;

-- ============================================================================
-- Script Execution Complete
-- ============================================================================
-- Sequence created: BUCKET_INSTANCE_SEQ
--
-- The sequence is configured with:
--   - Cache size of 1000 for better performance in batch operations
--   - No ordering to allow concurrent access
--   - No cycling to prevent ID reuse
-- ============================================================================
