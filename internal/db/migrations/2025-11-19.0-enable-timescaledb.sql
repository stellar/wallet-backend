-- +migrate Up

-- Enable TimescaleDB extension
CREATE EXTENSION IF NOT EXISTS timescaledb;

-- Enable Direct Compress
SET timescaledb.enable_direct_compress_copy = on;

-- +migrate Down

-- Note: Dropping TimescaleDB extension would require dropping all hypertables first
-- This is intentionally left as a no-op to prevent accidental data loss
-- If you need to remove TimescaleDB, first run the down migration for hypertables
