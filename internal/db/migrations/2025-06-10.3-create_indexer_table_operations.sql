-- +migrate Up

-- Table: operations
-- tx_hash removed - can be derived by joining transactions on (id & ~4095) = transactions.to_id
-- ledger_number removed - can be derived from id using: (id >> 32)::integer
-- operation_type changed from TEXT to SMALLINT for storage efficiency
CREATE TABLE operations (
    id BIGINT,
    operation_type SMALLINT NOT NULL,
    operation_xdr TEXT,
    ledger_created_at TIMESTAMPTZ NOT NULL
) WITH (
    timescaledb.hypertable,
    timescaledb.partition_column = 'ledger_created_at',
    timescaledb.chunk_interval = '1 week',
    timescaledb.order_by = 'ledger_created_at, id'
);

-- Index for state_changes JOIN to transactions
-- Supports: JOIN transactions t ON (sc.to_id & ~4095) = t.to_id
CREATE INDEX idx_operations_tx_to_id ON operations ((id & ~4095));

-- Table: operations_accounts
CREATE TABLE operations_accounts (
    operation_id BIGINT NOT NULL,
    account_id BYTEA NOT NULL,
    ledger_created_at TIMESTAMPTZ NOT NULL
) WITH (
    timescaledb.hypertable,
    timescaledb.partition_column = 'ledger_created_at',
    timescaledb.chunk_interval = '1 week',
    timescaledb.order_by = 'ledger_created_at, operation_id'
);

-- +migrate Down

-- Tables
DROP TABLE IF EXISTS operations_accounts CASCADE;
DROP TABLE IF EXISTS operations CASCADE;
