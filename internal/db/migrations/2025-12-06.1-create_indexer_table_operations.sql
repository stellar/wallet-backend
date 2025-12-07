-- +migrate Up

-- Table: operations
-- Stores Stellar operations with XDR data in binary format for storage efficiency.
CREATE TABLE operations (
    id BIGINT,
    tx_hash TEXT NOT NULL,
    operation_type TEXT NOT NULL,
    operation_xdr BYTEA,
    ledger_number INTEGER NOT NULL,
    ledger_created_at TIMESTAMPTZ NOT NULL,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
) WITH (
    timescaledb.hypertable,
    timescaledb.partition_column = 'ledger_created_at',
    timescaledb.order_by = 'ledger_created_at DESC, id DESC'
);

-- Table: operations_accounts
-- Junction table linking operations to participating accounts.
CREATE TABLE operations_accounts (
    operation_id BIGINT NOT NULL,
    account_id TEXT NOT NULL,
    ledger_created_at TIMESTAMPTZ NOT NULL,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
) WITH (
    timescaledb.hypertable,
    timescaledb.partition_column = 'ledger_created_at',
    timescaledb.order_by = 'ledger_created_at DESC, operation_id DESC'
);

CREATE INDEX idx_operations_accounts_account_id ON operations_accounts (account_id);

-- +migrate Down

-- Tables
DROP TABLE IF EXISTS operations_accounts CASCADE;
DROP TABLE IF EXISTS operations CASCADE;
DROP INDEX IF EXISTS idx_operations_accounts_account_id;