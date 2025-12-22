-- +migrate Up

-- Table: transactions
-- Primary key changed from hash to to_id (TOID) for storage efficiency.
-- ledger_number can be derived from to_id using: (to_id >> 32)::integer
CREATE TABLE transactions (
    to_id BIGINT PRIMARY KEY,
    hash TEXT NOT NULL UNIQUE,
    envelope_xdr TEXT,
    result_xdr TEXT,
    meta_xdr TEXT,
    ledger_created_at TIMESTAMPTZ NOT NULL
) WITH (
    timescaledb.hypertable,
    timescaledb.partition_column = 'ledger_created_at',
    timescaledb.chunk_interval = '1 week',
    timescaledb.order_by = 'ledger_created_at, to_id'
);

CREATE INDEX idx_transactions_hash ON transactions(hash);

-- Table: transactions_accounts
-- Uses tx_id (BIGINT) instead of tx_hash (TEXT) for storage efficiency.
CREATE TABLE transactions_accounts (
    tx_id BIGINT NOT NULL REFERENCES transactions(to_id) ON DELETE CASCADE,
    account_id BYTEA NOT NULL,
    PRIMARY KEY (account_id, tx_id)
) WITH (
    timescaledb.hypertable,
    timescaledb.partition_column = 'ledger_created_at',
    timescaledb.chunk_interval = '1 week',
    timescaledb.order_by = 'ledger_created_at, tx_id'
);

-- +migrate Down

-- Tables
DROP TABLE IF EXISTS transactions_accounts CASCADE;
DROP TABLE IF EXISTS transactions CASCADE;
