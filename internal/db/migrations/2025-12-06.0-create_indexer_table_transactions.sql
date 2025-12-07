-- +migrate Up

-- Table: transactions
-- Stores Stellar transactions with XDR data in binary format for storage efficiency.
CREATE TABLE transactions (
    hash TEXT,
    to_id BIGINT NOT NULL,
    envelope_xdr BYTEA,
    result_xdr BYTEA,
    meta_xdr BYTEA,
    ledger_number INTEGER NOT NULL,
    ledger_created_at TIMESTAMPTZ NOT NULL,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
) WITH (
    timescaledb.hypertable,
    timescaledb.partition_column = 'ledger_created_at'
);

-- Table: transactions_accounts
-- Junction table linking transactions to participating accounts.
CREATE TABLE transactions_accounts (
    tx_hash TEXT NOT NULL,
    to_id BIGINT NOT NULL,
    account_id TEXT NOT NULL,
    ledger_created_at TIMESTAMPTZ NOT NULL,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
) WITH (
    timescaledb.hypertable,
    timescaledb.segmentby = 'account_id',
    timescaledb.partition_column = 'ledger_created_at'
);

-- +migrate Down

-- Tables
DROP TABLE IF EXISTS transactions_accounts CASCADE;
DROP TABLE IF EXISTS transactions CASCADE;
