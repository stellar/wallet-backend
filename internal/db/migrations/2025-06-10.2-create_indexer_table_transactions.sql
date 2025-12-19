-- +migrate Up

-- Table: transactions
CREATE TABLE transactions (
    hash TEXT,
    to_id BIGINT NOT NULL,
    envelope_xdr TEXT,
    result_xdr TEXT,
    meta_xdr TEXT,
    ledger_number INTEGER NOT NULL,
    ledger_created_at TIMESTAMPTZ NOT NULL,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
) WITH (
    timescaledb.hypertable,
    timescaledb.partition_column = 'ledger_created_at',
    timescaledb.chunk_interval = '1 week',
    timescaledb.order_by = 'ledger_created_at, to_id'
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
    timescaledb.partition_column = 'ledger_created_at',
    timescaledb.chunk_interval = '1 week',
    timescaledb.order_by = 'ledger_created_at, to_id'
);

CREATE INDEX idx_transactions_accounts_account_id ON transactions_accounts (account_id);

-- +migrate Down

-- Tables
DROP TABLE IF EXISTS transactions_accounts CASCADE;
DROP TABLE IF EXISTS transactions CASCADE;
