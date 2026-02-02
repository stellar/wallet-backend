-- +migrate Up

-- Table: transactions (TimescaleDB hypertable with columnstore)
CREATE TABLE transactions (
    ledger_created_at TIMESTAMPTZ NOT NULL,
    to_id BIGINT NOT NULL,
    hash BYTEA NOT NULL,
    envelope_xdr TEXT,
    fee_charged BIGINT NOT NULL,
    result_code TEXT NOT NULL,
    meta_xdr TEXT,
    ledger_number INTEGER NOT NULL,
    is_fee_bump BOOLEAN NOT NULL DEFAULT false,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (ledger_created_at, to_id),
) WITH (
    tsdb.hypertable,
    tsdb.partition_column = 'ledger_created_at',
    tsdb.chunk_interval = '1 day',
    tsdb.orderby = 'ledger_created_at DESC'
);

CREATE INDEX idx_transactions_hash ON transactions(hash);
CREATE INDEX idx_transactions_to_id ON transactions(to_id);

-- Table: transactions_accounts (TimescaleDB hypertable for automatic cleanup with retention)
CREATE TABLE transactions_accounts (
    ledger_created_at TIMESTAMPTZ NOT NULL,
    tx_to_id BIGINT NOT NULL,
    account_id BYTEA NOT NULL,
    PRIMARY KEY (ledger_created_at, account_id, tx_to_id)
) WITH (
    tsdb.hypertable,
    tsdb.partition_column = 'ledger_created_at',
    tsdb.chunk_interval = '1 day',
    tsdb.orderby = 'ledger_created_at DESC'
);

CREATE INDEX idx_transactions_accounts_tx_to_id ON transactions_accounts(tx_to_id);
CREATE INDEX idx_transactions_accounts_account_id ON transactions_accounts(account_id);

-- +migrate Down

-- Tables
DROP TABLE IF EXISTS transactions_accounts CASCADE;
DROP TABLE IF EXISTS transactions CASCADE;
