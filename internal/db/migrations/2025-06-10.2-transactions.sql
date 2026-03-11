-- +migrate Up

-- Table: transactions (TimescaleDB hypertable with columnstore)
CREATE TABLE transactions (
    to_id BIGINT NOT NULL,
    hash BYTEA NOT NULL,
    envelope_xdr TEXT,
    fee_charged BIGINT NOT NULL,
    result_code TEXT NOT NULL,
    meta_xdr TEXT,
    ledger_number INTEGER NOT NULL,
    is_fee_bump BOOLEAN NOT NULL DEFAULT false,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    ledger_created_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (to_id, ledger_created_at)
) WITH (
    tsdb.hypertable,
    tsdb.partition_column = 'ledger_created_at',
    tsdb.chunk_interval = '1 day',
    tsdb.orderby = 'ledger_created_at DESC, to_id DESC',
    tsdb.sparse_index = 'bloom(hash)'
);

SELECT enable_chunk_skipping('transactions', 'to_id');

CREATE INDEX idx_transactions_hash ON transactions(hash);

-- Table: transactions_accounts (TimescaleDB hypertable for automatic cleanup with retention)
CREATE TABLE transactions_accounts (
    tx_to_id BIGINT NOT NULL,
    account_id BYTEA NOT NULL,
    ledger_created_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (account_id, tx_to_id, ledger_created_at)
) WITH (
    tsdb.hypertable,
    tsdb.partition_column = 'ledger_created_at',
    tsdb.chunk_interval = '1 day',
    tsdb.orderby = 'ledger_created_at DESC, tx_to_id DESC',
    tsdb.segmentby = 'account_id'
);

SELECT enable_chunk_skipping('transactions_accounts', 'tx_to_id');

CREATE INDEX idx_transactions_accounts_tx_to_id ON transactions_accounts(tx_to_id);
CREATE INDEX idx_transactions_accounts_account_id ON transactions_accounts(account_id, ledger_created_at DESC, tx_to_id DESC);

-- +migrate Down

-- Tables
DROP TABLE IF EXISTS transactions_accounts CASCADE;
DROP TABLE IF EXISTS transactions CASCADE;
