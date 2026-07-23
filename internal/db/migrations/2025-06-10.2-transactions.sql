-- +migrate Up

-- Table: transactions (TimescaleDB hypertable with columnstore)
CREATE TABLE transactions (
    to_id BIGINT NOT NULL,
    hash BYTEA NOT NULL,
    fee_charged BIGINT NOT NULL,
    result_code TEXT NOT NULL,
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
-- ledger_number rises monotonically with the partition column, so per-chunk min/max
-- ranges let ledger_number-windowed queries (e.g. backfill gap detection) exclude
-- non-overlapping chunks at plan time.
SELECT enable_chunk_skipping('transactions', 'ledger_number');

-- TimescaleDB's default single-column index on the partition column. Retention drops chunks by
-- range metadata, and no query path filters this table by bare ledger_created_at (reads go through
-- the primary key, idx_transactions_hash, or the transactions_accounts table), so it has zero
-- consumers.
DROP INDEX IF EXISTS transactions_ledger_created_at_idx;

CREATE INDEX idx_transactions_hash ON transactions(hash);

-- Table: transactions_accounts (TimescaleDB hypertable for automatic cleanup with retention)
CREATE TABLE transactions_accounts (
    tx_to_id BIGINT NOT NULL,
    account_id BYTEA NOT NULL,
    ledger_created_at TIMESTAMPTZ NOT NULL,
    -- Column order matches BatchGetByAccountAddress's access shape: account_id is pinned by
    -- equality, then the walk sorts by (ledger_created_at, tx_to_id). With account_id fixed,
    -- the planner serves that sort via an index (only) scan over this PK -- forward or
    -- backward -- so the dedicated (account_id, ledger_created_at DESC, tx_to_id DESC) index
    -- once needed alongside it is redundant.
    PRIMARY KEY (account_id, ledger_created_at, tx_to_id)
) WITH (
    tsdb.hypertable,
    tsdb.partition_column = 'ledger_created_at',
    tsdb.chunk_interval = '1 day',
    tsdb.orderby = 'ledger_created_at DESC, tx_to_id DESC',
    tsdb.segmentby = 'account_id'
);

SELECT enable_chunk_skipping('transactions_accounts', 'tx_to_id');

-- TimescaleDB's default single-column index on the partition column. Retention drops chunks
-- by range metadata, and no query path filters this table by bare ledger_created_at, so it
-- has zero consumers.
DROP INDEX IF EXISTS transactions_accounts_ledger_created_at_idx;

CREATE INDEX idx_transactions_accounts_tx_to_id ON transactions_accounts(tx_to_id);

-- +migrate Down

-- Tables
DROP TABLE IF EXISTS transactions_accounts CASCADE;
DROP TABLE IF EXISTS transactions CASCADE;
