-- +migrate Up

-- Table: operations (TimescaleDB hypertable with columnstore)
CREATE TABLE operations (
    id BIGINT NOT NULL,
    operation_type TEXT NOT NULL CHECK (
        operation_type IN (
            'CREATE_ACCOUNT', 'PAYMENT', 'PATH_PAYMENT_STRICT_RECEIVE',
            'MANAGE_SELL_OFFER', 'CREATE_PASSIVE_SELL_OFFER', 'SET_OPTIONS',
            'CHANGE_TRUST', 'ALLOW_TRUST', 'ACCOUNT_MERGE', 'INFLATION',
            'MANAGE_DATA', 'BUMP_SEQUENCE', 'MANAGE_BUY_OFFER',
            'PATH_PAYMENT_STRICT_SEND', 'CREATE_CLAIMABLE_BALANCE',
            'CLAIM_CLAIMABLE_BALANCE', 'BEGIN_SPONSORING_FUTURE_RESERVES',
            'END_SPONSORING_FUTURE_RESERVES', 'REVOKE_SPONSORSHIP',
            'CLAWBACK', 'CLAWBACK_CLAIMABLE_BALANCE', 'SET_TRUST_LINE_FLAGS',
            'LIQUIDITY_POOL_DEPOSIT', 'LIQUIDITY_POOL_WITHDRAW',
            'INVOKE_HOST_FUNCTION', 'EXTEND_FOOTPRINT_TTL', 'RESTORE_FOOTPRINT'
        )
    ),
    operation_xdr BYTEA,
    result_code TEXT NOT NULL,
    successful BOOLEAN NOT NULL,
    ledger_number INTEGER NOT NULL,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    ledger_created_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (id, ledger_created_at)
) WITH (
    tsdb.hypertable,
    tsdb.partition_column = 'ledger_created_at',
    tsdb.chunk_interval = '1 day',
    tsdb.orderby = 'ledger_created_at DESC, id DESC',
    tsdb.sparse_index = 'bloom(operation_type)'
);

SELECT enable_chunk_skipping('operations', 'id');

-- TimescaleDB's default single-column index on the partition column. Retention drops chunks by
-- range metadata, and no query path filters this table by bare ledger_created_at (reads go through
-- the primary key or the operations_accounts table), so it has zero consumers.
DROP INDEX IF EXISTS operations_ledger_created_at_idx;

-- Table: operations_accounts (TimescaleDB hypertable for automatic cleanup with retention)
CREATE TABLE operations_accounts (
    operation_id BIGINT NOT NULL,
    account_id BYTEA NOT NULL,
    ledger_created_at TIMESTAMPTZ NOT NULL,
    -- Column order (account_id, ledger_created_at, operation_id) serves both consumers as a
    -- single index: BatchGetByAccountAddress pins account_id by equality and sorts by
    -- (ledger_created_at, operation_id), which the planner walks as an index (only) scan over
    -- this PK -- forward or backward; BatchGetAccountOperationsByToIDs probes equality on
    -- account_id, equality on ledger_created_at, and a range on operation_id, which fits this
    -- key as a perfect prefix. The dedicated (account_id, ledger_created_at DESC, operation_id
    -- DESC) index once needed for the former is now redundant.
    PRIMARY KEY (account_id, ledger_created_at, operation_id)
) WITH (
    tsdb.hypertable,
    tsdb.partition_column = 'ledger_created_at',
    tsdb.chunk_interval = '1 day',
    tsdb.orderby = 'ledger_created_at DESC, operation_id DESC',
    tsdb.segmentby = 'account_id'
);

SELECT enable_chunk_skipping('operations_accounts', 'operation_id');

-- TimescaleDB's default single-column index on the partition column. Retention drops chunks
-- by range metadata, and no query path filters this table by bare ledger_created_at, so it
-- has zero consumers.
DROP INDEX IF EXISTS operations_accounts_ledger_created_at_idx;

CREATE INDEX idx_operations_accounts_operation_id ON operations_accounts(operation_id);

-- +migrate Down

-- Tables
DROP TABLE IF EXISTS operations_accounts CASCADE;
DROP TABLE IF EXISTS operations CASCADE;
