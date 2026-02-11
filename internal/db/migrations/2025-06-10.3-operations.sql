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
    ledger_created_at TIMESTAMPTZ NOT NULL
) WITH (
    tsdb.hypertable,
    tsdb.partition_column = 'ledger_created_at',
    tsdb.chunk_interval = '1 day',
    tsdb.orderby = 'ledger_created_at DESC, id DESC'
);

SELECT enable_chunk_skipping('operations', 'id');

CREATE INDEX idx_operations_id ON operations(id);

-- Table: operations_accounts (TimescaleDB hypertable for automatic cleanup with retention)
CREATE TABLE operations_accounts (
    operation_id BIGINT NOT NULL,
    account_id BYTEA NOT NULL,
    ledger_created_at TIMESTAMPTZ NOT NULL
) WITH (
    tsdb.hypertable,
    tsdb.partition_column = 'ledger_created_at',
    tsdb.chunk_interval = '1 day',
    tsdb.orderby = 'ledger_created_at DESC, operation_id DESC',
    tsdb.segmentby = 'account_id'
);

SELECT enable_chunk_skipping('operations_accounts', 'operation_id');

CREATE INDEX idx_operations_accounts_operation_id ON operations_accounts(operation_id);
CREATE INDEX idx_operations_accounts_account_id ON operations_accounts(account_id, ledger_created_at DESC, operation_id DESC);

-- +migrate Down

-- Tables
DROP TABLE IF EXISTS operations_accounts CASCADE;
DROP TABLE IF EXISTS operations CASCADE;
