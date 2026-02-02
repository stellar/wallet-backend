-- +migrate Up

-- Table: state_changes (TimescaleDB hypertable with columnstore)
-- Note: FK to transactions removed (hypertable FKs not supported)
CREATE TABLE state_changes (
    ledger_created_at TIMESTAMPTZ NOT NULL,
    to_id BIGINT NOT NULL,
    operation_id BIGINT NOT NULL,
    state_change_order BIGINT NOT NULL CHECK (state_change_order >= 1),
    state_change_category TEXT NOT NULL CHECK (
        state_change_category IN (
            'BALANCE', 'ACCOUNT', 'SIGNER', 'SIGNATURE_THRESHOLD',
            'METADATA', 'FLAGS', 'TRUSTLINE', 'RESERVES',
            'BALANCE_AUTHORIZATION', 'AUTHORIZATION'
        )
    ),
    state_change_reason TEXT CHECK (
        state_change_reason IS NULL OR state_change_reason IN (
            'CREATE', 'MERGE', 'DEBIT', 'CREDIT', 'MINT', 'BURN',
            'ADD', 'REMOVE', 'UPDATE', 'LOW', 'MEDIUM', 'HIGH',
            'HOME_DOMAIN', 'SET', 'CLEAR', 'DATA_ENTRY', 'SPONSOR', 'UNSPONSOR'
        )
    ),
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    ledger_number INTEGER NOT NULL,
    account_id TEXT NOT NULL,
    token_id TEXT,
    amount TEXT,
    signer_account_id TEXT,
    spender_account_id TEXT,
    sponsored_account_id TEXT,
    sponsor_account_id TEXT,
    deployer_account_id TEXT,
    funder_account_id TEXT,
    claimable_balance_id TEXT,
    liquidity_pool_id TEXT,
    sponsored_data TEXT,
    signer_weight_old SMALLINT,
    signer_weight_new SMALLINT,
    threshold_old SMALLINT,
    threshold_new SMALLINT,
    trustline_limit_old TEXT,
    trustline_limit_new TEXT,
    flags SMALLINT,
    key_value JSONB,
    PRIMARY KEY (ledger_created_at, to_id, operation_id, state_change_order)
) WITH (
    tsdb.hypertable,
    tsdb.partition_column = 'ledger_created_at',
    tsdb.chunk_interval = '1 day',
    tsdb.segmentby = 'state_change_category',
    tsdb.orderby = 'ledger_created_at DESC'
);

-- Index for account_id lookups (most common query pattern)
CREATE INDEX idx_state_changes_account_id ON state_changes(account_id);
CREATE INDEX idx_state_changes_ledger_created_at ON state_changes(ledger_created_at);

-- Index for to_id lookups (transaction-based queries)
CREATE INDEX idx_state_changes_to_id ON state_changes(to_id);

-- Index for operation_id lookups
CREATE INDEX idx_state_changes_operation_id ON state_changes(operation_id) WHERE operation_id > 0;

-- +migrate Down

-- Tables
DROP TABLE IF EXISTS state_changes CASCADE;
