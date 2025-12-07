-- +migrate Up

-- Table: state_changes
-- Stores blockchain state changes with optimized data types for storage efficiency.
CREATE TABLE state_changes (
    to_id BIGINT NOT NULL,
    state_change_order BIGINT NOT NULL,
    state_change_category TEXT NOT NULL,             -- Category (BALANCE, ACCOUNT, etc.)
    state_change_reason TEXT,                        -- Reason (CREATE, DEBIT, CREDIT, etc.)
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    ledger_created_at TIMESTAMPTZ NOT NULL,
    ledger_number INTEGER NOT NULL,
    account_id TEXT NOT NULL,
    operation_id BIGINT NOT NULL,
    tx_hash TEXT NOT NULL,
    token_id TEXT,
    amount TEXT,
    flags JSONB,
    key_value JSONB,
    offer_id TEXT,
    signer_account_id TEXT,
    signer_weights JSONB,
    spender_account_id TEXT,
    sponsored_account_id TEXT,
    sponsor_account_id TEXT,
    deployer_account_id TEXT,
    funder_account_id TEXT,
    thresholds JSONB,
    trustline_limit JSONB
) WITH (
    timescaledb.hypertable,
    timescaledb.partition_column = 'ledger_created_at',
    -- timescaledb.segment_by = 'state_change_category',
    timescaledb.order_by = 'ledger_created_at DESC, to_id DESC, state_change_order DESC'
);

CREATE INDEX idx_state_changes_account_id ON state_changes (account_id);

-- +migrate Down

-- Tables
DROP TABLE IF EXISTS state_changes CASCADE;
DROP INDEX IF EXISTS idx_state_changes_account_id;