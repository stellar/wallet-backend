-- +migrate Up

-- Table: state_changes
-- Stores blockchain state changes with explicit typed columns instead of JSONB for predictable data.
-- Only key_value remains as JSONB for truly variable metadata (data entries, home domain).
CREATE TABLE state_changes (
    to_id BIGINT NOT NULL,
    state_change_order BIGINT NOT NULL,
    state_change_category TEXT NOT NULL,
    state_change_reason TEXT,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    ledger_created_at TIMESTAMPTZ NOT NULL,
    ledger_number INTEGER NOT NULL,
    account_id TEXT NOT NULL,
    operation_id BIGINT NOT NULL,
    tx_hash TEXT NOT NULL REFERENCES transactions(hash),
    token_id TEXT,
    amount TEXT,
    signer_account_id TEXT,
    spender_account_id TEXT,
    sponsored_account_id TEXT,
    sponsor_account_id TEXT,
    deployer_account_id TEXT,
    funder_account_id TEXT,
    offer_id TEXT,
    claimable_balance_id TEXT,
    liquidity_pool_id TEXT,
    data_name TEXT,
    signer_weight_old SMALLINT,
    signer_weight_new SMALLINT,
    threshold_old SMALLINT,
    threshold_new SMALLINT,
    trustline_limit_old TEXT,
    trustline_limit_new TEXT,
    flags SMALLINT,
    key_value JSONB,

    PRIMARY KEY (to_id, state_change_order)
);

CREATE INDEX idx_state_changes_account_id ON state_changes(account_id);
CREATE INDEX idx_state_changes_tx_hash ON state_changes(tx_hash);
CREATE INDEX idx_state_changes_operation_id ON state_changes(operation_id);
CREATE INDEX idx_state_changes_ledger_created_at ON state_changes(ledger_created_at);


-- +migrate Down

-- Tables
DROP TABLE IF EXISTS state_changes CASCADE;
