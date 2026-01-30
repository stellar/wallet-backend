-- +migrate Up

-- Table: state_changes
-- Stores blockchain state changes with explicit typed columns instead of JSONB for predictable data.
-- Only key_value remains as JSONB for truly variable metadata (data entries, home domain).
CREATE TABLE state_changes (
    -- Primary key (composite for cursor pagination)
    to_id BIGINT NOT NULL,
    state_change_order BIGINT NOT NULL,

    -- Classification
    state_change_category TEXT NOT NULL,
    state_change_reason TEXT,

    -- Timestamps
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    ledger_created_at TIMESTAMPTZ NOT NULL,
    ledger_number INTEGER NOT NULL,

    -- Core relationships
    account_id TEXT NOT NULL,
    operation_id BIGINT NOT NULL,
    tx_hash TEXT NOT NULL REFERENCES transactions(hash),

    -- Balance fields
    token_id TEXT,
    amount TEXT,

    -- Account relationship fields
    signer_account_id TEXT,
    spender_account_id TEXT,
    sponsored_account_id TEXT,
    sponsor_account_id TEXT,
    deployer_account_id TEXT,
    funder_account_id TEXT,
    offer_id TEXT,

    -- Entity identifiers (moved from key_value)
    claimable_balance_id TEXT,
    liquidity_pool_id TEXT,
    data_name TEXT,

    -- Flattened from signer_weights JSONB (range 0-255)
    signer_weight_old SMALLINT,
    signer_weight_new SMALLINT,

    -- Flattened from thresholds JSONB (range 0-255)
    threshold_old SMALLINT,
    threshold_new SMALLINT,

    -- Flattened from trustline_limit JSONB
    trustline_limit_old TEXT,
    trustline_limit_new TEXT,

    -- Flags as bitmask instead of JSON array
    -- Bit 0: authorized, Bit 1: auth_required, Bit 2: auth_revocable,
    -- Bit 3: auth_immutable, Bit 4: auth_clawback_enabled, Bit 5: clawback_enabled,
    -- Bit 6: authorized_to_maintain_liabilities
    flags SMALLINT,

    -- ONLY truly variable data remains as JSONB (data entries, home domain)
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
