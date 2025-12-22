-- +migrate Up

-- Table: state_changes
-- Removed columns (can be derived from to_id):
--   - tx_hash: join transactions on (to_id & ~4095) = transactions.to_id
--   - operation_id: equals to_id when (to_id & 4095) != 0, else 0 for fee changes
--   - ledger_number: (to_id >> 32)::integer
--   - ingested_at: removed (not needed)
-- Changed columns:
--   - state_change_category: TEXT -> SMALLINT
--   - state_change_reason: TEXT -> SMALLINT
CREATE TABLE state_changes (
    to_id BIGINT NOT NULL,
    state_change_order BIGINT NOT NULL,
    state_change_category SMALLINT NOT NULL,
    state_change_reason SMALLINT,
    ledger_created_at TIMESTAMPTZ NOT NULL,
    account_id TEXT NOT NULL,
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
    trustline_limit JSONB,
    PRIMARY KEY (to_id, state_change_order)
);

-- Index for state_changes JOIN to transactions
-- Supports: JOIN transactions t ON (sc.to_id & ~4095) = t.to_id
CREATE INDEX idx_state_changes_tx_to_id ON state_changes ((to_id & ~4095));
CREATE INDEX idx_state_changes_account_id ON state_changes(account_id);
CREATE INDEX idx_state_changes_ledger_created_at ON state_changes(ledger_created_at);

-- +migrate Down

-- Tables
DROP TABLE IF EXISTS state_changes CASCADE;
