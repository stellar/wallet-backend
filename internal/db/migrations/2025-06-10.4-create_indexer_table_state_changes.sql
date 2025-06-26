-- +migrate Up

-- Table: state_changes
CREATE TABLE state_changes (
    id TEXT PRIMARY KEY,
    state_change_category TEXT NOT NULL,
    state_change_reason TEXT,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    ledger_created_at TIMESTAMPTZ NOT NULL,
    ledger_number INTEGER NOT NULL,
    account_id TEXT NOT NULL REFERENCES accounts(stellar_address),
    operation_id BIGINT NOT NULL REFERENCES operations(id),
    tx_hash TEXT NOT NULL REFERENCES transactions(hash),
    token TEXT,
    amount TEXT,
    claimable_balance_id TEXT,
    contract_id TEXT,
    flags JSONB,
    key_value JSONB,
    offer_id TEXT,
    signer_account_id TEXT,
    signer_weight BIGINT,
    spender_account_id TEXT,
    target_account_id TEXT,
    thresholds JSONB
);

CREATE INDEX idx_state_changes_account_id ON state_changes(account_id);
CREATE INDEX idx_state_changes_tx_hash ON state_changes(tx_hash);
CREATE INDEX idx_state_changes_operation_id ON state_changes(operation_id);
CREATE INDEX idx_state_changes_ledger_created_at ON state_changes(ledger_created_at);


-- +migrate Down

-- Tables
DROP TABLE IF EXISTS state_changes CASCADE;
