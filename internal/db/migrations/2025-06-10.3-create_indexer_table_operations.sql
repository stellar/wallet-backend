-- +migrate Up

-- Table: operations
-- tx_hash removed - can be derived by joining transactions on (id & ~4095) = transactions.to_id
-- ledger_number removed - can be derived from id using: (id >> 32)::integer
-- operation_type changed from TEXT to SMALLINT for storage efficiency
CREATE TABLE operations (
    id BIGINT PRIMARY KEY,
    operation_type SMALLINT NOT NULL,
    operation_xdr TEXT,
    ledger_created_at TIMESTAMPTZ NOT NULL
);

-- Index for state_changes JOIN to transactions
-- Supports: JOIN transactions t ON (sc.to_id & ~4095) = t.to_id
CREATE INDEX idx_operations_tx_to_id ON operations ((id & ~4095));
CREATE INDEX idx_operations_ledger_created_at ON operations(ledger_created_at);

-- Table: operations_accounts
CREATE TABLE operations_accounts (
    operation_id BIGINT NOT NULL REFERENCES operations(id) ON DELETE CASCADE,
    account_id BYTEA NOT NULL,
    PRIMARY KEY (account_id, operation_id)
);

-- +migrate Down

-- Tables
DROP TABLE IF EXISTS operations_accounts CASCADE;
DROP TABLE IF EXISTS operations CASCADE;
