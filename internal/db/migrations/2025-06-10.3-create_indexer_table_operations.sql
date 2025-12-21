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

CREATE INDEX idx_operations_operation_type ON operations(operation_type);
CREATE INDEX idx_operations_ledger_created_at ON operations(ledger_created_at);

-- Table: operations_accounts
CREATE TABLE operations_accounts (
    operation_id BIGINT NOT NULL REFERENCES operations(id) ON DELETE CASCADE,
    account_id TEXT NOT NULL,
    PRIMARY KEY (account_id, operation_id)
);

-- +migrate Down

-- Tables
DROP TABLE IF EXISTS operations_accounts CASCADE;
DROP TABLE IF EXISTS operations CASCADE;
