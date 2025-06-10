-- +migrate Up

-- Table: operations
CREATE TABLE operations (
    id TEXT PRIMARY KEY,
    tx_hash TEXT NOT NULL REFERENCES transactions(hash),
    operation_type TEXT NOT NULL,
    operation_xdr TEXT,
    ledger_created_at TIMESTAMPTZ NOT NULL,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_operations_tx_hash ON operations(tx_hash);
CREATE INDEX idx_operations_operation_type ON operations(operation_type);
CREATE INDEX idx_operations_ledger_created_at ON operations(ledger_created_at);

-- Table: operations_accounts
CREATE TABLE operations_accounts (
    operation_id TEXT NOT NULL REFERENCES operations(id) ON DELETE CASCADE,
    account_id TEXT NOT NULL REFERENCES accounts(stellar_address) ON DELETE CASCADE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (operation_id, account_id)
);

CREATE INDEX idx_operations_accounts_account_id ON operations_accounts(account_id);

-- +migrate Down

-- Tables
DROP TABLE IF EXISTS operations_accounts CASCADE;
DROP TABLE IF EXISTS operations CASCADE;
