-- +migrate Up

-- Table: operations
CREATE TABLE operations (
    id BIGINT PRIMARY KEY,
    tx_hash TEXT NOT NULL REFERENCES transactions(hash),
    operation_type TEXT NOT NULL,
    operation_xdr TEXT,
    ledger_number INTEGER NOT NULL,
    ledger_created_at TIMESTAMPTZ NOT NULL,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Table: operations_accounts
CREATE TABLE operations_accounts (
    operation_id BIGINT NOT NULL REFERENCES operations(id) ON DELETE CASCADE,
    account_id TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (account_id, operation_id)
);

-- +migrate Down

-- Tables
DROP TABLE IF EXISTS operations_accounts CASCADE;
DROP TABLE IF EXISTS operations CASCADE;
