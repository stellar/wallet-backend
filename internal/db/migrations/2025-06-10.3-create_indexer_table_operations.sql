-- +migrate Up

-- Table: operations
-- Stores Stellar operations with XDR data in binary format for storage efficiency.
CREATE TABLE operations (
    id BIGINT PRIMARY KEY,
    tx_hash CHAR(64) NOT NULL REFERENCES transactions(hash),
    operation_type VARCHAR(50) NOT NULL,    -- Bounded operation type string
    operation_xdr BYTEA,                    -- Binary XDR (25% smaller than base64 TEXT)
    ledger_number INTEGER NOT NULL,
    ledger_created_at TIMESTAMPTZ NOT NULL,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Table: operations_accounts
-- Junction table linking operations to participating accounts.
CREATE TABLE operations_accounts (
    operation_id BIGINT NOT NULL REFERENCES operations(id) ON DELETE CASCADE,
    account_id CHAR(56) NOT NULL,           -- Stellar addresses are always 56 characters
    ledger_created_at TIMESTAMPTZ NOT NULL,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (account_id, operation_id)
);

-- +migrate Down

-- Tables
DROP TABLE IF EXISTS operations_accounts CASCADE;
DROP TABLE IF EXISTS operations CASCADE;
