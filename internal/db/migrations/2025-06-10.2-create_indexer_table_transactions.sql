-- +migrate Up

-- Table: transactions
-- Stores Stellar transactions with XDR data in binary format for storage efficiency.
CREATE TABLE transactions (
    hash CHAR(64) PRIMARY KEY,              -- SHA-256 hex is always 64 characters
    to_id BIGINT NOT NULL,
    envelope_xdr BYTEA,                     -- Binary XDR (25% smaller than base64 TEXT)
    result_xdr BYTEA,                       -- Binary XDR
    meta_xdr BYTEA,                         -- Binary XDR
    ledger_number INTEGER NOT NULL,
    ledger_created_at TIMESTAMPTZ NOT NULL,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Table: transactions_accounts
-- Junction table linking transactions to participating accounts.
CREATE TABLE transactions_accounts (
    tx_hash CHAR(64) NOT NULL REFERENCES transactions(hash) ON DELETE CASCADE,
    account_id CHAR(56) NOT NULL,           -- Stellar addresses are always 56 characters
    ledger_created_at TIMESTAMPTZ NOT NULL,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (account_id, tx_hash)
);

-- +migrate Down

-- Tables
DROP TABLE IF EXISTS transactions_accounts CASCADE;
DROP TABLE IF EXISTS transactions CASCADE;
