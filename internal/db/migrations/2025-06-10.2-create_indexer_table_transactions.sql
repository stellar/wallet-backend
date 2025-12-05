-- +migrate Up

-- Table: transactions
-- Stores Stellar transactions with XDR data in binary format for storage efficiency.
CREATE TABLE transactions (
    hash TEXT PRIMARY KEY,
    to_id BIGINT NOT NULL,
    envelope_xdr BYTEA,
    result_xdr BYTEA,
    meta_xdr BYTEA,
    ledger_number INTEGER NOT NULL,
    ledger_created_at TIMESTAMPTZ NOT NULL,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Table: transactions_accounts
-- Junction table linking transactions to participating accounts.
CREATE TABLE transactions_accounts (
    tx_hash TEXT NOT NULL REFERENCES transactions(hash) ON DELETE CASCADE,
    account_id TEXT NOT NULL,
    ledger_created_at TIMESTAMPTZ NOT NULL,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (account_id, tx_hash)
);

-- +migrate Down

-- Tables
DROP TABLE IF EXISTS transactions_accounts CASCADE;
DROP TABLE IF EXISTS transactions CASCADE;
