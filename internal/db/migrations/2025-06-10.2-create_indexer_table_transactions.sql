-- +migrate Up

-- Table: transactions
CREATE TABLE transactions (
    hash TEXT PRIMARY KEY,
    to_id BIGINT NOT NULL,
    envelope_xdr TEXT,
    result_xdr TEXT,
    meta_xdr TEXT,
    ledger_number INTEGER NOT NULL,
    ledger_created_at TIMESTAMPTZ NOT NULL,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Table: transactions_accounts
CREATE TABLE transactions_accounts (
    tx_hash TEXT NOT NULL REFERENCES transactions(hash) ON DELETE CASCADE,
    account_id TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (account_id, tx_hash)
);

-- +migrate Down

-- Tables
DROP TABLE IF EXISTS transactions_accounts CASCADE;
DROP TABLE IF EXISTS transactions CASCADE;
