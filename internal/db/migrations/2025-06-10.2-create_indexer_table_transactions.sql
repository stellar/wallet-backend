-- +migrate Up

-- Table: transactions
CREATE TABLE transactions (
    hash TEXT PRIMARY KEY,
    envelope_xdr TEXT,
    result_xdr TEXT,
    meta_xdr TEXT,
    fee_charged_stroops INTEGER,
    ledger_number INTEGER NOT NULL,
    ledger_created_at TIMESTAMPTZ NOT NULL,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_transactions_ledger_created_at ON transactions(ledger_created_at);

-- Table: transactions_accounts
CREATE TABLE transactions_accounts (
    tx_hash TEXT NOT NULL REFERENCES transactions(hash) ON DELETE CASCADE,
    account_id TEXT NOT NULL REFERENCES accounts(stellar_address) ON DELETE CASCADE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (tx_hash, account_id)
);

CREATE INDEX idx_transactions_accounts_account_id ON transactions_accounts(account_id);

-- +migrate Down

-- Tables
DROP TABLE IF EXISTS transactions_accounts CASCADE;
DROP TABLE IF EXISTS transactions CASCADE;
