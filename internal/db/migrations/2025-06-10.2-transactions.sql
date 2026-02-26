-- +migrate Up

-- Table: transactions
CREATE TABLE transactions (
    to_id BIGINT PRIMARY KEY,
    hash BYTEA NOT NULL UNIQUE,
    envelope_xdr TEXT,
    fee_charged BIGINT NOT NULL,
    result_code TEXT NOT NULL,
    meta_xdr TEXT,
    ledger_number INTEGER NOT NULL,
    ledger_created_at TIMESTAMPTZ NOT NULL,
    is_fee_bump BOOLEAN NOT NULL DEFAULT false,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_transactions_ledger_created_at ON transactions(ledger_created_at);

-- Table: transactions_accounts
CREATE TABLE transactions_accounts (
    tx_to_id BIGINT NOT NULL REFERENCES transactions(to_id) ON DELETE CASCADE,
    account_id BYTEA NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (account_id, tx_to_id)
);

CREATE INDEX idx_transactions_accounts_tx_to_id ON transactions_accounts(tx_to_id);

-- +migrate Down

-- Tables
DROP TABLE IF EXISTS transactions_accounts CASCADE;
DROP TABLE IF EXISTS transactions CASCADE;
