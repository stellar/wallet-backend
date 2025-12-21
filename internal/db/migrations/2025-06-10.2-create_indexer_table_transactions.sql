-- +migrate Up

-- Table: transactions
-- Primary key changed from hash to to_id (TOID) for storage efficiency.
-- ledger_number can be derived from to_id using: (to_id >> 32)::integer
CREATE TABLE transactions (
    to_id BIGINT PRIMARY KEY,
    hash TEXT NOT NULL UNIQUE,
    envelope_xdr TEXT,
    result_xdr TEXT,
    meta_xdr TEXT,
    ledger_created_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX idx_transactions_ledger_created_at ON transactions(ledger_created_at);
CREATE INDEX idx_transactions_hash ON transactions(hash);

-- Table: transactions_accounts
-- Uses tx_id (BIGINT) instead of tx_hash (TEXT) for storage efficiency.
CREATE TABLE transactions_accounts (
    tx_id BIGINT NOT NULL REFERENCES transactions(to_id) ON DELETE CASCADE,
    account_id TEXT NOT NULL,
    PRIMARY KEY (account_id, tx_id)
);

-- +migrate Down

-- Tables
DROP TABLE IF EXISTS transactions_accounts CASCADE;
DROP TABLE IF EXISTS transactions CASCADE;
