-- +migrate Up

-- Table: native_balances
-- Stores native XLM balance data for accounts during ingestion.
CREATE TABLE native_balances (
    account_address TEXT PRIMARY KEY,
    balance BIGINT NOT NULL DEFAULT 0,
    minimum_balance BIGINT NOT NULL DEFAULT 0,
    buying_liabilities BIGINT NOT NULL DEFAULT 0,
    selling_liabilities BIGINT NOT NULL DEFAULT 0,
    last_modified_ledger INTEGER NOT NULL DEFAULT 0
);

-- +migrate Down

DROP TABLE IF EXISTS native_balances;
