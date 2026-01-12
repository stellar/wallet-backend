-- +migrate Up

-- Table: account_trustlines
-- Stores account-to-trustline-asset relationships using JSONB arrays for efficient storage.
-- Each row represents one account with all its trustline asset IDs stored as a JSONB array.
-- This replaces Redis hash storage for trustlines.
CREATE TABLE account_trustlines (
    account_address TEXT PRIMARY KEY,
    asset_ids JSONB NOT NULL DEFAULT '[]',
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Table: account_contracts
-- Stores account-to-contract relationships using JSONB arrays for efficient storage.
-- Each row represents one account with all contract IDs (C...) it has ever held balances in.
-- This replaces Redis set storage for contracts.
CREATE TABLE account_contracts (
    account_address TEXT PRIMARY KEY,
    contract_ids JSONB NOT NULL DEFAULT '[]',
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- +migrate Down

DROP TABLE IF EXISTS account_contracts;
DROP TABLE IF EXISTS account_trustlines;
