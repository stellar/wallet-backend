-- Schema migration to add balance tracking columns to account_trustlines table.
-- This enables storing trustline balances directly, eliminating RPC calls for balance queries.

-- +migrate Up
ALTER TABLE account_trustlines
    ADD COLUMN balance BIGINT NOT NULL DEFAULT 0,
    ADD COLUMN last_modified_ledger INTEGER NOT NULL DEFAULT 0;

-- +migrate Down
ALTER TABLE account_trustlines
    DROP COLUMN IF EXISTS last_modified_ledger,
    DROP COLUMN IF EXISTS balance;
