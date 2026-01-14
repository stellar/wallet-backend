-- Schema migration to add trustline XDR fields to account_trustlines table.
-- This enables storing complete trustline state directly from XDR, eliminating RPC calls.

-- +migrate Up
ALTER TABLE account_trustlines
    ADD COLUMN balance BIGINT NOT NULL DEFAULT 0,
    ADD COLUMN last_modified_ledger INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN trust_limit BIGINT NOT NULL DEFAULT 0,
    ADD COLUMN buying_liabilities BIGINT NOT NULL DEFAULT 0,
    ADD COLUMN selling_liabilities BIGINT NOT NULL DEFAULT 0,
    ADD COLUMN flags INTEGER NOT NULL DEFAULT 0;

-- +migrate Down
ALTER TABLE account_trustlines
    DROP COLUMN IF EXISTS flags,
    DROP COLUMN IF EXISTS selling_liabilities,
    DROP COLUMN IF EXISTS buying_liabilities,
    DROP COLUMN IF EXISTS trust_limit,
    DROP COLUMN IF EXISTS last_modified_ledger,
    DROP COLUMN IF EXISTS balance;
