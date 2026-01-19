-- Schema migration for account trustline balances table.
-- Stores account trustline balances with full XDR state data from Stellar network.

-- +migrate Up

CREATE TABLE trustline_balances (
    account_address TEXT NOT NULL,
    asset_id UUID NOT NULL,
    balance BIGINT NOT NULL DEFAULT 0,
    trust_limit BIGINT NOT NULL DEFAULT 0,
    buying_liabilities BIGINT NOT NULL DEFAULT 0,
    selling_liabilities BIGINT NOT NULL DEFAULT 0,
    flags INTEGER NOT NULL DEFAULT 0,
    last_modified_ledger BIGINT NOT NULL DEFAULT 0,
    PRIMARY KEY (account_address, asset_id),
    CONSTRAINT fk_trustline_asset
        FOREIGN KEY (asset_id) REFERENCES trustline_assets(id)
        DEFERRABLE INITIALLY DEFERRED
);

-- +migrate Down

DROP TABLE IF EXISTS trustline_balances;
