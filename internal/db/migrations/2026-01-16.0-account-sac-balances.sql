-- +migrate Up

-- Table: account_sac_balances
-- Stores SAC (Stellar Asset Contract) balance data for contract addresses (C...) during ingestion.
-- Classic Stellar accounts (G...) have SAC balances in their trustlines, so only contract holders are stored here.
CREATE TABLE account_sac_balances (
    account_address TEXT NOT NULL,
    contract_id UUID NOT NULL,
    balance TEXT NOT NULL DEFAULT '0',
    is_authorized BOOLEAN NOT NULL DEFAULT true,
    is_clawback_enabled BOOLEAN NOT NULL DEFAULT false,
    last_modified_ledger INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (account_address, contract_id)
);

-- +migrate Down

DROP TABLE IF EXISTS account_sac_balances;
