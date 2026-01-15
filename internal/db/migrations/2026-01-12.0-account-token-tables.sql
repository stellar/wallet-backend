-- +migrate Up

-- Table: account_trustline_balances
-- Stores account trustline balances with full XDR state data.
CREATE TABLE account_trustline_balances (
    account_address TEXT NOT NULL,
    asset_id UUID NOT NULL,
    balance BIGINT NOT NULL DEFAULT 0,
    trust_limit BIGINT NOT NULL DEFAULT 0,
    buying_liabilities BIGINT NOT NULL DEFAULT 0,
    selling_liabilities BIGINT NOT NULL DEFAULT 0,
    flags INTEGER NOT NULL DEFAULT 0,
    last_modified_ledger INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (account_address, asset_id)
);

-- Table: account_contracts
-- Junction table mapping accounts to their contract tokens.
CREATE TABLE account_contracts (
    account_address TEXT NOT NULL,
    contract_id UUID NOT NULL,
    PRIMARY KEY (account_address, contract_id)
);

-- +migrate Down

DROP TABLE IF EXISTS account_contracts;
DROP TABLE IF EXISTS account_trustline_balances;
