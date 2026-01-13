-- +migrate Up

-- Table: account_trustlines
-- Junction table mapping accounts to their trustline assets.
CREATE TABLE account_trustlines (
    account_address TEXT NOT NULL,
    asset_id BIGINT NOT NULL REFERENCES trustline_assets(id),
    PRIMARY KEY (account_address, asset_id)
);

-- Index for reverse lookups (find accounts holding a specific asset)
CREATE INDEX ON account_trustlines(asset_id);

-- Table: account_contracts
-- Junction table mapping accounts to their contract tokens.
CREATE TABLE account_contracts (
    account_address TEXT NOT NULL,
    contract_id BIGINT NOT NULL REFERENCES contract_tokens(id),
    PRIMARY KEY (account_address, contract_id)
);

-- Index for reverse lookups (find accounts holding a specific contract)
CREATE INDEX ON account_contracts(contract_id);

-- +migrate Down

DROP TABLE IF EXISTS account_contracts;
DROP TABLE IF EXISTS account_trustlines;
