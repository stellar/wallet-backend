-- +migrate Up

-- Table: account_trustlines
-- Junction table mapping accounts to their trustline assets.
CREATE TABLE account_trustlines (
    account_address TEXT NOT NULL,
    asset_id UUID NOT NULL,
    PRIMARY KEY (account_address, asset_id),
    CONSTRAINT fk_trustline_asset
        FOREIGN KEY (asset_id) REFERENCES trustline_assets(id)
        DEFERRABLE INITIALLY DEFERRED
);

-- Table: account_contracts
-- Junction table mapping accounts to their contract tokens.
CREATE TABLE account_contracts (
    account_address TEXT NOT NULL,
    contract_id UUID NOT NULL,
    PRIMARY KEY (account_address, contract_id),
    CONSTRAINT fk_contract_token
        FOREIGN KEY (contract_id) REFERENCES contract_tokens(id)
        DEFERRABLE INITIALLY DEFERRED
);

-- +migrate Down

DROP TABLE IF EXISTS account_contracts;
DROP TABLE IF EXISTS account_trustlines;
