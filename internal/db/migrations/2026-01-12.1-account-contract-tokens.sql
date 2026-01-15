-- Schema migration for account contract tokens junction table.
-- Maps accounts to their contract tokens (SAC and SEP-41 contracts).

-- +migrate Up

CREATE TABLE account_contracts (
    account_address TEXT NOT NULL,
    contract_id UUID NOT NULL,
    PRIMARY KEY (account_address, contract_id)
);

-- +migrate Down

DROP TABLE IF EXISTS account_contracts;
