-- Schema migration for account contract tokens junction table.
-- Maps accounts to their contract tokens (SAC and SEP-41 contracts).

-- +migrate Up

CREATE TABLE account_contract_tokens (
    account_address TEXT NOT NULL,
    contract_id UUID NOT NULL,
    PRIMARY KEY (account_address, contract_id),
    CONSTRAINT fk_contract_token
        FOREIGN KEY (contract_id) REFERENCES contract_tokens(id)
        DEFERRABLE INITIALLY DEFERRED
);

-- +migrate Down

DROP TABLE IF EXISTS account_contract_tokens;
