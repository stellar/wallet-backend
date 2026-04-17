-- +migrate Up

-- Table: sep41_allowances
-- Stores current SEP-41 token allowances (approve / transfer_from relationships).
-- Readers filter on expiration_ledger >= latest_ledger_cursor to hide expired grants.
CREATE TABLE sep41_allowances (
    owner_address TEXT NOT NULL,
    spender_address TEXT NOT NULL,
    contract_id UUID NOT NULL,
    amount TEXT NOT NULL DEFAULT '0',
    expiration_ledger INTEGER NOT NULL DEFAULT 0,
    last_modified_ledger INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (owner_address, spender_address, contract_id),
    CONSTRAINT fk_sep41_allowance_contract_token
        FOREIGN KEY (contract_id) REFERENCES contract_tokens(id)
        DEFERRABLE INITIALLY DEFERRED
) WITH (
    fillfactor = 80,
    autovacuum_vacuum_scale_factor = 0.02,
    autovacuum_vacuum_threshold = 50,
    autovacuum_analyze_scale_factor = 0.01,
    autovacuum_analyze_threshold = 50,
    autovacuum_vacuum_cost_delay = 0,
    autovacuum_vacuum_cost_limit = 1000
);

CREATE INDEX idx_sep41_allowances_owner ON sep41_allowances(owner_address);
CREATE INDEX idx_sep41_allowances_spender ON sep41_allowances(spender_address);

-- +migrate Down

DROP TABLE IF EXISTS sep41_allowances;
