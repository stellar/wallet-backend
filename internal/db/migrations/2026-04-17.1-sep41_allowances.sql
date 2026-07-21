-- +migrate Up

-- Table: sep41_allowances
-- Stores current SEP-41 token allowances (approve / transfer_from relationships).
-- Readers filter on expiration_ledger >= latest_ledger_cursor to hide expired grants.
CREATE TABLE sep41_allowances (
    owner_id BYTEA NOT NULL,
    spender_id BYTEA NOT NULL,
    contract_id UUID NOT NULL,
    amount NUMERIC NOT NULL DEFAULT 0,
    expiration_ledger INTEGER NOT NULL DEFAULT 0,
    last_modified_ledger INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (owner_id, spender_id, contract_id),
    CONSTRAINT fk_sep41_allowance_contract_token
        FOREIGN KEY (contract_id) REFERENCES contract_tokens(id)
        DEFERRABLE INITIALLY DEFERRED
) WITH (
    fillfactor = 90,
    autovacuum_vacuum_scale_factor = 0.02,
    autovacuum_vacuum_threshold = 50,
    autovacuum_analyze_scale_factor = 0.01,
    autovacuum_analyze_threshold = 50,
    autovacuum_vacuum_cost_delay = 0
);

-- owner_id is the PK's leading column, so a lookup by owner alone already uses the PK
-- index; a dedicated single-column index on it would be redundant.
-- No code path filters by spender_id alone, so a dedicated index for it would sit unused.
-- Backs the expired-allowance sweep's ORDER BY expiration_ledger LIMIT n. BatchUpsert
-- updates expiration_ledger, so this makes those updates non-HOT — an accepted
-- tradeoff at allowance write volumes.
CREATE INDEX idx_sep41_allowances_expiration_ledger ON sep41_allowances(expiration_ledger);

-- +migrate Down

DROP TABLE IF EXISTS sep41_allowances;
