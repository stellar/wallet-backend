-- +migrate Up

-- Table: sep41_balances
-- Stores SEP-41 token balance data for pure SEP-41 contracts (non-SAC).
-- SAC balances are tracked separately in sac_balances / trustline_balances.
-- Storage parameters tuned for heavy UPSERT/DELETE during ledger ingestion, mirroring sac_balances.
CREATE TABLE sep41_balances (
    account_address TEXT NOT NULL,
    contract_id UUID NOT NULL,
    balance TEXT NOT NULL DEFAULT '0',
    last_modified_ledger INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (account_address, contract_id),
    CONSTRAINT fk_sep41_contract_token
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

-- +migrate Down

DROP TABLE IF EXISTS sep41_balances;
