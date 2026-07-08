-- +migrate Up

-- Backstop user emissions (UEmisData(pool, user)) are stored with
-- source_contract_id = the POOL contract and token_id = -1, so per-pool
-- backstop streams stay distinct; reserve-emission rows use token_id >= 0
-- (reserve_index*2, +1 for bToken).
-- raw user emission accrual (reserve emissions on pool, backstop emissions on backstop)
CREATE TABLE blend_emissions (
    source_contract_id   BYTEA NOT NULL,      -- pool (reserve emis) or backstop
    user_account_id      BYTEA NOT NULL,
    token_id             INTEGER NOT NULL,    -- reserve_token_id (reserve_index*2 (+1 for bToken)) / backstop id
    emission_index       TEXT NOT NULL,       -- user's last-accrued index
    accrued              TEXT NOT NULL,
    last_modified_ledger INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (source_contract_id, user_account_id, token_id)
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

DROP TABLE IF EXISTS blend_emissions;
