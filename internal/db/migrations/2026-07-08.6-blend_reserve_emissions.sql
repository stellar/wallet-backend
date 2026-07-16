-- +migrate Up

-- reserve-level emission config/accrual (EmisData(reserve_token_id) entries on each
-- pool) — the eps source for emissions/net APY
CREATE TABLE blend_reserve_emissions (
    pool_contract_id     BYTEA NOT NULL,
    reserve_token_id     INTEGER NOT NULL,    -- reserve_index*2 (dToken) / +1 (bToken)
    eps                  BIGINT NOT NULL,
    emission_index       TEXT NOT NULL,
    expiration           BIGINT NOT NULL,
    last_time            BIGINT NOT NULL,
    last_modified_ledger INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (pool_contract_id, reserve_token_id)
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

DROP TABLE IF EXISTS blend_reserve_emissions;
