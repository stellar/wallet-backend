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
    -- Reserve 20% free space per page so PostgreSQL can do HOT (Heap-Only Tuple) updates.
    -- A row exists only for a reserve token that has emissions configured, and every such row
    -- re-accrues (emission_index, last_time) on each interaction with its reserve, so nearly the
    -- whole table turns over on every active ledger. Only the PK columns are indexed.
    fillfactor = 80,
    autovacuum_vacuum_scale_factor = 0.02,
    autovacuum_vacuum_threshold = 50,
    autovacuum_analyze_scale_factor = 0.01,
    autovacuum_analyze_threshold = 50,
    autovacuum_vacuum_cost_delay = 0
);

-- +migrate Down

DROP TABLE IF EXISTS blend_reserve_emissions;
