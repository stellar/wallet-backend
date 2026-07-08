-- +migrate Up

-- pool-level backstop totals (PoolBalance(pool) on the backstop contract) — for "Backstop $X"
CREATE TABLE blend_backstop_pools (
    pool_contract_id     BYTEA PRIMARY KEY,
    shares               TEXT NOT NULL DEFAULT '0',
    tokens               TEXT NOT NULL DEFAULT '0',   -- backstop-LP (BLND-USDC) token amount
    q4w                  TEXT NOT NULL DEFAULT '0',
    -- backstop emission accrual for this pool (BEmisData(pool); exact field layout:
    -- confirm at impl)
    emis_eps             BIGINT,
    emis_index           TEXT,
    emis_expiration      BIGINT,
    emis_last_time       BIGINT,
    last_modified_ledger INTEGER NOT NULL DEFAULT 0
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

DROP TABLE IF EXISTS blend_backstop_pools;
