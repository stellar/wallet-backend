-- +migrate Up

-- pool-level backstop totals (PoolBalance(pool) on the backstop contract) — for "Backstop $X"
CREATE TABLE blend_backstop_pools (
    pool_contract_id     BYTEA PRIMARY KEY,
    shares               NUMERIC NOT NULL DEFAULT 0,
    tokens               NUMERIC NOT NULL DEFAULT 0,   -- backstop-LP (BLND-USDC) token amount
    q4w                  NUMERIC NOT NULL DEFAULT 0,
    -- backstop emission accrual for this pool (BEmisData(pool); exact field layout:
    -- confirm at impl)
    emis_eps             BIGINT,
    emis_index           NUMERIC,
    emis_expiration      BIGINT,
    emis_last_time       BIGINT,
    last_modified_ledger INTEGER NOT NULL DEFAULT 0
) WITH (
    -- Row count is protocol-bounded (one row per deployed Blend pool, forever a small
    -- number), so default autovacuum behavior is fine.
    fillfactor = 90
);

-- +migrate Down

DROP TABLE IF EXISTS blend_backstop_pools;
