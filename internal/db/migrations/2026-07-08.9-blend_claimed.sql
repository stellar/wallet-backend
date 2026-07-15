-- +migrate Up

-- Lifetime pool-reserve BLND claimed per (pool, user), accumulated additively from
-- pool `claim` events during current-state indexing.
CREATE TABLE blend_pool_claimed (
    pool_contract_id     BYTEA NOT NULL,
    user_account_id      BYTEA NOT NULL,
    claimed_blnd         NUMERIC NOT NULL DEFAULT 0,   -- Σ claimed BLND (underlying, 7 decimals)
    last_modified_ledger INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (pool_contract_id, user_account_id)
) WITH (
    fillfactor = 90,
    autovacuum_vacuum_scale_factor = 0.02,
    autovacuum_vacuum_threshold = 50,
    autovacuum_analyze_scale_factor = 0.01,
    autovacuum_analyze_threshold = 50,
    autovacuum_vacuum_cost_delay = 0
);
-- The resolver reads by account; user is not the primary-key prefix.
CREATE INDEX idx_blend_pool_claimed_user ON blend_pool_claimed (user_account_id);

-- Lifetime backstop-emission claims in Comet LP tokens, account-wide. A backstop
-- `claim` auto-compounds the claimed BLND into LP across every pool the caller
-- claimed from and emits a single aggregate amount carrying NO pool address, so
-- this total can only be keyed by user, not by pool.
CREATE TABLE blend_backstop_claimed (
    user_account_id      BYTEA NOT NULL,
    claimed_lp           NUMERIC NOT NULL DEFAULT 0,   -- Σ claimed Comet LP (7 decimals)
    last_modified_ledger INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (user_account_id)
) WITH (
    fillfactor = 90,
    autovacuum_vacuum_scale_factor = 0.02,
    autovacuum_vacuum_threshold = 50,
    autovacuum_analyze_scale_factor = 0.01,
    autovacuum_analyze_threshold = 50,
    autovacuum_vacuum_cost_delay = 0
);

-- +migrate Down

DROP TABLE IF EXISTS blend_backstop_claimed;
DROP TABLE IF EXISTS blend_pool_claimed;
