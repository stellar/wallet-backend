-- +migrate Up

-- pool-level config + metadata (from PoolConfig instance storage + pool name)
CREATE TABLE blend_pools (
    pool_contract_id     BYTEA PRIMARY KEY,
    name                 TEXT,               -- "Fixed Pool v2" (also enriched onto protocol_contracts.name)
    oracle_contract_id   BYTEA,             -- SEP-40 oracle for this pool
    backstop_rate        INTEGER,           -- bstop_rate / take rate
    status               INTEGER,           -- 0..6 (Admin_Active..Setup)
    max_positions        INTEGER,
    min_collateral       TEXT,
    admin                BYTEA,              -- pool admin (instance "Admin" key); owned vs standard-pool trust signal
    in_reward_zone       BOOLEAN NOT NULL DEFAULT FALSE,  -- member of the backstop's RZ list (earns BLND emissions)
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

DROP TABLE IF EXISTS blend_pools;
