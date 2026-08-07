-- +migrate Up

-- per-pool reserve config + live rates (ResData + ResConfig + ResList)
CREATE TABLE blend_reserves (
    pool_contract_id     BYTEA NOT NULL,
    reserve_index        INTEGER NOT NULL,
    asset_contract_id    BYTEA NOT NULL,      -- reserve asset (token/SAC C-address)
    b_rate               TEXT NOT NULL,
    d_rate               TEXT NOT NULL,
    b_supply             TEXT NOT NULL,
    d_supply             TEXT NOT NULL,
    ir_mod               TEXT NOT NULL,
    backstop_credit      TEXT NOT NULL,
    last_time            BIGINT NOT NULL,
    decimals             INTEGER NOT NULL,
    c_factor             INTEGER NOT NULL,
    l_factor             INTEGER NOT NULL,
    util                 INTEGER NOT NULL,    -- target util
    max_util             INTEGER NOT NULL,
    r_base               INTEGER NOT NULL,
    r_one                INTEGER NOT NULL,
    r_two                INTEGER NOT NULL,
    r_three              INTEGER NOT NULL,
    reactivity           INTEGER NOT NULL,
    supply_cap           TEXT NOT NULL,
    enabled              BOOLEAN NOT NULL,
    last_modified_ledger INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (pool_contract_id, reserve_index),
    -- One reserve slot per (pool, asset): the fold SQL resolves reserve_index by
    -- joining on this pair (BatchApplyNetDeltas, ApplyAuctionAdjustments,
    -- BatchUpdateData), and UPDATE ... FROM silently applies an arbitrary source
    -- row if the join ever matched twice. The backing unique index also serves
    -- those joins.
    UNIQUE (pool_contract_id, asset_contract_id)
) WITH (
    -- Reserve 20% free space per page so PostgreSQL can do HOT (Heap-Only Tuple) updates.
    -- Unlike the per-user position tables, every reserve of an active pool is rewritten on
    -- each interaction with it (b_rate, d_rate, the supplies and last_time all accrue on every
    -- supply, borrow, repay and withdraw), so this table needs the full 20% reserve to sustain
    -- HOT updates. None of those accruing columns are indexed.
    fillfactor = 80,
    autovacuum_vacuum_scale_factor = 0.02,
    autovacuum_vacuum_threshold = 50,
    autovacuum_analyze_scale_factor = 0.01,
    autovacuum_analyze_threshold = 50,
    autovacuum_vacuum_cost_delay = 0
);
-- +migrate Down

DROP TABLE IF EXISTS blend_reserves;
