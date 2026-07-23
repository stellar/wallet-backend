-- +migrate Up

-- Table: liquidity_pools
-- Stores constant-product liquidity pool reserves (constituent assets + amounts) during ingestion.
-- Populated from LiquidityPoolEntry ledger entries and joined at query time with liquidity_pool_balances
-- to expose an account's pool-share balance alongside the pool's reserves.
-- Assets are stored canonically ("native" or "CODE:ISSUER").
-- Storage parameters tuned for heavy UPSERT during ledger ingestion (reserves shift on every
-- deposit/withdraw). UPSERTs only modify non-indexed columns (asset_a, amount_a, asset_b, amount_b,
-- last_modified_ledger) while the PK column (pool_id) is never changed.
CREATE TABLE liquidity_pools (
    pool_id TEXT PRIMARY KEY,
    asset_a TEXT NOT NULL,
    amount_a BIGINT NOT NULL DEFAULT 0,
    asset_b TEXT NOT NULL,
    amount_b BIGINT NOT NULL DEFAULT 0,
    last_modified_ledger BIGINT NOT NULL DEFAULT 0
) WITH (
    -- Reserve 20% free space per page so PostgreSQL can do HOT (Heap-Only Tuple) updates.
    -- Unlike the other balance/state tables, this table's reserves update on essentially
    -- every row per day (each pool's amount_a/amount_b shifts on every ledger it trades),
    -- so it needs the full 20% reserve to sustain HOT updates.
    fillfactor = 80,
    -- Trigger vacuum when 2% of rows are dead (default 20%).
    autovacuum_vacuum_scale_factor = 0.02,
    autovacuum_vacuum_threshold = 50,
    -- Refresh planner statistics at 1% change (default 10%). Reserves shift every ledger,
    -- so stale stats can cause bad query plans.
    autovacuum_analyze_scale_factor = 0.01,
    autovacuum_analyze_threshold = 50,
    -- Setting cost_delay=0 disables cost-based throttling for this table's autovacuum
    -- worker and, per PostgreSQL's balancing rules, exempts it from cross-worker cost
    -- balancing so other tables keep their full budget.
    autovacuum_vacuum_cost_delay = 0
);

-- +migrate Down

DROP TABLE IF EXISTS liquidity_pools;
