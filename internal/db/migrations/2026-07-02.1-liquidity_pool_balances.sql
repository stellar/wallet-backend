-- +migrate Up

-- Table: liquidity_pool_balances
-- Stores an account's liquidity-pool share balance (from pool_share trustlines) during ingestion.
-- Joined at query time with liquidity_pools to expose the pool's reserves alongside the shares.
-- Storage parameters tuned for heavy UPSERT/DELETE during ledger ingestion.
-- UPSERTs only modify non-indexed columns (shares, last_modified_ledger) while the PK columns
-- (account_id, pool_id) are never changed.
CREATE TABLE liquidity_pool_balances (
    account_id BYTEA NOT NULL,
    pool_id TEXT NOT NULL,
    shares BIGINT NOT NULL DEFAULT 0,
    last_modified_ledger BIGINT NOT NULL DEFAULT 0,
    PRIMARY KEY (account_id, pool_id)
) WITH (
    -- Reserve 20% free space per page so PostgreSQL can do HOT (Heap-Only Tuple) updates.
    -- HOT updates rewrite the row in-place on the same page without creating dead tuples
    -- or new index entries, since no indexed column is modified during UPSERTs.
    fillfactor = 80,
    -- Trigger vacuum when 2% of rows are dead (default 20%).
    autovacuum_vacuum_scale_factor = 0.02,
    autovacuum_vacuum_threshold = 50,
    -- Refresh planner statistics at 1% change (default 10%). Balances shift every ledger,
    -- so stale stats can cause bad query plans (e.g. on the GetByAccount JOIN).
    autovacuum_analyze_scale_factor = 0.01,
    autovacuum_analyze_threshold = 50,
    -- No sleep between vacuum page-processing cycles (default 2ms). Per-table setting,
    -- so only workers on this table run full-speed; other tables are unaffected.
    autovacuum_vacuum_cost_delay = 0,
    -- 5x the default page-processing budget per cycle (default 200). Combined with
    -- cost_delay=0, vacuum finishes quickly.
    autovacuum_vacuum_cost_limit = 1000
);

-- +migrate Down

DROP TABLE IF EXISTS liquidity_pool_balances;
