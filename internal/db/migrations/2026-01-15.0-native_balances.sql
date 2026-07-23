-- +migrate Up

-- Table: native_balances
-- Stores native XLM balance data for accounts during ingestion.
-- Storage parameters tuned for heavy UPSERT/DELETE during ledger ingestion.
-- UPSERTs only modify non-indexed columns (balance, minimum_balance, liabilities,
-- num_subentries, last_modified_ledger) while the PK column (account_id) is never changed.
CREATE TABLE native_balances (
    account_id BYTEA PRIMARY KEY,
    balance BIGINT NOT NULL DEFAULT 0,
    minimum_balance BIGINT NOT NULL DEFAULT 0,
    buying_liabilities BIGINT NOT NULL DEFAULT 0,
    selling_liabilities BIGINT NOT NULL DEFAULT 0,
    num_subentries INTEGER NOT NULL DEFAULT 0,
    last_modified_ledger BIGINT NOT NULL DEFAULT 0
) WITH (
    -- Reserve 10% free space per page so PostgreSQL can do HOT (Heap-Only Tuple) updates.
    -- HOT updates rewrite the row in-place on the same page without creating dead tuples
    -- or new index entries, since no indexed column is modified during UPSERTs. Measured
    -- churn on this table is ~0.1%/day; a 20% reserve was double what the workload can
    -- use, and COPY-loaded heaps carry the reserve permanently regardless.
    fillfactor = 90,
    -- Trigger vacuum when 2% of rows are dead (default 20%). For a 500K-row table,
    -- this means vacuum starts at ~10K dead rows instead of waiting for 100K.
    autovacuum_vacuum_scale_factor = 0.02,
    -- Base dead-row count added to (scale_factor * total_rows). Default is fine here
    -- since the scale factor already keeps the threshold low.
    autovacuum_vacuum_threshold = 50,
    -- Refresh planner statistics at 1% change (default 10%). Balances shift every ledger,
    -- so stale stats can cause bad query plans.
    autovacuum_analyze_scale_factor = 0.01,
    autovacuum_analyze_threshold = 50,
    -- Setting cost_delay=0 disables cost-based throttling for this table's autovacuum
    -- worker and, per PostgreSQL's balancing rules, exempts it from cross-worker cost
    -- balancing so other tables keep their full budget.
    autovacuum_vacuum_cost_delay = 0
);

-- +migrate Down

DROP TABLE IF EXISTS native_balances;
