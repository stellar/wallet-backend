-- +migrate Up

-- Table: native_balances
-- Stores native XLM balance data for accounts during ingestion.
--
-- Storage parameters tuned for heavy UPSERT/DELETE during ledger ingestion:
--   fillfactor=80: Reserves 20% free space per page to enable HOT (Heap-Only Tuple) updates.
--     Since UPSERTs only modify non-indexed columns (balance, minimum_balance, liabilities,
--     last_modified_ledger) while the PK column (account_address) is never changed,
--     PostgreSQL can update rows in-place without creating dead tuples or new index entries.
--   autovacuum_vacuum_scale_factor=0.02: Triggers vacuum at 2% dead rows instead of the default 20%.
--   autovacuum_analyze_scale_factor=0.01: Keeps planner statistics fresh as data changes every ledger.
--   autovacuum_vacuum_cost_delay=0: Runs vacuum at full speed (no throttling). Per-table setting
--     so it only affects workers on this table, not the whole cluster.
--   autovacuum_vacuum_cost_limit=1000: 5x the default budget. Workers with per-table cost settings
--     are exempt from global cost balancing, so this does not starve other tables.
CREATE TABLE native_balances (
    account_address TEXT PRIMARY KEY,
    balance BIGINT NOT NULL DEFAULT 0,
    minimum_balance BIGINT NOT NULL DEFAULT 0,
    buying_liabilities BIGINT NOT NULL DEFAULT 0,
    selling_liabilities BIGINT NOT NULL DEFAULT 0,
    last_modified_ledger BIGINT NOT NULL DEFAULT 0
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

DROP TABLE IF EXISTS native_balances;
