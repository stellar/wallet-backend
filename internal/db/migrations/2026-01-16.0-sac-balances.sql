-- +migrate Up

-- Table: sac_balances
-- Stores SAC (Stellar Asset Contract) balance data for contract addresses (C...) during ingestion.
-- Classic Stellar accounts (G...) have SAC balances in their trustlines, so only contract holders are stored here.
-- Storage parameters tuned for heavy UPSERT/DELETE during ledger ingestion.
-- UPSERTs only modify non-indexed columns (balance, is_authorized, is_clawback_enabled,
-- last_modified_ledger) while PK columns (account_id, contract_id) are never changed.
CREATE TABLE sac_balances (
    account_id BYTEA NOT NULL,
    contract_id UUID NOT NULL,
    balance NUMERIC NOT NULL DEFAULT 0,
    is_authorized BOOLEAN NOT NULL DEFAULT true,
    is_clawback_enabled BOOLEAN NOT NULL DEFAULT false,
    last_modified_ledger INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (account_id, contract_id),
    CONSTRAINT fk_contract_token
        FOREIGN KEY (contract_id) REFERENCES contract_tokens(id)
        DEFERRABLE INITIALLY DEFERRED
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

DROP TABLE IF EXISTS sac_balances;
