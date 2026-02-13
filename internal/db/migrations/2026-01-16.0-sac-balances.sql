-- +migrate Up

-- Table: sac_balances
-- Stores SAC (Stellar Asset Contract) balance data for contract addresses (C...) during ingestion.
-- Classic Stellar accounts (G...) have SAC balances in their trustlines, so only contract holders are stored here.
--
-- Storage parameters tuned for heavy UPSERT/DELETE during ledger ingestion:
--   fillfactor=80: Reserves 20% free space per page to enable HOT (Heap-Only Tuple) updates.
--     Since UPSERTs only modify non-indexed columns (balance, is_authorized, is_clawback_enabled,
--     last_modified_ledger) while PK columns (account_address, contract_id) are never changed,
--     PostgreSQL can update rows in-place without creating dead tuples or new index entries.
--   autovacuum_vacuum_scale_factor=0.02: Triggers vacuum at 2% dead rows instead of the default 20%.
--   autovacuum_analyze_scale_factor=0.01: Keeps planner statistics fresh as data changes every ledger.
--   autovacuum_vacuum_cost_delay=0: Runs vacuum at full speed (no throttling). Per-table setting
--     so it only affects workers on this table, not the whole cluster.
--   autovacuum_vacuum_cost_limit=1000: 5x the default budget. Workers with per-table cost settings
--     are exempt from global cost balancing, so this does not starve other tables.
CREATE TABLE sac_balances (
    account_address TEXT NOT NULL,
    contract_id UUID NOT NULL,
    balance TEXT NOT NULL DEFAULT '0',
    is_authorized BOOLEAN NOT NULL DEFAULT true,
    is_clawback_enabled BOOLEAN NOT NULL DEFAULT false,
    last_modified_ledger INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (account_address, contract_id),
    CONSTRAINT fk_contract_token
        FOREIGN KEY (contract_id) REFERENCES contract_tokens(id)
        DEFERRABLE INITIALLY DEFERRED
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

DROP TABLE IF EXISTS sac_balances;
