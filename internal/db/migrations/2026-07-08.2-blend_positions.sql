-- +migrate Up

-- current lending positions (one row per pool/user/reserve)
-- one Positions(user) entry fans out to N rows: persist is a full replace per
-- (pool, user) — upsert present reserve indexes, ZERO rows whose index is absent from
-- the maps (full exit removes the map key while the entry persists; zeroed rows keep
-- cost basis for lifetime earned). Entry removal deletes the rows.
CREATE TABLE blend_positions (
    pool_contract_id     BYTEA NOT NULL,
    user_account_id      BYTEA NOT NULL,
    reserve_index        INTEGER NOT NULL,
    -- current holdings: absolute snapshots from the Positions entry (last-write-wins)
    supply_b_tokens      TEXT NOT NULL DEFAULT '0',   -- non-collateral supply
    collateral_b_tokens  TEXT NOT NULL DEFAULT '0',
    liability_d_tokens   TEXT NOT NULL DEFAULT '0',
    -- cost basis for "earned to date": cumulative underlying, event-folded (additive)
    net_supplied         TEXT NOT NULL DEFAULT '0',   -- Σ(supply+collateral deposits) − Σ(withdrawals)
    net_borrowed         TEXT NOT NULL DEFAULT '0',   -- Σ(borrows) − Σ(repays)
    last_modified_ledger INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (pool_contract_id, user_account_id, reserve_index)
) WITH (
    fillfactor = 90,
    autovacuum_vacuum_scale_factor = 0.02,
    autovacuum_vacuum_threshold = 50,
    autovacuum_analyze_scale_factor = 0.01,
    autovacuum_analyze_threshold = 50,
    autovacuum_vacuum_cost_delay = 0
);
CREATE INDEX idx_blend_positions_user ON blend_positions (user_account_id);

-- +migrate Down

DROP TABLE IF EXISTS blend_positions;
