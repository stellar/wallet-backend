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
    PRIMARY KEY (pool_contract_id, reserve_index)
) WITH (
    -- Row count is protocol-bounded (a small number of reserves per pool, across a
    -- small number of pools), so default autovacuum behavior is fine.
    fillfactor = 90
);
-- asset-first lookup for "choose a token to earn" (all pools accepting a given asset)
CREATE INDEX idx_blend_reserves_asset ON blend_reserves (asset_contract_id);

-- +migrate Down

DROP TABLE IF EXISTS blend_reserves;
