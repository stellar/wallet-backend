-- +migrate Up

-- reserve-level emission config/accrual (EmisData(reserve_token_id) entries on each
-- pool) — the eps source for emissions/net APY
CREATE TABLE blend_reserve_emissions (
    pool_contract_id     BYTEA NOT NULL,
    reserve_token_id     INTEGER NOT NULL,    -- reserve_index*2 (dToken) / +1 (bToken)
    eps                  BIGINT NOT NULL,
    emission_index       NUMERIC NOT NULL,
    expiration           BIGINT NOT NULL,
    last_time            BIGINT NOT NULL,
    last_modified_ledger INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (pool_contract_id, reserve_token_id)
) WITH (
    -- Row count is protocol-bounded (a small number of reserve emission streams per
    -- pool, across a small number of pools), so default autovacuum behavior is fine.
    fillfactor = 90
);

-- +migrate Down

DROP TABLE IF EXISTS blend_reserve_emissions;
