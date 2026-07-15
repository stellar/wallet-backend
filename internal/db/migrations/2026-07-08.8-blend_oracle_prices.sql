-- +migrate Up

-- current-only price snapshot (NOT historical) — fed by the SEP-40 lastprice task
CREATE TABLE blend_oracle_prices (
    oracle_contract_id   BYTEA NOT NULL,
    asset_contract_id    BYTEA NOT NULL,      -- pools query SEP-40 with Asset::Stellar(address)
    price                NUMERIC NOT NULL,       -- fixed-point at price_decimals
    price_decimals       INTEGER NOT NULL,
    price_timestamp      BIGINT NOT NULL,     -- oracle-reported timestamp
    updated_at           TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (oracle_contract_id, asset_contract_id)
) WITH (
    -- Row count is protocol-bounded (one row per oracle/asset pair the deployed Blend
    -- pools reference, forever a small number), so default autovacuum behavior is fine.
    fillfactor = 90
);

-- +migrate Down

DROP TABLE IF EXISTS blend_oracle_prices;
