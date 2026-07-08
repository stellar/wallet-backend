-- +migrate Up

-- current-only price snapshot (NOT historical) — fed by the SEP-40 lastprice task
CREATE TABLE blend_oracle_prices (
    oracle_contract_id   BYTEA NOT NULL,
    asset_contract_id    BYTEA NOT NULL,      -- pools query SEP-40 with Asset::Stellar(address)
    price                TEXT NOT NULL,       -- fixed-point at price_decimals
    price_decimals       INTEGER NOT NULL,
    price_timestamp      BIGINT NOT NULL,     -- oracle-reported timestamp
    updated_at           TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (oracle_contract_id, asset_contract_id)
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

DROP TABLE IF EXISTS blend_oracle_prices;
