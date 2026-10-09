-- +migrate Up

-- Token prices: one row per executed fill, oriented so that base_token is the token being priced
-- and counter_token is the anchor side (USDC before XLM before anything else; see the trades
-- processor). usd_value is attached at persist time from the latest oracle anchor rate and is
-- NULL when the counter is not an anchor or no anchor was known yet. Exact raw amounts are kept so
-- derived columns can be recomputed. Prices and volumes are served from the continuous aggregates
-- below, never from this table directly.
CREATE TABLE trades (
    ledger_created_at TIMESTAMPTZ NOT NULL,
    operation_id      BIGINT NOT NULL,
    fill_index        SMALLINT NOT NULL,
    ledger_number     INTEGER NOT NULL,
    base_token        BYTEA NOT NULL,
    counter_token     BYTEA NOT NULL,
    base_amount       NUMERIC(39, 0) NOT NULL,
    counter_amount    NUMERIC(39, 0) NOT NULL,
    base_qty          DOUBLE PRECISION,
    usd_value         DOUBLE PRECISION,
    venue             SMALLINT NOT NULL,
    PRIMARY KEY (ledger_created_at, operation_id, fill_index)
) WITH (
    tsdb.hypertable,
    tsdb.partition_column = 'ledger_created_at',
    tsdb.chunk_interval = '1 day',
    tsdb.orderby = 'ledger_created_at DESC, operation_id DESC',
    tsdb.segmentby = 'base_token'
);

SELECT enable_chunk_skipping('trades', 'operation_id');
DROP INDEX IF EXISTS trades_ledger_created_at_idx;

-- Raw fills exist to build the aggregates and to audit them; 90 days exceeds every refresh window
-- below, so a refresh never sees an empty source range and drops aggregate buckets.
SELECT add_retention_policy('trades', drop_after => INTERVAL '90 days');

-- Per-token USD candles. Real-time aggregation is on at every level so a query unions the
-- materialized buckets with the rows above the watermark; each upper level reads the level below
-- it, never the raw table. The refresh windows are wide on purpose: startup crash cleanup deletes
-- and re-inserts recent fills, and a policy only repairs invalidations inside its window. Refresh
-- cost is proportional to the invalidated ranges, not the window width.
CREATE MATERIALIZED VIEW trades_1m
WITH (timescaledb.continuous, timescaledb.materialized_only = false) AS
SELECT
    time_bucket('1 minute', ledger_created_at) AS bucket,
    base_token,
    first(usd_value / base_qty, ledger_created_at) AS open,
    max(usd_value / base_qty) AS high,
    min(usd_value / base_qty) AS low,
    last(usd_value / base_qty, ledger_created_at) AS close,
    sum(usd_value) AS usd_volume,
    sum(base_qty) AS base_volume,
    count(*) AS trades
FROM trades
WHERE usd_value IS NOT NULL AND base_qty > 0
GROUP BY bucket, base_token
WITH NO DATA;

CREATE MATERIALIZED VIEW trades_1h
WITH (timescaledb.continuous, timescaledb.materialized_only = false) AS
SELECT
    time_bucket('1 hour', bucket) AS bucket,
    base_token,
    first(open, bucket) AS open,
    max(high) AS high,
    min(low) AS low,
    last(close, bucket) AS close,
    sum(usd_volume) AS usd_volume,
    sum(base_volume) AS base_volume,
    sum(trades) AS trades
FROM trades_1m
GROUP BY 1, 2
WITH NO DATA;

CREATE MATERIALIZED VIEW trades_1d
WITH (timescaledb.continuous, timescaledb.materialized_only = false) AS
SELECT
    time_bucket('1 day', bucket) AS bucket,
    base_token,
    first(open, bucket) AS open,
    max(high) AS high,
    min(low) AS low,
    last(close, bucket) AS close,
    sum(usd_volume) AS usd_volume,
    sum(base_volume) AS base_volume,
    sum(trades) AS trades
FROM trades_1h
GROUP BY 1, 2
WITH NO DATA;

SELECT add_continuous_aggregate_policy('trades_1m',
    start_offset => INTERVAL '1 day', end_offset => INTERVAL '1 minute', schedule_interval => INTERVAL '1 minute');
SELECT add_continuous_aggregate_policy('trades_1h',
    start_offset => INTERVAL '7 days', end_offset => INTERVAL '1 hour', schedule_interval => INTERVAL '15 minutes');
SELECT add_continuous_aggregate_policy('trades_1d',
    start_offset => INTERVAL '30 days', end_offset => INTERVAL '1 day', schedule_interval => INTERVAL '1 hour');

SELECT add_retention_policy('trades_1m', drop_after => INTERVAL '30 days');
SELECT add_retention_policy('trades_1h', drop_after => INTERVAL '2 years');

-- Soroban AMM pools whose swap events the trades processor trusts. Rows come from the venue's
-- factory events during ingestion and from the one-time `prices setup-pools` seed.
CREATE TABLE amm_pools (
    pool           BYTEA PRIMARY KEY,
    venue          SMALLINT NOT NULL,
    token_0        BYTEA NOT NULL,
    token_1        BYTEA NOT NULL,
    created_ledger INTEGER NOT NULL
);

-- Latest reading per asset from the price oracle; the anchor rates that price trades in USD.
CREATE TABLE oracle_prices (
    asset           BYTEA PRIMARY KEY,
    price_usd       DOUBLE PRECISION NOT NULL,
    price_timestamp BIGINT NOT NULL,
    updated_at      TIMESTAMPTZ NOT NULL
);

-- Last priced fill per token, upserted by the trades persist path from values it already holds.
-- Windowed statistics are not stored here; the snapshot loader derives them from the aggregates.
CREATE TABLE token_last_trades (
    token             BYTEA PRIMARY KEY,
    price_usd         DOUBLE PRECISION NOT NULL,
    ledger_created_at TIMESTAMPTZ NOT NULL,
    operation_id      BIGINT NOT NULL
) WITH (
    fillfactor = 80,
    autovacuum_vacuum_scale_factor = 0.02,
    autovacuum_vacuum_threshold = 50,
    autovacuum_analyze_scale_factor = 0.01,
    autovacuum_analyze_threshold = 50
);

-- Side-by-side validation against the external price source. The sampler trims rows older than
-- 30 days.
CREATE TABLE price_comparisons (
    sampled_at TIMESTAMPTZ NOT NULL,
    token      BYTEA NOT NULL,
    ours       DOUBLE PRECISION,
    theirs     DOUBLE PRECISION,
    PRIMARY KEY (sampled_at, token)
);

-- +migrate Down

DROP TABLE IF EXISTS price_comparisons;
DROP TABLE IF EXISTS token_last_trades;
DROP TABLE IF EXISTS oracle_prices;
DROP TABLE IF EXISTS amm_pools;
DROP MATERIALIZED VIEW IF EXISTS trades_1d;
DROP MATERIALIZED VIEW IF EXISTS trades_1h;
DROP MATERIALIZED VIEW IF EXISTS trades_1m;
DROP TABLE IF EXISTS trades CASCADE;
