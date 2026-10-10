-- +migrate Up

-- The account that took each fill. Price confidence counts distinct takers rather than fills,
-- so one account trading many times is one observation. Rows written before this column existed
-- stay NULL and are left out of the taker aggregates.
ALTER TABLE trades ADD COLUMN taker BYTEA;

-- Per-token, per-taker sums for the spot price: USD weight, base quantity and the USD-weighted
-- first and second moments of log price. All columns are additive, so the snapshot loader can
-- regroup any window by taker and then by token. Real-time aggregation keeps the newest fills
-- visible before the next refresh; the hourly level reads the minute level, never raw trades.
CREATE MATERIALIZED VIEW trades_takers_1m
WITH (timescaledb.continuous, timescaledb.materialized_only = false) AS
SELECT
    time_bucket('1 minute', ledger_created_at) AS bucket,
    base_token,
    taker,
    sum(usd_value) AS w,
    sum(base_qty) AS q,
    sum(usd_value * ln(usd_value / base_qty)) AS wl,
    sum(usd_value * ln(usd_value / base_qty) ^ 2) AS wll,
    max(ledger_created_at) AS last_at
FROM trades
WHERE usd_value > 0 AND base_qty > 0 AND taker IS NOT NULL
GROUP BY bucket, base_token, taker
WITH NO DATA;

CREATE MATERIALIZED VIEW trades_takers_1h
WITH (timescaledb.continuous, timescaledb.materialized_only = false) AS
SELECT
    time_bucket('1 hour', bucket) AS bucket,
    base_token,
    taker,
    sum(w) AS w,
    sum(q) AS q,
    sum(wl) AS wl,
    sum(wll) AS wll,
    max(last_at) AS last_at
FROM trades_takers_1m
GROUP BY 1, 2, 3
WITH NO DATA;

SELECT add_continuous_aggregate_policy('trades_takers_1m',
    start_offset => INTERVAL '1 day', end_offset => INTERVAL '1 minute', schedule_interval => INTERVAL '1 minute');
SELECT add_continuous_aggregate_policy('trades_takers_1h',
    start_offset => INTERVAL '7 days', end_offset => INTERVAL '1 hour', schedule_interval => INTERVAL '15 minutes');

SELECT add_retention_policy('trades_takers_1m', drop_after => INTERVAL '30 days');
SELECT add_retention_policy('trades_takers_1h', drop_after => INTERVAL '2 years');


-- +migrate Down

DROP MATERIALIZED VIEW IF EXISTS trades_takers_1h;
DROP MATERIALIZED VIEW IF EXISTS trades_takers_1m;
ALTER TABLE trades DROP COLUMN IF EXISTS taker;
