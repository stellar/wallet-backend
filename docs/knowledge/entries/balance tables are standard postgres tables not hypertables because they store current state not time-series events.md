---
description: trustline_balances, native_balances, sac_balances store current balance state; hypertable chunking adds overhead without benefit; aggressive autovacuum required because rows update every ledger
type: decision
status: current
confidence: proven
subsystem: data-layer
areas: [data-layer, timescaledb, postgresql, balance-tables, autovacuum]
---

# Balance tables are standard PostgreSQL tables, not hypertables, because they store current state not time-series events

## Context

The data layer uses TimescaleDB hypertables for ledger event data (transactions, operations, state changes). A decision was needed for balance tables that track current holdings rather than historical events.

## Detail

`trustline_balances`, `native_balances`, and `sac_balances` are regular PostgreSQL tables. They store the **current** balance state for each account-token pair, updated in place on every ledger that changes a balance.

Using hypertables for these tables would be wrong because:
1. TimescaleDB partitions by time, designed for append-only time-series data.
2. Balance tables are updated (not appended) on every ledger — chunking would create many small chunks with high update overhead.
3. Time-range queries on current balance don't make sense — balances are point-in-time snapshots.

However, their update-heavy access pattern requires aggressive autovacuum:
- `fillfactor=80` reserves 20% page space for HOT (Heap-Only Tuple) updates, avoiding index update dead tuples.
- `autovacuum_vacuum_scale_factor=0.02` triggers vacuum at 2% dead rows (vs default 20%).
- `autovacuum_vacuum_cost_delay=0` and `cost_limit=1000` make vacuuming more aggressive and faster.

## Implications

- Never make balance tables hypertables — this would break the upsert-based balance management.
- Monitor autovacuum activity on these tables. If replication lag or lock contention suggests vacuum is falling behind, tune the autovacuum settings further.
- The `BatchUpsert` method (via `pgx.Batch` with `ON CONFLICT DO UPDATE`) is the only way balances are written — COPY cannot be used for these tables.

## Source

`internal/db/migrations/2026-01-12.0-trustline_balances.sql` — autovacuum settings
`internal/db/migrations/2026-01-15.0-native_balances.sql`
`internal/db/migrations/2026-01-16.0-sac-balances.sql`

## Related

Because these tables are updated in place, [[balance tables require aggressive autovacuum tuning because rows are updated every ledger]] — the current-state design that makes hypertables wrong also makes default autovacuum settings inadequate.

Since balance tables use upsert-only writes, [[pgx copyfrom binary protocol is used for backfill bulk inserts unnest upsert for live ingestion]] clarifies the constraint: COPY cannot handle ON CONFLICT, so balance tables always use the UNNEST upsert path regardless of ingestion mode.

relevant_notes:
  - "[[balance tables require aggressive autovacuum tuning because rows are updated every ledger]] — extends this: the upsert-based current-state model creates the dead-tuple accumulation that requires aggressive autovacuum"
  - "[[pgx copyfrom binary protocol is used for backfill bulk inserts unnest upsert for live ingestion]] — grounds this: COPY's inability to handle ON CONFLICT means balance tables cannot use the backfill COPY path; they always require upsert"
