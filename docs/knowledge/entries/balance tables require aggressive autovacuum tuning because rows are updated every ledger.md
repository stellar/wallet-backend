---
description: fillfactor=80 for HOT updates; autovacuum_vacuum_scale_factor=0.02; autovacuum_vacuum_cost_delay=0; cost_limit=1000 prevents dead tuple accumulation in high-update-frequency tables
type: gotcha
status: current
confidence: proven
subsystem: data-layer
areas: [data-layer, postgresql, autovacuum, performance, balance-tables]
---

# Balance tables require aggressive autovacuum tuning because rows are updated every ledger

## Context

Stellar produces ~17,280 ledgers per day. Any account with active balance changes will have its balance row updated thousands of times per day. PostgreSQL's default autovacuum settings (triggering at 20% dead tuples, with cost delays) are too conservative for this rate.

## Detail

All three balance tables share identical autovacuum settings applied in their migration files:

| Setting | Value | Default | Effect |
|---------|-------|---------|--------|
| `fillfactor` | 80 | 100 | Reserves 20% page space for HOT updates — avoids creating dead index tuples when updating non-indexed columns |
| `autovacuum_vacuum_scale_factor` | 0.02 | 0.20 | Triggers vacuum at 2% dead rows instead of 20% |
| `autovacuum_analyze_scale_factor` | 0.01 | 0.10 | Refreshes statistics at 1% change to prevent stale query plans |
| `autovacuum_vacuum_cost_delay` | 0 | 2ms | Removes sleep between vacuum cycles — vacuum completes faster |
| `autovacuum_vacuum_cost_limit` | 1000 | 200 | 5x page budget per vacuum cycle |

HOT (Heap-Only Tuple) updates require `fillfactor < 100` — the reserved space allows the updated version to be placed on the same page as the old version, avoiding index updates entirely for non-indexed column changes.

## Implications

- If these settings are accidentally reset (e.g., by migration rollback), dead tuple accumulation will cause bloat and query slowdown. Monitor `pg_stat_user_tables.n_dead_tup`.
- The `fillfactor=80` means each page holds fewer rows — initial data load takes ~25% more storage. This is acceptable given the HOT update benefit.
- These settings must also be applied to any new balance-style tables added in the future.

## Source

`internal/db/migrations/2026-01-12.0-trustline_balances.sql`
`internal/db/migrations/2026-01-15.0-native_balances.sql`
`internal/db/migrations/2026-01-16.0-sac-balances.sql`

## Related

The autovacuum requirement follows directly from [[balance tables are standard postgres tables not hypertables because they store current state not time-series events]] — because they use upsert rather than append, every active account produces dead tuples that would accumulate without aggressive vacuuming.

relevant_notes:
  - "[[balance tables are standard postgres tables not hypertables because they store current state not time-series events]] — grounds this: the upsert-based current-state design is what creates the dead-tuple accumulation problem that aggressive autovacuum addresses"
