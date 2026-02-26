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

## Why Coexistence Works

Balance tables and history hypertables live in the same database without any API or tooling changes. This is possible because TimescaleDB is a regular PostgreSQL extension, not a separate database system. From the application's perspective, all tables — hypertables or standard — are accessed via the same PostgreSQL connection string, the same `pgx` client library, the same SQL dialect, and the same migration tooling.

TimescaleDB adds hypertable-specific behavior (chunk routing, compression, retention) transparently at the storage layer. Queries against hypertables and standard tables are written identically; the planner handles the difference. This means the wallet-backend could introduce hybrid table design (event hypertables + balance standard tables) without rewriting any application code, connection logic, or API layer. Crucially, since [[timescaledb automatic retention policy eliminates custom pruning code for blockchain history tables]], the chunk-drop automation that keeps history tables bounded applies only to hypertables — balance tables are excluded from this mechanism and manage their data lifecycle through autovacuum and upsert semantics instead.

If TimescaleDB were a standalone time-series system (e.g., InfluxDB, QuestDB), the wallet-backend would need two database connections, two query layers, and a strategy for transactional consistency across systems. The Postgres-extension model eliminates that complexity.

## Implications

- Never make balance tables hypertables — this would break the upsert-based balance management.
- Monitor autovacuum activity on these tables. If replication lag or lock contention suggests vacuum is falling behind, tune the autovacuum settings further.
- The `BatchUpsert` method (via `pgx.Batch` with `ON CONFLICT DO UPDATE`) is the only way balances are written — COPY cannot be used for these tables.
- New table types (future use cases beyond balances and events) can follow either model — the Postgres-extension nature means any standard table pattern remains available without architectural overhead.

## Source

`internal/db/migrations/2026-01-12.0-trustline_balances.sql` — autovacuum settings
`internal/db/migrations/2026-01-15.0-native_balances.sql`
`internal/db/migrations/2026-01-16.0-sac-balances.sql`

## Related

Because these tables are updated in place, [[balance tables require aggressive autovacuum tuning because rows are updated every ledger]] — the current-state design that makes hypertables wrong also makes default autovacuum settings inadequate.

Since balance tables use upsert-only writes, [[pgx copyfrom binary protocol is used for backfill bulk inserts unnest upsert for live ingestion]] clarifies the constraint: COPY cannot handle ON CONFLICT, so balance tables always use the UNNEST upsert path regardless of ingestion mode.

The coexistence of standard tables and hypertables in one database was part of the original database selection rationale: [[timescaledb was chosen over vanilla postgres because history tables exceeded feasible storage limits without columnar compression]] — pillar 4 of that decision was exactly that TimescaleDB is a Postgres extension, enabling hybrid table design without any migration cost.

The current-state design of balance tables is directly embedded in the ingestion persistence structure: in [[per-ledger persistence is a single atomic database transaction ensuring all-or-nothing ledger commits]], step 1 (`TrustlineAsset.BatchInsert()`) is the FK prerequisite for balance tables, and step 5 (`ProcessTokenChanges()`) is the write path — the upsert-based update model is a structural constraint on the transaction ordering.

Since balance tables are updated in place rather than appended, [[catchup mode merges all batch changes in a single db transaction using highest-operationid-wins]] explicitly defers token and balance changes to a final atomic commit, while hypertable event writes (transactions, operations, state changes) are committed per-batch during execution. The current-state update semantics of balance tables — where intermediate states would be wrong — is exactly what forces this split.

relevant_notes:
  - "[[balance tables require aggressive autovacuum tuning because rows are updated every ledger]] — extends this: the upsert-based current-state model creates the dead-tuple accumulation that requires aggressive autovacuum"
  - "[[pgx copyfrom binary protocol is used for backfill bulk inserts unnest upsert for live ingestion]] — grounds this: COPY's inability to handle ON CONFLICT means balance tables cannot use the backfill COPY path; they always require upsert"
  - "[[timescaledb was chosen over vanilla postgres because history tables exceeded feasible storage limits without columnar compression]] — provides the architectural origin: the Postgres-extension property (pillar 4) is what makes hybrid table design possible without a second database system"
  - "[[blockchain ledger data is append-only and time-series which makes timescaledb columnar compression maximally effective]] — provides the structural reason this decision exists: balance tables are NOT append-only time-series, which is exactly the property that makes columnar compression effective; the absence of that property is why they stay row-oriented"
  - "[[per-ledger persistence is a single atomic database transaction ensuring all-or-nothing ledger commits]] — grounds this: balance tables are step 1 (FK prerequisite) and step 5 (write path) in PersistLedgerData; the upsert-based current-state design constrains transaction ordering"
  - "[[catchup mode merges all batch changes in a single db transaction using highest-operationid-wins]] — exemplifies this: catchup defers balance writes to a final commit while committing event hypertable writes per-batch; the current-state update semantics of balance tables is what forces this split"
  - "[[timescaledb automatic retention policy eliminates custom pruning code for blockchain history tables]] — contrasts with this: chunk-drop retention is a hypertable-only capability; balance tables are excluded and manage data lifecycle through autovacuum and upsert semantics instead"
