---
description: Knowledge map for the data layer subsystem
type: reference
subsystem: data-layer
areas: [knowledge-map, timescaledb, hypertables, migrations, data-models]
vault: docs/knowledge
---

# Data Layer Knowledge Map

The data layer stores ledger events in five TimescaleDB hypertables partitioned by `ledger_created_at`. Balance tables (trustline, native, SAC) are regular PostgreSQL tables with high-fill-factor tuning for update-heavy workloads.

**Key code:** `internal/data/`, `internal/db/migrations/`, `internal/entities/`

## Reference Doc

[[references/data-layer]] — hypertable schema, compression policies, chunk skipping, migration patterns, balance table design

## Hypertable Design

- [[ledger_created_at is the hypertable partition column because time-bounded queries are the dominant API access pattern]] — the design decision that shapes all five hypertable schemas
- [[one-day chunk interval balances Stellar ledger volume against chunk management overhead]] — why 1 day; 17,280 ledgers/day fits comfortably
- [[segmentby account_id on join tables collocates same-account rows for vectorized filtering without cross-account scanning]] — co-location optimization on join tables only
- [[bloom filter sparse index on transactions hash enables chunk-level hash lookup pruning without full scan]] — the probabilistic index that covers hash-based GetByHash

## Balance Table Design

- [[balance tables use regular PostgreSQL tables not hypertables because they store current state not time-series events]] — the architecture split between event tables and state tables
- [[fillfactor 80 on balance tables reserves page space for HOT updates to avoid dead tuples on non-indexed column changes]] — PostgreSQL HOT update optimization for frequent balance upserts
- [[aggressive autovacuum tuning on balance tables prevents stale statistics and dead tuple accumulation from per-ledger updates]] — per-table autovacuum settings

## Model Interface Pattern

- [[hypertable models lack interfaces because they require real TimescaleDB for testing while balance models use interfaces for mock-based unit tests]] — the testability split in the Models struct
- [[deterministic UUID v5 eliminates DB roundtrips before BatchInsert by computing primary keys client-side]] — idempotent streaming ingestion without pre-querying for existing IDs

## Migration System

- [[go:embed compiles SQL migrations into the binary at build time making migration files unavailable at runtime]] — self-contained binary, no runtime file dependencies
- [[migrate down requires explicit count to prevent accidental full rollback while migrate up treats zero as apply-all]] — asymmetric CLI defaults
- [[multi-statement migration DDL requires StatementBegin and StatementEnd delimiters to prevent parser splitting on semicolons]] — critical gotcha for stored procedures

## Connection Pool

- [[dual connection pool uses sqlx for struct scanning and pgxpool for binary COPY because they serve different wire protocol needs]] — the two-pool architecture
- [[backfill connection pool sets synchronous_commit=off at connection-string level so all pool connections inherit the setting without per-query hints]] — pool-level async WAL for backfill throughput
- [[ConfigureBackfillSession disables FK constraint checking via session_replication_role replica because it cannot be set at connection-string level]] — why a separate session-setup step exists
- [[RunInTransactionWithResult generic helper avoids interface{} return type for typed results from inside transactions]] — Go 1.18+ generic transaction helper

## Query Patterns

- [[raw SQL with strings.Builder and positional parameters gives full control over TimescaleDB-specific query constructs no query builder can generate]] — the rationale for avoiding ORMs
- [[decomposed OR cursor condition enables TimescaleDB chunk pruning whereas ROW tuple comparison is opaque to the columnar scan engine]] — critical cursor pagination optimization
- [[MATERIALIZED CTE forces the planner to execute the join table subquery separately enabling ChunkAppend on the hypertable]] — the explicit planner hint for account-history queries
- [[LATERAL join for parent-row lookup is O(1) per row via primary key compared to a regular join requiring planner strategy selection]] — guaranteed primary-key lookup in batch queries
- [[ROW_NUMBER PARTITION BY parent_id guarantees each parent receives at most limit rows in dataloader batch queries preventing head-of-line starvation]] — per-parent pagination in dataloader batch queries

## Test Infrastructure

- [[dbtest Open enables TimescaleDB extension and chunk skipping before running migrations so hypertable queries behave identically to production]] — ordering requirement for test setup

## Observability

- [[GetDBErrorType maps PostgreSQL error codes to named Prometheus categories enabling structured DB error observability]] — error classification at the DB layer

## Tensions

- [[tension: transaction helpers auto-rollback via defer but log rollback errors separately to avoid masking the original error]] — rollback error handling design
- [[tension: pgx.CopyFrom for backfill versus pgx.Batch for live ingestion creates two insert code paths that must be maintained separately]] — dual code paths with no shared abstraction

## Open Questions

- [[whether synchronous_commit=off on the backfill pool creates a risk window where a crash leaves partially-committed ledger data]] — unanalyzed crash recovery gap with async WAL

## Cross-Subsystem Connections

Several ingestion entries straddle ingestion and data-layer concerns — they capture behavior that emerges from how the ingestion pipeline interacts with TimescaleDB chunks, hypertable schemas, and cursor storage:

- [[compression timing trade-off: uncompressed inserts are faster but require post-processing pass]] — TimescaleDB chunk compression behavior drives this ingestion design choice
- [[progressive recompression compresses TimescaleDB chunks as watermark advances during historical backfill]] — operates directly on TimescaleDB chunk boundaries
- [[ingest_store key-value table is the sole source of truth for cursor positions]] — a data-layer table (`ingest_store`) is the authoritative cursor store
- [[backfill uses gap detection via window function on transactions ledger_number]] — gap detection queries the `transactions` hypertable directly
- [[the six-step persist transaction ordering exists to satisfy foreign key prerequisites]] — FK relationships in the data layer constrain ingestion step ordering
- [[the unified StateChange struct uses nullable fields and a category discriminator to store all state change types in a single database table]] — the ingestion schema decision that directly shapes the `state_changes` hypertable; single-table design with nullable fields was chosen over polymorphic tables because the read pattern is by address+time, a data-layer access concern

These entries are catalogued in [[entries/ingestion]] but are equally relevant to data-layer understanding.

The GraphQL layer exposes the `ledger_created_at` partition design as query parameters — the cross-subsystem linkage where schema choices become API features:

- [[since and until parameters on Account queries enable TimescaleDB chunk exclusion to reduce scan cost for time-bounded account history]] — the GraphQL `since`/`until` parameters map directly to `ledger_created_at` predicates that activate chunk pruning; the API design only works because the hypertable is partitioned on this column

This entry is catalogued in [[entries/graphql-api]] but is equally relevant to understanding why the `ledger_created_at` partition choice matters beyond the data layer.

---

## Agent Notes

- 2026-02-24: added two cross-subsystem connections: (1) `the unified StateChange struct` — ingestion schema decision that directly shapes `state_changes` hypertable design, missed because it was tagged ingestion-only; (2) `since and until parameters` — GraphQL API entry that is the consumer-side of the `ledger_created_at` partition design, closes the loop between hypertable design and API ergonomics

---

## Topics

[[entries/index]] | [[references/data-layer]] | [[entries/ingestion]] | [[entries/graphql-api]]
