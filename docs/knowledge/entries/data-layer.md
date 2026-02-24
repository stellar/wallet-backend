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

## Decisions

<!-- - [[entries/why-timescaledb]] -->
<!-- - [[entries/one-day-chunk-interval]] -->
<!-- - [[entries/segmentby-account-id]] -->

## Insights

<!-- - [[entries/chunk-skipping-requires-extension]] -->
<!-- - [[entries/bloom-filter-sparse-index]] -->

## Patterns

<!-- - [[entries/interface-based-data-models]] -->
<!-- - [[entries/migration-naming-convention]] -->

## Gotchas

<!-- - [[entries/timescale-extension-must-be-enabled-first]] -->

## Cross-Subsystem Connections

Several ingestion entries straddle ingestion and data-layer concerns — they capture behavior that emerges from how the ingestion pipeline interacts with TimescaleDB chunks, hypertable schemas, and cursor storage:

- [[compression timing trade-off: uncompressed inserts are faster but require post-processing pass]] — TimescaleDB chunk compression behavior drives this ingestion design choice
- [[progressive recompression compresses TimescaleDB chunks as watermark advances during historical backfill]] — operates directly on TimescaleDB chunk boundaries
- [[ingest_store key-value table is the sole source of truth for cursor positions]] — a data-layer table (`ingest_store`) is the authoritative cursor store
- [[backfill uses gap detection via window function on transactions ledger_number]] — gap detection queries the `transactions` hypertable directly
- [[the six-step persist transaction ordering exists to satisfy foreign key prerequisites]] — FK relationships in the data layer constrain ingestion step ordering

These entries are catalogued in [[entries/ingestion]] but are equally relevant to data-layer understanding.

---

Agent Notes:
- 2026-02-24: no standalone data-layer entries yet; cross-subsystem connections from ingestion are the primary data-layer knowledge currently captured; entries above are good candidates for seeding this map's Patterns/Gotchas sections once data-layer entries are extracted

## Topics

[[entries/index]] | [[references/data-layer]] | [[entries/ingestion]]
