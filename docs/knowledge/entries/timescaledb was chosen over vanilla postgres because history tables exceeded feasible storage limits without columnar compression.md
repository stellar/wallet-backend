---
context: PoC showed 5 vanilla Postgres history tables were infeasible at 6-month+ scale; four TimescaleDB pillars resolved all constraints simultaneously; columnar compression was the primary driver
type: decision
status: active
created: 2026-02-26
areas: [data-layer, timescaledb, postgresql, architecture]
---

# TimescaleDB was chosen over vanilla Postgres because history tables exceeded feasible storage limits without columnar compression

## Context

When the wallet-backend was designed, the data model had five history tables capturing time-ordered ledger events: transactions, operations, and state changes. A proof-of-concept using vanilla PostgreSQL demonstrated that storing 6 months or more of blockchain history in these tables was not feasible — storage costs were prohibitive before the dataset was useful.

## Detail

The PoC revealed that vanilla Postgres cannot efficiently store append-only time-series blockchain data at meaningful retention windows. Blockchain history is write-once: every transaction, operation, and state change is permanently committed and never updated. Without columnar compression, a year of mainnet ledger data across five tables would require storage that made the service economically unviable.

TimescaleDB was chosen because it resolved all four constraints simultaneously:

1. **Columnar compression** — Since [[blockchain ledger data is append-only and time-series which makes timescaledb columnar compression maximally effective]], long runs of similar values in time-ordered columns (ledger sequence, asset code, account ID) compress extremely well. This is the primary driver: compression made the storage targets achievable.

2. **Automatic retention policy** — Vanilla Postgres would require custom pruning code (cron jobs, scheduled DELETE statements, partition management scripts) to drop old ledger data. Since [[timescaledb automatic retention policy eliminates custom pruning code for blockchain history tables]], this operational surface disappears: chunks are dropped by time range automatically without any application code.

3. **Chunk-based fast queries** — The primary wallet-backend query shape is an account's transaction or operation history over a time window. TimescaleDB stores hypertable data in time-ordered chunks, allowing entire historical chunks outside the query range to be physically discarded before scanning. This gives time-range queries sub-linear data access, but only when [[timescaledb chunk skipping must be explicitly enabled per column for sparse index scans to work]].

4. **Postgres extension, zero migration cost** — TimescaleDB is a regular PostgreSQL extension. All existing queries, client libraries, migrations, and tooling continued to work unmodified. No rewrite of the API layer, connection pooling, or schema tooling was required. This is also what makes [[the database is the sole integration point between the serve and ingest processes]] viable at scale: both hypertable event data and standard balance tables are accessed through the same connection string and the same SQL dialect.

The decision was not between TimescaleDB and a dedicated time-series system. It was between staying on vanilla Postgres (infeasible at scale) and adopting an extension that solved the storage problem while staying within the PostgreSQL ecosystem.

## Implications

- All five ledger event tables (transactions, operations, state changes, and related) are TimescaleDB hypertables. New event tables follow the same pattern.
- Balance tables — which store current state rather than time-series history — are standard PostgreSQL tables. The extension enables this hybrid: hypertable event data and standard balance data coexist in the same database with no API changes.
- Compression configuration, retention policy setup, and chunk skipping must be managed per hypertable. These are operational responsibilities that vanilla Postgres would not have.

## Source

PoC conducted prior to initial implementation — original four-pillar decision record (internal design notes, not checked in)

## Relevant Notes

- Since [[balance tables are standard postgres tables not hypertables because they store current state not time-series events]], the coexistence of hypertables and standard tables in one database is only possible because TimescaleDB is a Postgres extension — no API rewrite needed.
- [[timescaledb chunk skipping must be explicitly enabled per column for sparse index scans to work]] — chunk-based fast queries (pillar 3) depend on this configuration; the compression choice implies this operational requirement.
- [[hypertable settings are only applied in live mode to avoid overwriting a running instance config]] — retention policy configuration (pillar 2) is part of the hypertable settings that have mode-specific application rules.
- [[progressive recompression uses watermark to compress historical backfill chunks as contiguous batches complete]] — columnar compression (pillar 1) requires active management during backfill to compress chunks as data arrives.
- [[the database is the sole integration point between the serve and ingest processes]] — pillar 4 (Postgres-extension, zero migration cost) is the architectural prerequisite for this pattern: if TimescaleDB were a standalone time-series system, serve and ingest would need separate query layers rather than sharing one PostgreSQL connection string.
