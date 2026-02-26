---
description: Neither serve nor ingest talks directly to the other; all coordination goes through TimescaleDB; enables independent scaling and crash isolation
type: decision
status: current
confidence: proven
subsystem: ingestion
areas: [architecture, ingestion, serve, decoupling]
---

# The database is the sole integration point between the serve and ingest processes

## Context

The wallet-backend runs as two separate OS processes: `serve` (HTTP/GraphQL API) and `ingest` (ledger ingestion). A design decision was needed for how they share state.

## Detail

The two processes communicate exclusively through the TimescaleDB database:
- `ingest` writes ledger data into hypertables and updates `ingest_store` cursors.
- `serve` reads the same hypertables to answer GraphQL queries.
- Neither process has a direct network connection to the other.

This design is only coherent because [[timescaledb was chosen over vanilla postgres because history tables exceeded feasible storage limits without columnar compression]] — specifically pillar 4 of that decision (Postgres extension, zero migration cost). If the time-series store were a standalone system like InfluxDB or QuestDB, serve and ingest would require separate query layers and a transactional consistency strategy across two databases; the single-connection-string integration point would not exist.

This means there is no inter-process RPC, no message queue, and no shared memory. The `ingest` process's progress is visible to `serve` only after the atomic DB transaction for each ledger commits.

## Implications

- Both processes can be deployed independently, restarted, or scaled without coordination.
- A crash in `ingest` does not directly affect `serve` — it will simply serve stale data until ingestion resumes.
- The serve process reads data that may be a few ledgers behind the network tip, depending on ingestion lag.

## Source

`cmd/serve.go`, `cmd/ingest.go` — two separate cobra commands
`internal/serve/serve.go`, `internal/ingest/ingest.go` — independent startup flows

## Related

The ingest side's integrity is protected by [[advisory lock prevents concurrent ingestion instances via postgresql pg_try_advisory_lock]] — the lock ensures the single-DB integration model doesn't become a multi-writer corruption problem.

The data visible through this integration point is written by [[per-ledger persistence is a single atomic database transaction ensuring all-or-nothing ledger commits]] — `serve` only ever observes complete ledgers because the cursor advance is atomic with the data writes.

relevant_notes:
  - "[[advisory lock prevents concurrent ingestion instances via postgresql pg_try_advisory_lock]] — extends this: the single-integration-point design requires single-writer enforcement; the advisory lock provides that guarantee"
  - "[[per-ledger persistence is a single atomic database transaction ensuring all-or-nothing ledger commits]] — extends this: the atomic transaction is why the database as integration point is safe — serve cannot observe partial ledger writes"
  - "[[timescaledb was chosen over vanilla postgres because history tables exceeded feasible storage limits without columnar compression]] — grounds this: pillar 4 (Postgres-extension model) is the architectural prerequisite; a standalone time-series system would break the single-connection integration point"
