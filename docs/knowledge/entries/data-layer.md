---
description: Knowledge map for the data layer — TimescaleDB hypertables, model interfaces, query optimization patterns, and connection pools
type: reference
status: current
subsystem: data-layer
areas: [navigation, data-layer]
---

# Data Layer — Knowledge Map

The data layer provides all database access through model interfaces aggregated in `data.Models`. Five TimescaleDB hypertables hold ledger event data; three standard tables hold current balance state; metadata tables track assets and contract tokens.

## Model Architecture

- [[data.models aggregates all database model interfaces into a single injectable struct]] — The single-struct DI pattern that avoids large constructor signatures
- [[interface-backed models are for unit test mocking concrete models are tested against real timescaledb]] — Why some models have interfaces and others don't; dbtest.Open for hypertable model tests
- [[deterministic uuid from content hash enables idempotent batch inserts without db roundtrip]] — UUID v5 pattern for TrustlineAsset and ContractModel; no pre-insert SELECT needed

## TimescaleDB Hypertables

- [[timescaledb chunk skipping must be explicitly enabled per column for sparse index scans to work]] — The hidden session setting required for sparse index effectiveness; dbtest.Open handles this
- [[the decomposed cursor condition enables timescaledb chunk pruning where row tuple comparison cannot]] — Why ROW() comparisons break vectorized chunk filters; the OR-expanded decomposition
- [[materialized cte plus lateral join forces separate planning to preserve chunk pruning on account queries]] — MATERIALIZED keyword is load-bearing; why LATERAL gives O(1) join per account transaction

## Balance Tables

- [[balance tables are standard postgres tables not hypertables because they store current state not time-series events]] — Why current-state tables don't benefit from TimescaleDB partitioning
- [[balance tables require aggressive autovacuum tuning because rows are updated every ledger]] — fillfactor=80 and all 4 autovacuum settings; why defaults are too conservative

## Insert Strategies

- [[pgx copyfrom binary protocol is used for backfill bulk inserts unnest upsert for live ingestion]] — COPY vs upsert tradeoff; when each strategy is appropriate

## DataLoader Query Patterns

- [[row_number partition by parent_id prevents imbalanced pagination in dataloader batch queries]] — Why global LIMIT fails in DataLoader contexts; per-parent row ranking
- [[toid bit-masking recovers parent transaction id from operation id without a join]] — SEP-35 encoding; bitmask trick that avoids a lookup table

## Tensions

- Binary COPY vs conflict-safe upsert: backfill uses COPY for speed, live uses upsert for safety — different needs drive different strategies per path.
- Hypertable vs standard table: event data goes in hypertables (chunk pruning, compression), current-state balance data stays in standard tables (upsert-friendly, HOT updates). Adding a new data type requires choosing the right model.

---

Agent Notes:
- 2026-02-26: the three TimescaleDB query optimization techniques form a chain — [[timescaledb chunk skipping must be explicitly enabled per column for sparse index scans to work]] enables the sparse indexes, [[the decomposed cursor condition enables timescaledb chunk pruning where row tuple comparison cannot]] makes cursors use them, [[materialized cte plus lateral join forces separate planning to preserve chunk pruning on account queries]] prevents the planner from undoing the work. All three must be understood together.
- 2026-02-26: [[deterministic uuid from content hash enables idempotent batch inserts without db roundtrip]] connects directly to [[per-ledger persistence is a single atomic database transaction ensuring all-or-nothing ledger commits]] — the UUID idempotency is what makes steps 1-2 of PersistLedgerData crash-safe.
