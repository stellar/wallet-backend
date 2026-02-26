---
description: Knowledge map for the ingestion pipeline — live/backfill modes, indexer architecture, retry logic, and cursor management
type: reference
status: current
subsystem: ingestion
areas: [navigation, ingestion]
---

# Ingestion Pipeline — Knowledge Map

The ingestion pipeline reads Stellar ledger data and persists it into TimescaleDB. Two modes share a common Indexer core: live (sequential, one ledger at a time) and backfill (parallel batches for historical data).

## Process Startup & Coordination

- [[advisory lock prevents concurrent ingestion instances via postgresql pg_try_advisory_lock]] — Why only one live ingest process can run per network; FNV-64a lock key design
- [[the database is the sole integration point between the serve and ingest processes]] — No direct inter-process communication; cursor state is the synchronization primitive
- [[hypertable settings are only applied in live mode to avoid overwriting a running instance config]] — Why backfill skips configureHypertableSettings
- [[timescaledb automatic retention policy eliminates custom pruning code for blockchain history tables]] — What configureHypertableSettings achieves for data lifecycle: chunk-drop automation replaces cron jobs and custom DELETE logic

## Live Ingestion

- [[catchup threshold triggers parallel backfill instead of sequential processing when service falls behind]] — The 100-ledger threshold that triggers automatic catchup mode on restart
- [[per-ledger persistence is a single atomic database transaction ensuring all-or-nothing ledger commits]] — The 6-step PersistLedgerData atomicity guarantee
- [[channel account is released atomically with the confirming ledger commit in persistledgerdata]] — How channel accounts are freed when their ledger commits

## Backfill Mode

- [[backfill mode trades acid durability for insert throughput via synchronous_commit off]] — The intentional durability/throughput tradeoff for historical ingestion
- [[ledger backend is not goroutine-safe so backfill creates one instance per goroutine via factory]] — The factory pattern that enables safe parallel batch processing
- [[progressive recompression uses watermark to compress historical backfill chunks as contiguous batches complete]] — Watermark-based compression during parallel backfill
- [[catchup mode merges all batch changes in a single db transaction using highest-operationid-wins]] — How catchup atomically advances the cursor after parallel batches

## Indexer Architecture

- [[indexer processes transactions in parallel within a ledger using pond pool with per-transaction buffers]] — Within-ledger parallelism at transaction granularity
- [[canonical pointer pattern in indexerbuffer avoids duplicating large xdr structs across participants]] — Memory-efficient two-layer buffer design for multi-participant transactions
- [[dedup maps use highest-operationid-wins semantics to resolve intra-ledger state conflicts]] — How ADD→REMOVE no-ops and intra-ledger conflicts are resolved

## State Changes

- [[state changes use a two-axis category-reason taxonomy to classify every account history event]] — The category×reason classification system and its 4 producing processors
- [[state change ordering uses sortkey for deterministic re-ingestion of the same ledger]] — Deterministic SortKey ensures stable StateChangeOrder on re-processing

## Tensions

- **Catchup vs historical backfill**: [[catchup mode merges all batch changes in a single db transaction using highest-operationid-wins]] commits atomically at the end; historical backfill uses progressive recompression during the run. Same parallel machinery, different commit strategies — trade-off between intermediate compression and atomic cursor advance.

## Open Questions

- Is the channel account retry backoff (6 retries, 1s each) calibrated to actual burst patterns?
- Does the `reconcile_oldest_cursor` job advance accurately when retention drops partial chunks?

---

## Synthesis Opportunities

- [[backfill mode trades acid durability for insert throughput via synchronous_commit off]], [[pgx copyfrom binary protocol is used for backfill bulk inserts unnest upsert for live ingestion]], [[ledger backend is not goroutine-safe so backfill creates one instance per goroutine via factory]], and [[progressive recompression uses watermark to compress historical backfill chunks as contiguous batches complete]] together imply an uncaptured claim: "historical backfill achieves throughput through coordinated relaxations at every layer — WAL sync, write protocol, goroutine isolation, and compression timing — each targeting a different bottleneck."

Agent Notes:
- 2026-02-26: channel account release is step 4 inside PersistLedgerData's single transaction — connecting [[channel account is released atomically with the confirming ledger commit in persistledgerdata]] and [[per-ledger persistence is a single atomic database transaction ensuring all-or-nothing ledger commits]] is the key traversal path for understanding ingestion-signing integration.
- 2026-02-26: catchup and historical backfill share the same parallel batch infrastructure but diverge at commit strategy — good to follow [[catchup threshold triggers parallel backfill instead of sequential processing when service falls behind]] -> [[catchup mode merges all batch changes in a single db transaction using highest-operationid-wins]] -> [[progressive recompression uses watermark to compress historical backfill chunks as contiguous batches complete]] in that order for a complete picture.
- 2026-02-26: [[hypertable settings are only applied in live mode to avoid overwriting a running instance config]] and [[timescaledb automatic retention policy eliminates custom pruning code for blockchain history tables]] are a two-entry sequence: the first explains the guard (live-only), the second explains what the guarded setting does (keep history bounded). Always read them together when investigating retention configuration or unexpected table growth.
- 2026-02-26: the "why are balance writes deferred in catchup while event writes commit per-batch?" question is answered in the data-layer, not ingestion — follow [[balance tables are standard postgres tables not hypertables because they store current state not time-series events]] to understand that current-state tables cannot safely expose intermediate states, while append-only hypertable event writes are idempotent and safe to commit early. This is a cross-subsystem traversal: ingestion commit strategy is explained by data-layer table semantics.
