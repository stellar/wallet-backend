---
description: Knowledge map for the ingestion pipeline subsystem
type: reference
subsystem: ingestion
areas: [knowledge-map, ingestion, indexer, stellar-rpc]
vault: docs/knowledge
---

# Ingestion Pipeline Knowledge Map

The ingestion pipeline reads Stellar ledger data via the Stellar RPC node and persists it to TimescaleDB. It runs as two modes — live (follows the chain) and backfill (fills historical gaps) — sharing a common Indexer core.

**Key code:** `internal/ingest/`, `internal/services/ingest*.go`, `internal/indexer/`

## Reference Doc

[[references/ingestion-pipeline]] — comprehensive overview with live/backfill flow diagrams, retry logic, processor architecture

## Decisions

- [[advisory lock prevents concurrent ingestion instances via postgresql pg_try_advisory_lock]] — singleton guarantee via pg_try_advisory_lock with FNV-64a hash; testnet/pubnet get separate locks
- [[first-run live ingestion starts from history archive tip not genesis]] — PopulateAccountTokens snapshots state before ingestion; starting from genesis is impractical
- [[LedgerBackend interface abstracts between real-time RPC and cloud storage datastore backends]] — same pipeline code runs against Stellar RPC or S3/GCS datastore; config-driven selection
- [[operations within a transaction run processors sequentially not in parallel to avoid pool overhead]] — only 3 processors per operation; pool overhead exceeds parallelism savings at this granularity

## Insights

- [[catchup threshold triggers parallel backfill instead of sequential catchup after restart]] — 100-ledger default threshold; below it: sequential live; above it: parallel BackfillModeCatchup
- [[crash recovery relies on atomic transactions to make ledger re-processing idempotent]] — atomic transactions + advisory lock form the entire concurrency safety model
- [[backfill crash recovery relies on gap detection finding incomplete batches on next run]] — historical mode is partially resumable; catchup mode is all-or-nothing
- [[oldest ledger sync interval decouples live and backfill cursor tracking]] — live reads oldestLedgerCursor from DB every 100 ledgers to pick up backfill progress
- [[live ingestion and backfill share a common processLedger path through the Indexer]] — diverge only in ledger source and persistence; Indexer is the shared seam
- [[backfill batch size and DB insert batch size are separate concerns for memory bounding]] — 250 ledgers per batch (parallelism unit); 50 ledgers per DB flush (memory bound)
- [[compression timing trade-off: uncompressed inserts are faster but require post-processing pass]] — historical backfill writes uncompressed then runs progressiveRecompressor

## Patterns

- [[live ingestion persists each ledger in a single atomic database transaction]] — six ordered steps in one transaction; all or nothing
- [[the six-step persist transaction ordering exists to satisfy foreign key prerequisites]] — assets before balances (FK); cursor update last as commit marker
- [[backfill uses gap detection via window function on transactions ledger_number]] — LEAD() window function on distinct ledger_number; only finds internal gaps
- [[catchup mode collects BatchChanges and merges them in a single atomic transaction at end]] — all batch results merged; single final transaction advances latestLedgerCursor
- [[highest-OperationID-wins semantics handles concurrent batch deduplication]] — ADD+REMOVE in same range detected as no-op; applies in IndexerBuffer and BatchChanges merge
- [[progressive recompression compresses TimescaleDB chunks as watermark advances during historical backfill]] — watermark requires batch 0 first; final verification pass catches boundary chunks
- [[indexer processes transactions in parallel via pond.Pool but merges into single ledger buffer sequentially]] — fan-out to unbounded pool; fan-in sequential merge
- [[per-transaction IndexerBuffers are created independently then merged to enable safe parallelism]] — no shared mutable state during fan-out; merge runs after all goroutines complete
- [[UpdateMin cursor pattern ensures oldest cursor only moves backward not forward]] — LEAST() SQL ensures cursor reflects actual oldest regardless of batch completion order
- [[ingest_store key-value table is the sole source of truth for cursor positions]] — upsert semantics; Prometheus metrics are derived, not authoritative
- [[factory pattern for non-thread-safe resources enables safe parallelism]] — func(ctx) → Resource passed at wiring time; each goroutine calls factory for isolated instance

## Gotchas

- [[backfill batch processing uses one LedgerBackend per goroutine because LedgerBackend is not thread-safe]] — LedgerBackendFactory exists for this reason; sharing a backend causes non-deterministic failures
- [[IndexerBuffer uses canonical pointer pattern to avoid duplicating large XDR transaction structs]] — txByHash stores one *Transaction pointer; multiple participants share the same pointer
- [[IndexerBuffer Clear method preserves backing arrays to reduce GC pressure during backfill]] — Clear() resets to empty but does not release memory; intentional, not a leak

## Reference Entries

- [[nine specialized processors run per transaction in the indexer fan-out]] — three interface types; table of all nine processors with levels and output types
- [[ledger fetch uses 10-retry exponential backoff capped at 30 seconds]] — maxLedgerFetchRetries=10, maxRetryBackoff=30s; used by both live and backfill
- [[persist retries use 5-retry exponential backoff capped at 10 seconds]] — maxIngestProcessedDataRetries=5, 10s cap; more aggressive than fetch retries

## Tensions

- [[catchup mode atomicity vs per-batch durability trade-off]] — catchup: all-or-nothing cursor jump; historical: per-batch incremental durability

## Topics

[[entries/index]] | [[references/ingestion-pipeline]]
