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

## Reference Docs

[[references/ingestion-pipeline]] — comprehensive overview with live/backfill flow diagrams, retry logic, processor architecture

[[references/state-changes]] — state change concept, two-axis category-reason taxonomy, four producing processors; state changes are the primary indexer output that flows into `state_changes` hypertable

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
- [[processors use ErrInvalidOpType sentinel error to gracefully skip non-applicable operations]] — explicit sentinel distinguishes "not applicable" from "processed successfully" vs nil ambiguity
- [[every processor records its own processing duration via MetricsServiceInterface for per-processor observability]] — deferred timer.Since() in each ProcessOperation; enables per-processor Prometheus breakdowns
- [[BYTEA custom types implement sql Scanner and driver Valuer for transparent binary database round-tripping]] — ContractID and binary asset codes implement Scanner+Valuer; sqlx calls them automatically
- [[flag encoding uses bitmask SMALLINT instead of JSON array for compact storage and efficient querying]] — 3-bit bitmask for trustline flags; enables bitwise WHERE clauses with standard B-tree indexes

## Processor Mechanics

- [[TokenTransferProcessor processes at transaction level while other state change processors operate per-operation]] — SDK EventsProcessor aggregates fee events across all ops; cannot be split per-operation
- [[EffectsProcessor is the most complex processor handling seven distinct categories of account state changes]] — seven categories from 50+ SDK effect types; wraps stellar/go ingest.EffectsProcessor
- [[TokenTransferProcessor calculates net fee per transaction by summing all fee events rather than tracking individual fee and refund events]] — Soroban refund model requires collapsing charge+refund into one net FEE StateChange
- [[ContractDeployProcessor walks Soroban authorization invocations recursively to detect all contract deployments including subinvocations]] — factory contracts deploy at arbitrary depth; flat scan misses nested deploys
- [[SACEventsProcessor handles authorization differently for classic accounts vs contract accounts because their authorization storage differs]] — G-address flags in ledger entry; C-address flags in contract instance storage
- [[SACEventsProcessor adapts to tx meta version differences for set_authorized event topic layout]] — protocol upgrade shifted topic offsets; processor branches on meta version
- [[TrustlinesProcessor and AccountsProcessor read absolute values from ledger change Post entries not deltas]] — point-in-time snapshot design; each row is the complete state after the operation
- [[SACBalancesProcessor only tracks contract-address holders because G-address SAC balances are captured by TrustlinesProcessor]] — avoids double-counting; G-address SAC = classic trustline, C-address SAC = contract storage
- [[ParticipantsProcessor extracts Soroban contract addresses as participants by recursively walking authorization entries and subinvocations]] — cross-contract call chains can involve dozens of contracts at arbitrary depth
- [[AccountsProcessor calculates minimum balance from numSubEntries numSponsoring and numSponsored to track reserve requirements]] — derived minBalance = (2+subEntries+sponsoring-sponsored)*baseReserve stored for query convenience

## State Change Infrastructure

- [[StateChangeBuilder provides a fluent builder with Clone for constructing branching state change trees from a common base]] — Clone() enables setting shared tx/op context once, then branching into multiple category-specific records
- [[state change ordering uses per-operation 1-indexed counters set after sort-key-based deterministic sorting]] — sort by Category+Reason+Address then assign 1-indexed order; ensures idempotent re-processing
- [[the unified StateChange struct uses nullable fields and a category discriminator to store all state change types in a single database table]] — single state_changes hypertable; nullable fields per category; index on (address, created_at)
- [[contract type classification uses deterministic SAC contract ID derivation to distinguish SAC from SEP41 tokens]] — DeriveAssetContractID(asset, network) produces the canonical SAC ID; match = SAC, else = SEP-41

## Reference Entries

- [[nine specialized processors run per transaction in the indexer fan-out]] — three interface types; table of all nine processors with levels and output types; per-processor behavior summary
- [[ledger fetch uses 10-retry exponential backoff capped at 30 seconds]] — maxLedgerFetchRetries=10, maxRetryBackoff=30s; used by both live and backfill
- [[persist retries use 5-retry exponential backoff capped at 10 seconds]] — maxIngestProcessedDataRetries=5, 10s cap; more aggressive than fetch retries

## Tensions

- [[catchup mode atomicity vs per-batch durability trade-off]] — catchup: all-or-nothing cursor jump; historical: per-batch incremental durability
- [[effects-based processing vs ledger-change-based processing produces overlapping but different views of the same data]] — EffectsProcessor (semantic) vs TrustlinesProcessor (raw storage): both see trustline changes; dedup left to downstream
- [[SACEventsProcessor and EffectsProcessor both generate BALANCE_AUTHORIZATION state changes for trustline flag changes through different paths]] — unresolved duplication; same event from SAC diagnostic events and SDK effect stream

## Gotchas

- [[backfill batch processing uses one LedgerBackend per goroutine because LedgerBackend is not thread-safe]] — LedgerBackendFactory exists for this reason; sharing a backend causes non-deterministic failures
- [[IndexerBuffer uses canonical pointer pattern to avoid duplicating large XDR transaction structs]] — txByHash stores one *Transaction pointer; multiple participants share the same pointer; Merge() applies three different strategies per collection type
- [[IndexerBuffer Clear method preserves backing arrays to reduce GC pressure during backfill]] — Clear() resets to empty but does not release memory; intentional, not a leak
- [[EffectsProcessor sets effect Address to the operation source for trustline flag effects but the actual trustor is in effect Details]] — Details["trustor"] is the affected account, not effect.Address, for SetTrustLineFlagsOp effects
- [[SACEventsProcessor silently continues on extraction errors rather than failing the entire transaction]] — best-effort parsing; malformed events log warning and skip; systematic bugs produce silent coverage gaps
- [[AccountChangedExceptSigners skip in AccountsProcessor means signer-only operations produce no account balance entry]] — SetOptions with signer-only changes leaves no trace in account_balances; coverage gap for signer history

## Cross-Subsystem Connections

- [[entries/data-layer]] — ingestion writes into five TimescaleDB hypertables; entries like [[compression timing trade-off: uncompressed inserts are faster but require post-processing pass]], [[progressive recompression compresses TimescaleDB chunks as watermark advances during historical backfill]], and [[ingest_store key-value table is the sole source of truth for cursor positions]] all straddle this boundary; the data layer reference doc covers the schema that ingestion writes into
- [[entries/signing]] — the six-step persist transaction includes channel account unlock (step 4: `UnassignTxAndUnlockChannelAccounts()`); ingestion is the only place where channel account state transitions happen outside the transaction submission path; [[the six-step persist transaction ordering exists to satisfy foreign key prerequisites]] explains why the unlock must be inside the same atomic transaction as the data insert

## Synthesis Opportunities

- **Two-mechanism crash safety model**: [[advisory lock prevents concurrent ingestion instances via postgresql pg_try_advisory_lock]] and [[crash recovery relies on atomic transactions to make ledger re-processing idempotent]] are presented separately but together they form a complete crash safety model — the lock prevents concurrent writes (no racing re-processors), and atomicity makes re-processing idempotent (no partial state). Neither alone is sufficient. This compound claim is not yet captured as a single entry.

---

Agent Notes:
- 2026-02-24: cross-subsystem links to data-layer and signing added; state-changes reference doc linked (was missing despite being the primary indexer output); synthesis opportunity flagged — the two-mechanism crash safety model spans advisory lock + atomic transactions and should be its own entry
- 2026-02-24: Processor Mechanics (10 entries), State Change Infrastructure (4 entries), and 4 new Patterns added from indexer processor pipeline documentation; Tensions expanded to 3; Gotchas expanded to 6

## Topics

[[entries/index]] | [[references/ingestion-pipeline]] | [[references/state-changes]] | [[entries/data-layer]] | [[entries/signing]]
