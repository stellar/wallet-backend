---
description: Collects BatchChanges per batch; ADD→REMOVE within range detected as no-op; single atomic commit advances latestLedgerCursor to endLedger; failure leaves cursor unchanged for retry
type: insight
status: current
confidence: proven
subsystem: ingestion
areas: [ingestion, backfill, catchup, atomicity, postgresql]
---

# Catchup mode merges all batch changes in a single DB transaction using highest-OperationID-wins

## Context

Catchup mode fills the gap between the last ingested ledger and the current network tip. Unlike historical backfill (which uses many independent per-batch transactions), catchup must advance the `latestLedgerCursor` atomically — the live process depends on this cursor being accurate.

## Detail

In `BackfillModeCatchup`, each batch produces `BatchChanges` (trustline changes, account changes, contract changes, SAC balance changes) but does NOT update any cursor during batch execution. After all batches complete:

1. All `BatchChanges` are collected and merged using highest-OperationID-wins semantics.
2. ADD→REMOVE pairs detected as net no-ops are removed from the changeset.
3. A single DB transaction runs:
   - `processBatchChanges()` — applies merged token/balance changes.
   - `IngestStore.Update(latestLedgerCursor, endLedger)` — advances cursor.

If any batch fails, the entire catchup fails and the cursor is NOT advanced. The next restart sees the same gap and retries catchup.

Note that `transactions`, `operations`, and `state_changes` are written per-batch during execution (not deferred) — only token/balance changes are deferred to the final merge. Hypertable writes are effectively append-only and safe to commit per-batch.

## Implications

- After a successful catchup, the live ingestion process sees the cursor jump forward by potentially thousands of ledgers.
- The deferred merge means token balance state reflects the net change over the entire catchup range, not intermediate states.
- A partial catchup (some batches succeed, then failure) is safe — the cursor rollback ensures full retry on next run.

## Source

`internal/services/ingest_backfill.go:startBackfilling()` — BackfillModeCatchup path
`internal/services/ingest_backfill.go:processBatchChanges()`

## Related

The merge logic applies [[dedup maps use highest-operationid-wins semantics to resolve intra-ledger state conflicts]] at the batch-range level — what the IndexerBuffer does per-ledger, the catchup merger does across entire batch ranges.

[[catchup threshold triggers parallel backfill instead of sequential processing when service falls behind]] is what triggers this path; once triggered, the atomic merge described here is how catchup commits its result.

The single-transaction commit mirrors [[per-ledger persistence is a single atomic database transaction ensuring all-or-nothing ledger commits]] at ledger scale — both guarantee cursor advance is atomic with data writes.

The per-batch vs deferred split (hypertable event writes early, token/balance writes deferred) is a direct consequence of [[balance tables are standard postgres tables not hypertables because they store current state not time-series events]] — append-only hypertable writes have no intermediate-state problem, but current-state balance updates must reflect the net change over the entire catchup range, not any intermediate ledger.

relevant_notes:
  - "[[dedup maps use highest-operationid-wins semantics to resolve intra-ledger state conflicts]] — exemplifies this: catchup applies the same highest-operationid-wins dedup across the entire batch range, not just within a single ledger"
  - "[[catchup threshold triggers parallel backfill instead of sequential processing when service falls behind]] — grounds this: catchup mode is triggered by the threshold check; this entry describes what happens after trigger"
  - "[[per-ledger persistence is a single atomic database transaction ensuring all-or-nothing ledger commits]] — synthesizes with this: both patterns enforce all-or-nothing cursor advance; catchup is the batch-scale analog of per-ledger atomicity"
  - "[[balance tables are standard postgres tables not hypertables because they store current state not time-series events]] — grounds this: the per-batch vs deferred split in catchup is a direct consequence of table semantics — append-only hypertable writes are safe to commit incrementally; current-state balance updates are not"
