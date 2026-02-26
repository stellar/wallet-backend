---
description: ADD→REMOVE pairs within same buffer are detected and deleted as net no-op; applies to trustlines, accounts, SAC balances; highest OperationID wins when same key appears multiple times
type: insight
status: current
confidence: proven
subsystem: ingestion
areas: [ingestion, indexer, deduplication, state-management]
---

# Dedup maps use highest-OperationID-wins semantics to resolve intra-ledger state conflicts

## Context

A single ledger (or backfill batch) may contain multiple operations that affect the same trustline or account. For example, an account might be created and then deleted within the same ledger. The final state must reflect the net effect.

## Detail

The `IndexerBuffer` dedup maps (`trustlineChangesByTrustlineKey`, `accountChangesByAccountID`, `sacBalanceChangesByKey`) keep only the last-seen entry per natural key, using highest-OperationID-wins logic:

- When two changes arrive for the same key, the one with the higher OperationID replaces the earlier one.
- When an ADD and a REMOVE arrive for the same key within the same buffer (net no-op), both are deleted from the map — no database write occurs.

This is applied in both the `IndexerBuffer` (per-ledger) and in catchup mode's `BatchChanges` merger (per-batch-range).

## Implications

- The no-op detection is an optimization: it prevents unnecessary upsert+delete pairs in the DB. Without it, an account created and deleted in the same ledger would still require two DB writes.
- This logic is correct only because OperationIDs are monotonically increasing within a ledger, and the dedup maps are rebuilt fresh per ledger/batch.
- If a trustline has 3 changes in one ledger (create, update, update), only the highest-OperationID update is stored.

## Source

`internal/indexer/indexer_buffer.go` — dedup map operations
`internal/services/ingest_backfill.go` — catchup merge logic with ADD→REMOVE detection

## Related

The dedup maps live inside the [[canonical pointer pattern in indexerbuffer avoids duplicating large xdr structs across participants]] — the two-layer buffer design and the dedup maps are both properties of the same IndexerBuffer; dedup operates on the Layer 2 participant maps.

The net no-op detection reduces unnecessary DB writes in [[per-ledger persistence is a single atomic database transaction ensuring all-or-nothing ledger commits]] — fewer merged changes means shorter atomic transactions and less write contention.

relevant_notes:
  - "[[canonical pointer pattern in indexerbuffer avoids duplicating large xdr structs across participants]] — grounds this: dedup maps are part of IndexerBuffer; both entries describe different aspects of the same data structure"
  - "[[per-ledger persistence is a single atomic database transaction ensuring all-or-nothing ledger commits]] — enables this: dedup map no-op detection reduces the changeset passed to PersistLedgerData, improving atomic transaction efficiency"
