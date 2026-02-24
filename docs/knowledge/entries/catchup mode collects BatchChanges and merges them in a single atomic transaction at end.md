---
context: Contrast with historical backfill which updates oldestCursor per-batch; catchup trades per-batch durability for atomicity of the full cursor jump forward
type: pattern
status: current
subsystem: ingestion
areas: [ingestion]
created: 2026-02-24
---

# catchup mode collects BatchChanges and merges them in a single atomic transaction at end

Catchup mode (`BackfillModeCatchup`) differs from historical backfill in how it applies changes. Instead of updating the cursor after each batch, it collects `BatchChanges` from every parallel batch and then, after all batches complete, applies everything in a single database transaction:

1. All batch goroutines return `BatchChanges` (trustline, account, contract, SAC balance changes)
2. `analyzeBatchResults()` checks if any batches failed
3. If any failed: return error — `latestLedgerCursor` is NOT advanced
4. If all succeeded: merge all `BatchChanges` and run `processBatchChanges()` + update `latestLedgerCursor` to `endLedger` in one atomic transaction

The merge uses highest-OperationID-wins semantics with ADD→REMOVE no-op detection (see [[highest-OperationID-wins semantics handles concurrent batch deduplication]]).

The all-or-nothing characteristic is intentional: catchup's purpose is to jump `latestLedgerCursor` forward from a stale position to the current network tip. Partial cursor advancement would be confusing — live ingestion would then resume from a mid-catchup position that doesn't correspond to a clean state boundary.

Since [[catchup mode atomicity vs per-batch durability trade-off]], this means catchup is not incrementally resumable — a failure restarts the entire catchup range. For typical catchup scenarios (service down for minutes to hours), this is acceptable. For very long outages requiring catchup over thousands of ledgers, the behavior is more costly.

---

Source: [[references/ingestion-pipeline]]

Relevant Notes:
- [[catchup mode atomicity vs per-batch durability trade-off]] — the trade-off this pattern instantiates
- [[highest-OperationID-wins semantics handles concurrent batch deduplication]] — the merge strategy used
- [[backfill crash recovery relies on gap detection finding incomplete batches on next run]] — historical mode contrasts with this

Areas:
- [[entries/ingestion]]
