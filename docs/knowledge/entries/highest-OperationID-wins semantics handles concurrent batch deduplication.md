---
context: Applies to trustline, account, SAC balance dedup maps in IndexerBuffer and to BatchChanges merge in catchup; ADD+REMOVE within same range is detected and deleted as net no-op
type: pattern
status: current
subsystem: ingestion
areas: [ingestion]
created: 2026-02-24
---

# highest-OperationID-wins semantics handles concurrent batch deduplication

When multiple operations affect the same entity (same trustline key, same account ID, same SAC balance key) within a processing range, the system uses highest-OperationID-wins semantics to determine the final state.

This appears in two places:

**1. IndexerBuffer dedup maps:** `trustlineChangesByTrustlineKey`, `accountChangesByAccountID`, `sacBalanceChangesByKey` are maps where inserting a new entry replaces the existing one only if the new entry has a higher OperationID. This ensures the most-recent-within-ledger state is preserved when multiple operations modify the same entity.

**2. BatchChanges merge (catchup mode):** When merging `BatchChanges` from multiple parallel batches, the same highest-OperationID-wins rule applies across batches. Since batches cover non-overlapping ledger ranges, OperationIDs are globally ordered and the correct final state is the one from the latest operation.

**ADD→REMOVE no-op detection:** If an entity is created (ADD) and then removed (REMOVE) within the same buffer or merge scope, the net effect is nothing — the entry is deleted from the map entirely. This handles cases like: trustline created and removed in the same backfill range, account opened and closed in the same range.

The deduplication is necessary because parallel batch processing means changes to the same entity can arrive from different goroutines. Without dedup, the final state written to the database would depend on goroutine scheduling order.

---

Source: [[references/ingestion-pipeline]]

Relevant Notes:
- [[catchup mode collects BatchChanges and merges them in a single atomic transaction at end]] — uses this merge semantics
- [[IndexerBuffer uses canonical pointer pattern to avoid duplicating large XDR transaction structs]] — the buffer where per-ledger dedup maps live

Areas:
- [[entries/ingestion]]
