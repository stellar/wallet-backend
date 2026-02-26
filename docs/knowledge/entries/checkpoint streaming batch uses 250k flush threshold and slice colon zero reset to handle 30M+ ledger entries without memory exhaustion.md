---
context: The 250k threshold is a memory ceiling, not a DB write optimization; slice[:0] reuse is the Go-specific mechanism that avoids GC pressure from repeatedly allocating new backing arrays
type: pattern
status: active
created: 2026-02-26
---

# checkpoint streaming batch uses 250k flush threshold and slice colon zero reset to handle 30M+ ledger entries without memory exhaustion

During `PopulateAccountTokens()`, the streaming batch architecture processes the entire Stellar history archive (30M+ trustline entries) without loading it all into memory:

```go
const flushBatchSize = 250_000

// After adding entries to batch:
if batch.count() >= flushBatchSize {
    batch.flush(ctx, dbTx) // BatchCopy to DB
    batch.reset()          // slice[:0] — reuses backing array
}
```

The `batch.reset()` method uses `slice[:0]` semantics: it sets the slice length to zero without releasing the backing array. The next round of entries fills the same allocated memory, preventing repeated GC allocations for each 250k-entry window.

## Why 250k

250,000 entries is calibrated as a memory ceiling that keeps the batch struct from growing unboundedly. At this threshold, a single flush occurs before the process would begin to exhaust available memory. The exact value is a constant in `token_ingestion.go` (`flushBatchSize`).

## Three balance types flushed together

Each `flush()` call writes all three balance types simultaneously:
1. `trustlineBalanceModel.BatchCopy()`
2. `nativeBalanceModel.BatchCopy()`
3. `sacBalanceModel.BatchCopy()`

This means partial flushes write all pending data in a single coordinated call — there is no scenario where one balance type is flushed while another still has pending entries from the same window.

## Source

`internal/services/token_ingestion.go` — `streamCheckpointData()` (approximately lines 144–147 per reference doc)

## Related

- [[pgx copyfrom binary protocol is used for backfill bulk inserts unnest upsert for live ingestion]] — the write protocol used inside each flush call
- [[checkpoint population disables FK checks via session_replication_role replica to allow balance entries to be inserted before their parent contract rows]] — the FK strategy active during the same streaming phase

relevant_notes:
  - "[[pgx copyfrom binary protocol is used for backfill bulk inserts unnest upsert for live ingestion]] — grounds this: each flush call uses BatchCopy (pgx.CopyFrom binary protocol) — the streaming batch architecture is the consumer-side pattern; CopyFrom is the writer-side protocol"
  - "[[checkpoint population disables FK checks via session_replication_role replica to allow balance entries to be inserted before their parent contract rows]] — co-occurs: session_replication_role=replica is active for the entire transaction that contains these streaming flushes"
