---
context: maxIngestProcessedDataRetries=5, maxIngestProcessedDataRetryBackoff=10s, in ingest_live.go; used by both ingestProcessedDataWithRetry (live) and flushBatchBufferWithRetry (backfill)
type: reference
status: current
subsystem: ingestion
areas: [ingestion]
created: 2026-02-24
---

# persist retries use 5-retry exponential backoff capped at 10 seconds

`ingestProcessedDataWithRetry()` and `flushBatchBufferWithRetry()` retry database persistence with:

- **Max retries:** 5 (`maxIngestProcessedDataRetries`)
- **Backoff schedule:** 1s, 2s, 4s, 8s, 10s (capped), ...
- **Exit conditions:** success, `ctx.Done()`, or 5 attempts exhausted
- **Location:** `ingest_live.go`
- **Used by:** live ingestion (per-ledger persist) and backfill (per-DB-flush within a batch)

The more conservative limit compared to fetch retries reflects the assumption that database connectivity issues resolve faster than RPC node availability issues. A database transient error (connection pool exhaustion, temporary lock contention) typically resolves within seconds. An RPC node issue might take minutes.

Both live and backfill use the same constants because the retry behavior for a DB write failure is the same regardless of which pipeline mode triggered it — wait, retry, give up after 5 attempts.

If persist retries exhaust (5 failures), the error propagates up. For live ingestion, this halts ingestion. For backfill, the batch fails and the cursor for that batch is not updated, allowing gap detection to find and retry it on the next run.

---

Source: [[references/ingestion-pipeline]]

Relevant Notes:
- [[ledger fetch uses 10-retry exponential backoff capped at 30 seconds]] — the more aggressive retry config for RPC fetches
- [[live ingestion persists each ledger in a single atomic database transaction]] — what is being retried on the live path

Areas:
- [[entries/ingestion]]
