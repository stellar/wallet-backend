---
context: maxLedgerFetchRetries=10, maxRetryBackoff=30s, defined in ingest.go; shared by both live and backfill via getLedgerWithRetry(); context cancellation exits immediately
type: reference
status: current
subsystem: ingestion
areas: [ingestion]
created: 2026-02-24
---

# ledger fetch uses 10-retry exponential backoff capped at 30 seconds

`getLedgerWithRetry()` in `ingest.go` retries Stellar RPC ledger fetches with exponential backoff:

- **Max retries:** 10 (`maxLedgerFetchRetries`)
- **Backoff schedule:** 1s, 2s, 4s, 8s, 16s, 30s (capped), 30s, ... up to retry 10
- **Exit conditions:** success, `ctx.Done()` (context cancelled), or 10 attempts exhausted
- **Used by:** both live ingestion and backfill batch processing

The 30-second cap means the worst-case delay for a single ledger fetch is roughly 10 retries × 30s max backoff = ~5 minutes of retries before the pipeline gives up and propagates the error.

The asymmetry with persist retries (5 retries, 10s cap — see [[persist retries use 5-retry exponential backoff capped at 10 seconds]]) reflects a design judgment: RPC availability is more variable and potentially a longer-lived issue (network partition, RPC node restart). Database connectivity is a local issue that should resolve faster.

Context cancellation provides the clean shutdown path — when the service receives a termination signal, the context is cancelled and `getLedgerWithRetry()` exits without waiting out the remaining backoff, ensuring prompt shutdown.

---

Source: [[references/ingestion-pipeline]]

Relevant Notes:
- [[persist retries use 5-retry exponential backoff capped at 10 seconds]] — the different retry config for DB persistence
- [[crash recovery relies on atomic transactions to make ledger re-processing idempotent]] — what happens after a retry eventually exhausts and error propagates

Areas:
- [[entries/ingestion]]
