---
context: LedgerBackendFactory (func(ctx) → LedgerBackend) is the pattern that enables safe parallel backfill; each pond.Pool worker calls the factory to get its own isolated backend
type: gotcha
status: current
subsystem: ingestion
areas: [ingestion]
created: 2026-02-24
---

# backfill batch processing uses one LedgerBackend per goroutine because LedgerBackend is not thread-safe

`LedgerBackend` instances cannot be shared across goroutines. The `go-stellar-sdk/ingest/ledgerbackend` interface is not thread-safe, so each parallel batch worker must have its own instance.

The solution is `LedgerBackendFactory` — a `func(ctx) → LedgerBackend` passed into the backfill pipeline during dependency wiring. Each goroutine in the `pond.Pool` calls the factory to obtain a fresh, independent backend. The factory internally calls `NewLedgerBackend()` and then `PrepareRange(bounded)` for the specific batch range.

This is why live ingestion (sequential) can use a single shared `LedgerBackend`, while backfill (parallel) cannot. The same `LedgerBackend` interface is used in both cases, but the lifecycle differs:

- **Live:** one instance, `PrepareRange(unbounded)`, used for the entire process lifetime
- **Backfill batch:** one instance per goroutine, `PrepareRange(bounded)` with the batch's specific ledger range, closed when the batch completes

The factory pattern is also what enables the two backend types (RPC and Datastore) to work transparently — the factory is configured at startup based on `LedgerBackendType` and the parallel backfill code never needs to know which type it is getting.

If you attempt to share a `LedgerBackend` across goroutines, you will see non-deterministic failures (data corruption, panics, or stale reads) with no obvious error at the point of sharing.

---

Source: [[references/ingestion-pipeline]]

Relevant Notes:
- [[factory pattern for non-thread-safe resources enables safe parallelism]] — generalizes this pattern
- [[LedgerBackend interface abstracts between real-time RPC and cloud storage datastore backends]] — the two concrete types the factory can produce

Areas:
- [[entries/ingestion]]
