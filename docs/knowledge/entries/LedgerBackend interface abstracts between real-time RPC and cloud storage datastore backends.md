---
context: LedgerBackendType config selects RPC (live streaming) or datastore (S3/GCS pre-exported); same interface used by both live and backfill; enables testing with either backend
type: decision
status: current
subsystem: ingestion
areas: [ingestion, stellar-rpc]
created: 2026-02-24
---

# LedgerBackend interface abstracts between real-time RPC and cloud storage datastore backends

The `LedgerBackend` interface (from `go-stellar-sdk/ingest/ledgerbackend`) provides three methods:
- `PrepareRange(ctx, Range)` — initialize for a bounded or unbounded range
- `GetLedger(ctx, seq)` — fetch a single ledger's `LedgerCloseMeta`
- `Close()` — release resources

Two concrete implementations are supported:

| Backend | Config Key | Use Case |
|---------|-----------|----------|
| `RPCLedgerBackend` | `rpc` | Live real-time streaming from Stellar RPC node |
| `BufferedStorageBackend` | `datastore` | Cloud storage (S3/GCS) with pre-exported ledger files |

Selection is via `LedgerBackendType` in the service config. The rest of the pipeline code — `getLedgerWithRetry()`, `processLedger()`, the entire indexer path — works identically regardless of which backend is selected.

The Datastore backend is primarily for historical backfill scenarios where an RPC node's history window is limited (Stellar RPC nodes typically retain only recent ledgers). By pre-exporting ledger data to S3/GCS and using `BufferedStorageBackend`, you can backfill arbitrary historical ranges without requiring an archival RPC node.

The abstraction also enables testing: unit tests can inject a mock `LedgerBackend` without needing a real RPC connection.

---

Source: [[references/ingestion-pipeline]]

Relevant Notes:
- [[backfill batch processing uses one LedgerBackend per goroutine because LedgerBackend is not thread-safe]] — thread safety constraint on the interface
- [[factory pattern for non-thread-safe resources enables safe parallelism]] — how parallel backfill manages multiple instances

Areas:
- [[entries/ingestion]]
