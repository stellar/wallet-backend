---
paths:
  - "internal/ingest/**"
  - "internal/indexer/**"
---

# Knowledge Route: Ingestion Pipeline

Read before working in this subsystem:
- `docs/knowledge/references/ingestion-pipeline.md`
- `docs/knowledge/entries/ingestion.md`

## Section Focus

- **Process Startup & Coordination** — advisory lock, singleton enforcement, mode selection
- **Live Ingestion** — per-ledger atomic commit, catchup threshold triggering parallel backfill
- **Backfill Mode** — parallel goroutines, ledger backend factory pattern, COPY vs upsert
- **Indexer Architecture** — pond pool parallelism, per-transaction buffers, processor design
- **State Changes** — if touching `internal/indexer/processors/`, also read `references/state-changes.md`

## Inline Gotchas

- Advisory lock (`pg_try_advisory_lock`) prevents concurrent ingestion instances — only one process runs at a time
- Each ledger is committed in a **single atomic DB transaction** — all processors succeed or the whole ledger rolls back
- Catchup threshold triggers **parallel backfill** instead of sequential processing when service falls behind
- Channel account is **released atomically** with the confirming ledger commit inside `persistLedgerData` — never release it separately
