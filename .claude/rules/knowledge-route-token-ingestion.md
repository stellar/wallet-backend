---
paths:
  - "internal/services/token_ingestion*"
  - "internal/services/contract_*"
---

# Knowledge Route: Token Ingestion

Read before working in this subsystem:
- `docs/knowledge/references/token-ingestion.md`
- `docs/knowledge/entries/ingestion.md` — focus on the **Token Ingestion** section only
  (skip Process Startup, Live Ingestion, Backfill Mode, Indexer Architecture, State Changes)
- `docs/knowledge/entries/data-layer.md` — focus on **Token Balance Storage** section
  for balance table semantics (SAC vs trustline split)

## Inline Gotchas

- G-address SAC token holdings go to **`trustline_balances`**, NOT `sac_balances` — Stellar represents them as standard trustlines
- Checkpoint population disables FK checks via **`session_replication_role = replica`** to allow balance entries before parent contract rows
- Contract metadata fetching rate-limits to **20 parallel contracts per batch** with a 2s sleep — avoids overloading Stellar RPC
- UNKNOWN contract type entries are **silently skipped** during ingestion — ensure contract type is resolved before processing
