---
paths:
  - "internal/data/**"
  - "internal/db/**"
  - "internal/entities/**"
---

# Knowledge Route: Data Layer

Read before working in this subsystem:
- `docs/knowledge/references/data-layer.md`
- `docs/knowledge/entries/data-layer.md`

## Section Focus

- **Model Architecture** — `data.Models` aggregate struct, interface-backed mocking pattern
- **TimescaleDB Hypertables** — chunk skipping requirements, columnar compression applicability
- **Balance Tables** — current-state semantics (NOT time-series), why they are plain tables not hypertables
- **Token Balance Storage** — SAC/trustline split (G-address SACs go to `trustline_balances`, not `sac_balances`)
- **Insert Strategies** — pgx COPY binary protocol for backfill, unnest upsert for live ingestion

## Inline Gotchas

- Chunk skipping requires **explicit `timescaledb.enable_chunk_skipping`** per column — not enabled by default
- Balance tables are **standard Postgres tables, not hypertables** — they store current state, not time-series events
- Deterministic UUID from content hash enables **idempotent batch inserts** without a DB roundtrip
- Balance tables need **aggressive autovacuum tuning** — rows are updated every ledger, causing heavy bloat
