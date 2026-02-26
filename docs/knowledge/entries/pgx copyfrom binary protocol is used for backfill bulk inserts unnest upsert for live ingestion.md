---
description: COPY fails on conflict so only used for backfill where no duplicates expected; BatchUpsert uses pgx.Batch + ON CONFLICT DO UPDATE for live changes; different strategies for different contexts
type: insight
status: current
confidence: proven
subsystem: data-layer
areas: [data-layer, postgresql, performance, backfill, ingestion, pgx]
---

# pgx CopyFrom binary protocol is used for backfill bulk inserts; UNNEST upsert for live ingestion

## Context

Two different write scenarios exist: backfill (inserting historical ledger data that shouldn't exist yet) and live ingestion (inserting current ledger data that might overlap with a restart). These require different strategies.

## Detail

| Strategy | Method | Use Case | Conflict Handling |
|----------|--------|----------|-------------------|
| `pgx.CopyFrom` | `BatchCopy` | Backfill — no duplicates expected (fresh gaps) | None — COPY fails on any conflict |
| `UNNEST + ON CONFLICT DO UPDATE` | `BatchUpsert` (balance models) | Live ingestion — restarts may overlap | `ON CONFLICT ... DO UPDATE SET ...` |
| `ON CONFLICT DO NOTHING` | `BatchInsert` (assets/contracts) | Idempotent metadata inserts | Skips existing rows silently |
| `ON CONFLICT DO NOTHING` | `BatchInsert` (account_contract_tokens) | Append-only relationship inserts | Skips existing rows silently |
| `pgx.CopyFrom` (initial only) | `BatchCopy` (balance models) | `PopulateAccountTokens()` first run | None — called once on empty tables |

All four `BatchUpsert` calls in `ProcessTokenChanges()` (trustline, contract, native balance, SAC balance) use `pgx.Batch.SendBatch()` — a pipeline of multiple SQL statements sent in a single round-trip per ledger. This means the per-ledger live ingestion path makes exactly one DB round-trip for all four balance type updates combined.

`pgx.CopyFrom` uses PostgreSQL's binary COPY protocol, which is significantly faster than INSERT statements because:
1. Binary wire format has less overhead than text SQL.
2. The server processes rows in bulk, reducing per-row overhead.
3. No query parsing, planning, or parameter binding per row.

The tradeoff: COPY cannot handle `ON CONFLICT` clauses — it aborts the entire batch on the first duplicate. Since [[blockchain ledger data is append-only and time-series which makes timescaledb columnar compression maximally effective]], this limitation is not a constraint during backfill: historical records inserted into fresh gap ranges have no prior versions to conflict with — append-only data and COPY's conflict intolerance are structurally compatible.

## Implications

- Use `BatchCopy` only when the target rows are guaranteed to not already exist (fresh backfill gaps, first-run initialization).
- Use `BatchUpsert` for live ingestion where restarts may re-process the same ledger range.
- If a new model needs both backfill and live write paths, implement both `BatchCopy` and `BatchUpsert` methods.

## Source

`internal/data/transactions.go`, `operations.go`, `statechanges.go` — `BatchCopy` implementations
`internal/data/trustline_balances.go`, etc. — `BatchUpsert` and `BatchCopy` implementations

## Related

The COPY strategy pairs with [[backfill mode trades acid durability for insert throughput via synchronous_commit off]] — both are applied to historical ingestion only, together forming the performance profile of the backfill path where re-derivability justifies the tradeoffs.

relevant_notes:
  - "[[backfill mode trades acid durability for insert throughput via synchronous_commit off]] — synthesizes with this: synchronous_commit=off and COPY together form the backfill performance strategy; each targets a different bottleneck (WAL sync vs row-level overhead)"
  - "[[balance tables are standard postgres tables not hypertables because they store current state not time-series events]] — constrains this: COPY's conflict intolerance makes it unusable for balance tables regardless of ingestion mode; their current-state upsert semantics mandate the UNNEST upsert path always, not just during live ingestion"
