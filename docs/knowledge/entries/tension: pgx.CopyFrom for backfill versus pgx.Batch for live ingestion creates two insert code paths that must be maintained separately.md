---
context: Backfill uses pgx.CopyFrom (binary COPY, no conflict handling); live ingestion uses UNNEST + ON CONFLICT DO NOTHING or pgx.Batch upsert+delete; both paths implement the same logical operation but with different semantics
type: insight
created: 2026-02-24
---

# tension: pgx.CopyFrom for backfill versus pgx.Batch for live ingestion creates two insert code paths that must be maintained separately

The data layer has two distinct strategies for bulk inserts:

**Backfill path (`pgx.CopyFrom` / `BatchCopy`)**:
- Uses PostgreSQL's binary COPY protocol via `pgx.CopyFrom`
- Maximum throughput: binary encoding, no WAL per-row, no conflict checking
- No `ON CONFLICT` support — data must be structurally clean before insertion
- Assumes `session_replication_role = 'replica'` has disabled FK checking
- Used only during historical backfill where data volume justifies the complexity

**Live ingestion path (`UNNEST` + `ON CONFLICT DO NOTHING` / `pgx.Batch`)**:
- Uses `UNNEST($1::type[], $2::type[], ...)` to insert multiple rows in one statement
- `ON CONFLICT DO NOTHING` handles duplicates idempotently
- `pgx.Batch` for balance upserts: can combine upserts and deletes in a single round trip
- Transactional: runs inside the ledger commit transaction
- Slower per-row than COPY but handles real-time streaming correctly

The tension: every model that participates in both backfill and live ingestion must implement both `BatchCopy` and `BatchInsert` (or equivalent). When the schema changes — e.g., a new column — both paths must be updated. The COPY path is particularly fragile: column order in `CopyRows()` must exactly match the COPY target column list.

There is no shared abstraction that handles both patterns; the semantics are too different. The two paths are maintained as parallel implementations that must be kept in sync.

---

Relevant Notes:
- [[dual connection pool uses sqlx for struct scanning and pgxpool for binary COPY because they serve different wire protocol needs]] — the pool architecture that enables the two paths
- [[backfill connection pool sets synchronous_commit=off at connection-string level so all pool connections inherit the setting without per-query hints]] — the backfill-specific pool configuration

Areas:
- [[entries/data-layer]]
