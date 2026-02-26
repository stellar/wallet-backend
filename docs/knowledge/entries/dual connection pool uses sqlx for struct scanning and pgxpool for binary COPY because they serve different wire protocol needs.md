---
context: Each DBConnectionPool wraps both sqlx.DB (text protocol, struct scanning) and pgxpool.Pool (binary protocol, COPY); BatchCopy uses pgxpool directly; all other queries use sqlx
type: insight
created: 2026-02-24
status: current
subsystem: data-layer
areas: [data-layer]
---

# dual connection pool uses sqlx for struct scanning and pgxpool for binary COPY because they serve different wire protocol needs

The `DBConnectionPool` struct maintains two underlying connection pools to the same database:

1. **`sqlx.DB`** — used for all regular queries, struct scanning, and transaction management. `sqlx` extends `database/sql` with struct field binding via reflection and named parameter support. It uses PostgreSQL's text-based wire protocol.

2. **`pgxpool.Pool`** — used exclusively for `pgx.CopyFrom` (binary COPY operations) during backfill. The `pgx` driver speaks PostgreSQL's binary wire protocol for COPY, which is substantially faster than text-encoded inserts for bulk data. `database/sql` (and therefore `sqlx`) does not expose COPY-protocol access.

The two pools share the same connection parameters but are independent — each maintains its own connection lifecycle, idle limits, and health checks. This duplication is the cost of needing both interfaces.

Why not use only `pgx`? The `pgx` driver can be used with `database/sql` for regular queries, but it loses struct scanning conveniences that `sqlx` provides, making the majority of query code more verbose. The split is pragmatic: `sqlx` for ergonomic regular queries, `pgxpool` for raw COPY throughput.

The compile-time assertion `var _ SQLExecuter = (*DBConnectionPool)(nil)` ensures the pool satisfies the required interface, preventing silent interface drift.

---

Relevant Notes:
- [[backfill connection pool sets synchronous_commit=off at connection-string level so all pool connections inherit the setting without per-query hints]] — pool-level configuration on top of this dual-pool architecture
- [[raw SQL with strings.Builder and positional parameters gives full control over TimescaleDB-specific query constructs no query builder can generate]] — the query authoring approach used with sqlx

Areas:
- [[entries/data-layer]]
