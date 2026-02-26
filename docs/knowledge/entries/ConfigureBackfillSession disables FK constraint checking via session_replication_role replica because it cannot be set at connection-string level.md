---
context: session_replication_role='replica' disables FK and trigger checks; must be set via SET command per connection after opening because PostgreSQL does not honor it in connection string options
type: gotcha
created: 2026-02-24
status: current
subsystem: data-layer
areas: [data-layer, ingestion]
---

# ConfigureBackfillSession disables FK constraint checking via session_replication_role replica because it cannot be set at connection-string level

During historical backfill, large batches of rows are bulk-inserted across multiple tables. The FK relationships between these tables (operations → transactions, state_changes → operations, etc.) normally require that parent rows exist before child rows can be inserted. During backfill, the insertion order is carefully managed, but FK checking on every row adds significant overhead.

Setting `session_replication_role = 'replica'` disables FK constraint checking and most trigger execution for that session — simulating the behavior of a replica that applies WAL without re-validating constraints. This is appropriate for backfill because the data originates from the Stellar network and is structurally valid; the FK relationships will hold once the batch is complete.

The constraint: PostgreSQL does not allow `session_replication_role` to be set in connection string parameters (unlike `synchronous_commit`). It can only be set via an explicit `SET session_replication_role = 'replica'` command on an already-open connection. This is why `ConfigureBackfillSession()` exists — it runs this `SET` command on each connection in the backfill pool after opening, ensuring all backfill connections operate with FK checking disabled.

If `ConfigureBackfillSession()` is not called, or if a connection from the wrong pool is used, FK violations may occur during backfill even for structurally valid data depending on insertion order.

---

Relevant Notes:
- [[backfill connection pool sets synchronous_commit=off at connection-string level so all pool connections inherit the setting without per-query hints]] — the other backfill session configuration, which uses a different (connection-string) mechanism
- [[the six-step persist transaction ordering exists to satisfy foreign key prerequisites]] — FK ordering in live ingestion (without FK disabling)

Areas:
- [[entries/data-layer]]
