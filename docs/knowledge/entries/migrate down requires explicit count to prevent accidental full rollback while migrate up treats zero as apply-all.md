---
context: migrate up count=0 means "apply all pending"; migrate down count=0 is rejected — caller must specify how many to roll back; prevents running `migrate down` with no argument from wiping the schema
type: decision
created: 2026-02-24
status: current
subsystem: data-layer
areas: [data-layer]
---

# migrate down requires explicit count to prevent accidental full rollback while migrate up treats zero as apply-all

The CLI wraps `rubenv/sql-migrate` with asymmetric defaults for the `count` argument:

- **`migrate up [count]`**: if `count` is 0 (or omitted), all pending migrations are applied. This is the typical "bring database up to date" operation — safe to run unconditionally.
- **`migrate down <count>`**: `count` must be explicitly provided and greater than zero. A zero count is rejected with an error. This prevents the accidental invocation `migrate down` (without thinking about count) from rolling back all migrations and dropping the entire schema.

The asymmetry reflects the risk difference between the two directions. Running `migrate up` on an already-current database is a no-op — harmless. Running `migrate down` without a count could be catastrophic — it would roll back every migration ever applied, destroying all tables and data.

For operators, this means rollback commands must be deliberate: `go run main.go migrate down 1` to roll back exactly one migration, `go run main.go migrate down 3` for three. There is no "roll back everything" shortcut.

---

Relevant Notes:
- [[go:embed compiles SQL migrations into the binary at build time making migration files unavailable at runtime]] — the migration system architecture this CLI wraps
- [[multi-statement migration DDL requires StatementBegin and StatementEnd delimiters to prevent parser splitting on semicolons]] — another migration gotcha

Areas:
- [[entries/data-layer]]
