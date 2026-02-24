---
context: go:embed //go:embed migrations/*.sql embeds the migrations/ directory into the binary; rubenv/sql-migrate uses HttpFileSystemMigrationSource to read from the embedded FS
type: insight
created: 2026-02-24
---

# go:embed compiles SQL migrations into the binary at build time making migration files unavailable at runtime

Migration SQL files live in `internal/db/migrations/`. They are embedded into the compiled binary at build time using Go's `//go:embed` directive. The embedded filesystem is passed to `rubenv/sql-migrate` via `HttpFileSystemMigrationSource`, which reads migration files from the embedded FS rather than from disk.

The implication: at runtime, there is no external dependency on SQL files being present on the filesystem. The binary is self-contained — deploying it to production or a container requires no additional file staging, and there is no risk of a migration file being missing or modified after deployment.

The flip side: if a migration SQL file needs to be changed after a binary is built, the binary must be rebuilt. You cannot patch a migration by editing a file on a running server.

This also means the migration content is fixed for a given binary version — the migration set applied by `migrate up` is determined at build time, not at deploy time. This is the desired behavior for reproducible deployments.

For development: `make migrate up` uses the locally compiled binary's embedded migrations. Editing a migration file requires recompiling before the change takes effect.

---

Relevant Notes:
- [[migrate down requires explicit count to prevent accidental full rollback while migrate up treats zero as apply-all]] — the asymmetric CLI behavior for migration direction
- [[multi-statement migration DDL requires StatementBegin and StatementEnd delimiters to prevent parser splitting on semicolons]] — a gotcha in the migration format

Areas:
- [[entries/data-layer]]
