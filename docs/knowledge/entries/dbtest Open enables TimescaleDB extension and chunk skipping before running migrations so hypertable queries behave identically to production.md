---
context: dbtest.OpenWithoutMigrations runs ALTER DATABASE ... SET timescaledb.enable_chunk_skipping = on before migration runner executes; ensures test hypertable queries can skip chunks just like production
type: insight
created: 2026-02-24
status: current
subsystem: data-layer
areas: [data-layer, testing]
---

# dbtest Open enables TimescaleDB extension and chunk skipping before running migrations so hypertable queries behave identically to production

Tests that exercise hypertable models require a real TimescaleDB instance — not just PostgreSQL. The `dbtest.Open(t)` function uses `testcontainers-go` to spin up a TimescaleDB container with all migrations applied. This is the correct infrastructure for testing queries that rely on TimescaleDB-specific behavior.

A subtle but critical detail: `timescaledb.enable_chunk_skipping` must be enabled before any hypertables are created. If chunk skipping is enabled after hypertable creation, existing chunks are not retroactively indexed for skipping. The test setup handles this via `OpenWithoutMigrations()` which:

1. Starts the TimescaleDB container
2. Runs `ALTER DATABASE test SET timescaledb.enable_chunk_skipping = on` at the database level
3. Only then runs the migration runner, which creates hypertables

This ordering ensures that the test environment's chunk-skipping behavior matches production, where the database-level setting is applied before any hypertables exist. Without this ordering, test queries might execute full scans even when production would skip chunks, making test performance a misleading proxy for production behavior.

`t.Cleanup(func() { container.Terminate() })` handles teardown automatically via testify's cleanup mechanism.

---

Relevant Notes:
- [[hypertable models lack interfaces because they require real TimescaleDB for testing while balance models use interfaces for mock-based unit tests]] — why hypertable models need this infrastructure
- [[one-day chunk interval balances Stellar ledger volume against chunk management overhead]] — the chunk configuration that chunk skipping optimizes

Areas:
- [[entries/data-layer]]
