---
context: In the Models struct, Transaction/Operation/StateChange model fields are concrete pointers (*TransactionModel etc); NativeBalance/TrustlineBalance/ContractBalance/IngestStore are interfaces tested via mocks
type: decision
created: 2026-02-24
---

# hypertable models lack interfaces because they require real TimescaleDB for testing while balance models use interfaces for mock-based unit tests

The `Models` struct in `internal/data/` groups all data model types. Its fields are split into two categories based on testability:

**Hypertable models** (Transaction, Operation, StateChange and their account join variants): stored as concrete `*Model` pointers, not interfaces. These models execute TimescaleDB-specific queries — chunk pruning, MATERIALIZED CTE patterns, decomposed cursor conditions. Mocking them would require replicating the TimescaleDB planner behavior, which is not feasible. Tests for code that uses hypertable models use `dbtest.Open(t)` to spin up a real TimescaleDB instance.

**Balance and metadata models** (NativeBalance, TrustlineBalance, ContractBalance, IngestStore): stored as interfaces. These models perform simpler CRUD operations on regular PostgreSQL tables — operations that can be faithfully mocked. This enables unit tests of services that touch balance state without spinning up a database.

The compile-time interface assertion `var _ BalanceModelInterface = (*NativeBalanceModel)(nil)` is placed in model files to catch interface drift at build time rather than at runtime.

This split is a deliberate testability design: make the boundary between "must-test-with-real-DB" and "can-mock" explicit in the type system. Code that depends on hypertable models implicitly requires integration-test infrastructure; code that depends on interfaces can be unit-tested cheaply.

---

Relevant Notes:
- [[dbtest Open enables TimescaleDB extension and chunk skipping before running migrations so hypertable queries behave identically to production]] — the test infrastructure that hypertable model tests rely on
- [[balance tables use regular PostgreSQL tables not hypertables because they store current state not time-series events]] — the architectural split that makes this interface pattern appropriate

Areas:
- [[entries/data-layer]]
