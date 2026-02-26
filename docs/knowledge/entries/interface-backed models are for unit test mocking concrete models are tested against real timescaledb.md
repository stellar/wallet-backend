---
description: Balance and metadata models implement interfaces for unit test mocks; TransactionModel, OperationModel, StateChangeModel use dbtest.Open because mocks can't validate TimescaleDB-specific queries
type: pattern
status: current
confidence: proven
subsystem: data-layer
areas: [data-layer, testing, timescaledb, mocking]
---

# Interface-backed models are for unit test mocking; concrete models are tested against real TimescaleDB

## Context

Some models need to be swapped for mocks in unit tests (injected into services under test). Other models use TimescaleDB-specific query constructs (decomposed cursors, `ROW_NUMBER PARTITION BY`, MATERIALIZED CTEs) that cannot be validated by a mock.

## Detail

The split follows a clear rule:
- **Interface-backed** (Contract, TrustlineAsset, TrustlineBalance, NativeBalance, SACBalance, AccountContractTokens): tested in services via mocks; their logic involves non-trivial business rules.
- **Concrete** (TransactionModel, OperationModel, StateChangeModel): tested against a real TimescaleDB instance via `dbtest.Open(t)` because their queries rely on TimescaleDB-specific features that mocks cannot verify.

`dbtest.Open(t)` creates an isolated test database, enables the TimescaleDB extension, sets `enable_chunk_skipping = on`, and runs all migrations before returning. This makes model tests integration-style but fully isolated between test functions.

## Implications

- If you add business logic to a concrete model, write the test using `dbtest.Open`, not a mock.
- If you change a service that depends on an interface-backed model, the mock must be updated to match the new interface.
- `dbtest.Open` requires Docker — use `make unit-test` (which skips integration tests) for fast iteration; use `make integration-test` for full coverage.

## Source

`internal/db/dbtest/dbtest.go:Open(t)` — test database helper
`internal/data/transactions.go`, `operations.go`, `statechanges.go` — concrete models with TimescaleDB queries

## Related

Both kinds of models are assembled into [[data.models aggregates all database model interfaces into a single injectable struct]] — the Models struct is the delivery mechanism; this entry explains the testing strategy that motivates the interface/concrete split within it.

The concrete model tests require `dbtest.Open`, which sets up [[timescaledb chunk skipping must be explicitly enabled per column for sparse index scans to work]] at the session level — this is why concrete model tests validate TimescaleDB-specific query behavior that mocks cannot simulate.

relevant_notes:
  - "[[data.models aggregates all database model interfaces into a single injectable struct]] — grounds this: the Models struct aggregates both interface-backed and concrete models; this entry explains why the split exists within that struct"
  - "[[timescaledb chunk skipping must be explicitly enabled per column for sparse index scans to work]] — extends this: dbtest.Open enables chunk skipping for concrete model tests, which is why those tests catch TimescaleDB-specific query regressions that mocks miss"
