---
description: Created once via NewModels(db, metricsService); passed by pointer to everything needing DB access; all 11 models receive both connection pool and metrics service automatically
type: pattern
status: current
confidence: proven
subsystem: services
areas: [data-layer, dependency-injection, models, services]
---

# data.Models aggregates all database model interfaces into a single injectable struct

## Context

With 11 data models spanning hypertables, balance tables, and metadata tables, passing each model individually to every service would create extremely large constructor signatures and make adding new models a multi-file refactoring exercise.

## Detail

`data.NewModels(db.ConnectionPool, metrics.MetricsService)` constructs all 11 models and returns them in a single `data.Models` struct:

```go
type Models struct {
    DB                   db.ConnectionPool
    Account              *AccountModel
    Contract             ContractModelInterface
    TrustlineAsset       TrustlineAssetModelInterface
    TrustlineBalance     TrustlineBalanceModelInterface
    NativeBalance        NativeBalanceModelInterface
    SACBalance           SACBalanceModelInterface
    AccountContractTokens AccountContractTokensModelInterface
    IngestStore          *IngestStoreModel
    Operations           *OperationModel
    Transactions         *TransactionModel
    StateChanges         *StateChangeModel
}
```

Services receive `*data.Models` or individual interface fields from it. Every model constructor receives both the connection pool and the metrics service — metrics are wired uniformly at the model layer.

## Implications

- Adding a new data model: add it to `Models`, construct it in `NewModels`, done. Existing services don't need updates.
- Interface-backed models (Contract, TrustlineAsset, balance models) are mockable in unit tests. Concrete models (Transactions, Operations, StateChanges) are tested against real TimescaleDB via `dbtest.Open`.
- `DB` (raw pool) is exposed on the struct for cases where services need to begin a transaction explicitly.

## Source

`internal/data/models.go:Models`, `NewModels()`

## Related

The struct exposes interface-backed and concrete models, following the split described in [[interface-backed models are for unit test mocking concrete models are tested against real timescaledb]] — the Models struct is the delivery vehicle for both categories; callers don't need to know which are interfaces and which are concrete.

Services that receive `*data.Models` also use [[options struct plus validateoptions enables compile-time-visible dependency validation]] — the Models struct replaces what would otherwise be 11 separate positional arguments, enabling the Options pattern to remain readable.

relevant_notes:
  - "[[interface-backed models are for unit test mocking concrete models are tested against real timescaledb]] — grounds this: the Models struct contains both mockable interfaces and concrete models; this entry is the assembly point for the testing split described there"
  - "[[options struct plus validateoptions enables compile-time-visible dependency validation]] — enables this: passing *data.Models as one field in an Options struct makes complex service constructors readable; without aggregation, Options structs would have 11+ model fields"
