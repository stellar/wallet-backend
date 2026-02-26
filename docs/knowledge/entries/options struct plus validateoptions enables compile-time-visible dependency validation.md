---
description: Services with 5+ dependencies use Options struct with ValidateOptions; missing dependency is a visible struct field at construction site, not a runtime nil panic at first use
type: pattern
status: current
confidence: proven
subsystem: services
areas: [services, dependency-injection, options-pattern, go]
---

# Options struct plus ValidateOptions enables compile-time-visible dependency validation

## Context

Services with many dependencies can become error-prone when constructed with positional arguments. A missing dependency may only surface as a nil pointer panic at runtime, potentially far from the construction site.

## Detail

Services with 5+ dependencies use an `Options` struct:
```go
type TransactionServiceOptions struct {
    DB                                 db.ConnectionPool
    DistributionAccountSignatureClient signing.SignatureClient
    ChannelAccountSignatureClient      signing.SignatureClient
    ChannelAccountStore                store.ChannelAccountStore
    RPCService                         RPCService
    BaseFee                            int64
}
```

The constructor calls `opts.ValidateOptions()` before constructing the service. All nil-check and range-check errors surface at startup, not at first use.

Services with ≤4 deps use direct constructor parameters with inline validation (`NewRPCService`, `NewContractMetadataService`). `ContractValidator` takes no deps at all.

## Implications

- When adding a new dependency to a complex service, add it to the `Options` struct and add a nil-check in `ValidateOptions()`.
- Construction errors from `ValidateOptions()` are wrapped and bubbled up to `setupDeps` / `initHandlerDeps`, which exits the process — misconfiguration always fails fast.
- The pattern makes dependency requirements visible to callers at the call site: every required field in the struct is a documented dependency.

## Source

`internal/services/transaction_service.go:TransactionServiceOptions` — canonical example (lines 52–101)
`internal/services/fee_bump_service.go:FeeBumpServiceOptions` — Validate() variant
`internal/services/rpc_service.go:NewRPCService()` — direct params pattern

## Related

The Options structs receive `*data.Models` as a single field because [[data.models aggregates all database model interfaces into a single injectable struct]] — without that aggregation, an Options struct for TransactionService would need 11 model fields, making ValidateOptions boilerplate-heavy.

`ContractValidator` is the exception to this pattern because [[wazero pure-go wasm runtime validates sep-41 contract specs in-process by compiling wasm and parsing the contractspecv0 custom section]] — since wazero is a pure-Go runtime initialized entirely in-process, `ContractValidator` has no external dependencies to inject and uses `NewContractValidator()` with no arguments.

relevant_notes:
  - "[[data.models aggregates all database model interfaces into a single injectable struct]] — enables this: data.Models aggregation is what keeps Options structs readable; together the two patterns form the dependency injection strategy for complex services"
  - "[[wazero pure-go wasm runtime validates sep-41 contract specs in-process by compiling wasm and parsing the contractspecv0 custom section]] — explains exception: ContractValidator's no-dep design is explained by wazero being an in-process pure-Go runtime — no external dependencies means nothing to inject"
