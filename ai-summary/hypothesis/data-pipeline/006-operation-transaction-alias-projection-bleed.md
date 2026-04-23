# H006: Operation.transaction Aliases Reuse the First Projection

**Date**: 2026-04-23
**Subsystem**: data-pipeline
**Severity**: Medium
**Impact**: Integrity
**Hypothesis by**: gpt-5.4, high

## Expected Behavior

Two aliased `transaction` selections on the same `Operation` should each be resolved from the same canonical transaction row while honoring their own field projections. If one alias asks only for `ledgerNumber` and another alias asks for `hash`, the second alias should still receive the real transaction hash, not a zero-value placeholder.

## Mechanism

The resolver puts the requested DB columns into the dataloader key, but `transactionByOperationIDLoader` ignores every key except `keys[0].Columns` when a batch contains multiple loads. `newOneToOneLoader` then re-associates the fetched row only by `OperationID`, so every alias for that operation receives the same partially-populated `types.Transaction` built from whichever projection was batched first.

## Trigger

Issue one GraphQL request that asks for the same operation's transaction twice with different aliases and different sub-selections, for example: `operations(first: 1) { edges { node { txLite: transaction { ledgerNumber } txFull: transaction { hash } } } }`. If `txLite` is batched first, `txFull.hash` should come back empty or incorrect even though the backing row has a real hash.

## Target Code

- `internal/serve/graphql/resolvers/operation.resolvers.go:28-45` — builds a loader key whose `Columns` depend on the alias-local GraphQL selection set
- `internal/serve/graphql/dataloaders/transaction_loaders.go:23-31` — multi-key batch path uses `keys[0].Columns` for every key
- `internal/serve/graphql/dataloaders/loaders.go:156-166` — one-to-one loader groups results only by `OperationID`
- `internal/data/transactions.go:181-204` — dynamic projection query returns sparse `Transaction` rows based on the supplied `columns`

## Evidence

`GetDBColumnsForFields` derives different column lists per resolver invocation, so aliased fields can legitimately produce distinct `Columns` values in the same request. The transaction loader's batch function drops per-key projections, and the generic one-to-one loader returns the fetched struct to every key sharing the same parent primary key.

## Anti-Evidence

If only one alias is present, or both aliases request the same transaction fields, the bug does not manifest. This also depends on the loads batching together within the dataloader wait window.
