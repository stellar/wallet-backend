# H008: Transaction.operations Aliases Collapse to One Page Definition

**Date**: 2026-04-23
**Subsystem**: data-pipeline
**Severity**: Medium
**Impact**: Integrity
**Hypothesis by**: gpt-5.4, high

## Expected Behavior

Two aliased `operations` connections on the same `Transaction` should honor their own `first`/`last`/cursor arguments and return distinct pages. A caller asking for `ops1: operations(first: 1)` and `ops2: operations(first: 25, after: "...")` should not get the same edge list for both aliases.

## Mechanism

The resolver includes `Limit`, `Cursor`, `SortOrder`, and `Columns` in the loader key, but the multi-key branch in `operationsByToIDLoader` reads only `keys[0]` and ignores the rest. `newOneToManyLoader` then groups results solely by `ToID`, so all aliased loads for that transaction reuse the same slice even when their page boundaries differ.

## Trigger

Issue a single GraphQL request such as `transactionByHash(...) { ops1: operations(first: 1) { edges { node { id } } } ops2: operations(first: 25, after: "<cursor>") { edges { node { id resultCode } } } }`. Both aliases should come back with the same page, determined by whichever loader key was first in the batch.

## Target Code

- `internal/serve/graphql/resolvers/transaction.resolvers.go:26-50` — puts per-alias pagination and projection values into the loader key
- `internal/serve/graphql/dataloaders/operation_loaders.go:41-58` — multi-key path uses only `keys[0].Columns`, `keys[0].Limit`, and `keys[0].SortOrder`
- `internal/serve/graphql/dataloaders/loaders.go:107-118` — one-to-many loader returns one grouped slice per `ToID`, not per full key
- `internal/data/operations.go:115-161` — batched multi-parent query applies a single per-parent limit
- `internal/data/operations.go:166-210` — single-parent query is the only path that respects cursor pagination

## Evidence

The code explicitly falls back to a separate single-key query because only that path can honor a cursor; once the batch has multiple keys, cursor and limit individuality are discarded. Because the generic loader groups by transaction `ToID` only, different aliased keys for the same transaction cannot remain distinct after fetch.

## Anti-Evidence

If only one `operations` field is requested, or if every alias uses identical pagination and projections, the results look correct. Different parent transactions are also less interesting here; the corruption is strongest when the same `ToID` appears multiple times in one batch.
