# H007: StateChange.operation Aliases Reuse the First Projection

**Date**: 2026-04-23
**Subsystem**: data-pipeline
**Severity**: Medium
**Impact**: Integrity
**Hypothesis by**: gpt-5.4, high

## Expected Behavior

Two aliased `operation` selections on the same `BaseStateChange` should both resolve against the real operation row while respecting their own requested fields. Asking for `id` under one alias must not cause a sibling alias requesting `operationXdr` or `resultCode` to receive zero values.

## Mechanism

`resolveStateChangeOperation` puts the alias-local DB column set into the loader key, but `operationByStateChangeIDLoader` fetches all batched keys with `keys[0].Columns`. `newOneToOneLoader` then maps the loaded `Operation` only by the synthetic `stateChangeID`, so both aliases for the same state change get the same sparse struct shaped by the first alias rather than their own selection.

## Trigger

Query a state change's parent operation twice in one request with different aliases and different sub-selections, for example: `stateChanges(first: 1) { edges { node { opLite: operation { id } opFull: operation { operationXdr resultCode } } } }`. When `opLite` is processed first, `opFull` should expose empty or incorrect operation data.

## Target Code

- `internal/serve/graphql/resolvers/resolver.go:124-139` — builds the `StateChangeID`/`Columns` loader key from the alias-local selection set
- `internal/serve/graphql/dataloaders/operation_loaders.go:77-99` — state-change batch path uses only `keys[0].Columns`
- `internal/serve/graphql/dataloaders/loaders.go:156-166` — one-to-one loader reuses one fetched row per synthetic `stateChangeID`
- `internal/data/operations.go:295-323` — sparse projection query for operations by state-change tuple

## Evidence

The dataloader key intentionally carries `Columns`, proving the resolver expects projection-sensitive batching. The batch function discards that distinction, and the generic loader returns a single fetched struct to every matching key regardless of which alias asked for which operation fields.

## Anti-Evidence

Single-alias requests are safe, and matching alias projections would hide the problem. The issue also requires the duplicated aliased field loads to coalesce into the same dataloader batch.
