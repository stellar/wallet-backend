---
description: Build() generates 18-field SortKey; processTransaction sorts and assigns 1-based StateChangeOrder per operation; deterministic ordering required because PK includes state_change_order
type: insight
status: current
confidence: proven
subsystem: ingestion
areas: [ingestion, state-changes, indexer, determinism, primary-key]
---

# State change ordering uses SortKey for deterministic re-ingestion of the same ledger

## Context

Re-ingesting the same ledger (after a crash or re-processing) must produce identical `StateChangeOrder` values. The composite primary key `(to_id, operation_id, state_change_order, ledger_created_at)` would be violated if ordering were non-deterministic across runs.

## Detail

`StateChangeBuilder.Build()` calls `generateSortKey()` to produce an 18-field deterministic string encoding all meaningful state change fields:

```
{toID}:{category}:{reason}:{accountID}:{tokenID}:{amount}:{signerID}:{...etc}
```

In `processTransaction()`, all state changes from all 4 processors are collected, then `sort.Slice` sorts them by SortKey lexicographically. The sorted position determines `StateChangeOrder` (1-based, per-operation):
- Fee state changes (OperationID == 0) always get `StateChangeOrder = 1`.
- Operation-level changes use a per-operation counter `perOpIdx[operationID]++`.

The SortKey is never persisted to the database — it's only used for this sorting step.

## Implications

- The SortKey's 18 fields encode enough content to distinguish all valid state changes that could arise from a single transaction, even if two state changes have the same category/reason/account.
- Never add non-deterministic fields to the SortKey (timestamps, random IDs). The sort must produce the same order regardless of processor execution order or Go map iteration order.
- Processors run in an unspecified order within the goroutine pool — the SortKey-based sort unifies their output into a stable sequence.

## Source

`internal/indexer/processors/state_change_builder.go:generateSortKey()`
`internal/indexer/indexer.go:processTransaction()` (lines 264–280)

## Related

The SortKey encodes fields from [[state changes use a two-axis category-reason taxonomy to classify every account history event]] — category and reason are among the 18 SortKey fields — which is what makes the sort stable across all valid event combinations.

relevant_notes:
  - "[[state changes use a two-axis category-reason taxonomy to classify every account history event]] — grounds this entry: the SortKey's 18 fields include category and reason from the taxonomy, making state change ordering taxonomy-aware"
