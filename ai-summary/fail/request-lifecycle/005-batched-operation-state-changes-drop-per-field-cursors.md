# H005: aliased Operation.stateChanges batches ignore each field's cursor

**Date**: 2026-04-21
**Subsystem**: request-lifecycle
**Severity**: Medium
**Impact**: Integrity / cursor-confused nested pagination can replay first-page state changes and hide later ones
**Hypothesis by**: gpt-5.4, high

## Expected Behavior

If a GraphQL query asks for `stateChanges(first: 2, after: "<cursor>")` on two different operations in the same response, each operation's field should apply its own cursor and return the next page for that specific operation. Batched execution should preserve per-field cursor semantics even when the loads are coalesced.

## Mechanism

`operationResolver.StateChanges` includes the caller-supplied cursor in `StateChangeColumnsKey`, but the dataloader's multi-key path ignores every key's `Cursor` and calls `BatchGetByOperationIDs` with only `(operationIDs, columns, limit, sortOrder)`. Two aliased root lookups can therefore batch together and both receive first-page state changes again, even though each field supplied a later-page cursor, which lets an attacker or malicious dataset keep later state changes out of a client workflow that trusts nested pagination.

## Trigger

1. Pick two operations that each have at least three state changes and obtain a valid `after` cursor for page 2 of each operation.
2. Send a single query such as:
   `query { a: operationById(id: 101) { stateChanges(first: 2, after: "<cursor-a>") { edges { node { type reason } } pageInfo { endCursor } } } b: operationById(id: 202) { stateChanges(first: 2, after: "<cursor-b>") { edges { node { type reason } } pageInfo { endCursor } } } }`
3. Observe that both aliased fields can return rows from the start of each operation instead of honoring the supplied page-2 cursors because the batched loader never forwards those cursors to SQL.

## Target Code

- `internal/serve/graphql/schema/queries.graphqls:3-10` — exposes `operationById`, which can be aliased to force multiple `Operation.stateChanges` loads in one request
- `internal/serve/graphql/resolvers/operation.resolvers.go:StateChanges:74-113` — captures a per-field cursor in `StateChangeColumnsKey`
- `internal/serve/graphql/dataloaders/statechange_loaders.go:stateChangesByOperationIDLoader:71-94` — ignores `keys[i].Cursor` when `len(keys) > 1`
- `internal/data/statechanges.go:BatchGetByOperationID:429-485` — single-operation path supports decomposed cursor pagination
- `internal/data/statechanges.go:BatchGetByOperationIDs:488-537` — batched path has no cursor parameter at all

## Evidence

The single-operation SQL path explicitly expands the supplied cursor into a decomposed `AND (...)` clause, but the multi-operation SQL path cannot do that because `BatchGetByOperationIDs` receives no cursor input. Since dataloadgen batches requests across the request with a 5 ms window, aliased `operationById` fields can land in the same batch and silently lose their per-field cursor boundaries.

## Anti-Evidence

If only one operation's `stateChanges` field is resolved in the request, the loader uses `BatchGetByOperationID` and pagination behaves correctly. The issue depends on coalescing multiple `Operation.stateChanges` loads into one dataloader batch.

---

## Review

**Verdict**: NOT_VIABLE
**Date**: 2026-04-23
**Reviewed by**: claude-opus-4-6, high
**Novelty**: PASS — not previously investigated (related to H003 but targets a different loader/entity)
**Failed At**: reviewer

### Trace Summary

Traced the full execution path from `operationResolver.StateChanges` (line 74) through the `StateChangeColumnsKey` construction (including `Cursor` at line 87), into `stateChangesByOperationIDLoader` (line 71), and through both the single-key path (`BatchGetByOperationID`, line 86, which supports cursor pagination) and the multi-key path (`BatchGetByOperationIDs`, line 93, which drops cursors entirely). Confirmed the bug is real: when multiple `Operation.stateChanges` fields batch together via dataloadgen's 5ms window, the per-field cursor is silently dropped and both fields receive first-page results. Also confirmed `BatchGetByOperationIDs` (line 488-537 in statechanges.go) has no cursor parameter — it uses only `ROW_NUMBER() OVER (PARTITION BY operation_id)` with a limit.

### Code Paths Examined

- `internal/serve/graphql/resolvers/operation.resolvers.go:StateChanges:74-113` — confirmed `StateChangeColumnsKey` includes `Cursor: params.StateChangeCursor` (line 87), but this cursor is only consumed by the single-key path
- `internal/serve/graphql/dataloaders/statechange_loaders.go:stateChangesByOperationIDLoader:71-105` — confirmed `len(keys) > 1` triggers multi-key path (line 88-93) which passes only `operationIDs, columns, limit, sortOrder` — NO cursor
- `internal/data/statechanges.go:BatchGetByOperationID:429-486` — confirmed single-key path supports decomposed cursor pagination via `buildDecomposedCursorCondition` (lines 444-453)
- `internal/data/statechanges.go:BatchGetByOperationIDs:488-537` — confirmed batched path has no cursor parameter; uses `ROW_NUMBER() OVER (PARTITION BY operation_id)` with only a limit cap
- `internal/serve/graphql/resolvers/utils.go:NewConnectionWithRelayPagination` — confirmed relay pagination function compares `len(nodes)` against `params.Limit` to compute `hasNextPage`

### Why It Failed

The bug is confirmed real — batched state-change queries via operation ID silently drop per-field cursors and return first-page results for all batched operations. However, the impact cannot be argued up to Medium severity per the objective's severity scale:

1. **Not auth bypass** — no authentication or authorization boundary is crossed.
2. **Not balance-integrity loss** — this affects state changes (public blockchain operation metadata), not account balances or token attribution. The `tokenIngestionService` and balance cache paths are entirely unaffected.
3. **Not persistent state corruption** — the database is correct; only the API response is incorrect when batching occurs.
4. **Not a crash/panic** — no denial of service.
5. **Not cache desync** — Redis/DB consistency is unaffected.
6. **The data is public** — all Stellar state changes are publicly visible on-chain; a client can query individual operations for correct paginated results via the single-key path.
7. **Same class of bug as H003** — H003 documented the identical pattern for operations batched by transaction ID (truncation + cursor drop in `operationsByToIDLoader`). That finding was also marked NOT_VIABLE for identical severity reasons.
8. **Intentional design trade-off** — the `TODO: this should be configurable via config` comment on `MaxStateChangesPerBatch` (line 14) and the code comments documenting the single-key vs multi-key split confirm this is a known limitation of the batching strategy.

The worst-case impact (incorrect pagination of public blockchain state changes in nested GraphQL views) maps to Low/Informational severity, which is below the minimum Medium filing threshold for this security scan.

### Lesson Learned

DataLoader batching cursor-drop bugs are a systematic pattern across the wallet-backend's one-to-many loaders (operations-by-toID, state-changes-by-toID, state-changes-by-operationID). While they are real correctness bugs, they consistently fail to reach Medium severity in this threat model because: (a) the underlying data is public blockchain data with no confidentiality boundary, (b) the database state is unaffected, (c) the single-key query path provides a correct alternative, and (d) the batching trade-off is documented as intentional. Future hypotheses about dataloader cursor/limit handling in this codebase should be rejected early unless they can demonstrate balance-integrity corruption or persistent state divergence.
