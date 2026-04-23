# H004: batched Transaction.stateChanges truncates per-transaction evidence at ten rows

**Date**: 2026-04-21
**Subsystem**: request-lifecycle
**Severity**: Medium
**Impact**: Integrity / nested state-change views can omit later account mutations while appearing complete
**Hypothesis by**: gpt-5.4, high

## Expected Behavior

Nested `Transaction.stateChanges(first: N)` queries should honor the requested page size for every transaction in the response, and `PageInfo` should reliably indicate whether additional state changes exist. A transaction with dozens of state changes should not collapse to 10 rows merely because another transaction was fetched in the same GraphQL request.

## Mechanism

The transaction state-change resolver also asks for `first + 1` rows, but the multi-parent loader path rewrites that to `min(limit, 10)` and stops carrying the per-transaction cursor. The downstream SQL uses `ROW_NUMBER() OVER (PARTITION BY sc.to_id)` with the reduced limit, so later state changes for a high-fanout transaction disappear from the nested GraphQL connection even though the client asked for a much larger page and will build `PageInfo` from an artificially shortened slice.

## Trigger

1. Ensure the first page of `transactions(first: 2)` contains a transaction with more than 10 indexed state changes.
2. Send `query { transactions(first: 2) { edges { node { hash stateChanges(first: 50) { edges { node { account { address } type reason } } pageInfo { hasNextPage endCursor } } } } } }`.
3. Observe that the busy transaction returns at most 10 state changes when another transaction is present in the same response, allowing later mutations to be hidden from the client.

## Target Code

- `internal/serve/graphql/resolvers/transaction.resolvers.go:StateChanges:90-128` — requests `first + 1` rows for nested transaction state changes
- `internal/serve/graphql/dataloaders/statechange_loaders.go:stateChangesByToIDLoader:31-55` — multi-key path caps the requested limit at `MaxStateChangesPerBatch = 10` and omits cursor handling
- `internal/data/statechanges.go:BatchGetByToIDs:379-427` — applies `ROW_NUMBER() OVER (PARTITION BY sc.to_id)` and `WHERE rn <= limit`
- `internal/data/statechanges_test.go:609-614` — demonstrates the per-`to_id` truncation behavior

## Evidence

The resolver's `queryLimit := *params.Limit + 1` is supposed to support Relay pagination, but `stateChangesByToIDLoader` substitutes `maxLimit := min(*limit, MaxStateChangesPerBatch)` whenever more than one transaction is batched. The SQL query then hard-limits each `to_id` partition with `WHERE rn <= <limit>`, so a client asking for 50 rows gets at most 10 rows per transaction without the connection layer knowing the fetch was truncated early.

## Anti-Evidence

Single-transaction requests use `BatchGetByToID`, which does accept a cursor and the real requested limit. The vulnerability therefore requires a multi-parent GraphQL batch, not a lone `transactionByHash { stateChanges(...) }` request.

---

## Review

**Verdict**: NOT_VIABLE
**Date**: 2026-04-23
**Reviewed by**: claude-opus-4-6, high
**Novelty**: FAIL — duplicate of H003 (003-batched-transaction-operations-hide-late-ops.md)
**Failed At**: reviewer

### Trace Summary

Traced the execution path from `transactionResolver.StateChanges` (line 90) through `StateChangesByToIDLoader.Load` into the `stateChangesByToIDLoader` fetcher (statechange_loaders.go:31-65), confirming the multi-key path at line 50 applies `maxLimit := min(*limit, MaxStateChangesPerBatch)` (capped at 10) and drops the cursor. The relay pagination function at `utils.go:69` then checks `int32(len(nodes)) > *params.Limit`, which evaluates to `10 > 50 = false`, incorrectly reporting `hasNextPage: false`. This is structurally identical to the operations truncation in H003.

### Code Paths Examined

- `internal/serve/graphql/resolvers/transaction.resolvers.go:StateChanges:90-128` — confirmed `queryLimit = *params.Limit + 1` passed to loader key, original `params` passed to `NewConnectionWithRelayPagination`
- `internal/serve/graphql/dataloaders/statechange_loaders.go:31-65` — confirmed `len(keys) > 1` triggers `maxLimit = min(*limit, MaxStateChangesPerBatch=10)`, cursor dropped at line 54 (not passed to `BatchGetByToIDs`)
- `internal/serve/graphql/resolvers/utils.go:NewConnectionWithRelayPagination:62-108` — confirmed `hasNextPage = int32(len(nodes)) > *params.Limit` evaluates to `false` when loader returns ≤10 nodes against a limit of 50

### Why It Failed

This is a **duplicate of H003** (003-batched-transaction-operations-hide-late-ops.md). H003 covered the identical bug pattern — multi-parent dataloader batching truncation with incorrect `hasNextPage` — for `Transaction.operations`. H004 applies the same pattern to `Transaction.stateChanges`. Both use the same architectural mechanism (`MaxXxxPerBatch = 10`, cursor drop, `ROW_NUMBER()` partition, relay pagination mismatch), and the severity assessment is identical.

Even setting aside the duplicate status, the impact does not reach Medium severity per the objective's severity scale:

1. **Not auth bypass** — no authentication or authorization boundary is crossed.
2. **Not balance-integrity loss** — state changes are public blockchain event records, not account balances or token attribution.
3. **Not persistent state corruption** — the database is correct; only the nested API response is incomplete.
4. **Not a crash/panic** — no denial of service.
5. **Not cache desync** — Redis/DB consistency is unaffected.
6. **The data is public** — all Stellar state changes are publicly visible on-chain; clients can query individual transactions via the single-key path for complete results.
7. **Intentional design trade-off** — the code has a `TODO: this should be configurable via config` comment on `MaxStateChangesPerBatch`.

### Lesson Learned

When a hypothesis restates an already-investigated pattern (multi-parent dataloader truncation) for a different child type (state changes vs operations), it is a duplicate regardless of which field is affected, because the mechanism, severity assessment, and mitigation are identical. The `TODO` comments on both `MaxOperationsPerBatch` and `MaxStateChangesPerBatch` confirm the truncation is a known intentional trade-off.
