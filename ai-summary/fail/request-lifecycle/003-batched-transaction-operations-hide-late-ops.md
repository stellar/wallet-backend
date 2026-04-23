# H003: batched Transaction.operations silently hides operations after the tenth child

**Date**: 2026-04-21
**Subsystem**: request-lifecycle
**Severity**: Medium
**Impact**: Integrity / incomplete transaction rendering can hide malicious operations in nested GraphQL views
**Hypothesis by**: gpt-5.4, high

## Expected Behavior

If a client asks for `operations(first: N)` on multiple transactions in one GraphQL response, each transaction should still honor the requested page size and return a correct `PageInfo` that signals whether more operations exist. A transaction with more than 10 operations should not be silently truncated to 10 while reporting that no further page exists.

## Mechanism

`transactionResolver.Operations` computes `queryLimit = first + 1` so Relay pagination can determine `hasNextPage`, but the multi-parent dataloader path clamps that value to `min(limit, 10)` and drops the per-transaction cursor entirely. When a query batches more than one transaction, a transaction with more than 10 operations can be rendered as if it were complete even though later operations were never fetched, letting a malicious transaction bury sensitive operations beyond the tenth child in a view the client believes is exhaustive.

## Trigger

1. Ensure the indexed data contains at least two transactions in the requested page, and one of them has more than 10 operations.
2. Send `query { transactions(first: 2) { edges { node { hash operations(first: 50) { edges { node { id operationType } } pageInfo { hasNextPage endCursor } } } } } }`.
3. Observe that the high-fanout transaction returns only 10 operations because the loader's multi-key path capped the per-parent query, and `pageInfo` can under-report the existence of later operations because the resolver expected `first + 1` rows but never received them.

## Target Code

- `internal/serve/graphql/resolvers/transaction.resolvers.go:Operations:26-63` — requests `first + 1` rows for Relay pagination
- `internal/serve/graphql/dataloaders/operation_loaders.go:operationsByToIDLoader:39-58` — switches to a multi-key path when more than one transaction is present, clamps the limit to `MaxOperationsPerBatch = 10`, and drops `Cursor`
- `internal/data/operations.go:BatchGetByToIDs:115-161` — enforces the reduced per-transaction row cap via `ROW_NUMBER() ... WHERE rn <= limit`
- `internal/data/operations_test.go:320-380` — confirms the per-parent `ROW_NUMBER` limiting behavior

## Evidence

The resolver computes `queryLimit := *params.Limit + 1`, but `operationsByToIDLoader` replaces that with `maxLimit := min(*limit, MaxOperationsPerBatch)` whenever `len(keys) > 1`. The data-layer query then applies `WHERE rn <= <limit>` per `to_id`, so a request for `first: 50` across multiple parent transactions fetches at most 10 rows per transaction even though the GraphQL connection logic still assumes it asked for 51.

## Anti-Evidence

The truncation is intentional for batched performance, so this is not a random edge case. Single-transaction queries use `BatchGetByToID`, which honors the real cursor and limit, so the integrity issue depends on getting multiple parent transactions into the same GraphQL batch.

---

## Review

**Verdict**: NOT_VIABLE
**Date**: 2026-04-21
**Reviewed by**: claude-opus-4-6, high
**Novelty**: PASS — not previously investigated
**Failed At**: reviewer

### Trace Summary

Traced the full execution path from `transactionResolver.Operations` through the dataloader batching logic into `BatchGetByToIDs` and back through `NewConnectionWithRelayPagination`. Confirmed the bug is real: when multiple transactions batch together, operations are clamped to 10 per parent and `hasNextPage` incorrectly reports `false` because the relay pagination function compares `len(nodes)` (≤10) against the original `params.Limit` (e.g. 50). However, the impact does not reach the minimum Medium severity threshold for this security scan.

### Code Paths Examined

- `internal/serve/graphql/resolvers/transaction.resolvers.go:Operations:26-63` — confirmed `queryLimit = *params.Limit + 1` passed to loader, `params.Limit` (original) passed to `NewConnectionWithRelayPagination`
- `internal/serve/graphql/dataloaders/operation_loaders.go:40-58` — confirmed `len(keys) > 1` triggers `maxLimit = min(*limit, MaxOperationsPerBatch=10)`, cursor dropped
- `internal/data/operations.go:BatchGetByToIDs:115-162` — confirmed `ROW_NUMBER() OVER (PARTITION BY) ... WHERE rn <= limit` caps at 10 per parent
- `internal/serve/graphql/resolvers/utils.go:NewConnectionWithRelayPagination:62-108` — confirmed `hasNextPage = int32(len(nodes)) > *params.Limit` evaluates to `10 > 50 = false`

### Why It Failed

The bug is confirmed real — batched operation queries silently truncate to 10 results per transaction with incorrect `hasNextPage: false`. However, the impact cannot be argued up to Medium severity per the objective's severity scale:

1. **Not auth bypass** — no authentication or authorization boundary is crossed.
2. **Not balance-integrity loss** — this affects operations (public blockchain data), not account balances or token attribution.
3. **Not persistent state corruption** — the database is correct; only the API response is incomplete.
4. **Not a crash/panic** — no denial of service from unauthenticated input.
5. **Not cache desync** — Redis/DB consistency is unaffected.
6. **The data is public** — all Stellar operations are publicly visible on-chain; a client can query individual transactions for full results via the single-key path.
7. **Intentional design trade-off** — the code has a `TODO: this should be configurable via config` comment on `MaxOperationsPerBatch`, and the anti-evidence section of the hypothesis itself acknowledges this.

The worst-case impact (incomplete rendering of public blockchain operations in nested GraphQL views) maps to Low/Informational severity, which is below the minimum Medium filing threshold for this security scan.

### Lesson Learned

API pagination accuracy bugs in read-only public-data indexers are real correctness issues but do not constitute security vulnerabilities when: (a) the underlying data has no confidentiality boundary, (b) the database state is unaffected, (c) alternative query paths exist to retrieve complete data, and (d) the truncation is a documented intentional design trade-off. When assessing "hidden data" claims, verify whether the data carries any trust or confidentiality requirement per the threat model.
