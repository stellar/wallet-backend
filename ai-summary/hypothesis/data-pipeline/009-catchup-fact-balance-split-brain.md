# H009: Catchup Backfill Publishes History Before Balances Catch Up

**Date**: 2026-04-23
**Subsystem**: data-pipeline
**Severity**: Medium
**Impact**: Integrity
**Hypothesis by**: gpt-5.4, high

## Expected Behavior

When optimized catchup is running, callers should never be able to observe transaction/state-change history from a ledger before the corresponding trustline/native/SAC balance tables reflect the same ledger. History and balance views should advance atomically from the perspective of concurrent API readers.

## Mechanism

Catchup batches periodically call `flushBatchBufferWithRetry`, which immediately commits transactions, operations, and state changes to PostgreSQL, but only stores token deltas in in-memory `BatchChanges`. The actual balance-table updates are deferred until `processBatchChanges` runs once after all parallel batches complete, so concurrent GraphQL readers can observe newer history rows from the fact tables while balance reads still come from pre-catchup cache tables.

## Trigger

Let the ingest service fall behind until it enters optimized catchup, then create or monitor a transfer affecting a target account during the catchup window. While catchup is still running, query `account.transactions` or `account.stateChanges` alongside `account.balances`; the history side should show the newer ledger while balances still reflect the older pre-catchup state.

## Target Code

- `internal/services/ingest_backfill.go:204-256` — catchup mode defers `processBatchChanges` and latest-cursor update until after all batches finish
- `internal/services/ingest_backfill.go:426-475` — periodic batch flush commits fact tables immediately
- `internal/services/ingest_backfill.go:558-575` — intermediate flushes happen before the final catchup merge
- `internal/services/ingest_backfill.go:601-659` — token/balance tables are updated only in the final merged transaction
- `internal/serve/graphql/resolvers/account.resolvers.go:34-49,108-154` — account history queries read directly from fact tables
- `internal/serve/graphql/resolvers/balance_reader.go:31-58` — balances come from the separate balance tables

## Evidence

`flushBatchBufferWithRetry` calls `insertIntoDB` even when `batchChanges` is non-nil, and `insertIntoDB` does not touch the balance models. The only place catchup calls `ProcessTokenChanges` is `processBatchChanges`, which runs later after all parallel batch work has already committed the history rows.

## Anti-Evidence

The inconsistency window closes once the final merged catchup transaction succeeds, so this is not necessarily persistent corruption. It also requires a separate API reader process (or any concurrent DB reader) to observe the shared database while catchup is still in progress.
