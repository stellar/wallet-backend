# H003: Unknown Soroban enum values panic operation-detail generation and crash ingest

**Date**: 2026-04-21
**Subsystem**: ingest
**Severity**: Medium
**Impact**: availability
**Hypothesis by**: gpt-5.4, high

## Expected Behavior

Unknown or newly introduced Soroban operation/host-function enum values should be surfaced as ordinary errors or ignored fields, not trigger a process panic during ingest.

## Mechanism

`TransactionOperationWrapper.Details()` still panics on unknown operation, host-function, or contract-preimage enum arms. If ingest used this serializer in production, a future valid ledger enum could crash the process instead of producing a degraded-but-safe record.

## Trigger

1. Feed ingest a Soroban operation whose enum arm is not handled by `Details()`.
2. Ensure the ingest pipeline calls `TransactionOperationWrapper.Details()`.
3. Observe the `panic(...)` on the default switch branch.

## Target Code

- `internal/indexer/processors/transaction_operation_wrapper.go:164-355` — panics on unknown operation/host-function type
- `internal/indexer/processors/transaction_operation_wrapper.go:644-660` — panics on unknown contract-id preimage type

## Evidence

The serializer has explicit `panic(fmt.Errorf(...))` defaults rather than returning an error for unknown enum arms.

## Anti-Evidence

There are no in-tree production call sites for `TransactionOperationWrapper.Details()` on main. The ingest persistence path stores raw operation XDR and never invokes this serializer, so the panic path is currently dead code.

---

## Review

**Verdict**: NOT_VIABLE
**Date**: 2026-04-21
**Failed At**: hypothesis
**Novelty**: PASS — not previously investigated

### Why It Failed

The panic branches are real, but the method is not exercised by any production ingest path on main. Without a reachable caller, this is a dormant code smell rather than an exploitable ingest crash.

### Lesson Learned

Enum-panic scans need a call-graph check. Several scary `panic` sites in the indexer live behind unused helper methods and are out of scope until a production caller appears.
