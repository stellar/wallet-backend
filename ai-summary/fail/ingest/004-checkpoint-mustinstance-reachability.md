# H004: Checkpoint bootstrap panics on instance-key / non-instance-value contract data

**Date**: 2026-04-21
**Subsystem**: ingest
**Severity**: Medium
**Impact**: availability
**Hypothesis by**: gpt-5.4, high

## Expected Behavior

Checkpoint bootstrap should tolerate unexpected `ContractData` union combinations and either skip them or return an ordinary error, matching the safer `GetInstance()+ok` pattern already present in adjacent ingest code.

## Mechanism

`processContractInstanceChange` calls `contractDataEntry.Val.MustInstance()` after checking only that the key arm is `ScvLedgerKeyContractInstance`. If a checkpoint snapshot ever contains the same key arm paired with a different value arm, bootstrap would panic during initial population.

## Trigger

1. Start ingest from an empty DB so `PopulateFromCheckpoint` runs.
2. Feed it a checkpoint snapshot containing a `ContractData` entry whose key is `ScvLedgerKeyContractInstance` but whose value is not an instance.
3. Observe the `MustInstance()` panic in checkpoint processing.

## Target Code

- `internal/services/checkpoint.go:326-340` — routes instance-key entries into `processContractInstanceChange`
- `internal/services/checkpoint.go:502-545` — uses `Val.MustInstance()` without checking the union arm

## Evidence

The sibling `ProtocolContractsProcessor` already documents this exact malformed shape and switched to `GetInstance()+ok` to avoid panicking on it.

## Anti-Evidence

I could not tie this snapshot shape to a client-controlled, honest-upstream ledger path in the current trust model. Without proof that a network client can actually get such an entry into checkpoint state, the hypothesis remains speculative.

---

## Review

**Verdict**: NOT_VIABLE
**Date**: 2026-04-21
**Failed At**: hypothesis
**Novelty**: PASS — not previously investigated

### Why It Failed

The code smell is real, but reachability from attacker-controlled ledger input is unproven under the trusted-upstream model. I could not show how a normal network client can force this specific union mismatch into checkpoint state.

### Lesson Learned

Checkpoint-only panic candidates need both code evidence and a credible path from client-controlled ledger state into the checkpoint snapshot. A nearby defensive fix alone is not enough.
