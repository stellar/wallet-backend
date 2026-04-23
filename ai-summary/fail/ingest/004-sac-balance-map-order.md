# H004: Live SAC balance ingestion assumes ScMap order and can misparse or panic on reordered balance values

**Date**: 2026-04-21
**Subsystem**: ingest
**Severity**: High
**Impact**: balance integrity / availability
**Hypothesis by**: gpt-5.4, high

## Expected Behavior

SAC balance ingestion should read `amount`, `authorized`, and `clawback` by key name and should not depend on the serialized order of `ScMap` entries. Live ingestion and checkpoint ingestion should parse the same on-ledger balance value identically.

## Mechanism

After `sac.ContractBalanceFromContractData` returns success, the live path dereferences `val.MustMap()` and assumes `entries[0]` is `amount` and `entries[1:2]` are booleans. A valid SAC-like map serialized in another order can make `MustI128` / `MustB` read the wrong union arm and panic, or can silently swap persisted balance/authorization fields; the checkpoint path does not share this assumption because it scans the map by symbol key.

## Trigger

1. Produce a contract-data balance value whose keys are ordered differently from `[amount, authorized, clawback]`, such as `[authorized, amount, clawback]`.
2. Let live ingest process that ledger update.
3. Observe either a panic during `MustI128` / `MustB` or incorrect persisted `balance`, `is_authorized`, or `is_clawback_enabled` values.

## Target Code

- `internal/indexer/processors/sac_balances.go:96-145` — validates with SDK, then extracts fields positionally
- `internal/services/token_ingestion.go:139-170` — persists the extracted SAC fields directly
- `internal/services/checkpoint.go:619-655` — checkpoint counterpart parses the same value by key name

## Evidence

`extractSACBalanceFields` does `entries := *val.MustMap()` and returns `entries[0].Val.MustI128(), entries[1].Val.MustB(), entries[2].Val.MustB()`. The safer checkpoint implementation iterates the map and switches on `"amount"`, `"authorized"`, and `"clawback"` instead, which shows the codebase already has a non-order-dependent parser for the same shape.

## Anti-Evidence

If upstream Soroban serialization canonically preserves this exact field order for all SAC balance values, practical reachability is reduced. But the live parser itself does not enforce or re-check that invariant before using positional `Must*` accessors.

---

## Review

**Verdict**: NOT_VIABLE
**Date**: 2026-04-21
**Reviewed by**: claude-opus-4-6, high
**Novelty**: PASS — not previously investigated
**Failed At**: reviewer

### Trace Summary

Traced the full code path from `processSACBalanceChange` (sac_balances.go:74) through the SDK's `sac.ContractBalanceFromContractData` (go-stellar-sdk/ingest/sac/contract_data.go) and into `extractSACBalanceFields` (sac_balances.go:142-145). The SDK validation function explicitly checks positional order of map entries — `balanceMap[0].Key` must be `"amount"`, `balanceMap[1].Key` must be `"authorized"`, `balanceMap[2].Key` must be `"clawback"` — returning `false` for any other order. This means a reordered map never reaches the positional extraction code.

### Code Paths Examined

- `internal/indexer/processors/sac_balances.go:96-104` — calls `sac.ContractBalanceFromContractData(*entry, p.networkPassphrase)` and returns `skip=true` if `ok` is false
- `go-stellar-sdk@v0.1.0/ingest/sac/contract_data.go:ContractBalanceFromContractData` — validates `len(balanceMap) == 3`, then checks `balanceMap[0].Key == "amount"`, `balanceMap[1].Key == "authorized"` (with IsBool), `balanceMap[2].Key == "clawback"` (with IsBool); returns `false` if any positional check fails
- `internal/indexer/processors/sac_balances.go:142-145` — `extractSACBalanceFields` uses positional access `entries[0]`, `entries[1]`, `entries[2]`; only reachable after SDK validation passes

### Why It Failed

The SDK's `ContractBalanceFromContractData` acts as a positional-order gate. It explicitly validates that `balanceMap[0]` is `"amount"`, `balanceMap[1]` is `"authorized"`, and `balanceMap[2]` is `"clawback"` — using the exact same positional assumption the live parser uses. A map with reordered entries would fail the SDK check (returning `ok=false`), causing the live processor to skip the entry at line 103 before `extractSACBalanceFields` is ever called. The hypothesis's mechanism — that positional access can misparse or panic on reordered maps — is impossible because the SDK precondition guarantees the order.

Additionally, the Soroban runtime's SAC implementation always serializes balance maps in canonical `[amount, authorized, clawback]` order, making non-canonical ordering unreachable from honest upstream (which is the trust model).

### Lesson Learned

When a hypothesis claims positional access is unsafe, check whether upstream validation functions enforce the same positional invariant. The SDK's `ContractBalanceFromContractData` and the live processor's `extractSACBalanceFields` share the same positional assumption — the SDK validates it, and the processor relies on it. The checkpoint path's key-name iteration is more defensive but functionally equivalent given the SDK gate.
