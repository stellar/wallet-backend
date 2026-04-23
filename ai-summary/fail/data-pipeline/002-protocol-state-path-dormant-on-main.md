# H002: Protocol Catchup/Cache Desync Corrupts Live Protocol State

**Date**: 2026-04-23
**Subsystem**: data-pipeline
**Severity**: High
**Impact**: Integrity
**Hypothesis by**: gpt-5.4, high

## Expected Behavior

If live ingest is producing protocol state, optimized catchup and the protocol-contract cache should keep those processor paths synchronized with the ledgers being ingested. Falling behind should not silently skip protocol history or current-state updates.

## Mechanism

The live-ingest path has explicit protocol cursor CAS logic and a stale-by-100-ledgers contract cache, while catchup batching only persists fact tables and token balances. That looked like it could let newly classified contracts or catchup-ledgers bypass protocol processors and corrupt downstream protocol state.

## Trigger

Run live ingest with protocol processors enabled, let the node fall behind into catchup, and submit activity to a protocol-tracked contract during the stale-cache window or catchup range.

## Target Code

- `internal/ingest/ingest.go:217-243` — live ingest only wires processors from the registry
- `internal/services/processor_registry.go:11-35` — processor registry definition
- `internal/services/validator_registry.go:11-24` — validator registry definition

## Evidence

There is substantial protocol-processing code in `internal/services`, including live CAS gating, migration services, and protocol setup. That makes the catchup/cache surface look important at first pass.

## Anti-Evidence

I found no in-tree production `RegisterProcessor(...)` or `RegisterValidator(...)` call sites on `main`; only the registries, tests, and command wiring remain. Without any registered processor or validator, the suspected corruption path is dormant rather than attacker-reachable in the current tree.

---

## Review

**Verdict**: NOT_VIABLE
**Date**: 2026-04-23
**Failed At**: hypothesis
**Novelty**: PASS — not previously investigated

### Why It Failed

The protocol-state machinery appears unwired on `main`, so the catchup/cache desync concern is not presently reachable through an active production processor path.

### Lesson Learned

For this repo, protocol-state bugs need an explicit in-tree registration path before they qualify as live security findings. Registry-driven subsystems can look hot in isolation while being effectively dead code in the current build.
