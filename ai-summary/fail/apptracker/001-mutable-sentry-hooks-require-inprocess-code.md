# H001: Mutable Sentry hook globals allow runtime capture hijacking

**Date**: 2026-04-21
**Subsystem**: apptracker
**Severity**: High
**Impact**: telemetry integrity
**Hypothesis by**: gpt-5.4, high

## Expected Behavior

Runtime telemetry hooks should not be replaceable by untrusted actors. If an attacker can redirect `CaptureException` or `Init`, they could suppress error reporting or exfiltrate captured exceptions to an attacker-controlled sink.

## Mechanism

`internal/apptracker/sentry/sentry_tracker.go` exposes mutable package-level function variables for `Init`, `Flush`, `Recover`, and capture calls. In principle, reassigning those globals at runtime would let later capture paths execute attacker-controlled code instead of the Sentry SDK.

## Trigger

1. Import `internal/apptracker/sentry` from attacker-controlled in-process code.
2. Reassign `InitFunc`, `FlushFunc`, `RecoverFunc`, or the capture function vars.
3. Wait for application startup or an error-reporting path.

## Target Code

- `internal/apptracker/sentry/sentry_tracker.go:12-18` — mutable package-level hook variables
- `internal/apptracker/sentry/sentry_tracker.go:22-39` — runtime call sites that trust those hooks

## Evidence

The package-level vars are explicitly designed to be swapped in tests, and no synchronization or immutability guard exists around them.

## Anti-Evidence

The trust model for this scan is a network client, not an in-process adversary. Reaching these assignments already requires code execution inside the wallet-backend process, which is out of scope and strictly stronger than the vulnerability itself.

---

## Review

**Verdict**: NOT_VIABLE
**Date**: 2026-04-21
**Failed At**: hypothesis
**Novelty**: PASS — not previously investigated

### Why It Failed

This issue only exists for an attacker who can already run arbitrary Go code in-process, which is outside the stated threat model.

### Lesson Learned

For `apptracker`, mutable test seams are only security-relevant if a network-reachable path can mutate them or if the threat model includes malicious plugins/in-process code.
