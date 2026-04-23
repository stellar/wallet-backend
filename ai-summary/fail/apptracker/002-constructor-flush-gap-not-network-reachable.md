# H002: Constructor-scoped Flush/Recover defer lets attackers drop runtime Sentry events

**Date**: 2026-04-21
**Subsystem**: apptracker
**Severity**: Medium
**Impact**: telemetry availability
**Hypothesis by**: gpt-5.4, high

## Expected Behavior

Error events captured late in process lifetime should be flushed on orderly shutdown so operational telemetry is durable. A remote attacker should not be able to exploit tracker lifecycle mistakes to suppress evidence of their activity.

## Mechanism

`NewSentryTracker` defers `FlushFunc` and `RecoverFunc` inside the constructor, so the defers run immediately on return instead of at process shutdown. That means the apptracker package itself installs no shutdown-time flush, and late exceptions could be lost if the process exits quickly.

## Trigger

1. Start the server with Sentry enabled.
2. Cause an exception shortly before process termination.
3. Observe that apptracker itself has no later flush hook.

## Target Code

- `internal/apptracker/sentry/sentry_tracker.go:30-39` — `FlushFunc` and `RecoverFunc` are deferred in the constructor
- `cmd/serve.go:88-93` — constructor return value is stored and reused; no shutdown flush is added here

## Evidence

The only explicit `FlushFunc` call in this subsystem is deferred inside `NewSentryTracker`, so it fires at construction time rather than shutdown.

## Anti-Evidence

The consequence is observability loss, not a direct confidentiality, integrity, or availability failure for wallet data. A network client also cannot deterministically force process exit from this subsystem alone, so the issue does not reach the minimum Medium security bar for this objective.

---

## Review

**Verdict**: NOT_VIABLE
**Date**: 2026-04-21
**Failed At**: hypothesis
**Novelty**: PASS — not previously investigated

### Why It Failed

This is an operational telemetry durability bug, not an exploitable network-reachable security vulnerability under the stated severity scale.

### Lesson Learned

For apptracker lifecycle issues, require a clear attacker-controlled path from network input to wallet data loss, auth bypass, or a qualifying crash/DoS before filing.
