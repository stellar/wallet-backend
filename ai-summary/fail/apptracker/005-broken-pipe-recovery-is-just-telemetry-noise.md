# H005: Client disconnects during UInt32 serialization cause recovered panics that hit Sentry

**Date**: 2026-04-21
**Subsystem**: apptracker / serve-graphql
**Severity**: Medium
**Impact**: availability
**Hypothesis by**: gpt-5.4, high

## Expected Behavior

Client disconnects during response serialization should be treated as aborted requests, not as application panics that generate stack traces and apptracker events. An attacker closing the socket should not be able to force panic-reporting paths.

## Mechanism

`MarshalUInt32` panics on `io.WriteString` failure, and `RecoverHandler` only special-cases `http.ErrAbortHandler`, not broken-pipe / connection-reset write errors. A caller who disconnects while GraphQL is serializing `UInt32` fields could therefore induce recovered panics and `CaptureException` calls.

## Trigger

1. Authenticate and request a GraphQL object that includes `UInt32` fields such as `ledgerNumber`.
2. Close the TCP connection while the response body is being written.
3. Let `RecoverHandler` catch the resulting write-error panic.

## Target Code

- `internal/serve/graphql/scalars/uint32.go:16-24` — marshaler panics on writer error
- `internal/serve/middleware/middleware.go:49-70` — recover path reports non-`http.ErrAbortHandler` panics
- `internal/serve/httperror/errors.go:65-74` — recovered panic is sent to apptracker

## Evidence

The marshaler explicitly `panic(err)` on any write failure, and the recover middleware converts arbitrary panic values into `error`s before reporting them.

## Anti-Evidence

The panic is recovered inside middleware, so I did not identify a path to process crash or persistent service degradation. The remaining effect is exception noise rather than a clear Medium-impact vulnerability.

---

## Review

**Verdict**: NOT_VIABLE
**Date**: 2026-04-21
**Failed At**: hypothesis
**Novelty**: PASS — not previously investigated

### Why It Failed

This path likely exists, but it stops at a recovered 500 and telemetry event, which is not enough by itself for the accepted severity bar.

### Lesson Learned

Recovered panics need a follow-on consequence such as process crash, connection desync, or durable resource exhaustion to qualify for this objective.
