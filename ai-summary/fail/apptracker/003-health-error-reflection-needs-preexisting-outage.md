# H003: Unauthenticated health checks exfiltrate RPC and DB errors to Sentry

**Date**: 2026-04-21
**Subsystem**: apptracker / serve-api
**Severity**: Medium
**Impact**: confidentiality
**Hypothesis by**: gpt-5.4, high

## Expected Behavior

The unauthenticated `/health` endpoint should reduce backend failures to coarse health states instead of reflecting detailed upstream or database errors into both the HTTP response and Sentry. A random network client should not be able to trigger third-party telemetry emission of internal diagnostics.

## Mechanism

`HealthHandler.GetHealth` wraps RPC and ingest-store failures with `err.Error()` and passes the same error into `InternalServerError`/`ServiceUnavailable`, which forwards it to `CaptureException`. `rpcService.sendRPCRequest` also embeds raw RPC response bodies in several error strings, so detailed diagnostics would propagate if the health dependencies fail.

## Trigger

1. Put the backend into a degraded state where `RPCService.GetHealth()` or `IngestStore.Get()` fails.
2. Call `GET /health`.
3. Observe the detailed error reflected to the client and captured by apptracker.

## Target Code

- `internal/serve/httphandler/health.go:27-53` — health handler forwards backend errors directly
- `internal/serve/httperror/errors.go:65-89` — internal/service-unavailable handlers call `appTracker.CaptureException`
- `internal/services/rpc_service.go:355-377` — raw RPC bodies are interpolated into error strings

## Evidence

The health handler uses `err.Error()` as the client-facing message on both RPC and DB failure branches, and the same `err` is sent to apptracker without redaction.

## Anti-Evidence

The attacker does not control the upstream RPC or DB error content under the trust model, and exploitation depends on a preexisting degraded infrastructure state. Because the same details are already returned directly in the `/health` response, the apptracker path does not create a new attacker capability on its own.

---

## Review

**Verdict**: NOT_VIABLE
**Date**: 2026-04-21
**Failed At**: hypothesis
**Novelty**: PASS — not previously investigated

### Why It Failed

This path is real but relies on an outage condition outside attacker control, and the exploitable disclosure is already in the HTTP response rather than being introduced by apptracker itself.

### Lesson Learned

For tracker-backed egress findings, prefer cases where apptracker uniquely crosses a trust boundary or materially amplifies attacker control, not ones where the same data is already returned to the caller.
