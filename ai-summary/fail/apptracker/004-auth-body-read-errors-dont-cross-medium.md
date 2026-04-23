# H004: Broken request bodies force Sentry exception traffic through auth middleware

**Date**: 2026-04-21
**Subsystem**: apptracker / auth / serve-api
**Severity**: Medium
**Impact**: availability
**Hypothesis by**: gpt-5.4, high

## Expected Behavior

Malformed or aborted request bodies on authenticated endpoints should be rejected as bad input without being promoted to internal exceptions. A client that disconnects mid-upload should not be able to manufacture apptracker traffic from the edge of the API.

## Mechanism

`VerifyHTTPRequest` reads the request body before signature verification, and any `io.ReadAll` failure is returned as a plain error rather than `ErrUnauthorized`. `AuthenticationMiddleware` treats non-`ErrUnauthorized` failures as internal errors and passes them to `InternalServerError`, which in turn reports them through apptracker.

## Trigger

1. Send a request into `/graphql/query` with an `Authorization: Bearer ...` header.
2. Abort the body upload mid-stream so `io.ReadAll(io.LimitReader(...))` fails.
3. Let middleware route the resulting error through `InternalServerError`.

## Target Code

- `pkg/wbclient/auth/jwt_http_signer_verifier.go:79-89` — body-read failure path
- `internal/serve/middleware/middleware.go:25-35` — non-`ErrUnauthorized` auth failures become internal errors
- `internal/serve/httperror/errors.go:65-74` — apptracker capture on internal errors

## Evidence

The verifier returns `fmt.Errorf("reading request body: %w", err)` without wrapping `ErrUnauthorized`, and middleware explicitly routes that branch to `InternalServerError`.

## Anti-Evidence

This produces logged exception traffic, but I did not find a concrete path from that behavior to a qualifying crash, permanent desync, or cross-tenant disclosure. In practice it looks closer to noisy telemetry / rate-limit pressure than to a Medium security issue under the project rubric.

---

## Review

**Verdict**: NOT_VIABLE
**Date**: 2026-04-21
**Failed At**: hypothesis
**Novelty**: PASS — not previously investigated

### Why It Failed

The attacker can likely generate exception noise, but the impact does not clearly rise above operational log spam within this objective's severity thresholds.

### Lesson Learned

For auth-to-apptracker paths, require a demonstrable crash, auth bypass, or durable availability failure rather than generic exception amplification.
