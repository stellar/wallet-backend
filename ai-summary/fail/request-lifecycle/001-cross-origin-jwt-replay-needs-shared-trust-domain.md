# H001: Cross-origin JWT replay via missing host or audience binding

**Date**: 2026-04-21
**Subsystem**: request-lifecycle
**Severity**: Medium
**Impact**: Authentication / replay across deployments
**Hypothesis by**: gpt-5.4, high

## Expected Behavior

A JWT minted for one wallet-backend deployment should not be accepted by a different deployment unless the server explicitly intends those deployments to share an authorization domain. A verifier that wants origin isolation should bind the token to deployment identity via `aud`, `iss`, host, or another origin-specific claim.

## Mechanism

The generated JWT contains only `sub`, `iat`, `exp`, `bodyHash`, and `methodAndPath`, and verification checks only those fields against the current request. That initially suggests a replay path where a signed `/graphql/query` request could be reused against another deployment that shares the same allowlisted key.

## Trigger

1. Capture or mint a valid JWT for `POST /graphql/query` on deployment A.
2. Replay the same method, URI, and body to deployment B.
3. Check whether deployment B accepts it when both deployments trust the same client key.

## Target Code

- `pkg/wbclient/auth/jwt_manager.go:GenerateJWT:70-80` — only sets `Subject`, `IssuedAt`, and `ExpiresAt` in `RegisteredClaims`
- `pkg/wbclient/auth/claims.go:Validate:19-50` — validates `sub`, `bodyHash`, and `methodAndPath`, but not host or audience
- `pkg/wbclient/auth/jwt_http_signer_verifier.go:VerifyHTTPRequest:92-99` — binds the signature to `req.URL.RequestURI()` rather than a broader origin

## Evidence

There is no `Audience`, `Issuer`, or host binding in the token format, and `VerifyHTTPRequest` compares only method, `RequestURI`, and body hash. On the surface, that means the same signed request could verify anywhere that accepts the same public key and exposes the same path.

## Anti-Evidence

The trust model for this scan does not give the attacker passive network visibility to steal someone else's JWT, and a second deployment must already be configured to trust the same public key before acceptance is even possible. Under that shared-trust configuration, the second deployment has explicitly authorized the same caller, so the missing host/audience claim does not create a new in-scope unauthorized capability by itself.

---

## Review

**Verdict**: NOT_VIABLE
**Date**: 2026-04-21
**Failed At**: hypothesis
**Novelty**: PASS — not previously investigated

### Why It Failed

This attack only materializes when another deployment already trusts the same client key, and the stated attacker model does not include stealing somebody else's signed request in transit. Without a separate trust domain boundary enforced elsewhere, the missing host or audience binding is a deployment-design concern rather than an exploitable in-scope auth bypass on `main`.

### Lesson Learned

For this repo and threat model, replay findings need a concrete path that gives the attacker a token they otherwise could not mint themselves, or a way to cross a trust boundary that the server code actually distinguishes. "No audience claim" is not enough on its own when the verifier's only authorization primitive is the shared public-key allowlist.
