---
description: bodyHash = hex(SHA-256(body)) in JWT claims; methodAndPath = "METHOD /path"; token signed for one endpoint with one body cannot be reused for different body at same endpoint
type: insight
status: current
confidence: proven
subsystem: auth
areas: [auth, jwt, security, replay-attack-prevention, ed25519]
---

# JWT auth binds each token to a specific request body and method-path to prevent replay attacks

## Context

Standard JWT bearer token auth allows token reuse — a captured token can be replayed for different requests until it expires. This is particularly dangerous for mutation endpoints.

## Detail

Every JWT token in the wallet-backend system carries three security binding fields:

1. `bodyHash`: `hex(SHA-256(request body))` — binds the token to exact request content.
2. `methodAndPath`: e.g. `"POST /graphql/query"` — binds the token to the specific endpoint.
3. `exp` / `iat` window: default 5s (client-side) or max 15s (server-side) — short expiry window.

`customClaims.Validate()` checks all three before the Ed25519 signature is verified:
- `claims.MethodAndPath == "METHOD /path"` — wrong endpoint → rejected.
- `SHA-256(body) == claims.bodyHash` — different body → rejected.
- `exp <= now + MaxTimeout` — token too far in the future → rejected.

The 5-second client-side expiry and body binding together mean a captured token is useless for replaying even the same query with different parameters.

## Implications

- The body must be read and then restored on the client side before sending (`req.Body = io.NopCloser(bytes.NewBuffer(body))`).
- The `MultiJWTTokenParser` uses the JWT's `sub` claim (the Stellar public key) as the map key for O(1) parser lookup — no need to try all registered keys.
- Expired tokens return an `ExpiredTokenError`, which increments a distinct Prometheus metric (`metricsService.IncSignatureVerificationExpired`) for monitoring.

## Source

`pkg/wbclient/auth/jwt_manager.go:GenerateJWT()` — token generation with body hash
`pkg/wbclient/auth/jwt_http_signer_verifier.go:VerifyHTTPRequest()` — server-side verification
`internal/serve/middleware/middleware.go:AuthenticationMiddleware()` — middleware integration

## Related

Because [[authentication is opt-in empty client-auth-public-keys disables the auth middleware entirely]], the per-request binding described here only applies when production keys are configured — development deployments skip this verification entirely.

relevant_notes:
  - "[[authentication is opt-in empty client-auth-public-keys disables the auth middleware entirely]] — extends this: the binding is enforced only when auth middleware is active; without keys, no binding occurs"
