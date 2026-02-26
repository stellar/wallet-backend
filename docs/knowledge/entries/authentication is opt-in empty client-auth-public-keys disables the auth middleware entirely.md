---
description: If --client-auth-public-keys is unset, RequestAuthVerifier is nil and AuthenticationMiddleware is skipped; serves development without auth while production enforces Ed25519 JWT verification
type: decision
status: current
confidence: proven
subsystem: auth
areas: [auth, middleware, configuration, jwt, ed25519]
---

# Authentication is opt-in: empty client-auth-public-keys disables the auth middleware entirely

## Context

Running the wallet-backend in development requires an authenticated client setup (generating Ed25519 keypairs, signing every request). This friction is unnecessary when the server isn't exposed to untrusted callers.

## Detail

In `serve.go:initHandlerDeps()`, if `ClientAuthPublicKeys` is empty, `NewMultiJWTTokenParser` and `NewHTTPRequestVerifier` are not constructed and `RequestAuthVerifier` remains `nil`. In `handler()`, the middleware is conditionally mounted:

```go
if cfg.RequestAuthVerifier != nil {
    r.Use(middleware.AuthenticationMiddleware(...))
}
```

This means `/graphql/query` is completely open without authentication when no public keys are configured. `/health` and `/api-metrics` are always unauthenticated regardless.

## Implications

- **Production deployments must set `--client-auth-public-keys`** — omitting it leaves the GraphQL API fully open.
- Development and testing environments can omit the flag to avoid auth overhead.
- There is no "allow all keys" mode — auth is binary: off (empty keys) or on (at least one key).

## Source

`internal/serve/serve.go:initHandlerDeps()` — conditional verifier construction
`internal/serve/serve.go:handler()` — conditional middleware mounting
`cmd/serve.go` — `--client-auth-public-keys` flag definition

## Related

When auth IS enabled, the middleware enforces [[jwt auth binds each token to a specific request body and method-path to prevent replay attacks]] — the opt-in model described here is the on/off switch; that entry describes what happens when the switch is on.

relevant_notes:
  - "[[jwt auth binds each token to a specific request body and method-path to prevent replay attacks]] — grounds this: the per-request body binding is only enforced when auth is active; this entry controls whether that enforcement runs at all"
