# H001: AccountByAddress Is Not Bound To JWT Subject

**Date**: 2026-04-21
**Subsystem**: sorobanauth
**Severity**: High
**Impact**: confidentiality / GraphQL authz bypass
**Hypothesis by**: gpt-5.4, high

## Expected Behavior

After JWT verification succeeds, the GraphQL layer should know which Stellar address authenticated the request and reject `accountByAddress(address: ...)` requests for other accounts/contracts unless an explicit server-side authorization rule allows them. Account-scoped fields such as `balances`, `transactions`, `operations`, and `stateChanges` should only return data for the authenticated subject (or an authorized delegate), not for an arbitrary caller-supplied address.

## Mechanism

`VerifyHTTPRequest` only returns success/failure and discards the parsed JWT claims, while `AuthenticationMiddleware` forwards the request unchanged when verification succeeds. `AccountByAddress` then instantiates an account object directly from the caller-supplied `address`, and every account field resolver queries storage with that address without comparing it to any authenticated subject, so any holder of a valid Ed25519 JWT can read another account's indexed data.

## Trigger

1. Authenticate with any valid Stellar keypair accepted by `CLIENT_AUTH_PUBLIC_KEYS`.
2. Send a GraphQL request like `accountByAddress(address: "G...victim...") { balances { edges { node { tokenId balance } } } transactions { edges { node { hash } } } operations { edges { node { id } } } stateChanges { edges { node { __typename } } } }`.
3. Observe that the backend returns the victim account's data instead of rejecting the query as cross-account access.

## Target Code

- `pkg/wbclient/auth/jwt_http_signer_verifier.go:67-102` — verification succeeds/fails but does not propagate claims/subject.
- `internal/serve/middleware/middleware.go:16-44` — authenticated requests are passed downstream without caller identity in context.
- `internal/serve/graphql/resolvers/queries.resolvers.go:70-82` — `AccountByAddress` trusts the caller-supplied address.
- `internal/serve/graphql/resolvers/account.resolvers.go:26-172` — account field resolvers query by `obj.StellarAddress` with no subject check.

## Evidence

The auth middleware calls `requestAuthVerifier.VerifyHTTPRequest(req)` and, on success, immediately executes `next.ServeHTTP(rw, req)` without attaching claims to the request context. `AccountByAddress` only validates StrKey syntax, returns `&types.Account{StellarAddress: ...}`, and the downstream resolvers use that raw address as the database lookup key.

## Anti-Evidence

The request must still carry a valid JWT signed by an allowed key, so this is not an unauthenticated bypass. Complexity limits and pagination validation reduce query size, but they do not enforce tenant scoping.

---

## Review

**Verdict**: NOT_VIABLE
**Date**: 2026-04-21
**Reviewed by**: claude-opus-4-6, high
**Novelty**: PASS — not previously investigated
**Failed At**: reviewer

### Trace Summary

Traced the full authentication and query path: JWT claims contain a `Subject` field that is validated as a Stellar public key, but this subject identifies the **client application** (from `CLIENT_AUTH_PUBLIC_KEYS`), not an end user. `VerifyHTTPRequest` returns only `error`; the middleware passes requests downstream without context enrichment. The GraphQL schema has no authorization directives and no per-account scoping. Root queries (`Transactions`, `Operations`, `StateChanges`) return ALL data without any account binding, confirming the system is designed as a client-app-authenticated data API, not a per-user-scoped service.

### Code Paths Examined

- `pkg/wbclient/auth/claims.go:13-17,38-39` — `customClaims.Subject` is validated as a Stellar public key, used to identify the client app
- `pkg/wbclient/auth/jwt_manager.go:47,77,159-161` — `Subject` is matched against `CLIENT_AUTH_PUBLIC_KEYS`; it identifies the client application, not an end user
- `pkg/wbclient/auth/jwt_http_signer_verifier.go:67-102` — `VerifyHTTPRequest` returns `error` only, discards claims (confirmed)
- `internal/serve/middleware/middleware.go:16-44` — middleware calls `next.ServeHTTP(rw, req)` on success with no context enrichment (confirmed)
- `internal/serve/serve.go:186-210` — auth middleware is conditionally applied; GraphQL routes are behind client-app auth, not user-scoped auth
- `internal/serve/graphql/resolvers/queries.resolvers.go:36-68` — root `Transactions` query returns ALL transactions without account scoping
- `internal/serve/graphql/resolvers/queries.resolvers.go:70-82` — `AccountByAddress` validates StrKey syntax only (confirmed)
- `internal/serve/graphql/resolvers/account.resolvers.go:26-172` — account resolvers query by `obj.StellarAddress` (confirmed)
- `internal/serve/graphql/schema/queries.graphqls:1-10` — no authorization directives; `accountByAddress` is a general query
- `internal/serve/graphql/schema/directives.graphqls` — only `@goField` directive exists (code generation, not authorization)

### Why It Failed

This hypothesis describes **working-as-designed behavior**, not a security vulnerability. The JWT `Subject` identifies a **client application** (one of `CLIENT_AUTH_PUBLIC_KEYS`), not an individual end user. The system is intentionally designed as a client-app-authenticated blockchain data API where any authenticated client can query any account's indexed data — analogous to a blockchain explorer with API key authentication.

Key evidence that this is by-design:
1. Root queries (`Transactions`, `Operations`, `StateChanges`) return ALL data across all accounts without any address binding — there is no concept of tenant scoping anywhere in the system.
2. The auth middleware is even optional (`if deps.RequestAuthVerifier != nil`), confirming it gates access to the API, not to specific data.
3. The GraphQL schema has zero authorization directives — no `@auth`, `@hasRole`, or `@owner` patterns.
4. Stellar blockchain data is inherently public; the backend indexes public ledger data and serves it to authorized client applications.

### Lesson Learned

The JWT subject in this system identifies a client application, not an end user. Any hypothesis claiming per-account authorization bypass must first verify that per-account authorization exists in the design. The presence of root-level unscoped queries (Transactions, Operations) is the fastest signal that the system does not implement tenant scoping.
