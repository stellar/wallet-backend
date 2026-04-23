# H002: Root GraphQL History Queries Ignore Caller Identity

**Date**: 2026-04-21
**Subsystem**: sorobanauth
**Severity**: High
**Impact**: confidentiality / GraphQL authz bypass
**Hypothesis by**: gpt-5.4, high

## Expected Behavior

Authenticated GraphQL callers should only be able to enumerate transactions, operations, and state changes they are authorized to see. Root history queries should either be denied for end-user JWTs or filtered to records associated with the authenticated subject, rather than exposing the entire indexed ledger history to any valid keypair.

## Mechanism

The server treats JWT authentication as a binary "has any valid token" check and never binds the authenticated subject into GraphQL authorization. As a result, root resolvers call `GetAll`, `GetByHash`, and `GetByID` directly on global tables, letting any valid JWT enumerate or fetch arbitrary transactions, operations, and state changes across all tenants.

## Trigger

1. Authenticate with any valid Stellar keypair accepted by the server.
2. Issue one of:
   - `transactions(first: 50) { edges { node { hash ledgerNumber } } }`
   - `operations(first: 50) { edges { node { id operationType } } }`
   - `stateChanges(first: 50) { edges { node { __typename ledgerNumber } } }`
   - `transactionByHash(hash: "<someone-else's-hash>") { hash }`
   - `operationById(id: <someone-else's-id>) { id }`
3. Observe that the backend returns global history instead of enforcing per-subject visibility.

## Target Code

- `internal/serve/middleware/middleware.go:16-44` — authentication is presence-only after verify.
- `internal/serve/graphql/resolvers/queries.resolvers.go:22-33` — `TransactionByHash` reads by arbitrary hash.
- `internal/serve/graphql/resolvers/queries.resolvers.go:39-67` — `Transactions` reads from `models.Transactions.GetAll`.
- `internal/serve/graphql/resolvers/queries.resolvers.go:84-121` — `Operations` and `OperationByID` expose arbitrary operations.
- `internal/serve/graphql/resolvers/queries.resolvers.go:123-154` — `StateChanges` exposes arbitrary state changes.

## Evidence

Every root history resolver obtains DB columns and then calls the corresponding model with no reference to request identity. There is no context value, subject extraction, or ownership filter between `AuthenticationMiddleware` and these resolver methods.

## Anti-Evidence

These endpoints are behind JWT authentication when auth is configured, so the attacker still needs one valid signing key. Pagination and complexity limits constrain volume per request but not access scope.

---

## Review

**Verdict**: NOT_VIABLE
**Date**: 2026-04-21
**Reviewed by**: claude-opus-4-6, high
**Novelty**: FAIL — substantially equivalent to `ai-summary/fail/apptracker/001-account-balances-authz-bypass.md` (same auth-model misunderstanding applied to different query types)
**Failed At**: reviewer

### Trace Summary

Traced the authentication middleware, JWT verification chain, and all root GraphQL resolvers for transactions, operations, and state changes. The JWT auth model uses `ClientAuthPublicKeys` — a server-configured list of trusted client application public keys. The JWT `Subject` claim identifies the calling **client application**, not an end-user Stellar address. Authentication is service-to-service: "Is this request from a trusted client app?" Once authenticated, all queries are intentionally accessible to any authorized client for all indexed data.

### Code Paths Examined

- `internal/serve/serve.go:142-146` — `NewMultiJWTTokenParser` built from `cfg.ClientAuthPublicKeys`, confirming app-level auth model
- `pkg/wbclient/auth/jwt_manager.go:47-49` — `JWTManager.ParseJWT` checks `claims.Subject != m.PublicKey`, verifying against the *client app key*, not any user address
- `pkg/wbclient/auth/jwt_manager.go:152-165` — `MultiJWTTokenParser.ParseJWT` routes by `claims.Subject` to pre-configured client parsers
- `pkg/wbclient/auth/jwt_http_signer_verifier.go:67-103` — `VerifyHTTPRequest` returns nil on success, discarding parsed claims entirely
- `internal/serve/middleware/middleware.go:16-44` — `AuthenticationMiddleware` only checks `err == nil`; stores nothing in context
- `internal/serve/graphql/resolvers/queries.resolvers.go:22-154` — All root resolvers (TransactionByHash, Transactions, Operations, OperationByID, StateChanges) read from global tables with no identity filter
- `internal/serve/serve.go:186-236` — GraphQL route gated by auth middleware, no identity propagation to resolvers

### Why It Failed

The hypothesis describes **working-as-designed behavior** based on a fundamental misunderstanding of the authentication model. The JWT does not represent an end-user Stellar account — its `Subject` is a **client application's public key** from the server's `client-auth-public-keys` configuration. There is no concept of "tenants" or "per-user data." The wallet-backend authenticates trusted client applications (service-to-service), and all GraphQL queries are designed to serve the full indexed ledger dataset to any authenticated client. Furthermore, Stellar ledger data (transactions, operations, state changes) is inherently **public on the blockchain** — the wallet-backend is an indexer caching this public data, so per-account access control would provide no security benefit. This is the identical reasoning that rejected `apptracker/001-account-balances-authz-bypass.md`.

### Lesson Learned

The wallet-backend JWT auth model is client-application-level, not per-user. Any hypothesis claiming "cross-tenant" or "cross-caller" data access across *any* GraphQL query type (balances, transactions, operations, state changes) is not viable because the system has no tenant boundaries. The `ClientAuthPublicKeys` config gates access to the entire API as a unit.
