# H001: AccountByAddress exposes arbitrary account balances to any valid JWT holder

**Date**: 2026-04-21
**Subsystem**: apptracker / serve-graphql / auth
**Severity**: High
**Impact**: GraphQL authz bypass / cross-tenant balance disclosure
**Hypothesis by**: gpt-5.4, high

## Expected Behavior

A valid Ed25519 JWT should authorize account-scoped GraphQL reads only for the Stellar address bound to that JWT. When a caller asks for another account's balances, the server should reject the request instead of returning native, trustline, SAC, or SEP-41 balance rows for the victim address.

## Mechanism

`AuthenticationMiddleware` only verifies that the request carries *some* valid JWT; it never stores the verified subject in request context or compares it against GraphQL targets. `Query.AccountByAddress` accepts any syntactically valid address and `Account.Balances` forwards that address straight into `getAccountBalances`, so a caller with their own valid keypair can read another account's balance inventory.

## Trigger

1. Authenticate as attacker account `GA...ATTACKER` with a valid JWT.
2. Send:
   ```graphql
   query {
     accountByAddress(address: "GA...VICTIM") {
       address
       balances(first: 10) {
         edges { node { __typename } }
       }
     }
   }
   ```
3. Observe the victim account's balance collection instead of an authorization failure.

## Target Code

- `internal/serve/middleware/middleware.go:16-44` — request auth only verifies signature validity
- `internal/serve/serve.go:185-236` — GraphQL route is gated by middleware but no identity is propagated to resolvers
- `internal/serve/graphql/resolvers/queries.resolvers.go:70-81` — `AccountByAddress` constructs an account object for any requested address
- `internal/serve/graphql/resolvers/account.resolvers.go:25-28` — `Balances` blindly resolves the parent account
- `internal/serve/graphql/resolvers/account_balances.go:200-259` — balance fetch path uses the supplied account address directly

## Evidence

There is no `context.WithValue` or equivalent subject-binding step anywhere in the request auth or GraphQL stack. `AccountByAddress` does not consult caller identity; it only validates address syntax, then hands the chosen address to the balance resolver.

## Anti-Evidence

The endpoint is not anonymous: the caller still needs a valid JWT from one of the configured public keys. Address parsing and balance pagination validation are present, but neither enforces ownership of the target account.

---

## Review

**Verdict**: NOT_VIABLE
**Date**: 2026-04-21
**Reviewed by**: claude-opus-4-6, high
**Novelty**: PASS — not previously investigated
**Failed At**: reviewer

### Trace Summary

Traced the complete authentication and GraphQL resolution chain from HTTP middleware through JWT verification to the `AccountByAddress` resolver and balance fetching. The JWT auth model uses `ClientAuthPublicKeys` — a server-configured list of trusted client application public keys. The JWT `Subject` claim is the client application's key, NOT an end-user Stellar address. The authentication model is service-to-service: "Is this request from a trusted client app?" All GraphQL queries (transactions, operations, accounts, state changes) are intentionally accessible to any authenticated client for any address.

### Code Paths Examined

- `cmd/serve.go:41-47` — `client-auth-public-keys` config: "A comma-separated list of public keys whose private keys are authorized to sign the payloads when making HTTP requests to this server."
- `internal/serve/serve.go:142` — `NewMultiJWTTokenParser` built from `cfg.ClientAuthPublicKeys`, confirming app-level auth
- `pkg/wbclient/auth/jwt_manager.go:47-49` — `JWTManager.ParseJWT` checks `claims.Subject != m.PublicKey`, verifying against the *client app key*, not any user address
- `pkg/wbclient/auth/jwt_manager.go:152-165` — `MultiJWTTokenParser.ParseJWT` routes by `claims.Subject` to one of the pre-configured client parsers
- `pkg/wbclient/auth/jwt_http_signer_verifier.go:67-103` — `VerifyHTTPRequest` returns nil on success, discarding the parsed claims and subject entirely
- `internal/serve/middleware/middleware.go:16-44` — `AuthenticationMiddleware` only checks `err == nil` from verify; stores nothing in context
- `internal/serve/graphql/resolvers/queries.resolvers.go:71-82` — `AccountByAddress` validates address syntax only, returns `Account` for any valid address
- `internal/serve/graphql/schema/queries.graphqls` — No auth directives on any query fields

### Why It Failed

The hypothesis misunderstands the authentication model. The JWT does not represent an end-user Stellar account — its `Subject` is a **client application's public key** from the server's `client-auth-public-keys` configuration. There is no concept of "the Stellar address bound to this JWT." The wallet-backend authenticates trusted client applications (service-to-service), not individual end-users. All GraphQL queries (`accountByAddress`, `transactions`, `operations`, `stateChanges`) are designed to serve data for any requested address to any authenticated client. Furthermore, Stellar ledger data (account balances, transactions) is inherently public on the blockchain — the wallet-backend is an indexer caching this public data. This is working-as-designed behavior, not an authorization bypass.

### Lesson Learned

The wallet-backend JWT auth model is client-application-level, not per-user. The `Subject` claim identifies the calling application, not an end-user. Any hypothesis claiming "cross-tenant" data access must first verify that the auth model actually binds tokens to individual user identities — in this codebase, it does not. Additionally, since Stellar blockchain data is publicly queryable, per-account access control would provide no real security benefit.
