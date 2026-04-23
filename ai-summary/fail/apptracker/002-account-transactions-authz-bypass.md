# H002: AccountByAddress exposes arbitrary account transaction history to any valid JWT holder

**Date**: 2026-04-21
**Subsystem**: apptracker / serve-graphql / auth
**Severity**: High
**Impact**: GraphQL authz bypass / cross-tenant transaction-history disclosure
**Hypothesis by**: gpt-5.4, high

## Expected Behavior

Transaction history under `Account.transactions` should be returned only when the authenticated caller is authorized for that account. Requests for another account's transaction timeline should be denied rather than translated into a DB lookup keyed by the victim address.

## Mechanism

The same missing identity propagation that affects balances also affects the transaction connection. `AccountByAddress` lets the caller mint an in-memory `*types.Account` for any target address, and `accountResolver.Transactions` immediately executes `BatchGetByAccountAddress` for that address without any subject-vs-target comparison.

## Trigger

1. Authenticate as attacker account `GA...ATTACKER`.
2. Send:
   ```graphql
   query {
     accountByAddress(address: "GA...VICTIM") {
       transactions(first: 5) {
         edges { node { hash ledgerNumber } }
       }
     }
   }
   ```
3. Observe the victim account's transaction history instead of an authorization error.

## Target Code

- `internal/serve/middleware/middleware.go:16-44` — JWT validity check only
- `internal/serve/serve.go:185-236` — no caller identity is threaded into the GraphQL context
- `internal/serve/graphql/resolvers/queries.resolvers.go:70-81` — arbitrary target account selection
- `internal/serve/graphql/resolvers/account.resolvers.go:30-67` — `Transactions` queries by `obj.StellarAddress`

## Evidence

`accountResolver.Transactions` computes pagination, then calls `r.models.Transactions.BatchGetByAccountAddress(ctx, string(obj.StellarAddress), ...)`. The only provenance of `obj.StellarAddress` on this path is the caller-controlled `accountByAddress(address: ...)` argument.

## Anti-Evidence

The resolver validates pagination and optional time filters, which limits malformed requests but does not restrict who may inspect a given account's history. Authentication still requires a valid JWT, so this is an authorization gap rather than full anonymous access.

---

## Review

**Verdict**: NOT_VIABLE
**Date**: 2026-04-21
**Reviewed by**: claude-opus-4-6, high
**Novelty**: FAIL — duplicate of 001-account-balances-authz-bypass
**Failed At**: reviewer

### Trace Summary

This hypothesis is substantively identical to H001 (account-balances-authz-bypass), which was already reviewed and rejected. Both hypotheses claim that `AccountByAddress` enables cross-tenant data access because no caller identity is propagated into the GraphQL context. The only difference is the target field (`transactions` vs `balances`). The same auth model analysis that invalidated H001 applies here: the JWT `Subject` is a client application public key, not an end-user Stellar address, so there is no per-account authorization to bypass.

### Code Paths Examined

- `internal/serve/middleware/middleware.go:16-44` — `AuthenticationMiddleware` verifies JWT validity only, stores nothing in context (same as H001)
- `internal/serve/serve.go:185-236` — GraphQL route gated by middleware, no identity propagated (same as H001)
- `internal/serve/graphql/resolvers/queries.resolvers.go:70-81` — `AccountByAddress` validates address syntax, returns `Account` for any valid address (same as H001)
- `internal/serve/graphql/resolvers/account.resolvers.go:30-67` — `Transactions` resolver calls `BatchGetByAccountAddress` using `obj.StellarAddress` from the parent account object

### Why It Failed

Duplicate of H001. The hypothesis itself states "The same missing identity propagation that affects balances also affects the transaction connection," explicitly acknowledging it is the same root cause. The wallet-backend JWT auth model authenticates trusted client applications (service-to-service), not individual end-users. The JWT `Subject` claim is a client app's public key from `client-auth-public-keys`, not a Stellar account address. All GraphQL queries are designed to serve data for any requested address to any authenticated client. Additionally, Stellar transaction data is publicly available on the blockchain, so per-account access control provides no security benefit.

### Lesson Learned

When a hypothesis explicitly cites another hypothesis as sharing the same root mechanism ("the same missing identity propagation that affects balances"), it is almost certainly a duplicate and should be rejected on novelty grounds without further investigation.
