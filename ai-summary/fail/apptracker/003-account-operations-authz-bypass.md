# H003: AccountByAddress exposes arbitrary account operations to any valid JWT holder

**Date**: 2026-04-21
**Subsystem**: apptracker / serve-graphql / auth
**Severity**: High
**Impact**: GraphQL authz bypass / cross-tenant operation-history disclosure
**Hypothesis by**: gpt-5.4, high

## Expected Behavior

`Account.operations` should only resolve for the account that the authenticated JWT represents, or for an explicitly authorized delegate. A caller should not be able to request another account's operation stream simply by naming a different address in GraphQL.

## Mechanism

No resolver-layer authorization exists between `AccountByAddress` and `accountResolver.Operations`. Once the caller chooses a victim address, the resolver converts that address into a database lookup over `BatchGetByAccountAddress`, disclosing operation records for the victim while the JWT proves only that the caller controls *some* allowed keypair.

## Trigger

1. Authenticate as attacker account `GA...ATTACKER`.
2. Send:
   ```graphql
   query {
     accountByAddress(address: "GA...VICTIM") {
       operations(first: 5) {
         edges { node { id operationType successful } }
       }
     }
   }
   ```
3. Observe operations associated with `GA...VICTIM`.

## Target Code

- `internal/serve/middleware/middleware.go:16-44` — middleware verifies token validity but never binds subject to context
- `internal/serve/serve.go:185-236` — GraphQL handler receives no requester identity
- `internal/serve/graphql/resolvers/queries.resolvers.go:70-81` — root query accepts arbitrary account addresses
- `internal/serve/graphql/resolvers/account.resolvers.go:70-105` — operations resolver queries by the parent account address directly

## Evidence

`accountResolver.Operations` derives `string(obj.StellarAddress)` from the root-selected account object and passes it to `r.models.Operations.BatchGetByAccountAddress(...)`. There is no call site in this path that reads JWT claims, compares addresses, or enforces per-account ownership.

## Anti-Evidence

The operation resolver includes pagination and time-range handling, so it is not a raw unbounded dump. Those checks do not change the core issue that the target account is fully caller-chosen.

---

## Review

**Verdict**: NOT_VIABLE
**Date**: 2026-04-21
**Reviewed by**: claude-opus-4-6, high
**Novelty**: FAIL — duplicate of 001-account-balances-authz-bypass and 002-account-transactions-authz-bypass
**Failed At**: reviewer

### Trace Summary

This hypothesis is substantively identical to H001 (account-balances-authz-bypass) and H002 (account-transactions-authz-bypass), both of which were already reviewed and rejected. All three hypotheses claim the same root mechanism: `AccountByAddress` enables cross-tenant data access because the JWT auth middleware does not propagate caller identity into the GraphQL context. The only variation is the target resolver field — `balances` (H001), `transactions` (H002), and now `operations` (H003). The auth model analysis from H001 applies identically here.

### Code Paths Examined

- `internal/serve/middleware/middleware.go:16-44` — `AuthenticationMiddleware` verifies JWT validity only, stores nothing in context (same as H001/H002)
- `internal/serve/serve.go:185-236` — GraphQL route gated by middleware, no identity propagated (same as H001/H002)
- `internal/serve/graphql/resolvers/queries.resolvers.go:70-81` — `AccountByAddress` validates address syntax, returns `Account` for any valid address (same as H001/H002)
- `internal/serve/graphql/resolvers/account.resolvers.go:70-105` — `Operations` resolver calls `BatchGetByAccountAddress` using `obj.StellarAddress` from the parent account object

### Why It Failed

Duplicate of H001 and H002. The wallet-backend JWT auth model authenticates trusted client applications (service-to-service), not individual end-users. The JWT `Subject` claim is a client application's public key from the `client-auth-public-keys` server configuration, not a Stellar account address. There is no concept of "the account that the authenticated JWT represents" — the JWT represents a trusted client app, and all GraphQL queries are designed to serve data for any requested address to any authenticated client. Additionally, Stellar operation data is publicly available on the blockchain, so per-account access control provides no security benefit. This is working-as-designed behavior, not an authorization bypass.

### Lesson Learned

The same "any authenticated caller can query any address" pattern has now been hypothesized three times across different resolver fields (balances, transactions, operations). All share the identical root cause misunderstanding: the auth model is app-level, not user-level. Future hypotheses targeting GraphQL field-level authz should be immediately checked against H001's finding that the JWT subject is a client app key, not a user identity.
