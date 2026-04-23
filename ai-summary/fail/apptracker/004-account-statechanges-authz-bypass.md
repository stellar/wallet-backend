# H004: AccountByAddress exposes arbitrary account state changes to any valid JWT holder

**Date**: 2026-04-21
**Subsystem**: apptracker / serve-graphql / auth
**Severity**: High
**Impact**: GraphQL authz bypass / cross-tenant state-change disclosure
**Hypothesis by**: gpt-5.4, high

## Expected Behavior

Account-scoped state-change feeds should be restricted to the JWT subject's own account, because they expose a richer history than raw balances alone: change categories, reasons, linked operations/transactions, and account metadata transitions. A caller asking for another address's state changes should receive an authorization failure.

## Mechanism

`Account.stateChanges` repeats the same ownership gap as the other `Account` field resolvers. After `AccountByAddress` accepts an arbitrary target address, `accountResolver.StateChanges` passes that address into `BatchGetByAccountAddress` and returns the resulting change records without ever consulting the authenticated subject.

## Trigger

1. Authenticate as attacker account `GA...ATTACKER`.
2. Send:
   ```graphql
   query {
     accountByAddress(address: "GA...VICTIM") {
       stateChanges(first: 5) {
         edges {
           node {
             __typename
             type
             reason
           }
         }
       }
     }
   }
   ```
3. Observe the victim account's state-change history.

## Target Code

- `internal/serve/middleware/middleware.go:16-44` — auth verifies only that a JWT is valid
- `internal/serve/serve.go:185-236` — no subject is propagated into GraphQL execution context
- `internal/serve/graphql/resolvers/queries.resolvers.go:70-81` — arbitrary account selection by address
- `internal/serve/graphql/resolvers/account.resolvers.go:108-172` — state changes are fetched by the parent account address
- `internal/serve/graphql/schema/statechange.graphqls:1-18` — the exposed interface includes linked account/operation/transaction context

## Evidence

The resolver extracts optional filters, then executes `r.models.StateChanges.BatchGetByAccountAddress(ctx, string(obj.StellarAddress), ...)`. The `BaseStateChange` schema shows that successful exploitation reveals structured account-linked history, not just one scalar field.

## Anti-Evidence

Input validation exists for transaction-hash filters and pagination cursors. Those checks prevent malformed requests, but they do not enforce that the caller owns the account whose state changes are being queried.

---

## Review

**Verdict**: NOT_VIABLE
**Date**: 2026-04-21
**Reviewed by**: claude-opus-4-6, high
**Novelty**: FAIL — duplicate of 001-account-balances-authz-bypass, 002-account-transactions-authz-bypass, and 003-account-operations-authz-bypass
**Failed At**: reviewer

### Trace Summary

This hypothesis is the fourth iteration of the same root mechanism already rejected in H001, H002, and H003. All four hypotheses claim that `AccountByAddress` enables cross-tenant data access because the JWT auth middleware does not propagate caller identity into the GraphQL context. The only variation is the target resolver field — `balances` (H001), `transactions` (H002), `operations` (H003), and now `stateChanges` (H004). The comprehensive auth model analysis from H001 applies identically here.

### Code Paths Examined

- `internal/serve/middleware/middleware.go:16-44` — `AuthenticationMiddleware` verifies JWT validity only, stores nothing in context (same as H001/H002/H003)
- `internal/serve/serve.go:185-236` — GraphQL route gated by middleware, no identity propagated (same as H001/H002/H003)
- `internal/serve/graphql/resolvers/queries.resolvers.go:70-81` — `AccountByAddress` validates address syntax, returns `Account` for any valid address (same as H001/H002/H003)
- `internal/serve/graphql/resolvers/account.resolvers.go:108-172` — `StateChanges` resolver calls `BatchGetByAccountAddress` using `obj.StellarAddress` from the parent account object

### Why It Failed

Duplicate of H001, H002, and H003. The wallet-backend JWT auth model authenticates trusted client applications (service-to-service), not individual end-users. The JWT `Subject` claim is a client application's public key from the `client-auth-public-keys` server configuration, not a Stellar account address. There is no concept of "the JWT subject's own account" — the JWT subject identifies a trusted client app, and all GraphQL queries are designed to serve data for any requested address to any authenticated client. Additionally, Stellar state-change data derives from publicly available blockchain data, so per-account access control provides no security benefit. This is working-as-designed behavior, not an authorization bypass.

### Lesson Learned

The same "any authenticated caller can query any address" pattern has now been hypothesized four times across different resolver fields (balances, transactions, operations, state changes). All share the identical root cause misunderstanding: the auth model is app-level, not user-level. Any future hypothesis claiming per-account authz bypass on any `Account` field resolver should be immediately rejected as a duplicate of H001 without further code tracing.
