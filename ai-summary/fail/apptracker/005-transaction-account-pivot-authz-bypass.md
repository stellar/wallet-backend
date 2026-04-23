# H005: Transaction-to-account traversal bypasses any address-knowledge barrier for cross-tenant reads

**Date**: 2026-04-21
**Subsystem**: apptracker / serve-graphql / auth
**Severity**: High
**Impact**: GraphQL authz bypass / graph-traversal-based cross-tenant disclosure
**Hypothesis by**: gpt-5.4, high

## Expected Behavior

Even if the API exposes transaction-level data, nested account relationships should not let a caller pivot from a public ledger object into private account-scoped reads for unrelated accounts. Once a transaction returns related accounts, any subsequent `Account` subfields should still enforce that the requester is authorized for each account.

## Mechanism

`Query.transactions` returns arbitrary transactions to any valid JWT holder, and `Transaction.accounts` materializes the participating accounts with no ownership check. Those nested `Account` objects can immediately resolve `balances`, `transactions`, `operations`, or `stateChanges`, so an attacker does not even need to know a victim address ahead of time: they can harvest addresses from transaction relationships and pivot into account-scoped data inside the same query.

## Trigger

1. Authenticate as attacker account `GA...ATTACKER`.
2. Send:
   ```graphql
   query {
     transactions(first: 10) {
       edges {
         node {
           hash
           accounts {
             address
             balances(first: 1) {
               edges { node { __typename } }
             }
           }
         }
       }
     }
   }
   ```
3. Observe balances for accounts discovered through unrelated transactions.

## Target Code

- `internal/serve/graphql/schema/queries.graphqls:3-10` — any valid JWT can start from the `transactions` root
- `internal/serve/graphql/resolvers/queries.resolvers.go:36-68` — root transaction listing has no subject-based filtering
- `internal/serve/graphql/resolvers/transaction.resolvers.go:66-84` — `Transaction.accounts` returns related accounts without authz
- `internal/serve/graphql/resolvers/account.resolvers.go:25-28` — nested `Account.balances` trusts the parent account object
- `internal/serve/middleware/middleware.go:16-44` — middleware verifies only validity, not target ownership

## Evidence

The transaction root query fetches global transaction records, `Transaction.accounts` loads all related account rows via dataloaders, and the nested `Account` resolver surface is identical to the direct `accountByAddress` path. There is still no request-context identity available anywhere in that chain to stop the pivot.

## Anti-Evidence

This path still requires a valid JWT and the attacker only learns accounts that already appear in returned transaction data. That limits the issue to authenticated callers, but it does not prevent cross-tenant access once the graph traversal begins.

---

## Review

**Verdict**: NOT_VIABLE
**Date**: 2026-04-21
**Reviewed by**: claude-opus-4-6, high
**Novelty**: FAIL — duplicate of 001-account-balances-authz-bypass (and 002, 003)
**Failed At**: reviewer

### Trace Summary

Traced the `Query.Transactions` → `Transaction.Accounts` → `Account.Balances` path through the resolver chain. The `Transactions` root query calls `r.models.Transactions.GetAll` with no caller filter. `Transaction.Accounts` uses `AccountsByToIDLoader` to batch-load related accounts. Nested `Account.Balances` calls `getAccountBalances` on the parent address. All code paths are real and reachable — but the auth model is service-to-service, not per-user, so there is no "cross-tenant" boundary to bypass.

### Code Paths Examined

- `internal/serve/graphql/resolvers/queries.resolvers.go:36-68` — `Transactions` root query fetches global transactions via `GetAll`, no caller identity filter
- `internal/serve/graphql/resolvers/transaction.resolvers.go:66-84` — `Transaction.Accounts` loads related accounts via `AccountsByToIDLoader` dataloader, no authz check
- `internal/serve/graphql/resolvers/account.resolvers.go:25-28` — `Balances` resolves via `getAccountBalances(ctx, string(obj.StellarAddress), ...)`, trusts parent object
- `internal/serve/middleware/middleware.go:16-44` — `AuthenticationMiddleware` verifies JWT validity only, stores nothing in context
- `pkg/wbclient/auth/jwt_manager.go:47-49` — JWT `Subject` is a client application public key, not an end-user Stellar address (established in H001 review)

### Why It Failed

This is the same root cause as H001, H002, and H003: the hypothesis assumes the JWT binds to an individual Stellar account and that "cross-tenant" access controls should exist. In reality, the wallet-backend JWT auth model authenticates **trusted client applications** (service-to-service), not individual end-users. The JWT `Subject` claim is a client app's public key from the server's `client-auth-public-keys` config — there is no concept of "the account this JWT represents." All GraphQL queries (root-level and nested) are designed to serve data for any address to any authenticated client. The "graph traversal pivot" from transactions to accounts is simply a different entry path to the same by-design behavior already analyzed in H001. Additionally, Stellar ledger data (accounts, balances, transactions) is inherently public on the blockchain — the wallet-backend is an indexer caching this public data, so per-account access control would provide no real security benefit.

### Lesson Learned

This is the fourth hypothesis (H001 balances, H002 transactions, H003 operations, H005 transaction-account pivot) claiming cross-tenant authz bypass through different GraphQL traversal paths. All share the identical misunderstanding: the auth model is app-level, not user-level. The "pivot through transactions" angle does not change the fundamental analysis — if there is no per-user identity binding, there is no tenant boundary to cross regardless of which resolver chain is used as the entry point.
