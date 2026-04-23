# H002: root ledger queries expose global history to any whitelisted JWT

**Date**: 2026-04-21
**Subsystem**: request-lifecycle
**Severity**: High
**Impact**: Confidentiality / GraphQL authz bypass returning global transaction and state-change data
**Hypothesis by**: gpt-5.4, high

## Expected Behavior

Authenticated callers should not be able to enumerate the entire indexed ledger unless the API explicitly defines a privileged role for that subject. Root queries that return transactions, operations, or state changes should either scope results to the verified caller or reject the request for non-privileged identities.

## Mechanism

The request lifecycle never propagates the verified JWT subject into resolver authorization, and the root GraphQL schema exposes global collection queries plus direct object lookups. As a result, any valid JWT can call `transactions`, `operations`, `stateChanges`, `transactionByHash`, or `operationById` and receive records unrelated to the caller, enabling broad cross-tenant history extraction without even knowing a victim address up front.

## Trigger

1. Obtain a valid JWT for any configured client key.
2. Send `query { transactions(first: 50) { edges { node { hash ledgerNumber accounts { address } } } } }` or `query { stateChanges(first: 50) { edges { node { account { address } type reason } } } }`.
3. Observe that the API returns globally indexed ledger data rather than data scoped to the JWT subject.

## Target Code

- `internal/serve/graphql/schema/queries.graphqls:3-10` — exposes global `transactions`, `operations`, `stateChanges`, `transactionByHash`, and `operationById` entry points
- `pkg/wbclient/auth/jwt_http_signer_verifier.go:VerifyHTTPRequest:95-103` — verifies but discards the caller identity
- `internal/serve/middleware/middleware.go:AuthenticationMiddleware:25-27` — does not attach claims or role information to the request context
- `internal/serve/graphql/resolvers/queries.resolvers.go:TransactionByHash/Transactions/Operations/OperationByID/StateChanges:19-154` — serves global records with no subject-based filtering

## Evidence

The schema exposes global root collections and direct object lookups. The corresponding resolvers call `r.models.Transactions.GetAll`, `r.models.Operations.GetAll`, `r.models.StateChanges.GetAll`, `r.models.Transactions.GetByHash`, and `r.models.Operations.GetByID` directly, and none of those resolver paths read any authenticated identity from context because none was stored there.

## Anti-Evidence

The GraphQL endpoint is not public: requests still require a valid JWT from a configured public key. Complexity limits and pagination restrict query volume, but they do not reduce the authorization gap once a caller is authenticated.

---

## Review

**Verdict**: NOT_VIABLE
**Date**: 2026-04-21
**Reviewed by**: claude-opus-4-6, high
**Novelty**: PASS — not previously investigated
**Failed At**: reviewer

### Trace Summary

Traced the full request lifecycle from JWT verification through GraphQL resolution. Confirmed that `VerifyHTTPRequest` (line 97) discards the parsed token and claims (`_, _, err := s.parser.ParseJWT(...)`), `AuthenticationMiddleware` does not attach identity to context, and all root query resolvers (`Transactions`, `Operations`, `StateChanges`, `TransactionByHash`, `OperationByID`) call unscoped model methods. However, this is the intended architecture of the system.

### Code Paths Examined

- `pkg/wbclient/auth/jwt_http_signer_verifier.go:VerifyHTTPRequest:95-103` — confirmed token/claims return values are discarded; only error is checked
- `internal/serve/middleware/middleware.go:AuthenticationMiddleware:22-29` — confirmed no context enrichment with caller identity; just pass/fail gate
- `internal/serve/graphql/schema/queries.graphqls:3-10` — confirmed global root queries including `transactions`, `operations`, `stateChanges` with no caller-scoping parameters
- `internal/serve/graphql/resolvers/queries.resolvers.go:19-154` — confirmed all resolvers call unscoped `GetAll`/`GetByHash`/`GetByID` with no identity filtering
- `internal/serve/graphql/resolvers/queries.resolvers.go:71-82` — `AccountByAddress` takes an explicit `address` parameter, confirming the design intent is for clients to look up arbitrary accounts

### Why It Failed

This describes **working-as-designed behavior**, not a bug. The wallet-backend is a Stellar blockchain indexer, and all Stellar ledger data is inherently public — anyone can query any transaction, account, or operation on the public Stellar network via Horizon or any block explorer. The system has no multi-tenant concept: there is no "your data" vs "someone else's data" because the underlying blockchain is a public ledger. The JWT authentication gates API access (only trusted clients may use the indexer), not data visibility. The `accountByAddress(address: String!)` query takes an explicit address argument — confirming the design intent that callers look up data about any account, not just "their" account. The hypothesis's "Expected Behavior" asserts a per-caller data-scoping model that the system was never designed to implement.

### Lesson Learned

When evaluating authorization gaps on a blockchain indexer, consider whether the underlying data has a meaningful confidentiality property. Stellar ledger data is public, so serving it to any authenticated client is not an authz bypass — it is the intended function of the service. A true authz finding here would require showing that the API exposes *non-public* data (e.g., server-internal state, private keys, or operator secrets) to unauthorized callers, not that it serves public blockchain records without per-caller scoping.
