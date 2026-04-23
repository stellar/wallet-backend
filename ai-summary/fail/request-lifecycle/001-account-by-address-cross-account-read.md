# H001: accountByAddress allows arbitrary cross-account reads with any valid JWT

**Date**: 2026-04-21
**Subsystem**: request-lifecycle
**Severity**: High
**Impact**: Confidentiality / GraphQL authz bypass returning cross-tenant account data
**Hypothesis by**: gpt-5.4, high

## Expected Behavior

A JWT that proves control of one whitelisted Stellar key should only authorize reads for that caller's own account data, or for some explicitly authorized account set. A request for `accountByAddress(address: "<victim>")` should be rejected when the verified JWT subject does not match `<victim>`.

## Mechanism

The HTTP auth layer only answers the boolean question "is this request signed by any configured key" and then discards the verified claims. `accountByAddress` accepts any syntactically valid Stellar address, constructs an `Account` object for that address, and the `Account` field resolvers then read balances, transactions, operations, and state changes for that user-controlled address without comparing it to the authenticated subject.

## Trigger

1. Obtain a valid JWT for an attacker-controlled whitelisted key.
2. Send `POST /graphql/query` with `query { accountByAddress(address: "<victim G-address>") { balances(first: 10) { edges { node { ... on NativeBalance { balance } ... on TrustlineBalance { code issuer balance } } } } transactions(first: 10) { edges { node { hash } } } } }`.
3. Observe that the server returns the victim account's balance and history instead of rejecting the cross-account read.

## Target Code

- `pkg/wbclient/auth/jwt_http_signer_verifier.go:VerifyHTTPRequest:95-103` — verifies the signature but returns only success/failure, not the caller identity
- `internal/serve/middleware/middleware.go:AuthenticationMiddleware:25-27` — forwards the request after a successful boolean auth check without attaching claims to context
- `internal/serve/graphql/resolvers/queries.resolvers.go:AccountByAddress:70-82` — accepts any valid address supplied by the caller
- `internal/serve/graphql/resolvers/account.resolvers.go:Balances/Transactions/Operations/StateChanges:25-173` — reads account-scoped data keyed solely by `obj.StellarAddress`

## Evidence

`VerifyHTTPRequest` discards the parsed claims (`_, _, err := s.parser.ParseJWT(...)`), so no verified subject survives into GraphQL execution. `AuthenticationMiddleware` simply calls `next.ServeHTTP` on success, while `AccountByAddress` returns `&types.Account{StellarAddress: types.AddressBytea(address)}` for any valid address and each `Account` resolver uses that user-controlled address directly in database reads.

## Anti-Evidence

The JWT still provides strong proof that the caller controls some whitelisted key, so this is not an unauthenticated bypass. Invalid Stellar addresses and malformed transaction hashes are rejected, which limits the issue to authorization rather than input validation.

---

## Review

**Verdict**: NOT_VIABLE
**Date**: 2026-04-21
**Reviewed by**: claude-opus-4-6, high
**Novelty**: PASS — not previously investigated
**Failed At**: reviewer

### Trace Summary

Traced the full request lifecycle from `VerifyHTTPRequest` through `AuthenticationMiddleware` into the `accountByAddress` resolver and its child resolvers (`Balances`, `Transactions`, `Operations`, `StateChanges`). Confirmed that the JWT claims are discarded after verification and no caller identity is propagated to the resolver layer. However, also confirmed that this is intentional design, not a bug, because the wallet-backend indexes public Stellar blockchain data and provides unrestricted read access to any authenticated client.

### Code Paths Examined

- `pkg/wbclient/auth/jwt_http_signer_verifier.go:VerifyHTTPRequest:95-103` — confirmed `_, _, err := s.parser.ParseJWT(...)` discards claims; returns only error
- `internal/serve/middleware/middleware.go:AuthenticationMiddleware:22-28` — confirmed boolean auth gate with no claims propagated to context
- `internal/serve/graphql/resolvers/queries.resolvers.go:AccountByAddress:71-82` — confirmed returns `Account` for any valid address
- `internal/serve/graphql/resolvers/queries.resolvers.go:Transactions:39-68` — root query returns ALL transactions with no account scoping
- `internal/serve/graphql/resolvers/queries.resolvers.go:Operations:86-115` — root query returns ALL operations with no account scoping
- `internal/serve/graphql/resolvers/queries.resolvers.go:StateChanges:124-154` — root query returns ALL state changes with no account scoping
- `internal/serve/graphql/resolvers/account.resolvers.go:25-173` — account resolvers use `obj.StellarAddress` directly

### Why It Failed

The hypothesis describes **working-as-designed behavior** and incorrectly states the expected behavior. The wallet-backend is a read-only indexer over public Stellar blockchain data — all account balances, transactions, operations, and state changes on the Stellar ledger are inherently public and queryable by anyone via Horizon or stellar-rpc. The JWT authentication gates access to the API service itself, not to specific accounts' data.

Three pieces of evidence confirm this is by-design:

1. **Root queries expose all data without scoping.** The `Transactions`, `Operations`, and `StateChanges` root queries return ALL indexed data with pagination but no account filter. If per-account authorization were intended, these queries would also need scoping — their existence proves the API deliberately exposes all indexed ledger data.

2. **The subsystem's own security surface documentation states this explicitly**: "Any valid token signed by any whitelisted public key can query any account, transaction, or state change. Resolver-level authorization does not exist."

3. **Trust model alignment.** The objective's trust model states "The server custodies no funds but gates access to account metadata" — meaning the auth gates API access, not per-account data. Per the severity criteria, "GraphQL auth/authz bypass" requires bypassing authorization that exists. There is no per-account authorization to bypass.

### Lesson Learned

When evaluating cross-account read hypotheses against blockchain indexers, always check whether the underlying data is inherently public. A read-only index over a public ledger has no confidentiality boundary between accounts — the JWT authentication is an API-access gate, not a data-isolation mechanism. Also check for unscoped root queries (e.g., `Transactions` without account filter) as evidence that the API intentionally exposes all data to any authenticated caller.
