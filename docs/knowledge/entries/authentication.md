---
description: Knowledge map for the authentication subsystem — HTTP JWT/Ed25519 middleware, client library, and Soroban contract invocation auth
type: reference
status: current
subsystem: auth
areas: [navigation, auth]
---

# Authentication — Knowledge Map

Two independent authentication systems: HTTP request authentication (JWT/Ed25519 middleware gating the GraphQL API) and Soroban contract invocation auth (XDR-level authorization entries for smart contract calls).

## HTTP Authentication

- [[authentication is opt-in empty client-auth-public-keys disables the auth middleware entirely]] — The binary auth model; development vs production configuration
- [[jwt auth binds each token to a specific request body and method-path to prevent replay attacks]] — bodyHash + methodAndPath binding; short expiry; no session tokens

## Soroban Authorization

- [[soroban auth check prevents channel accounts from being used as contract signers]] — CheckForForbiddenSigners guard in adjustParamsForSoroban; FORBIDDEN_SIGNER error code

## Cross-References

- For channel account security context: [[signing]] map
- For middleware placement in the request stack: [[graphql-api]] map

---

Agent Notes:
- 2026-02-26: [[authentication is opt-in empty client-auth-public-keys disables the auth middleware entirely]] and [[jwt auth binds each token to a specific request body and method-path to prevent replay attacks]] are ordered: first decide if auth is on (opt-in entry), then understand what auth enforces (JWT binding entry).
- 2026-02-26: [[soroban auth check prevents channel accounts from being used as contract signers]] connects auth to signing — the channel account pool design (in the signing map) is the security context for the Soroban guard.
