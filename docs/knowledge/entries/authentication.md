---
description: Knowledge map for the authentication subsystem
type: reference
subsystem: auth
areas: [knowledge-map, auth, jwt, ed25519, soroban]
vault: docs/knowledge
---

# Authentication Knowledge Map

The auth system uses JWT tokens signed with Ed25519 keys. Multiple client public keys are supported (comma-separated `CLIENT_AUTH_PUBLIC_KEYS`). Soroban auth provides contract invocation authorization for Stellar smart contracts.

**Key code:** `internal/serve/middleware/`, `pkg/wbclient/auth/`, `pkg/sorobanauth/`

## Reference Doc

[[references/authentication]] — JWT/Ed25519 flow, client library, Soroban auth

## Decisions

<!-- - [[entries/why-ed25519-not-rsa]] -->

## Insights

<!-- - [[entries/multi-key-auth-support]] -->

## Patterns

<!-- - [[entries/client-jwt-creation-pattern]] -->

## Gotchas

<!-- - [[entries/jwt-clock-skew]] -->

## Topics

[[entries/index]] | [[references/authentication]]
