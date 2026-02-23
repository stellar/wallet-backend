---
description: JWT/Ed25519 auth flow, client library usage, and Soroban contract invocation auth
type: reference
subsystem: auth
areas: [auth, jwt, ed25519, soroban]
status: partial
vault: docs/knowledge
---

# Authentication Architecture

## Auth Flow

<!-- TODO: Read internal/serve/middleware/middleware.go (look for AuthenticationMiddleware or similar).
     Create ASCII diagram:
       Client Request
         → Extract JWT from Authorization header
         → Verify Ed25519 signature against CLIENT_AUTH_PUBLIC_KEYS
         → Decode claims (expiry, issuer, etc.)
         → Inject authenticated context
         → Handler

     Document:
     - JWT structure (header, claims, signature)
     - Which Ed25519 curve is used
     - How multiple public keys are supported (CLIENT_AUTH_PUBLIC_KEYS)
     - Error responses for invalid/expired/missing tokens -->

## Client Library

<!-- TODO: Read pkg/wbclient/auth/ for JWT creation and HTTP signing.
     Document:
     - How clients create JWT tokens
     - How tokens are attached to HTTP requests
     - Token expiry and refresh patterns
     - Example usage of the client library -->

## Soroban Auth

<!-- TODO: Read pkg/sorobanauth/ for contract invocation auth.
     Document:
     - What Soroban auth is and when it's used
     - How contract invocation authorization works
     - The auth entry creation flow
     - Integration with the signing providers -->


---

**Topics:** [[entries/index]] | [[entries/authentication]]
