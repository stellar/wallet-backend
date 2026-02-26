---
paths:
  - "internal/serve/middleware/**"
  - "internal/validators/**"
  - "pkg/wbclient/auth/**"
---

# Knowledge Route: Authentication

Read before working in this subsystem:
- `docs/knowledge/references/authentication.md`
- `docs/knowledge/entries/authentication.md`

## Section Focus

The map has a flat structure (3 entries) — read it in full.

## Inline Gotchas

- Auth is **opt-in**: empty `CLIENT_AUTH_PUBLIC_KEYS` disables the auth middleware entirely — the server runs unauthenticated
- JWT tokens are **bound to a specific request body and method-path** — prevents replay attacks across different endpoints
- Soroban auth check validates that the submitted transaction's auth entries match the expected contract invocation
