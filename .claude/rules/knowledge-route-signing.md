---
paths:
  - "internal/signing/**"
  - "internal/services/transaction_service*"
  - "internal/services/channel_account*"
---

# Knowledge Route: Signing & Channel Accounts

Read before working in this subsystem:
- `docs/knowledge/references/signing-and-channels.md`
- `docs/knowledge/entries/signing.md`

## Section Focus

The map has a flat structure (6 entries) — read it in full. Key areas:
- Channel account lifecycle: acquisition, usage, release atomicity
- Provider selection: ENV for dev, KMS for production distribution account

## Inline Gotchas

- Channel account acquisition uses **`FOR UPDATE SKIP LOCKED`** — concurrent callers get different accounts, never block each other
- KMS provider **decrypts the private key on every signing call** — no caching by design (security requirement)
- Channel account private keys are encrypted with **AES-128-GCM** using SHA-256 passphrase derivation
- `buildTransaction` **returns signed XDR to the client** — it never submits to the network directly
