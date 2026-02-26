---
description: ENV parses private key once at startup into memory with no KMS dependency; KMS decrypts on every signing call with no caching; channel accounts always use DB-backed AES-GCM provider
type: decision
status: current
confidence: proven
subsystem: signing
areas: [signing, kms, env, distribution-account, security]
---

# ENV signature client is appropriate for development; KMS for production distribution account

## Context

The distribution account's private key must be protected. In development, quick setup matters more than key isolation. In production, the key should never reside in process memory.

## Detail

Three `SignatureClient` implementations exist for the distribution account:

- **ENV**: Reads `DISTRIBUTION_ACCOUNT_PRIVATE_KEY` at startup, parses it into an in-memory `keypair.Full`. All signing is in-memory with no DB or network calls. The key lives in process memory for the process lifetime.

- **KMS**: Reads the encrypted key from the `keypairs` DB table on every signing call, then calls AWS KMS `Decrypt` with an encryption context `{"pubkey": distributionPublicKey}`. No caching. The decrypted key is constructed per-call and immediately discarded after signing.

- **CHANNEL_ACCOUNT**: Always used for channel accounts. Keys are stored AES-128-GCM encrypted in `channel_accounts` table. Decryption uses `SHA-256(passphrase)[:16]` as the key. This provider cannot sign fee-bump transactions (`ErrNotImplemented`).

## Implications

- Production operators must use `DISTRIBUTION_ACCOUNT_SIGNATURE_PROVIDER=KMS` and store the encrypted key in the `keypairs` table via `KMSImportService`.
- The ENV provider is appropriate for testnet and local development only — the private key in an environment variable is visible to anyone with process access.
- KMS adds one DB read + one AWS API call per signature. High-throughput fee-bump scenarios should account for this latency.

## Source

`internal/signing/env_signature_client.go`
`internal/signing/kms_signature_client.go`
`internal/signing/channel_account_db_signature_client.go`
`cmd/utils/utils.go:SignatureClientResolver()`

## Related

Channel accounts always use the CHANNEL_ACCOUNT provider described here, which stores keys via [[channel account private keys are encrypted with aes-128-gcm using sha-256 passphrase derivation]] — that AES-GCM scheme is the implementation detail of the DB-backed provider this entry introduces.

The KMS provider's no-caching behavior is elaborated in [[kms provider decrypts private key on every signing call with no caching by design]] — this entry positions the three options; that entry explains the KMS mechanism and its latency implications.

relevant_notes:
  - "[[channel account private keys are encrypted with aes-128-gcm using sha-256 passphrase derivation]] — extends this: AES-GCM is the specific encryption scheme for the CHANNEL_ACCOUNT provider introduced in this entry"
  - "[[kms provider decrypts private key on every signing call with no caching by design]] — extends this: that entry details the per-call decrypt behavior briefly mentioned here for the KMS option"
