---
description: No in-memory key material in KMS client; each SignStellarTransaction reads from keypairs table and calls AWS KMS Decrypt; encryption context binds ciphertext to specific public key
type: insight
status: current
confidence: proven
subsystem: signing
areas: [signing, kms, aws, security, distribution-account]
---

# KMS provider decrypts private key on every signing call with no caching by design

## Context

The KMS signing client is designed for production use where the distribution account private key must never reside in process memory for longer than required to sign a single transaction.

## Detail

`kmsSignatureClient.SignStellarTransaction()` flow:
1. Validates that `stellarAccounts` contains only the distribution account's public key.
2. Calls `keypairsDB.GetByPublicKey(distributionAccountPublicKey)` — DB read for `EncryptedPrivateKey` bytes.
3. Calls AWS KMS `Decrypt(ciphertextBlob, encryptionContext: {"pubkey": publicKey}, keyId: kmsKeyARN)`.
4. `keypair.ParseFull(plaintext)` — constructs the signing keypair.
5. `tx.Sign(networkPassphrase, keypairFull)` — signs the transaction.
6. The keypair is garbage-collected after the function returns.

The encryption context `{"pubkey": distributionPublicKey}` was set during the original KMS `Encrypt` call. AWS KMS enforces that the decryption context must match the encryption context — this prevents the ciphertext from being decrypted with a different public key substituted.

## Implications

- Each distribution account signature (fee-bump) requires: 1 DB read + 1 AWS KMS API call. At high fee-bump throughput, this adds up.
- The no-caching design is intentional: even a process compromise cannot extract the key material since it's never stored in memory long-term.
- `SignStellarFeeBumpTransaction` follows the same pattern.

## Source

`internal/signing/kms_signature_client.go:SignStellarTransaction()`
`internal/signing/awskms/utils.go:GetPrivateKeyEncryptionContext()`

## Related

The KMS client is one of three signing strategies described in [[env signature client is appropriate for development kms for production distribution account]] — it is the production option where key material never persists in memory longer than a single signing call.

The same per-call decrypt philosophy applies to channel accounts: [[channel account private keys are encrypted with aes-128-gcm using sha-256 passphrase derivation]] decrypts AES-GCM on every signing call, not once at startup, for the same security rationale.

relevant_notes:
  - "[[env signature client is appropriate for development kms for production distribution account]] — grounds this: KMS is the production choice described in that entry; this entry explains the no-caching mechanism in detail"
  - "[[channel account private keys are encrypted with aes-128-gcm using sha-256 passphrase derivation]] — synthesizes with this: both providers share the per-call decrypt philosophy; KMS uses AWS API, channel accounts use AES-GCM — different mechanisms, same principle"
