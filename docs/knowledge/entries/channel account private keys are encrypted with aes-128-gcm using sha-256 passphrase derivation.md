---
description: SHA-256(passphrase)[:16] derives 128-bit key; random 12-byte nonce prepended to ciphertext; output is base64(nonce || gcm_ciphertext_with_tag); decryption on every sign call
type: insight
status: current
confidence: proven
subsystem: signing
areas: [signing, encryption, aes-gcm, channel-accounts, security]
---

# Channel account private keys are encrypted with AES-128-GCM using SHA-256 passphrase derivation

## Context

Channel account private keys must be stored in the database but protected from unauthorized access. A symmetric encryption scheme with a passphrase-derived key is used.

## Detail

`DefaultPrivateKeyEncrypter` in `internal/signing/utils/encrypter.go`:

**Encrypt:**
1. Derive 128-bit key: `SHA-256(passphrase)[:16]`
2. Create AES cipher and GCM AEAD block
3. Generate random 12-byte nonce (`rand.Read`)
4. Encrypt: `gcm.Seal(nonce, nonce, message, nil)` — prepends nonce to ciphertext
5. Output: `base64(nonce || gcm_ciphertext_with_authentication_tag)`

**Decrypt:**
1. Derive key (same steps 1-2)
2. `base64.Decode(encryptedMessage)`
3. Split: `nonce = decoded[:12]`, `ciphertext = decoded[12:]`
4. `gcm.Open(nil, nonce, ciphertext, nil)` — authenticates and decrypts

The GCM authentication tag provides integrity verification — any tampering with the ciphertext or nonce causes `gcm.Open` to return an error.

## Implications

- The passphrase must be kept secret — it's the sole protection for all channel account keys. Rotate it by re-encrypting all keys.
- AES-128-GCM is not the strongest option (AES-256 is preferred by modern standards), but it's sufficient for this use case.
- Every `SignStellarTransaction` call on a channel account client decrypts the key from DB. This adds DB read + AES decrypt latency to every signing operation.

## Source

`internal/signing/utils/encrypter.go:DefaultPrivateKeyEncrypter`

## Related

The three signing strategies — ENV (in-memory), KMS (AWS decrypt per call), and the AES-GCM scheme described here — are compared in [[env signature client is appropriate for development kms for production distribution account]]: channel accounts always use this DB-backed AES-GCM provider.

Like [[kms provider decrypts private key on every signing call with no caching by design]], this scheme decrypts on every signing call — neither provider caches key material in process memory between calls.

relevant_notes:
  - "[[env signature client is appropriate for development kms for production distribution account]] — grounds this: that entry positions the three providers; channel accounts always use this AES-GCM scheme regardless of distribution account choice"
  - "[[kms provider decrypts private key on every signing call with no caching by design]] — synthesizes with this: both share the per-call decrypt principle; channel accounts use symmetric AES-GCM, KMS uses asymmetric AWS API — same security property, different mechanisms"
