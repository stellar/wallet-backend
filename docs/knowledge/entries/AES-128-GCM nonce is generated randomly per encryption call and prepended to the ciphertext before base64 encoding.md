---
context: Nonce is never reused because it is freshly generated on each call; output format is base64(nonce || gcm_ciphertext_with_tag)
type: pattern
status: active
subsystem: signing
areas: [signing, security, channel-accounts]
created: 2026-02-24
---

The AES-128-GCM nonce is generated randomly per encryption call and prepended to the ciphertext before base64 encoding. Each call to `Encrypt` generates a fresh 12-byte random nonce using `crypto/rand`. The nonce is then prepended to the GCM-authenticated ciphertext (which includes the authentication tag) and the entire byte sequence is base64-encoded for storage in the database. Decryption reverses this by splitting the first 12 bytes as the nonce and passing the remainder as the ciphertext+tag to GCM. Fresh nonces per call prevent nonce reuse, which would be catastrophic for GCM security.

The key fed into GCM is derived by [[AES-128-GCM key derivation uses SHA-256(passphrase) truncated to 16 bytes rather than a password-based KDF]] — nonce freshness is the scheme's strongest security property; the key derivation is the weaker link for low-entropy passphrases.

Areas: [[entries/signing]]
