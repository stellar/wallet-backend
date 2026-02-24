---
context: The first 16 bytes of SHA-256(passphrase) are used as the AES key — not PBKDF2, not argon2, not scrypt; fast but provides no key stretching against brute-force on weak passphrases
type: insight
status: active
subsystem: signing
areas: [signing, security, channel-accounts]
created: 2026-02-24
---

AES-128-GCM key derivation uses `SHA-256(passphrase)[:16]` — SHA-256 truncation rather than a password-based KDF. The `DefaultPrivateKeyEncrypter` derives the 16-byte AES key by taking the first 16 bytes of `SHA-256(CHANNEL_ACCOUNT_ENCRYPTION_PASSPHRASE)`. This is computationally fast, which is a weakness: an attacker with the ciphertext and the ability to guess passphrases can iterate billions of candidates per second. PBKDF2, argon2, or scrypt would add deliberate computational cost to slow brute-force. A high-entropy passphrase from a secret manager makes this a non-issue in practice; a low-entropy human-chosen passphrase represents real risk.
