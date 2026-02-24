---
description: Knowledge map for the signing and channel accounts subsystem
type: reference
subsystem: signing
areas: [knowledge-map, signing, channel-accounts, kms, encryption]
vault: docs/knowledge
---

# Signing & Channel Accounts Knowledge Map

The signing subsystem provides pluggable signature providers (ENV for dev, KMS for production) and manages channel accounts — pre-funded Stellar accounts used to parallelize transaction submission without sequence number conflicts. Channel account security rests on two layers: encrypted at-rest private keys with AES-128-GCM and time-bounded database locks that prevent concurrent acquisition races.

**Key code:** `internal/signing/`, `internal/services/channel_account_service.go`

## Reference Doc

[[references/signing-and-channels]] — SignatureClient interface, ENV/KMS providers, channel account lifecycle, encryption scheme

## Architecture: Two Independent Signing Identities

- [[the distribution account and channel accounts are two independent signing identities with different providers]] — the architecture split: distribution uses ENV/KMS, channel accounts use DB-backed CHANNEL_ACCOUNT provider; never interchangeable

## Signing Providers

- [[ENV provider validates that stellarAccounts matches the distribution account on every sign call to prevent misuse]] — even the simplest provider guards against routing signing requests to non-distribution accounts
- [[KMS provider decrypts the private key on every signing call with no in-memory caching]] — intentional security trade-off: key never persists in process memory beyond one signing call, at the cost of per-call KMS API latency
- [[KMS encryption context binds the ciphertext to a specific Stellar public key preventing cross-account decryption]] — AWS encryption context as account binding; ciphertext for account A cannot be decrypted presenting account B's context
- [[GetAccountPublicKey uses a variadic opts parameter as a channel-account-specific lock-duration extension that ENV and KMS providers silently ignore]] — leaky abstraction: lock-duration behavior bleeds through a shared interface that ENV/KMS providers accept but ignore

## Channel Account Encryption (At-Rest Security)

- [[AES-128-GCM key derivation uses SHA-256(passphrase) truncated to 16 bytes rather than a password-based KDF]] — mechanism: `SHA-256(CHANNEL_ACCOUNT_ENCRYPTION_PASSPHRASE)[:16]`; fast hash, no key stretching
- [[AES-128-GCM nonce is generated randomly per encryption call and prepended to the ciphertext before base64 encoding]] — fresh 12-byte nonce per call prevents GCM nonce reuse; output format: `base64(nonce || ciphertext+tag)`

## Channel Account Provisioning & Lifecycle

- [[channel accounts are created with amount=0 because the distribution account sponsors the reserve via BeginSponsoringFutureReserves]] — sponsorship eliminates per-account XLM funding; only transaction fees are needed
- [[MaximumCreateAccountOperationsPerStellarTx equals 19 because sponsored account creation requires 3 operations and Stellar imposes a 100-operation payload cap]] — 19 × 3 = 57 ops, comfortably below the 100-op Stellar limit
- [[ensureChannelAccounts runs in a goroutine at startup so the HTTP server becomes ready before channel account provisioning completes]] — non-blocking startup; implication: cold starts may briefly return ErrNoChannelAccountConfigured

## Channel Account Acquisition & Locking

- [[channel account acquisition is a single atomic UPDATE-in-subselect that eliminates TOCTOU races between SELECT and UPDATE]] — `UPDATE ... WHERE public_key = (SELECT ... FOR UPDATE SKIP LOCKED LIMIT 1)` — atomic identify-and-lock in one statement
- [[FOR UPDATE SKIP LOCKED on channel accounts means concurrent callers never block they skip held rows and acquire different accounts]] — SKIP LOCKED makes busy accounts invisible rather than causing waiters to queue; enables lock-free concurrent acquisition
- [[ORDER BY random() in channel account acquisition distributes load across all idle accounts]] — prevents hot-spots on lowest-PK account; ensures even utilization of all provisioned channel accounts
- [[channel account time-bounded locks auto-release after 30 seconds if a client crashes after receiving the signed XDR]] — safety valve for crash case; normal unlock path is on-chain confirmation via ingestion
- [[MaxTimeoutInSeconds caps transaction time bounds at 5 minutes preventing channel accounts from being locked indefinitely by far-future expiry transactions]] — `min(requestedTimeout, 300)`; bounds worst-case lock window regardless of caller behavior
- [[channel account unlock is keyed on inner transaction hashes so fee-bumped transactions release correctly]] — `locked_tx_hash` stores inner hash; outer fee-bump hash differs; matching on inner prevents permanent lock after fee-bump
- [[the two error types ErrNoIdleChannelAccountAvailable and ErrNoChannelAccountConfigured distinguish between pool exhaustion and empty pool]] — pool exhaustion (add accounts or reduce concurrency) vs empty pool (run ensure command); different operator responses

## Transaction Building & Fee-Bump

- [[createFeeBumpTransaction requires the inner transaction to already be signed before wrapping and returns ErrNoSignaturesProvided otherwise]] — distribution account only adds outer fee-bump signature; clients must pass signed inner XDR
- [[mutations do not submit transactions — clients receive signed XDR and submit directly to Stellar network]] — security boundary: wallet-backend never proxies submission; client crash after receiving XDR loses the transaction
- [[fee-bump eligibility check prevents the distribution account from sponsoring arbitrary non-tracked accounts]] — `IsAccountFeeBumpEligible` queries the ingested accounts table; fee-bump is for managed accounts only

## Cross-Subsystem Connections

- [[entries/ingestion]] — the ingestion pipeline's six-step persist transaction includes channel account unlock as step 4 (`UnassignTxAndUnlockChannelAccounts()`); this is the only place outside the transaction submission path where channel account state transitions occur; see [[the six-step persist transaction ordering exists to satisfy foreign key prerequisites]] for why the unlock must be inside the atomic data-insert transaction
- [[ingestion-based channel account release ties unlock to on-chain confirmation by running inside the same DB transaction as ledger commit]] — the full lifecycle: `BuildAndSignTransaction` locks the account (signing subsystem); `PersistLedgerData` releases it atomically with the ledger commit (ingestion subsystem)
- [[entries/graphql-api]] — mutations (`BuildAndSignTransaction`, `FeeBump`) are the acquisition trigger; the GraphQL resolver calls into the signing service; channel account state persists in the DB between the mutation response and ingestion confirmation

## Tensions

- [[GetAccountPublicKey uses a variadic opts parameter as a channel-account-specific lock-duration extension that ENV and KMS providers silently ignore]] and [[the distribution account and channel accounts are two independent signing identities with different providers]] create an interface design tension: the shared `SignatureClient` interface carries channel-account-specific behavior that other providers must silently swallow; a richer interface (or separate types) would be cleaner but would break the uniform substitution currently used in tests

## Open Questions

- [[AES-128-GCM key derivation via SHA-256 truncation trades deployment simplicity against missing key stretching for low-entropy passphrases]] — unresolved: should `CHANNEL_ACCOUNT_ENCRYPTION_PASSPHRASE` have an entropy requirement enforced at startup? The current design assumes high-entropy injection from a secrets manager but does not verify this

---

Agent Notes:
- 2026-02-24: channel account unlock inside PersistLedgerData() is the key ingestion↔signing coupling; if channel account lock/unlock semantics change, ingestion's six-step transaction must be revisited
- 2026-02-24: populated all 20+ signing entries created 2026-02-24; grouped into 7 clusters; cross-subsystem path to ingestion and graphql-api added; interface design tension between SKIP LOCKED entries and provider-architecture entries flagged; AES-128-GCM key-derivation question is the only unresolved open question in this subsystem

## Topics

[[entries/index]] | [[references/signing-and-channels]] | [[entries/ingestion]] | [[entries/graphql-api]]
