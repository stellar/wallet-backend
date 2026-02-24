---
description: Knowledge map for the signing and channel accounts subsystem
type: reference
subsystem: signing
areas: [knowledge-map, signing, channel-accounts, kms, encryption]
vault: docs/knowledge
---

# Signing & Channel Accounts Knowledge Map

The signing subsystem provides pluggable signature providers (ENV for dev, KMS for production) and manages channel accounts — pre-funded Stellar accounts used to parallelize transaction submission without sequence number conflicts.

**Key code:** `internal/signing/`, `internal/services/channel_account_service.go`

## Reference Doc

[[references/signing-and-channels]] — SignatureClient interface, ENV/KMS providers, channel account lifecycle, encryption scheme

## Decisions

<!-- - [[entries/why-channel-accounts]] -->
<!-- - [[entries/why-nacl-encryption]] -->

## Insights

<!-- - [[entries/channel-account-locking-mechanism]] -->

## Patterns

<!-- - [[entries/pluggable-signing-provider]] -->

## Gotchas

<!-- - [[entries/channel-account-sequence-number-drift]] -->

## Cross-Subsystem Connections

- [[entries/ingestion]] — the ingestion pipeline's six-step persist transaction includes channel account unlock as step 4 (`UnassignTxAndUnlockChannelAccounts()`); this is the only place outside the transaction submission path where channel account state transitions occur; see [[the six-step persist transaction ordering exists to satisfy foreign key prerequisites]] for why the unlock must be inside the atomic data-insert transaction

---

Agent Notes:
- 2026-02-24: channel account unlock inside PersistLedgerData() is the key ingestion↔signing coupling; if channel account lock/unlock semantics change, ingestion's six-step transaction must be revisited

## Topics

[[entries/index]] | [[references/signing-and-channels]] | [[entries/ingestion]]
