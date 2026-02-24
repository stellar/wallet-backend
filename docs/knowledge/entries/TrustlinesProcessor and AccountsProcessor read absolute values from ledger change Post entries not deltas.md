---
context: LedgerChangeProcessor receives Pre+Post pairs; reading Post directly gives current absolute state without needing to apply delta arithmetic
type: pattern
status: active
subsystem: ingestion
areas: [ingestion, indexer]
created: 2026-02-24
---

# TrustlinesProcessor and AccountsProcessor read absolute values from ledger change Post entries not deltas

`TrustlinesProcessor` and `AccountsProcessor` both implement `LedgerChangeProcessor` and receive ledger change records that contain both `Pre` (state before the operation) and `Post` (state after the operation) entries. Rather than computing the delta between Pre and Post, both processors read directly from the `Post` entry to capture the absolute current state.

For trustlines, this means reading the `Post.TrustLine` struct to get the current balance, limit, and flags. For accounts, it means reading `Post.Account` for balance, sequence number, subentry count, sponsorship counts, and signers.

This design choice produces a full-snapshot record per change: every row in the `trustline_balances` or `account_balances` hypertable represents the complete state of that entity at that point in time, not an incremental change. This makes time-travel queries trivial (find the row at or before timestamp T) but means each change record is larger than a diff-based approach would require.

The tradeoff is storage size vs query simplicity. Wallet UIs need current balances, not accumulated deltas, so point-in-time snapshots align with the primary read pattern.

---

Relevant Notes:
- [[AccountsProcessor calculates minimum balance from numSubEntries numSponsoring and numSponsored to track reserve requirements]] — additional derived field computed during Post reading
- [[the unified StateChange struct uses nullable fields and a category discriminator to store all state change types in a single database table]] — how these snapshots are stored

Areas:
- [[entries/ingestion]]
