---
context: UnassignTxAndUnlockChannelAccounts runs inside PersistLedgerData's DB transaction; if the ledger commit rolls back, the unlock also rolls back
type: insight
created: 2026-02-24
---

Channel account unlock is not a separate post-commit step — it is performed inside the same database transaction that commits the ledger data. `UnassignTxAndUnlockChannelAccounts` is called within `PersistLedgerData`, meaning the unlock is atomic with the ledger write. This ensures that a channel account is only freed once the corresponding ledger has been durably recorded. If the ledger commit fails and rolls back, the channel account remains locked and will be retried when the ledger is reprocessed. This is a cross-subsystem linkage between the GraphQL mutation path (which locks the account) and the ingestion pipeline (which releases it). The unlock SQL sets `locked_tx_hash=NULL, locked_at=NULL, locked_until=NULL` WHERE `locked_tx_hash = ANY(txHashes)` — the match is always on the inner transaction hash, not the outer fee-bump hash. See [[channel account unlock is keyed on inner transaction hashes so fee-bumped transactions release correctly]].

The time-bounded lock from [[channel account time-bounded locks auto-release after 30 seconds if a client crashes after receiving the signed XDR]] is the fallback when this on-chain confirmation path never fires — for example when the client crashes after receiving the XDR without submitting it.

Areas: [[entries/signing]] | [[entries/ingestion]]
