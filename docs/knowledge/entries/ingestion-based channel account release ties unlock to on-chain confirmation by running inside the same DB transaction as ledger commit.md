---
context: UnassignTxAndUnlockChannelAccounts runs inside PersistLedgerData's DB transaction; if the ledger commit rolls back, the unlock also rolls back
type: insight
created: 2026-02-24
---

Channel account unlock is not a separate post-commit step — it is performed inside the same database transaction that commits the ledger data. `UnassignTxAndUnlockChannelAccounts` is called within `PersistLedgerData`, meaning the unlock is atomic with the ledger write. This ensures that a channel account is only freed once the corresponding ledger has been durably recorded. If the ledger commit fails and rolls back, the channel account remains locked and will be retried when the ledger is reprocessed. This is a cross-subsystem linkage between the GraphQL mutation path (which locks the account) and the ingestion pipeline (which releases it).
