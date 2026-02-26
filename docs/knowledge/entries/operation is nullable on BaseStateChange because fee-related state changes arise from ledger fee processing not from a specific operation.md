---
context: Fee-phase state changes (e.g., account balance deducted for fees) occur before any operation executes; they have no op_id to reference
type: insight
created: 2026-02-24
status: current
subsystem: ingestion
areas: [ingestion, indexer]
---

The `operation` field on `BaseStateChange` is `Operation` (nullable) rather than `Operation!`. This surprises newcomers who expect every state change to be caused by an operation. Fee-related state changes — such as the deduction of the transaction fee from the source account's balance — occur during the ledger's fee-processing phase, before any operations execute. These state changes have `op_id = NULL` in the database and return `null` for `operation` in GraphQL. All non-fee state changes do link to an operation and will return a non-null value.
