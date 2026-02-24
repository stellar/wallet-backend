---
context: Soroban fee model includes refunds for unused resource fees; net fee = sum of all FEE_EVENT amounts (some negative for refunds) rather than fee minus refund
type: insight
status: active
subsystem: ingestion
areas: [ingestion, indexer]
created: 2026-02-24
---

# TokenTransferProcessor calculates net fee per transaction by summing all fee events rather than tracking individual fee and refund events

When `TokenTransferProcessor` processes a transaction, it sums all `FEE_EVENT` amounts across all token transfer events emitted by the SDK's `EventsProcessor` to produce a single net fee `StateChange` record.

This matters because Soroban transactions have a fee refund mechanism: if a transaction reserves more resource fee than it actually uses, the unused portion is refunded. The SDK emits this as separate events — one FEE_EVENT with the full charged amount and one with a negative amount representing the refund. Rather than storing two separate state changes (charge + refund), `TokenTransferProcessor` collapses them into a single net fee entry.

The implementation iterates all events, accumulates FEE_EVENT totals, and emits one `StateChange` with `Category=FEE` and the net amount. This means the persisted record reflects the actual economic cost of the transaction, not the gross fee before refund — which is the useful quantity for wallet UI display.

---

Relevant Notes:
- [[TokenTransferProcessor processes at transaction level while other state change processors operate per-operation]] — why tx-level processing is needed for correct aggregation
- [[the unified StateChange struct uses nullable fields and a category discriminator to store all state change types in a single database table]] — how FEE state changes are stored

Areas:
- [[entries/ingestion]]
