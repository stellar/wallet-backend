---
context: SDK EventsProcessor emits token transfer events per-transaction not per-operation; wrapping per-operation would produce incorrect aggregation
type: insight
status: active
subsystem: ingestion
areas: [ingestion, indexer]
created: 2026-02-24
---

# TokenTransferProcessor processes at transaction level while other state change processors operate per-operation

`TokenTransferProcessor` implements `TokenTransferProcessorInterface` — a distinct interface that accepts a full transaction, not a single operation. All other state-change-producing processors (`EffectsProcessor`, `ContractDeployProcessor`, `SACEventsProcessor`) implement `OperationProcessorInterface` which receives one operation at a time.

This distinction exists because `TokenTransferProcessor` wraps the Stellar SDK's `EventsProcessor`, which emits token transfer events at the transaction level by consuming `ingest.LedgerTransaction`. The SDK aggregates fee events, refund events, and token transfers across all operations in a transaction before emitting — breaking this down per-operation would require re-aggregating and would produce incorrect fee calculations.

The practical consequence: in the Indexer fan-out, `TokenTransferProcessor.ProcessTransaction()` is called once per transaction, while `EffectsProcessor.ProcessOperation()`, `ContractDeployProcessor.ProcessOperation()`, and `SACEventsProcessor.ProcessOperation()` are each called once per operation per transaction.

---

Relevant Notes:
- [[nine specialized processors run per transaction in the indexer fan-out]] — full processor table with interface types and levels
- [[TokenTransferProcessor calculates net fee per transaction by summing all fee events rather than tracking individual fee and refund events]] — why aggregation at tx level matters

Areas:
- [[entries/ingestion]]
