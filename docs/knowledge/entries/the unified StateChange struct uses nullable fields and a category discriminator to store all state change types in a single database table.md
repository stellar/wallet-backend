---
context: Seven StateChange categories (PAYMENT, TRADE, OFFER, etc.) have overlapping but different fields; nullable fields + category enum avoids JOIN-heavy polymorphic tables
type: decision
status: active
subsystem: ingestion
areas: [ingestion, indexer]
created: 2026-02-24
---

# the unified StateChange struct uses nullable fields and a category discriminator to store all state change types in a single database table

All state change types — payments, trades, offers, claimable balances, liquidity pool events, balance flags, contract deployments, SAC events — are stored in a single `state_changes` hypertable using a unified `StateChange` Go struct with nullable fields and a `Category` discriminator.

The struct has a core set of always-present fields (transaction hash, ledger number, operation index, address, timestamp) plus optional fields that are only populated for certain categories:

- `AssetCode`, `AssetIssuer` — present for PAYMENT, TRADE, SAC events; null for CONTRACT_DEPLOY
- `Amount` — present for PAYMENT, FEE; null for OFFER (which has price instead)
- `ContractID` — present for CONTRACT_DEPLOY, SAC events; null for PAYMENT
- `CounterAsset*`, `CounterAmount` — present for TRADE; null for all others

This design was chosen over a polymorphic table design (one table per category with a JOIN to a parent) because the read pattern is predominantly by address+time rather than by category. A unified table allows a single index on `(address, created_at)` to serve all category queries — this is the same access pattern that [[ledger_created_at is the hypertable partition column because time-bounded queries are the dominant API access pattern]] optimizes for. The tradeoff is nullable columns and the need for category-aware application code.

---

Relevant Notes:
- [[StateChangeBuilder provides a fluent builder with Clone for constructing branching state change trees from a common base]] — builder that constructs these structs
- [[contract type classification uses deterministic SAC contract ID derivation to distinguish SAC from SEP41 tokens]] — fills the ContractType field in this struct
- [[ledger_created_at is the hypertable partition column because time-bounded queries are the dominant API access pattern]] — the data-layer partition design that makes the address+time read pattern efficient in this hypertable

Areas:
- [[entries/ingestion]] | [[entries/data-layer]]
