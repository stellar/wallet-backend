---
context: Wraps stellar/go ingest.EffectsProcessor; seven categories emerge from classifying which account attributes each ledger change type touches
type: insight
status: active
subsystem: ingestion
areas: [ingestion, indexer]
created: 2026-02-24
---

# EffectsProcessor is the most complex processor handling seven distinct categories of account state changes

`EffectsProcessor` wraps the Stellar Go SDK's `ingest.EffectsProcessor` and maps the SDK's effect stream to `StateChange` records. It handles seven categories of account-level state changes:

1. **PAYMENT** — XLM and asset transfer payments
2. **TRADE** — DEX trade executions (offer-based)
3. **OFFER** — Offer creation, update, deletion
4. **CLAIMABLE_BALANCE** — Claimable balance create/claim/delete
5. **LIQUIDITY_POOL** — LP deposit/withdrawal events
6. **BALANCE_CHANGE** — Balance flag changes (trustline authorization)
7. **BALANCE_AUTHORIZATION** — Trustline flag state changes (authorized/deauthorized)

The complexity comes from two sources: (1) the SDK's effect stream uses a flat type enum with 50+ effect types that must be classified into these seven categories, and (2) several effect types require looking up the actual trustor address from `effect.Details` because `effect.Address` is set to the operation source account (not the affected party) for certain trustline operations.

Since [[EffectsProcessor sets effect Address to the operation source for trustline flag effects but the actual trustor is in effect Details]], callers expecting `Address` to identify the affected account will get incorrect results for this category.

---

Relevant Notes:
- [[nine specialized processors run per transaction in the indexer fan-out]] — EffectsProcessor is one of three OperationProcessorInterface processors
- [[EffectsProcessor sets effect Address to the operation source for trustline flag effects but the actual trustor is in effect Details]] — specific gotcha for trustline categories
- [[effects-based processing vs ledger-change-based processing produces overlapping but different views of the same data]] — EffectsProcessor and TrustlinesProcessor both touch trustline state

Areas:
- [[entries/ingestion]]
