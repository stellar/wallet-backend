---
context: set_authorized SAC events and TrustlineAuthorized effects both fire on the same operation; without deduplication downstream consumers see double records for authorization changes
type: tension
status: active
subsystem: ingestion
areas: [ingestion, indexer]
created: 2026-02-24
---

# SACEventsProcessor and EffectsProcessor both generate BALANCE_AUTHORIZATION state changes for trustline flag changes through different paths

When a trustline authorization flag changes on a SAC-managed asset, both `SACEventsProcessor` and `EffectsProcessor` produce a `StateChange` with `Category=BALANCE_AUTHORIZATION` for the same event.

- `SACEventsProcessor` detects it via the `set_authorized` diagnostic event emitted by the SAC contract
- `EffectsProcessor` detects it via the `TrustlineAuthorized` / `TrustlineDeauthorized` effect in the SDK effect stream

Both paths observe the same underlying ledger state change (the trustline `AUTHORIZED` flag bit toggling), but arrive at it through different data sources (contract diagnostic events vs operation effects). The resulting `StateChange` records have the same address, category, and ledger/operation provenance, but may differ in subtle fields like `ContractID` (present in SACEventsProcessor output, absent in EffectsProcessor output).

This duplication is unresolved as of 2026-02-24. The current behavior is that both records are inserted, and GraphQL query logic must handle the possibility of duplicates when querying authorization changes on SAC assets. A future resolution could suppress one source for SAC assets, but this requires distinguishing SAC assets from non-SAC assets at the EffectsProcessor level.

---

Relevant Notes:
- [[effects-based processing vs ledger-change-based processing produces overlapping but different views of the same data]] — root tension this instance illustrates
- [[SACEventsProcessor handles authorization differently for classic accounts vs contract accounts because their authorization storage differs]] — SACEventsProcessor side

Areas:
- [[entries/ingestion]]
