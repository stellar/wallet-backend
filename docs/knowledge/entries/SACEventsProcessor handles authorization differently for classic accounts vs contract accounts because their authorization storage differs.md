---
context: Classic G-address accounts use ledger entry flags; contract accounts use instance storage; the processor reads from whichever is present for the account type
type: insight
status: active
subsystem: ingestion
areas: [ingestion, indexer]
created: 2026-02-24
---

# SACEventsProcessor handles authorization differently for classic accounts vs contract accounts because their authorization storage differs

When `SACEventsProcessor` processes a `set_authorized` SAC event (trustline authorization change), it must determine the previous and new authorization state to produce a `StateChange`. Classic Stellar accounts (G-addresses) store trustline authorization in ledger entry flags, while Soroban contract accounts store it in contract instance storage.

The processor branches on the account type: for classic accounts, it reads the `TrustLineFlags` from the relevant ledger change's `Pre` and `Post` entries. For contract accounts, it inspects the contract's instance storage entries to find the authorization state before and after the operation.

This dual handling exists because SAC (Stellar Asset Contract) trustlines bridge the classic and Soroban worlds. When a SAC is authorized to operate on behalf of a Soroban contract account, the authorization record lives in Soroban contract storage rather than in a classic ledger entry.

Since [[SACEventsProcessor and EffectsProcessor both generate BALANCE_AUTHORIZATION state changes for trustline flag changes through different paths]], downstream consumers must be aware that the same authorization event can appear from either source.

---

Relevant Notes:
- [[SACEventsProcessor adapts to tx meta version differences for set_authorized event topic layout]] — another edge case in the same processor
- [[SACEventsProcessor silently continues on extraction errors rather than failing the entire transaction]] — error handling posture

Areas:
- [[entries/ingestion]]
