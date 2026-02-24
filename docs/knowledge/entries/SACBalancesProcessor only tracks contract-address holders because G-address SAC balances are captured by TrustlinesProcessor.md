---
context: SAC tokens are ERC20-like Soroban contracts; G-address holders have classic trustlines tracked separately; only C-address holders need SACBalancesProcessor
type: insight
status: active
subsystem: ingestion
areas: [ingestion, indexer]
created: 2026-02-24
---

# SACBalancesProcessor only tracks contract-address holders because G-address SAC balances are captured by TrustlinesProcessor

`SACBalancesProcessor` records SAC (Stellar Asset Contract) token balances only for contract-address holders (C-addresses). It skips ledger changes where the account key is a classic Stellar address (G-address).

The reason: SAC tokens are Soroban smart contracts that wrap classic Stellar assets. When a classic G-address account holds a SAC-tokenized asset, the balance is stored in a standard Stellar trustline — which `TrustlinesProcessor` already captures. The SAC contract layer is transparent for G-address holders at the storage level.

Only Soroban contract accounts (C-addresses) holding SAC balances use Soroban contract storage rather than classic trustlines. These are the entries `SACBalancesProcessor` captures.

This split avoids double-counting: if `SACBalancesProcessor` also processed G-address SAC holders, every G-address balance change for a SAC-tokenized asset would appear twice in the output — once from `TrustlinesProcessor` reading the ledger entry and once from `SACBalancesProcessor` reading the contract storage.

---

Relevant Notes:
- [[TrustlinesProcessor and AccountsProcessor read absolute values from ledger change Post entries not deltas]] — TrustlinesProcessor handles the G-address side
- [[the unified StateChange struct uses nullable fields and a category discriminator to store all state change types in a single database table]] — both feed into state_changes

Areas:
- [[entries/ingestion]]
