---
context: Stellar Asset Contracts have contract IDs deterministically derived from asset+network; computing expected ID and comparing against actual ID avoids maintaining a registry
type: insight
status: active
subsystem: ingestion
areas: [ingestion, indexer]
created: 2026-02-24
---

# contract type classification uses deterministic SAC contract ID derivation to distinguish SAC from SEP41 tokens

When `SACInstanceProcessor` records a new contract instance, it must classify the contract as either a SAC (Stellar Asset Contract — the canonical Stellar-to-Soroban bridge) or a SEP-41 token (a user-deployed token following the SEP-41 standard). The classification is stored in the `contract_type` field of the `contracts` table.

The classification uses a key property of SAC contracts: their contract IDs are deterministically derived from the wrapped Stellar asset and the network passphrase using the same derivation function the Stellar network uses when deploying the asset contract. To classify a contract:

1. Take the contract's asset information from the instance storage
2. Compute the expected SAC contract ID using `DeriveAssetContractID(asset, networkPassphrase)`
3. If the computed ID matches the actual contract ID → it's a SAC
4. Otherwise → it's a SEP-41 token

This approach requires no external registry or on-chain lookup beyond the ledger data already present. The determinism of SAC ID derivation is a protocol guarantee: any given asset can only have one canonical SAC contract ID on a given network.

---

Relevant Notes:
- [[the unified StateChange struct uses nullable fields and a category discriminator to store all state change types in a single database table]] — ContractType field lives in this struct
- [[SACBalancesProcessor only tracks contract-address holders because G-address SAC balances are captured by TrustlinesProcessor]] — downstream consumer of this classification

Areas:
- [[entries/ingestion]]
