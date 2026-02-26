---
context: Querying sac_balances for a G-address returns nothing even if it holds SAC tokens — the split mirrors Stellar ledger representation, not token type
type: gotcha
status: active
created: 2026-02-26
---

# G-address SAC token holdings are stored in trustline_balances not sac_balances because Stellar represents them as standard trustlines

The `sac_balances` table is exclusively for **contract addresses (C...)** holding SAC positions. When a G-address holds a SAC token, Stellar's ledger represents it as a standard trustline — so the wallet-backend stores it in `trustline_balances` accordingly.

This is the storage split:

| Holder Type | Token Type | Storage Table |
|-------------|-----------|---------------|
| G... account | SAC token | `trustline_balances` (FK → `trustline_assets`) |
| C... contract | SAC token | `sac_balances` (FK → `contract_tokens`) |

The split is not an arbitrary design choice — it directly mirrors the Stellar ledger's own representation. SAC tokens for G-addresses appear as `LedgerEntryTypeTrustline` entries in the ledger stream, which is why they are processed by `TrustlinesProcessor` and stored as trustlines.

## Implication

Any query asking "what SAC tokens does this account hold?" must:
1. If the account is a G-address: query `trustline_balances JOIN trustline_assets WHERE contract_id IS NOT NULL`
2. If the account is a C-address: query `sac_balances`

Querying only `sac_balances` for a G-address will silently return empty results.

## Source

`docs/knowledge/references/token-ingestion.md` — Token Type Taxonomy table (lines 68–76)

## Related

- [[sac_balances exclusively tracks contract-address SAC positions because SAC balances for non-contract holders appear in their respective balance tables]] — the symmetric complement of this entry
- [[balance tables are standard postgres tables not hypertables because they store current state not time-series events]] — why these are regular Postgres tables

relevant_notes:
  - "[[sac_balances exclusively tracks contract-address SAC positions because SAC balances for non-contract holders appear in their respective balance tables]] — symmetric complement: this entry covers the G-address side; that entry covers the C-address side"
  - "[[balance tables are standard postgres tables not hypertables because they store current state not time-series events]] — grounds this: both trustline_balances and sac_balances are regular tables; the split is about ledger representation, not storage architecture"
