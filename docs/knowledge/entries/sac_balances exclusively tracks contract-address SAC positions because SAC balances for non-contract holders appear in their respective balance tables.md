---
context: The C... address restriction on sac_balances is not arbitrary — it is the complement of how Stellar ledger represents G-address SAC holdings as trustlines
type: fact
status: active
created: 2026-02-26
---

# sac_balances exclusively tracks contract-address (C...) SAC positions because SAC balances for non-contract holders appear in their respective balance tables

The `sac_balances` table holds SAC token balances **only for contract addresses (C...)**. G-addresses holding SAC tokens are stored in `trustline_balances` because Stellar represents them as standard trustlines in the ledger.

This means the `sac_balances` table has a narrower scope than its name implies — it is not "all SAC balances" but specifically "C-address SAC balances."

## Why the split exists

The Stellar ledger emits different entry types depending on the holder:
- G-address SAC holding → `LedgerEntryTypeTrustline` (processed by `TrustlinesProcessor`)
- C-address SAC holding → `LedgerEntryTypeContractData` (processed by `SACBalancesProcessor`)

The ingestion subsystem stores these in the tables that match their ledger origin, not their token type.

## Implication

The `sac_balances` FK references `contract_tokens`, not `trustline_assets` — this is consistent with the fact that only C-address holders appear here. If you see a C-address balance, it's in `sac_balances`. If you see a G-address balance for a SAC token, it's in `trustline_balances`.

## Source

`docs/knowledge/references/token-ingestion.md` — Token Type Taxonomy table (lines 68–76)

## Related

- [[G-address SAC token holdings are stored in trustline_balances not sac_balances because stellar represents them as standard trustlines]] — the symmetric complement covering the G-address side

relevant_notes:
  - "[[G-address SAC token holdings are stored in trustline_balances not sac_balances because stellar represents them as standard trustlines]] — symmetric complement: these two entries together fully describe the G/C address split for SAC token storage"
