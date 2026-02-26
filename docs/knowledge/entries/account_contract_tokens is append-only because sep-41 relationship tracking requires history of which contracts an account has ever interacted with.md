---
context: Preliminary — the reference doc states "no deletes" but the rationale (historical relationship tracking vs current state) is inferred; needs source code confirmation of the intent
type: decision
status: preliminary
created: 2026-02-26
---

# account_contract_tokens is append-only because SEP-41 relationship tracking requires history of which contracts an account has ever interacted with

The `account_contract_tokens` table has no DELETE path in its model interface. Unlike the balance tables (`trustline_balances`, `sac_balances`, `native_balances`) which upsert current state and delete on removal, `account_contract_tokens` only appends:

```go
// AccountContractTokensModelInterface
BatchInsert(ctx, dbTx pgx.Tx, rows []AccountContractToken) error  // ON CONFLICT DO NOTHING
GetByAccount(ctx, accountAddress string) ([]AccountContractToken, error)

// No Delete or BatchRemove method
```

This tracks every contract an account has *ever* interacted with — not just current holdings. This preserves historical exposure data: even after an account's SEP-41 balance goes to zero (and the account is removed from `sac_balances`), the `account_contract_tokens` record remains.

## Implication

`account_contract_tokens` grows monotonically. Queries reading from this table must be aware that rows represent historical relationships, not current active positions. For current positions, cross-reference against `sac_balances`.

## Source

`docs/knowledge/references/token-ingestion.md` — Relationship Tables schema section (lines 306–308), Interface Reference table (lines 349–357)

## Related

- [[sac_balances exclusively tracks contract-address SAC positions because SAC balances for non-contract holders appear in their respective balance tables]] — the current-state complement to this historical table

relevant_notes:
  - "[[sac_balances exclusively tracks contract-address SAC positions because SAC balances for non-contract holders appear in their respective balance tables]] — contrast: sac_balances tracks current state (upsert+delete); account_contract_tokens tracks historical relationships (append-only)"
