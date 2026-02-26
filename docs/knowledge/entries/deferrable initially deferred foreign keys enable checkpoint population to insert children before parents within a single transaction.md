---
context: Schema-design pattern used on all three balance table FKs; the check moves from per-statement to per-COMMIT, which is essential when insert ordering cannot be guaranteed
type: pattern
status: active
created: 2026-02-26
---

# deferrable initially deferred foreign keys enable checkpoint population to insert children before parents within a single transaction

All FK constraints on the balance tables are declared `DEFERRABLE INITIALLY DEFERRED`:

```sql
-- trustline_balances → trustline_assets
CONSTRAINT fk_asset FOREIGN KEY (asset_id) REFERENCES trustline_assets(id) DEFERRABLE INITIALLY DEFERRED

-- sac_balances → contract_tokens
CONSTRAINT fk_contract FOREIGN KEY (contract_id) REFERENCES contract_tokens(id) DEFERRABLE INITIALLY DEFERRED

-- account_contract_tokens → contract_tokens
CONSTRAINT fk_contract FOREIGN KEY (contract_id) REFERENCES contract_tokens(id) DEFERRABLE INITIALLY DEFERRED
```

With `INITIALLY DEFERRED`, PostgreSQL checks FK constraints at COMMIT rather than at each INSERT statement. This means a child row can be inserted before its parent row exists, as long as the parent is present by the time the transaction commits.

## Why this is needed

The Stellar history archive does not emit entries in parent-before-child order. During checkpoint streaming, a `sac_balances` row may arrive before its parent `contract_tokens` row. DEFERRABLE FK constraints allow this — the constraint violation check only fires at COMMIT, by which time `storeTokensInDB()` has inserted all parent rows.

## Difference from session_replication_role

The two strategies handle different scopes:
- `session_replication_role = 'replica'` — disables FK checks entirely for the checkpoint streaming phase (the multi-batch COPY loop)
- `DEFERRABLE INITIALLY DEFERRED` — defers FK checks to COMMIT for live ingestion's per-ledger transactions

Both are needed because checkpoint and live ingestion have different transaction structures.

## Source

`docs/knowledge/references/token-ingestion.md` — Database Schema section (lines 309–326)

## Related

- [[checkpoint population disables FK checks via session_replication_role replica to allow balance entries to be inserted before their parent contract rows]] — the other FK constraint strategy, used for the streaming phase
- [[balance tables require aggressive autovacuum tuning because rows are updated every ledger]] — the other schema-level setting on balance tables

relevant_notes:
  - "[[checkpoint population disables FK checks via session_replication_role replica to allow balance entries to be inserted before their parent contract rows]] — counterpart: the two FK strategies are complementary — session_replication_role covers the streaming COPY phase, DEFERRABLE covers per-ledger transactions in live ingestion"
  - "[[balance tables require aggressive autovacuum tuning because rows are updated every ledger]] — co-located in the same migration: both autovacuum settings and DEFERRABLE FK declarations are in the same migration files for balance tables"
