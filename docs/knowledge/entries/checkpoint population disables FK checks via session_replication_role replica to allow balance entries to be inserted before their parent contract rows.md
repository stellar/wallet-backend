---
context: The ledger stream does not guarantee parent-before-child ordering, so FK checks must be deferred for the entire streaming transaction; integrity is manually guaranteed by storeTokensInDB inserting parents before COMMIT
type: gotcha
status: active
created: 2026-02-26
---

# checkpoint population disables FK checks via session_replication_role replica to allow balance entries to be inserted before their parent contract rows

During `PopulateAccountTokens()`, the checkpoint transaction sets:

```sql
SET LOCAL session_replication_role = 'replica';
```

This disables all foreign key constraint checks for the duration of the transaction. The reason: the Stellar history archive emits ledger entries in an order that does not guarantee parent rows (`contract_tokens`, `trustline_assets`) appear before their child rows (`sac_balances`, `trustline_balances`). Without this setting, any child row arriving before its parent would immediately violate the FK constraint and abort the transaction.

## How integrity is maintained

The code guarantees integrity without relying on FK checks during streaming:

1. The streaming phase collects all balance entries into the batch struct (up to 250k, then flush)
2. After streaming completes, `storeTokensInDB()` inserts all parent rows (`trustline_assets`, `contract_tokens`) first
3. Only then does the transaction COMMIT — at which point FK integrity holds

The `session_replication_role = 'replica'` setting is LOCAL to the transaction and automatically reverts on commit or rollback.

## Requires superuser

This setting requires PostgreSQL superuser or `pg_write_server_files` privilege. If the DB user lacks this permission, `PopulateAccountTokens()` will fail.

## Source

`internal/services/token_ingestion.go` — `PopulateAccountTokens()` (approximately lines 149–152 per reference doc)

## Related

- [[deferrable initially deferred foreign keys enable checkpoint population to insert children before parents within a single transaction]] — the complementary FK strategy used in live ingestion
- [[backfill mode trades acid durability for insert throughput via synchronous_commit off]] — another session-local performance setting applied during checkpoint

relevant_notes:
  - "[[deferrable initially deferred foreign keys enable checkpoint population to insert children before parents within a single transaction]] — counterpart: session_replication_role disables FK checks for the streaming phase; DEFERRABLE INITIALLY DEFERRED defers them to COMMIT for live ingestion's per-ledger transactions"
  - "[[backfill mode trades acid durability for insert throughput via synchronous_commit off]] — co-occurs in the same transaction: both session_replication_role and synchronous_commit=off are applied as LOCAL settings during checkpoint to maximize throughput"
