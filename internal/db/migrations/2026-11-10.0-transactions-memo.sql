-- +migrate Up

-- memo_type holds the XDR MemoType discriminant (0 none, 1 text, 2 id, 3 hash, 4 return);
-- memo holds the raw value bytes (text bytes as-is, id as 8-byte big-endian, hash/return as
-- 32 bytes; NULL for none). Both NULL on a row means the memo backfill has not reached it yet.
-- Nullable with no default so the ALTER is metadata-only on compressed chunks.
ALTER TABLE transactions ADD COLUMN memo_type SMALLINT, ADD COLUMN memo BYTEA;

-- Memo backfill cursor: the lowest ledger whose rows already carry a memo. 0 = not started;
-- the live ingester sets it to latest_ingest_ledger+1 on its first run after this migration
-- and walks it down. Done once it is at or below oldest_ingest_ledger. The row is never deleted.
INSERT INTO ingest_store (key, value) VALUES ('memo_backfill_cursor', '0')
ON CONFLICT (key) DO NOTHING;

-- +migrate Down

DELETE FROM ingest_store WHERE key = 'memo_backfill_cursor';
ALTER TABLE transactions DROP COLUMN IF EXISTS memo, DROP COLUMN IF EXISTS memo_type;
