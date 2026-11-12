-- +migrate Up

-- memo_type holds the XDR MemoType discriminant (0 none, 1 text, 2 id, 3 hash, 4 return);
-- memo holds the raw value bytes (text bytes as-is, id as 8-byte big-endian, hash/return as
-- 32 bytes; NULL for none). Both NULL on a row means the transactions-memo data migration
-- has not reached it yet.
-- Nullable with no default so the ALTER is metadata-only on compressed chunks.
ALTER TABLE transactions ADD COLUMN memo_type SMALLINT, ADD COLUMN memo BYTEA;

-- +migrate Down

ALTER TABLE transactions DROP COLUMN IF EXISTS memo, DROP COLUMN IF EXISTS memo_type;
