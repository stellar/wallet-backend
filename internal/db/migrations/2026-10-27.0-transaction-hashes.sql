-- +migrate Up

-- Table: transaction_hashes (TimescaleDB hypertable with columnstore)
--
-- Maps a transaction hash to the ledger close time of its transactions row, so a hash lookup reads
-- one transactions chunk instead of every chunk. transactions is partitioned by ledger_created_at
-- and a hash says nothing about time, so without this table a lookup by hash alone checks the
-- bloom(hash) sparse index of every compressed batch in the hypertable.
--
-- Each row holds only part of the hash. hash_prefix is the top 40 bits; two hashes can share it, so
-- a lookup confirms the full hash against transactions. hash_bucket is the top 12 bits of the
-- prefix. Segmenting by it puts the bucket first in the compressed chunk index, so an equality
-- lookup on (hash_bucket, hash_prefix) is a short index scan inside one bucket. Without it the index
-- leads with the batch's first prefix, and "first <= X" scans every batch below X.
--
-- ledger_close_epoch is the close time in unix seconds. Ledger close times are whole seconds, and in
-- hash order the time column is random within a chunk: seconds compress to about 3 bytes a row,
-- where microsecond timestamps take 8. The table is partitioned by this integer column, so the
-- retention and columnstore policies use transaction_hashes_now() as their "now".
CREATE TABLE transaction_hashes (
    hash_bucket SMALLINT NOT NULL,
    hash_prefix BIGINT NOT NULL,
    ledger_close_epoch BIGINT NOT NULL
) WITH (
    tsdb.hypertable,
    tsdb.partition_column = 'ledger_close_epoch',
    tsdb.chunk_interval = 604800,
    tsdb.segmentby = 'hash_bucket',
    tsdb.orderby = 'hash_prefix'
);

-- +migrate StatementBegin
CREATE FUNCTION transaction_hashes_now() RETURNS BIGINT LANGUAGE SQL STABLE AS $$
    SELECT extract(epoch FROM now())::bigint
$$;
-- +migrate StatementEnd

SELECT set_integer_now_func('transaction_hashes', 'transaction_hashes_now');

-- TimescaleDB's default single-column index on the partition column. Retention drops chunks by
-- range metadata and no query filters this table by bare ledger_close_epoch, so it has zero
-- consumers.
DROP INDEX IF EXISTS transaction_hashes_ledger_close_epoch_idx;

-- Serves TransactionModel.GetByHash on chunks still in the rowstore. Compressed chunks answer the
-- same lookup from their own (hash_bucket, first/last hash_prefix) index.
CREATE INDEX idx_transaction_hashes_bucket_prefix ON transaction_hashes(hash_bucket, hash_prefix);

-- Hash lookups now go through transaction_hashes and then read transactions by ledger_created_at,
-- which the bloom(hash) sparse index narrows to one batch. Nothing filters transactions by hash
-- alone any more, and this index only ever covered chunks still in the rowstore.
DROP INDEX IF EXISTS idx_transactions_hash;

-- Transactions ingested from here on get their lookup row from ingestion. History already in
-- transactions gets none: re-backfill it, or run the one-off fill in docs/deploy/upgrading.md.

-- +migrate Down

CREATE INDEX IF NOT EXISTS idx_transactions_hash ON transactions(hash);
DROP TABLE IF EXISTS transaction_hashes CASCADE;
DROP FUNCTION IF EXISTS transaction_hashes_now();
