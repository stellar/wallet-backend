-- +migrate Up

-- Table: transaction_hashes (TimescaleDB hypertable with columnstore)
--
-- Maps a transaction hash to the ledger close time of its transactions row, so a hash lookup reads
-- one transactions chunk instead of every chunk. transactions is partitioned by ledger_created_at
-- and a hash says nothing about time, so without this table a lookup by hash alone checks the
-- bloom(hash) sparse index of every compressed batch in the hypertable.
--
-- Each row holds only part of the hash. hash_prefix is the top 40 bits; two hashes can share it, so
-- a lookup confirms the full hash against transactions. hash_bucket is the top 20 bits of the
-- prefix. Segmenting by it puts the bucket first in the compressed chunk index, so an equality
-- lookup on (hash_bucket, hash_prefix) is a short index scan inside one bucket. Without it the index
-- leads with the batch's first prefix, and "first <= X" scans every batch below X. The scan inside
-- a bucket walks that bucket's batches, so the bucket count sets how far a lookup can ever walk:
-- 1,048,576 buckets hold about 4,000 rows each at 4.4 billion transactions and about 600,000 at a
-- hundred times that, a few index pages either way, for one partial batch per bucket (about 1 GB).
--
-- The table is partitioned by hash_prefix, the only column a lookup filters on, in 2^30-wide
-- ranges: 1,024 chunks of 1,024 buckets each over the 40-bit space. A lookup is planned and run on
-- exactly one chunk however long the history is. Partitioned by close time, a lookup would have to
-- open every chunk, because the close time is what it is looking for.
--
-- Every chunk keeps receiving rows: a ledger's hashes spread over all of them. Rows written after a
-- chunk's compression sit in its rowstore part until transaction_hashes_maintenance recompresses the
-- chunk. TimescaleDB's own columnstore and retention policies act on a chunk's range of the
-- partition column, a hash range here, so the table has neither: the maintenance job below
-- recompresses, runs as several jobs over disjoint chunks, and deletes the lookup rows of
-- transactions that retention removed.
--
-- close_period is the close time in 30-day periods. It is the second segmentby column, so each
-- compressed batch holds one bucket and one period: recompression rewrites only the current
-- period's batch of each bucket (68 ms and 3.4 MB of WAL per chunk for a week of rows, measured,
-- against 1.2 s and 31 MB with the bucket alone), and retention deletes whole batches by period
-- without decompressing anything (15 ms and 181 kB of WAL per chunk, no lock). A lookup reads the
-- bucket's batches whose prefix range covers the prefix, one per period at most: 7 pages cold,
-- measured, the same as with the bucket alone. The smaller batches cost about a third more space:
-- 8.5 bytes per row against 6.5, measured on 4.4 billion rows.
--
-- ledger_close_epoch is the close time in unix seconds. Ledger close times are whole seconds, and in
-- hash order the time column is random within a chunk: seconds compress to about 3 bytes a row,
-- where microsecond timestamps take 8.
CREATE TABLE transaction_hashes (
    hash_bucket INTEGER NOT NULL,
    hash_prefix BIGINT NOT NULL,
    ledger_close_epoch BIGINT NOT NULL,
    close_period INTEGER NOT NULL GENERATED ALWAYS AS ((ledger_close_epoch / 2592000)::integer) STORED
) WITH (
    tsdb.hypertable,
    tsdb.partition_column = 'hash_prefix',
    tsdb.chunk_interval = 1073741824,
    tsdb.segmentby = 'hash_bucket, close_period',
    tsdb.orderby = 'hash_prefix'
);

-- TimescaleDB's default single-column index on the partition column. Nothing filters this table by
-- bare hash_prefix, so it has zero consumers.
DROP INDEX IF EXISTS transaction_hashes_hash_prefix_idx;

-- Serves TransactionModel.GetByHash on the rowstore part of a chunk: the rows written since the
-- chunk's last recompression. The compressed part answers the same lookup from its own
-- (hash_bucket, first/last hash_prefix) index.
CREATE INDEX idx_transaction_hashes_bucket_prefix ON transaction_hashes(hash_bucket, hash_prefix);

-- The policy TimescaleDB creates with the table compresses chunks whose partition range is older
-- than the chunk interval, meaningless on a hash range. transaction_hashes_maintenance compresses
-- the table instead.
CALL remove_columnstore_policy('transaction_hashes');

-- +migrate StatementBegin
-- Maintenance of transaction_hashes, one chunk per transaction so locks stay short and a failed
-- run resumes where it stopped. config.workers jobs each take the chunks whose prefix range index
-- (range start / 2^30) falls on config.worker, so a run finishes workers times faster.
--
-- Retention first deletes, from each chunk, the rows of periods that closed before the floor:
-- whole compressed batches, no decompression. That is TimescaleDB's direct batch delete on a
-- segmentby predicate (timescaledb.enable_compressed_direct_batch_delete, on by default); with it
-- off the delete decompresses every batch of those periods first. The floor is the older of two
-- periods: the one of the oldest transactions row, and the retention cutoff, now minus the
-- drop_after of the transactions retention policy. Without a retention policy nothing is ever
-- dropped from transactions, so no lookup row is ever orphaned and the run deletes nothing. The
-- cutoff bound protects a backfill: a backfill lowers the oldest row while a run may be in
-- progress, and the run's floor was read once at its start; a backfill writes no row below the
-- cutoff, so none of its rows fall under the bounded floor. With no transactions row, or with
-- job_id 0 (backfill, fill script), nothing is deleted. Recompression
-- then folds the rows written since the last run from the chunk's rowstore part into the
-- columnstore. Neither step takes an exclusive lock; a lookup or insert on the chunk proceeds
-- alongside.
CREATE PROCEDURE transaction_hashes_maintenance(job_id INT, config JSONB)
LANGUAGE plpgsql AS $$
DECLARE
    workers INT := COALESCE((config->>'workers')::int, 1);
    this_worker INT := COALESCE((config->>'worker')::int, 0);
    floor_period INT;
    ch RECORD;
    started TIMESTAMPTZ := clock_timestamp();
    chunks INT := 0;
    rowstore_rows BIGINT := 0;
    deleted_rows BIGINT := 0;
    n BIGINT;
BEGIN
    IF job_id <> 0 THEN
        -- The jobs view's config column is qualified so it does not resolve to this procedure's
        -- config parameter. Either period missing (no retention policy, no transactions row)
        -- leaves the floor NULL and the run deletes nothing.
        SELECT CASE WHEN oldest.period IS NULL OR cutoff.period IS NULL THEN NULL
                    ELSE LEAST(oldest.period, cutoff.period) END
        INTO floor_period
        FROM (SELECT (floor(extract(epoch FROM t.ledger_created_at))::bigint / 2592000)::int AS period
              FROM transactions t ORDER BY t.ledger_created_at LIMIT 1) oldest
        FULL JOIN (SELECT (floor(extract(epoch FROM now() - (j.config->>'drop_after')::interval))::bigint / 2592000)::int AS period
                   FROM timescaledb_information.jobs j
                   WHERE j.proc_name = 'policy_retention' AND j.hypertable_name = 'transactions'
                   LIMIT 1) cutoff ON true;
    END IF;

    FOR ch IN
        SELECT c.chunk_schema, c.chunk_name
        FROM timescaledb_information.chunks c
        WHERE c.hypertable_name = 'transaction_hashes'
          AND ((c.range_start_integer >> 30) % workers) = this_worker
        ORDER BY c.range_start_integer
    LOOP
        IF floor_period IS NOT NULL THEN
            EXECUTE format('DELETE FROM %I.%I WHERE close_period < $1', ch.chunk_schema, ch.chunk_name) USING floor_period;
            GET DIAGNOSTICS n = ROW_COUNT;
            deleted_rows := deleted_rows + n;
        END IF;
        -- ONLY reads the chunk's rowstore part: the rows this run folds into the columnstore.
        EXECUTE format('SELECT count(*) FROM ONLY %I.%I', ch.chunk_schema, ch.chunk_name) INTO n;
        rowstore_rows := rowstore_rows + n;
        PERFORM compress_chunk(format('%I.%I', ch.chunk_schema, ch.chunk_name)::regclass, if_not_compressed => true);
        chunks := chunks + 1;
        COMMIT;
    END LOOP;

    -- One line per run in the server log. The rowstore rows per run are the number to size
    -- the schedule by: keep them in the low hundreds of millions.
    RAISE LOG 'transaction_hashes_maintenance worker %/%: % chunks, % rowstore rows compressed, % rows deleted, floor period %, %',
        this_worker, workers, chunks, rowstore_rows, deleted_rows, floor_period, clock_timestamp() - started;
END $$;
-- +migrate StatementEnd

-- One worker by default. The job stays unscheduled until a live ingester starts: the ingester
-- schedules it, sets its interval and adds or removes jobs to match its worker setting, so a
-- database nothing writes to runs no maintenance, and the fill script runs alone.
SELECT add_job('transaction_hashes_maintenance', INTERVAL '1 day', config => '{"workers": 1, "worker": 0}', scheduled => false);

-- Step 2 of a hash lookup reads the transactions of one second of ledger_created_at. A compressed
-- chunk narrows that with its batch metadata; a chunk still in the rowstore has only the primary
-- key (to_id, ledger_created_at) and would walk it. This index covers the open chunks; the key is
-- append-only, and TimescaleDB keeps no btree on compressed chunks.
CREATE INDEX idx_transactions_ledger_created_at ON transactions(ledger_created_at);

-- Hash lookups go through transaction_hashes and then read transactions by ledger_created_at.
-- Nothing filters transactions by hash alone, and this index only ever covered chunks still in
-- the rowstore.
DROP INDEX IF EXISTS idx_transactions_hash;

-- Transactions ingested from here on get their lookup row from ingestion. History already in
-- transactions gets none: re-backfill it, or run the one-off fill in docs/deploy/upgrading.md.

-- +migrate Down

CREATE INDEX IF NOT EXISTS idx_transactions_hash ON transactions(hash);
DROP INDEX IF EXISTS idx_transactions_ledger_created_at;
SELECT delete_job(job_id) FROM timescaledb_information.jobs WHERE proc_name = 'transaction_hashes_maintenance';
DROP PROCEDURE IF EXISTS transaction_hashes_maintenance(INT, JSONB);
DROP TABLE IF EXISTS transaction_hashes CASCADE;
