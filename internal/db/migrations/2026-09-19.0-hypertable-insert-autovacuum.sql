-- +migrate Up

-- The five history hypertables are insert-only: rows arrive by COPY, are
-- never updated, and leave by chunk drop. Autovacuum still fires on them
-- through the insert trigger (autovacuum_vacuum_insert_threshold +
-- autovacuum_vacuum_insert_scale_factor × reltuples, default 1,000 + 20 %),
-- so an active chunk that grows to several GB inside its interval is
-- vacuumed four to five times, each pass reading the newly inserted pages to
-- set visibility-map bits and freeze tuples. Compression then rewrites the
-- chunk and drops the original heap, discarding that work. On a single
-- primary the passes compete with the write path for disk bandwidth: during
-- a compression run the data volume sits near saturation and every COPY's
-- tail rises together (measured on the load rig at 10k tx/s: disk 85–92 %
-- busy, autovacuum reading 4 GB and writing 2.3 GB per two minutes, more
-- than the compression job itself; with the trigger out of reach the same
-- runs sit at ~42 % busy).
--
-- Push the insert trigger out of reach so no chunk is vacuumed for inserts
-- during its life. Autoanalyze keeps its defaults, so new chunks still get
-- planner statistics. Anti-wraparound vacuum ignores these settings and
-- still runs when a relation's oldest transaction id nears
-- autovacuum_freeze_max_age; at the ingest transaction rate that is months
-- away and retention drops chunks well before it.
--
-- TimescaleDB copies hypertable reloptions to its chunks, existing and new.
-- Cost: index-only scans on the newest, uncompressed chunk lose their
-- all-visible bits and fetch heap pages instead.
ALTER TABLE transactions          SET (autovacuum_vacuum_insert_scale_factor = 100, autovacuum_vacuum_insert_threshold = 2000000000);
ALTER TABLE transactions_accounts SET (autovacuum_vacuum_insert_scale_factor = 100, autovacuum_vacuum_insert_threshold = 2000000000);
ALTER TABLE operations            SET (autovacuum_vacuum_insert_scale_factor = 100, autovacuum_vacuum_insert_threshold = 2000000000);
ALTER TABLE operations_accounts   SET (autovacuum_vacuum_insert_scale_factor = 100, autovacuum_vacuum_insert_threshold = 2000000000);
ALTER TABLE state_changes         SET (autovacuum_vacuum_insert_scale_factor = 100, autovacuum_vacuum_insert_threshold = 2000000000);

-- +migrate Down

ALTER TABLE transactions          RESET (autovacuum_vacuum_insert_scale_factor, autovacuum_vacuum_insert_threshold);
ALTER TABLE transactions_accounts RESET (autovacuum_vacuum_insert_scale_factor, autovacuum_vacuum_insert_threshold);
ALTER TABLE operations            RESET (autovacuum_vacuum_insert_scale_factor, autovacuum_vacuum_insert_threshold);
ALTER TABLE operations_accounts   RESET (autovacuum_vacuum_insert_scale_factor, autovacuum_vacuum_insert_threshold);
ALTER TABLE state_changes         RESET (autovacuum_vacuum_insert_scale_factor, autovacuum_vacuum_insert_threshold);
