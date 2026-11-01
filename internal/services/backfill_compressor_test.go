package services

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/wallet-backend/internal/data"
	"github.com/stellar/wallet-backend/internal/db"
	"github.com/stellar/wallet-backend/internal/db/dbtest"
	"github.com/stellar/wallet-backend/internal/metrics"
)

func openBackfillTestDB(t *testing.T) (*pgxpool.Pool, *data.Models) {
	t.Helper()
	dbt := dbtest.Open(t)
	t.Cleanup(func() { dbt.Close() })
	pool, err := db.OpenDBConnectionPool(context.Background(), dbt.DSN)
	require.NoError(t, err)
	t.Cleanup(func() { pool.Close() })
	m := metrics.NewMetrics(prometheus.NewRegistry())
	models, err := data.NewModels(pool, m.DB)
	require.NoError(t, err)
	return pool, models
}

// insertTestTransaction writes one transactions row at the given ledger and
// close time, which creates that day's chunk.
func insertTestTransaction(t *testing.T, pool *pgxpool.Pool, ledger uint32, closeTime time.Time) {
	t.Helper()
	_, err := pool.Exec(context.Background(),
		`INSERT INTO transactions (to_id, hash, fee_charged, result_code, ledger_number, is_fee_bump, ledger_created_at)
		 VALUES (($1::bigint << 32), decode(md5($1::text), 'hex') || decode(md5($1::text || 'b'), 'hex'), 100, 'txSUCCESS', $1, false, $2)`,
		int64(ledger), closeTime)
	require.NoError(t, err)
}

func insertTestLookupRow(t *testing.T, pool *pgxpool.Pool, epoch int64) {
	t.Helper()
	_, err := pool.Exec(context.Background(),
		`INSERT INTO transaction_hashes (hash_bucket, hash_prefix, ledger_close_epoch) VALUES (($1 >> 20)::integer, $1, $1)`, epoch)
	require.NoError(t, err)
}

func chunkCompressed(t *testing.T, pool *pgxpool.Pool, table string) map[string]bool {
	t.Helper()
	rows, err := pool.Query(context.Background(),
		`SELECT chunk_schema || '.' || chunk_name, is_compressed FROM timescaledb_information.chunks WHERE hypertable_name = $1`, table)
	require.NoError(t, err)
	defer rows.Close()
	out := map[string]bool{}
	for rows.Next() {
		var name string
		var compressed bool
		require.NoError(t, rows.Scan(&name, &compressed))
		out[name] = compressed
	}
	require.NoError(t, rows.Err())
	return out
}

func Test_readBulkChunkInterval(t *testing.T) {
	pool, _ := openBackfillTestDB(t)
	interval, err := readBulkChunkInterval(context.Background(), pool)
	require.NoError(t, err)
	assert.Equal(t, 24*time.Hour, interval)
}

func Test_readChunkRanges_andWindowChunks(t *testing.T) {
	pool, _ := openBackfillTestDB(t)
	ctx := context.Background()
	day := 24 * time.Hour
	d27 := time.Date(2026, 10, 27, 0, 0, 0, 0, time.UTC)
	insertTestTransaction(t, pool, 1000, d27.Add(13*time.Hour))

	ranges, err := readChunkRanges(ctx, pool, data.BulkCopyTableNames())
	require.NoError(t, err)
	require.Len(t, ranges, 1)
	assert.Equal(t, "transactions", ranges[0].Table)
	assert.True(t, ranges[0].Start.Equal(d27), "chunk starts at UTC midnight, got %s", ranges[0].Start)
	assert.True(t, ranges[0].End.Equal(d27.Add(day)))
	assert.True(t, overlapsAnyChunk(ranges, d27, d27.Add(day)))
	assert.False(t, overlapsAnyChunk(ranges, d27.Add(-day), d27))

	c, _ := newBackfillCompressor(ctx, pool, metrics.NewMetrics(prometheus.NewRegistry()).Ingestion, 1, d27.Add(10*day))
	defer func() { require.NoError(t, c.Close()) }()
	chunks, err := c.windowChunks(ctx, backfillWindow{RangeStart: d27, RangeEnd: d27.Add(day)})
	require.NoError(t, err)
	assert.Len(t, chunks, 1)
	chunks, err = c.windowChunks(ctx, backfillWindow{RangeStart: d27.Add(-day), RangeEnd: d27})
	require.NoError(t, err)
	assert.Empty(t, chunks)
}

func Test_pauseAndResumeCompressionPolicies(t *testing.T) {
	pool, _ := openBackfillTestDB(t)
	ctx := context.Background()
	scheduled := func() int {
		var n int
		require.NoError(t, pool.QueryRow(ctx,
			`SELECT count(*) FROM timescaledb_information.jobs
			  WHERE proc_name IN ('policy_compression', 'transaction_hashes_maintenance') AND scheduled`).Scan(&n))
		return n
	}
	// The migrations create one columnstore policy per bulk hypertable and the
	// lookup table's maintenance job, which live mode schedules.
	_, err := pool.Exec(ctx, `SELECT alter_job(job_id, scheduled => true, next_start => now() + INTERVAL '1 day') FROM timescaledb_information.jobs WHERE proc_name = 'transaction_hashes_maintenance'`)
	require.NoError(t, err)
	require.Equal(t, len(data.BulkCopyTableNames())+1, scheduled())

	paused, err := pauseCompressionPolicies(ctx, pool)
	require.NoError(t, err)
	assert.Len(t, paused, len(data.BulkCopyTableNames())+1)
	assert.Equal(t, 0, scheduled())

	// Pausing again finds nothing scheduled and pauses nothing.
	again, err := pauseCompressionPolicies(ctx, pool)
	require.NoError(t, err)
	assert.Empty(t, again)

	require.NoError(t, resumeCompressionPolicies(ctx, pool, paused))
	assert.Equal(t, len(data.BulkCopyTableNames())+1, scheduled())
}

func Test_backfillCompressor_compressesOwnedWindows(t *testing.T) {
	pool, _ := openBackfillTestDB(t)
	ctx := context.Background()
	day := 24 * time.Hour
	d27 := time.Date(2026, 10, 27, 0, 0, 0, 0, time.UTC)
	insertTestTransaction(t, pool, 1000, d27.Add(time.Hour))
	insertTestTransaction(t, pool, 2000, d27.Add(day+time.Hour)) // the next day: shared with live

	// The run's top is inside the second day, so only the first day is owned.
	runTop := d27.Add(day + 2*time.Hour)
	c, _ := newBackfillCompressor(ctx, pool, metrics.NewMetrics(prometheus.NewRegistry()).Ingestion, 2, runTop)

	owned := backfillWindow{RangeStart: d27, RangeEnd: d27.Add(day), FirstLedger: 1000, LastLedger: 1000, BelowCursor: true}
	shared := backfillWindow{RangeStart: d27.Add(day), RangeEnd: d27.Add(2 * day), FirstLedger: 2000, LastLedger: 2000, BelowCursor: true}
	assert.True(t, c.Owns(owned))
	assert.False(t, c.Owns(shared), "the window holding live rows stays with the live policy")
	assert.False(t, c.Owns(backfillWindow{RangeStart: d27, RangeEnd: d27.Add(day), BelowCursor: false}), "gap fills above the cursor are never compressed here")

	require.NoError(t, c.AcquireSlot(ctx))
	require.NoError(t, c.WindowClosed(ctx, shared))
	require.NoError(t, c.AcquireSlot(ctx))
	require.NoError(t, c.WindowClosed(ctx, owned))
	require.NoError(t, c.Close())

	state := chunkCompressed(t, pool, "transactions")
	require.Len(t, state, 2)
	compressedCount := 0
	for _, compressed := range state {
		if compressed {
			compressedCount++
		}
	}
	assert.Equal(t, 1, compressedCount, "exactly the owned day is compressed: %v", state)

	// Compressing an already compressed chunk is not an error.
	c2, _ := newBackfillCompressor(ctx, pool, metrics.NewMetrics(prometheus.NewRegistry()).Ingestion, 1, runTop)
	for chunk, compressed := range state {
		if compressed {
			require.NoError(t, c2.compress(ctx, chunk))
		}
	}
	require.NoError(t, c2.Close())
}

func Test_backfillCompressor_slotsBoundWindowsInFlight(t *testing.T) {
	pool, _ := openBackfillTestDB(t)
	ctx := context.Background()
	c, _ := newBackfillCompressor(ctx, pool, metrics.NewMetrics(prometheus.NewRegistry()).Ingestion, 5, time.Now())
	defer func() { require.NoError(t, c.Close()) }()

	// K=5 allows two windows in flight; the third acquire must block until a
	// window closes.
	require.NoError(t, c.AcquireSlot(ctx))
	require.NoError(t, c.AcquireSlot(ctx))
	blocked, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
	defer cancel()
	err := c.AcquireSlot(blocked)
	require.Error(t, err)
	assert.ErrorIs(t, err, context.DeadlineExceeded)

	// A window the compressor does not own releases its slot at once.
	require.NoError(t, c.WindowClosed(ctx, backfillWindow{BelowCursor: false}))
	require.NoError(t, c.AcquireSlot(ctx))
}

// partialLookupChunksCount counts the transaction_hashes chunks that hold rows
// in their rowstore part next to compressed data.
func partialLookupChunksCount(t *testing.T, pool *pgxpool.Pool) int {
	t.Helper()
	var n int
	require.NoError(t, pool.QueryRow(context.Background(), `
		SELECT count(*) FROM _timescaledb_catalog.chunk c
		JOIN _timescaledb_catalog.hypertable h ON h.id = c.hypertable_id
		WHERE h.table_name = 'transaction_hashes' AND c.status & 8 <> 0`).Scan(&n))
	return n
}

func Test_backfillCompressor_LookupRows(t *testing.T) {
	pool, _ := openBackfillTestDB(t)
	ctx := context.Background()
	// A compressed lookup chunk that then receives rows: the shape every
	// window leaves behind, since a window's hashes spread over every chunk.
	insertTestLookupRow(t, pool, 1792627200)
	_, err := pool.Exec(ctx, `SELECT compress_chunk(c) FROM show_chunks('transaction_hashes') c`)
	require.NoError(t, err)
	insertTestLookupRow(t, pool, 1792627300)
	require.Equal(t, 1, partialLookupChunksCount(t, pool))

	runTop := time.Unix(1792627200+3600, 0).UTC()

	t.Run("the pending rows queue a recompression at the threshold", func(t *testing.T) {
		// Two connections: the pass runs as two share jobs over disjoint chunks.
		c, _ := newBackfillCompressor(ctx, pool, metrics.NewMetrics(prometheus.NewRegistry()).Ingestion, 2, runTop)
		require.NoError(t, c.LookupRows(ctx, lookupRecompressRows-1)) // not yet
		require.Equal(t, 1, partialLookupChunksCount(t, pool))
		require.NoError(t, c.LookupRows(ctx, 1)) // queues it
		require.NoError(t, c.Close())
		assert.Zero(t, partialLookupChunksCount(t, pool))
	})

	t.Run("Close recompresses once more when rows are pending", func(t *testing.T) {
		insertTestLookupRow(t, pool, 1792627400)
		require.Equal(t, 1, partialLookupChunksCount(t, pool))
		c, _ := newBackfillCompressor(ctx, pool, metrics.NewMetrics(prometheus.NewRegistry()).Ingestion, 1, runTop)
		require.NoError(t, c.LookupRows(ctx, 1))
		require.NoError(t, c.Close())
		assert.Zero(t, partialLookupChunksCount(t, pool))
	})

	t.Run("Close does nothing when no rows were written", func(t *testing.T) {
		insertTestLookupRow(t, pool, 1792627500)
		c, _ := newBackfillCompressor(ctx, pool, metrics.NewMetrics(prometheus.NewRegistry()).Ingestion, 1, runTop)
		require.NoError(t, c.Close())
		assert.Equal(t, 1, partialLookupChunksCount(t, pool))
	})
}

func Test_checkRetentionKeeps(t *testing.T) {
	pool, models := openBackfillTestDB(t)
	ctx := context.Background()
	svc := &ingestService{models: models}

	require.NoError(t, svc.checkRetentionKeeps(ctx, time.Now().Add(-365*24*time.Hour)), "no retention policy keeps everything")

	_, err := pool.Exec(ctx, `SELECT add_retention_policy('transactions', INTERVAL '30 days')`)
	require.NoError(t, err)
	require.NoError(t, svc.checkRetentionKeeps(ctx, time.Now().Add(-10*24*time.Hour)))
	err = svc.checkRetentionKeeps(ctx, time.Now().Add(-60*24*time.Hour))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "raise retention")
	assert.Contains(t, err.Error(), fmt.Sprint(time.Now().Year()))
}
