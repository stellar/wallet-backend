package data

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/wallet-backend/internal/db"
	"github.com/stellar/wallet-backend/internal/db/dbtest"
)

func Test_DataMigrationModel(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()
	ctx := context.Background()
	pool, err := db.OpenDBConnectionPool(ctx, dbt.DSN)
	require.NoError(t, err)
	defer pool.Close()
	m := &DataMigrationModel{DB: pool}

	get := func(t *testing.T, name string) *DataMigration {
		t.Helper()
		row, err := m.Get(ctx, name)
		require.NoError(t, err)
		require.NotNil(t, row)
		return row
	}
	advance := func(t *testing.T, name string, cursor uint32) {
		t.Helper()
		require.NoError(t, db.RunInTransaction(ctx, pool, func(dbTx pgx.Tx) error {
			return m.Advance(ctx, dbTx, name, cursor)
		}))
	}

	t.Run("get_returns_nil_when_missing", func(t *testing.T) {
		row, err := m.Get(ctx, "missing")
		require.NoError(t, err)
		assert.Nil(t, row)
	})

	t.Run("insert_if_missing_keeps_the_first_boundary", func(t *testing.T) {
		require.NoError(t, m.InsertIfMissing(ctx, pool, "pending-mig", DataMigrationPending, 500))
		require.NoError(t, m.InsertIfMissing(ctx, pool, "pending-mig", DataMigrationDone, 900))

		row := get(t, "pending-mig")
		assert.Equal(t, DataMigrationPending, row.Status)
		assert.Equal(t, uint32(500), row.BoundaryLedger)
		assert.Equal(t, uint32(500), row.CursorLedger)
		assert.Nil(t, row.StartedAt)
		assert.Nil(t, row.FinishedAt, "a pending row is not finished")
		assert.Nil(t, row.LastError)
	})

	t.Run("insert_if_missing_as_done_sets_finished_at", func(t *testing.T) {
		require.NoError(t, m.InsertIfMissing(ctx, pool, "done-mig", DataMigrationDone, 700))

		row := get(t, "done-mig")
		assert.Equal(t, DataMigrationDone, row.Status)
		assert.Equal(t, uint32(700), row.BoundaryLedger)
		assert.NotNil(t, row.FinishedAt)
	})

	t.Run("advance_never_raises_the_cursor", func(t *testing.T) {
		require.NoError(t, m.InsertIfMissing(ctx, pool, "advance-mig", DataMigrationPending, 500))

		advance(t, "advance-mig", 400)
		assert.Equal(t, uint32(400), get(t, "advance-mig").CursorLedger)
		advance(t, "advance-mig", 450)
		assert.Equal(t, uint32(400), get(t, "advance-mig").CursorLedger, "a higher cursor is ignored")
		assert.Equal(t, uint32(500), get(t, "advance-mig").BoundaryLedger)
	})

	t.Run("start_set_error_mark_done", func(t *testing.T) {
		const name = "lifecycle-mig"
		require.NoError(t, m.InsertIfMissing(ctx, pool, name, DataMigrationPending, 500))

		require.NoError(t, m.Start(ctx, name))
		row := get(t, name)
		assert.Equal(t, DataMigrationInProgress, row.Status)
		require.NotNil(t, row.StartedAt)
		firstStart := *row.StartedAt

		require.NoError(t, m.SetError(ctx, name, "slice failed"))
		row = get(t, name)
		assert.Equal(t, DataMigrationInProgress, row.Status, "an error leaves the run resumable")
		require.NotNil(t, row.LastError)
		assert.Equal(t, "slice failed", *row.LastError)

		// A later run keeps the first start time and clears the error.
		time.Sleep(10 * time.Millisecond)
		require.NoError(t, m.Start(ctx, name))
		row = get(t, name)
		assert.Nil(t, row.LastError)
		require.NotNil(t, row.StartedAt)
		assert.True(t, firstStart.Equal(*row.StartedAt))

		require.NoError(t, m.SetError(ctx, name, "slice failed again"))
		require.NoError(t, m.MarkDone(ctx, name))
		row = get(t, name)
		assert.Equal(t, DataMigrationDone, row.Status)
		assert.NotNil(t, row.FinishedAt)
		assert.Nil(t, row.LastError)
	})

	t.Run("list_orders_by_name", func(t *testing.T) {
		rows, err := m.List(ctx)
		require.NoError(t, err)
		names := make([]string, len(rows))
		for i, r := range rows {
			names[i] = r.Name
		}
		assert.Equal(t, []string{"advance-mig", "done-mig", "lifecycle-mig", "pending-mig"}, names)
	})
}

func Test_ListCompressedChunksAndCompressChunk(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()
	ctx := context.Background()
	pool, err := db.OpenDBConnectionPool(ctx, dbt.DSN)
	require.NoError(t, err)
	defer pool.Close()

	// Three transactions on three days: one chunk each.
	day1 := time.Date(2026, time.March, 1, 12, 0, 0, 0, time.UTC)
	for i := range 3 {
		closedAt := day1.Add(time.Duration(i) * 24 * time.Hour)
		hash := make([]byte, 32)
		hash[0] = byte(i)
		_, err = pool.Exec(ctx, `INSERT INTO transactions (hash, to_id, fee_charged, result_code, ledger_number, ledger_created_at, is_fee_bump)
			VALUES ($1, $2, 100, 'TransactionResultCodeTxSuccess', $3, $4, false)`, hash, int64(i+1)<<32, 100+i, closedAt)
		require.NoError(t, err)
	}
	// Compress days 1 and 2.
	_, err = pool.Exec(ctx, `SELECT compress_chunk(c) FROM show_chunks('transactions', older_than => $1::timestamptz) c`, day1.Add(48*time.Hour))
	require.NoError(t, err)

	chunks, err := ListCompressedChunks(ctx, pool, []string{"transactions", "operations"})
	require.NoError(t, err)
	require.Len(t, chunks, 2)
	assert.True(t, chunks[0].RangeStart.After(chunks[1].RangeStart), "newest first")
	assert.False(t, chunks[0].RangeStart.After(day1.Add(24*time.Hour)))
	assert.False(t, chunks[1].RangeStart.After(day1))

	t.Run("compresses_an_uncompressed_chunk", func(t *testing.T) {
		var day3 string
		require.NoError(t, pool.QueryRow(ctx, `SELECT c::text FROM show_chunks('transactions', newer_than => $1::timestamptz) c`, day1.Add(24*time.Hour)).Scan(&day3))
		require.NoError(t, CompressChunk(ctx, pool, day3))

		chunks, err := ListCompressedChunks(ctx, pool, []string{"transactions"})
		require.NoError(t, err)
		assert.Len(t, chunks, 3)
	})

	t.Run("compressed_chunk_is_a_no_op", func(t *testing.T) {
		require.NoError(t, CompressChunk(ctx, pool, chunks[0].Name))
	})

	t.Run("missing_chunk_is_a_no_op", func(t *testing.T) {
		require.NoError(t, CompressChunk(ctx, pool, "_timescaledb_internal._hyper_1_999_chunk"))
	})
}
