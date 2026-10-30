package data

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stellar/go-stellar-sdk/toid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/wallet-backend/internal/db"
	"github.com/stellar/wallet-backend/internal/db/dbtest"
	"github.com/stellar/wallet-backend/internal/indexer/types"
	"github.com/stellar/wallet-backend/internal/metrics"
)

// insertTransactionHashesFromTransactions replaces transaction_hashes with one row per
// transactions row, for tests that insert transactions with plain SQL. It uses the same SQL
// expressions as PopulateHashes.
func insertTransactionHashesFromTransactions(ctx context.Context, t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	_, err := pool.Exec(ctx, `DELETE FROM transaction_hashes`)
	require.NoError(t, err)
	_, err = pool.Exec(ctx, fmt.Sprintf(`
		INSERT INTO transaction_hashes (hash_bucket, hash_prefix, ledger_close_epoch)
		SELECT %s, %s, floor(extract(epoch FROM ledger_created_at))::bigint FROM transactions`, sqlHashBucket, sqlHashPrefix))
	require.NoError(t, err)
}

func randomHash(t *testing.T) types.HashBytea {
	t.Helper()
	b := make([]byte, 32)
	_, err := rand.Read(b)
	require.NoError(t, err)
	return types.HashBytea(hex.EncodeToString(b))
}

func newTestTransactionModel(t *testing.T) (*TransactionModel, *pgxpool.Pool, context.Context) {
	t.Helper()
	dbt := dbtest.Open(t)
	t.Cleanup(func() { dbt.Close() })
	ctx := context.Background()
	pool, err := db.OpenDBConnectionPool(ctx, dbt.DSN)
	require.NoError(t, err)
	t.Cleanup(pool.Close)
	return &TransactionModel{DB: pool, Metrics: metrics.NewMetrics(prometheus.NewRegistry()).DB}, pool, ctx
}

func insertTestTransaction(ctx context.Context, t *testing.T, pool *pgxpool.Pool, hash types.HashBytea, toID int64, ledgerCreatedAt time.Time) {
	t.Helper()
	_, err := pool.Exec(ctx, `
		INSERT INTO transactions (hash, to_id, fee_charged, result_code, ledger_number, ledger_created_at, is_fee_bump)
		VALUES ($1, $2, 100, 'TransactionResultCodeTxSuccess', $3, $4, false)`,
		hash, toID, toID>>32, ledgerCreatedAt)
	require.NoError(t, err)
}

func TestHashBytea_LookupKeyMatchesSQL(t *testing.T) {
	m, pool, ctx := newTestTransactionModel(t)
	_ = m

	closeTime := time.Unix(1_790_000_000, 0).UTC()
	hashes := []types.HashBytea{
		"0000000000000000000000000000000000000000000000000000000000000000",
		"ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff",
		"8000000000ffffffffffffffffffffffffffffffffffffffffffffffffffffff",
	}
	for range 200 {
		hashes = append(hashes, randomHash(t))
	}
	for i, h := range hashes {
		insertTestTransaction(ctx, t, pool, h, toid.New(1000, int32(i+1), 0).ToInt64(), closeTime)
	}

	rows, err := pool.Query(ctx, fmt.Sprintf(`SELECT encode(hash, 'hex'), %s, %s FROM transactions`, sqlHashBucket, sqlHashPrefix))
	require.NoError(t, err)
	defer rows.Close()
	n := 0
	for rows.Next() {
		var hash string
		var sqlBucket int16
		var sqlPrefix int64
		require.NoError(t, rows.Scan(&hash, &sqlBucket, &sqlPrefix))
		bucket, prefix, err := types.HashBytea(hash).LookupKey()
		require.NoError(t, err)
		assert.Equal(t, sqlPrefix, prefix, hash)
		assert.Equal(t, sqlBucket, bucket, hash)
		assert.GreaterOrEqual(t, prefix, int64(0))
		assert.Less(t, prefix, int64(1)<<40)
		assert.Less(t, bucket, int16(4096))
		n++
	}
	require.NoError(t, rows.Err())
	assert.Equal(t, len(hashes), n)
}

func TestTransactionModel_BatchCopyHashes(t *testing.T) {
	m, pool, ctx := newTestTransactionModel(t)

	closeTime := time.Unix(1_790_000_123, 0).UTC()
	txs := []*types.Transaction{
		{Hash: randomHash(t), LedgerCreatedAt: closeTime},
		{Hash: randomHash(t), LedgerCreatedAt: closeTime.Add(5 * time.Second)},
	}
	err := db.RunInTransaction(ctx, pool, func(tx pgx.Tx) error {
		n, err := m.BatchCopyHashes(ctx, tx, txs)
		require.Equal(t, 2, n)
		return err
	})
	require.NoError(t, err)

	for _, tx := range txs {
		bucket, prefix, err := tx.Hash.LookupKey()
		require.NoError(t, err)
		var epoch int64
		err = pool.QueryRow(ctx, `SELECT ledger_close_epoch FROM transaction_hashes WHERE hash_bucket = $1 AND hash_prefix = $2`, bucket, prefix).Scan(&epoch)
		require.NoError(t, err)
		assert.Equal(t, tx.LedgerCreatedAt.Unix(), epoch)
	}

	t.Run("empty input writes nothing", func(t *testing.T) {
		err := db.RunInTransaction(ctx, pool, func(tx pgx.Tx) error {
			n, err := m.BatchCopyHashes(ctx, tx, nil)
			assert.Zero(t, n)
			return err
		})
		require.NoError(t, err)
	})
}

func TestTransactionModel_GetByHash(t *testing.T) {
	m, pool, ctx := newTestTransactionModel(t)

	older := time.Unix(1_790_000_000, 0).UTC()
	newer := older.Add(8 * 24 * time.Hour) // a different transaction_hashes chunk
	hit := randomHash(t)
	insertTestTransaction(ctx, t, pool, hit, toid.New(1000, 1, 0).ToInt64(), older)

	// Two hashes sharing the 40-bit prefix (first 5 bytes), so both rows land on the same
	// (bucket, prefix) and only the full-hash check tells them apart.
	sharedA := types.HashBytea("abcdef0123" + "00" + string(randomHash(t))[12:])
	sharedB := types.HashBytea("abcdef0123" + "ff" + string(randomHash(t))[12:])
	insertTestTransaction(ctx, t, pool, sharedA, toid.New(1000, 2, 0).ToInt64(), older)
	insertTestTransaction(ctx, t, pool, sharedB, toid.New(2000, 1, 0).ToInt64(), newer)
	insertTransactionHashesFromTransactions(ctx, t, pool)

	t.Run("finds a transaction by hash", func(t *testing.T) {
		tx, err := m.GetByHash(ctx, hit.String(), "")
		require.NoError(t, err)
		require.NotNil(t, tx)
		assert.Equal(t, hit, tx.Hash)
		assert.Equal(t, toid.New(1000, 1, 0).ToInt64(), tx.ToID)
		assert.True(t, older.Equal(tx.LedgerCreatedAt))
	})

	t.Run("an unknown hash returns nil and no error", func(t *testing.T) {
		tx, err := m.GetByHash(ctx, randomHash(t).String(), "")
		require.NoError(t, err)
		assert.Nil(t, tx)
	})

	t.Run("hashes sharing a prefix resolve to their own transactions", func(t *testing.T) {
		a, err := m.GetByHash(ctx, sharedA.String(), "")
		require.NoError(t, err)
		require.NotNil(t, a)
		assert.Equal(t, sharedA, a.Hash)

		b, err := m.GetByHash(ctx, sharedB.String(), "hash")
		require.NoError(t, err)
		require.NotNil(t, b)
		assert.Equal(t, sharedB, b.Hash)
	})

	t.Run("duplicate lookup rows from a re-ingested ledger still return one transaction", func(t *testing.T) {
		bucket, prefix, err := hit.LookupKey()
		require.NoError(t, err)
		_, err = pool.Exec(ctx, `INSERT INTO transaction_hashes VALUES ($1, $2, $3)`, bucket, prefix, older.Unix())
		require.NoError(t, err)

		tx, err := m.GetByHash(ctx, hit.String(), "")
		require.NoError(t, err)
		require.NotNil(t, tx)
		assert.Equal(t, hit, tx.Hash)
	})

	t.Run("a transaction above the committed ingest cursor is hidden", func(t *testing.T) {
		_, err := pool.Exec(ctx, `INSERT INTO ingest_store (key, value) VALUES ($1, '1500')
			ON CONFLICT (key) DO UPDATE SET value = excluded.value`, LatestLedgerCursorName)
		require.NoError(t, err)
		t.Cleanup(func() {
			_, _ = pool.Exec(ctx, `DELETE FROM ingest_store WHERE key = $1`, LatestLedgerCursorName) //nolint:errcheck
		})

		below, err := m.GetByHash(ctx, hit.String(), "")
		require.NoError(t, err)
		assert.NotNil(t, below, "ledger 1000 is at or below the cursor")

		above, err := m.GetByHash(ctx, sharedB.String(), "")
		require.NoError(t, err)
		assert.Nil(t, above, "ledger 2000 is above the cursor")
	})

	t.Run("a close time with a fractional second is still found, from SQL and Go lookup rows", func(t *testing.T) {
		fractional := older.Add(700 * time.Millisecond)
		viaSQL := randomHash(t)
		insertTestTransaction(ctx, t, pool, viaSQL, toid.New(1000, 3, 0).ToInt64(), fractional)
		insertTransactionHashesFromTransactions(ctx, t, pool) // floor(extract(epoch ...))

		viaGo := &types.Transaction{Hash: randomHash(t), ToID: toid.New(1000, 4, 0).ToInt64(), LedgerCreatedAt: fractional}
		insertTestTransaction(ctx, t, pool, viaGo.Hash, viaGo.ToID, fractional)
		require.NoError(t, db.RunInTransaction(ctx, pool, func(tx pgx.Tx) error {
			_, err := m.BatchCopyHashes(ctx, tx, []*types.Transaction{viaGo}) // time.Unix()
			return err
		}))

		for _, h := range []types.HashBytea{viaSQL, viaGo.Hash} {
			tx, err := m.GetByHash(ctx, h.String(), "")
			require.NoError(t, err)
			require.NotNil(t, tx, h)
			assert.Equal(t, h, tx.Hash)
		}
	})

	t.Run("a malformed hash is an error", func(t *testing.T) {
		_, err := m.GetByHash(ctx, "not-hex", "")
		require.Error(t, err)
	})
}

// TestTransactionModel_GetByHash_CompressedChunks runs the lookup against compressed chunks of
// both tables, the production shape: the compressed transaction_hashes chunk answers from its
// (hash_bucket, hash_prefix) index and the transactions probe from the bloom(hash) sparse index.
func TestTransactionModel_GetByHash_CompressedChunks(t *testing.T) {
	m, pool, ctx := newTestTransactionModel(t)

	older := time.Unix(1_790_000_000, 0).UTC()
	newer := older.Add(8 * 24 * time.Hour) // a different transaction_hashes chunk
	hit := randomHash(t)
	insertTestTransaction(ctx, t, pool, hit, toid.New(1000, 1, 0).ToInt64(), older)
	sharedA := types.HashBytea("abcdef0123" + "00" + string(randomHash(t))[12:])
	sharedB := types.HashBytea("abcdef0123" + "ff" + string(randomHash(t))[12:])
	insertTestTransaction(ctx, t, pool, sharedA, toid.New(1000, 2, 0).ToInt64(), older)
	insertTestTransaction(ctx, t, pool, sharedB, toid.New(2000, 1, 0).ToInt64(), newer)
	insertTransactionHashesFromTransactions(ctx, t, pool)

	for _, table := range []string{"transaction_hashes", "transactions"} {
		_, err := pool.Exec(ctx, fmt.Sprintf(`SELECT compress_chunk(c) FROM show_chunks('%s') c`, table))
		require.NoError(t, err)
	}
	compressed, err := db.QueryOne[int](ctx, pool,
		`SELECT count(*) FROM timescaledb_information.chunks WHERE hypertable_name IN ('transaction_hashes', 'transactions') AND NOT is_compressed`)
	require.NoError(t, err)
	require.Zero(t, compressed, "every chunk of both tables is compressed")

	tx, err := m.GetByHash(ctx, hit.String(), "")
	require.NoError(t, err)
	require.NotNil(t, tx)
	assert.Equal(t, hit, tx.Hash)

	tx, err = m.GetByHash(ctx, randomHash(t).String(), "")
	require.NoError(t, err)
	assert.Nil(t, tx)

	for _, h := range []types.HashBytea{sharedA, sharedB} {
		tx, err := m.GetByHash(ctx, h.String(), "")
		require.NoError(t, err)
		require.NotNil(t, tx, h)
		assert.Equal(t, h, tx.Hash)
	}

	toID, ledgerCreatedAt, found, err := transactionKeyByHash(ctx, pool, hit.String())
	require.NoError(t, err)
	require.True(t, found)
	assert.Equal(t, toid.New(1000, 1, 0).ToInt64(), toID)
	assert.True(t, older.Equal(ledgerCreatedAt))
}
