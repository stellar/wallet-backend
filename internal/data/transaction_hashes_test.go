package data

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
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
// expressions as the one-off fill in docs/deploy/upgrading.md.
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
		var sqlBucket int32
		var sqlPrefix int64
		require.NoError(t, rows.Scan(&hash, &sqlBucket, &sqlPrefix))
		bucket, prefix, err := types.HashBytea(hash).LookupKey()
		require.NoError(t, err)
		assert.Equal(t, sqlPrefix, prefix, hash)
		assert.Equal(t, sqlBucket, bucket, hash)
		assert.GreaterOrEqual(t, prefix, int64(0))
		assert.Less(t, prefix, int64(1)<<40)
		assert.Less(t, bucket, int32(1)<<20)
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
	newer := older.Add(8 * 24 * time.Hour) // a different transactions chunk
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

// sameChunkHash returns a random hash whose prefix falls in the same 2^30-wide transaction_hashes
// chunk as like: the chunk is decided by the top 10 bits of the 40-bit prefix, so sharing the
// first 3 hex characters is enough.
func sameChunkHash(t *testing.T, like types.HashBytea) types.HashBytea {
	t.Helper()
	return types.HashBytea(string(like)[:3] + string(randomHash(t))[3:])
}

// partialLookupChunks counts the transaction_hashes chunks that hold rows in their rowstore part
// next to compressed data: status bit 8 in TimescaleDB's chunk catalog.
func partialLookupChunks(ctx context.Context, t *testing.T, pool *pgxpool.Pool) int {
	t.Helper()
	n, err := db.QueryOne[int](ctx, pool, `
		SELECT count(*) FROM _timescaledb_catalog.chunk c
		JOIN _timescaledb_catalog.hypertable h ON h.id = c.hypertable_id
		WHERE h.table_name = 'transaction_hashes' AND c.status & 8 <> 0`)
	require.NoError(t, err)
	return n
}

// TestTransactionModel_GetByHash_CompressedChunks runs the lookup against compressed chunks of
// both tables, the production shape: the compressed transaction_hashes chunk answers from its
// (hash_bucket, hash_prefix) index and the transactions probe from the bloom(hash) sparse index.
// It then writes into a compressed lookup chunk, the shape of every live ledger after the first
// maintenance run, and recompresses it.
func TestTransactionModel_GetByHash_CompressedChunks(t *testing.T) {
	m, pool, ctx := newTestTransactionModel(t)

	older := time.Unix(1_790_000_000, 0).UTC()
	newer := older.Add(8 * 24 * time.Hour) // a different transactions chunk
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

	t.Run("a row written into a compressed chunk is found before and after recompression", func(t *testing.T) {
		late := sameChunkHash(t, hit)
		latest := newer.Add(24 * time.Hour)
		insertTestTransaction(ctx, t, pool, late, toid.New(3000, 1, 0).ToInt64(), latest)
		err := db.RunInTransaction(ctx, pool, func(tx pgx.Tx) error {
			_, copyErr := m.BatchCopyHashes(ctx, tx, []*types.Transaction{{Hash: late, LedgerCreatedAt: latest}})
			return copyErr
		})
		require.NoError(t, err)
		require.Equal(t, 1, partialLookupChunks(ctx, t, pool), "the row sits in the rowstore part of hit's compressed chunk")

		tx, err := m.GetByHash(ctx, late.String(), "")
		require.NoError(t, err)
		require.NotNil(t, tx)
		assert.Equal(t, late, tx.Hash)

		_, err = pool.Exec(ctx, `SELECT compress_chunk(c, if_not_compressed => true) FROM show_chunks('transaction_hashes') c`)
		require.NoError(t, err)
		require.Zero(t, partialLookupChunks(ctx, t, pool), "compress_chunk recompresses a partial chunk")

		for _, h := range []types.HashBytea{late, hit} {
			tx, err := m.GetByHash(ctx, h.String(), "")
			require.NoError(t, err)
			require.NotNil(t, tx, h)
			assert.Equal(t, h, tx.Hash)
		}
	})
}

// TestLookupCloseTimes_PlansOneChunk pins what the hash partitioning is for: with rows in more
// than one transaction_hashes chunk, the lookup is planned on exactly one of them. It runs under
// pgx's Exec query mode, the API server's, where the parameters are bound before planning.
func TestLookupCloseTimes_PlansOneChunk(t *testing.T) {
	m, pool, ctx := newTestTransactionModel(t)

	closeTime := time.Unix(1_790_000_000, 0).UTC()
	hashes := []types.HashBytea{
		types.HashBytea("0123456789" + string(randomHash(t))[10:]), // chunk of prefix range 0x004
		types.HashBytea("abcdef0123" + string(randomHash(t))[10:]), // chunk of prefix range 0x2af
		types.HashBytea("fedcba9876" + string(randomHash(t))[10:]), // chunk of prefix range 0x3fb
	}
	// Close times weeks apart: partitioned by close time, the lookup would have to open every
	// chunk; partitioned by prefix, it opens the one chunk of the prefix.
	closeTimes := map[types.HashBytea]time.Time{}
	for i, h := range hashes {
		closeTimes[h] = closeTime.Add(time.Duration(i) * 8 * 24 * time.Hour)
		insertTestTransaction(ctx, t, pool, h, toid.New(int32(1000*(i+1)), 1, 0).ToInt64(), closeTimes[h])
	}
	insertTransactionHashesFromTransactions(ctx, t, pool)
	chunkRows, err := pool.Query(ctx, `SELECT c::text FROM show_chunks('transaction_hashes') c`)
	require.NoError(t, err)
	lookupChunkNames, err := pgx.CollectRows(chunkRows, pgx.RowTo[string])
	require.NoError(t, err)
	require.Len(t, lookupChunkNames, 3)
	for i, name := range lookupChunkNames {
		lookupChunkNames[i] = name[strings.LastIndex(name, ".")+1:] // schema-qualified in show_chunks
	}

	cfg := db.DefaultPoolConfig()
	cfg.QueryExecMode = pgx.QueryExecModeExec
	apiPool, err := db.OpenDBConnectionPool(ctx, pool.Config().ConnString(), cfg)
	require.NoError(t, err)
	t.Cleanup(apiPool.Close)

	for _, compressed := range []bool{false, true} {
		if compressed {
			_, err := pool.Exec(ctx, `SELECT compress_chunk(c) FROM show_chunks('transaction_hashes') c`)
			require.NoError(t, err)
		}
		for _, h := range hashes {
			bucket, prefix, err := h.LookupKey()
			require.NoError(t, err)
			rows, err := apiPool.Query(ctx,
				`EXPLAIN SELECT DISTINCT ledger_close_epoch FROM transaction_hashes WHERE hash_bucket = $1 AND hash_prefix = $2 ORDER BY ledger_close_epoch DESC`,
				bucket, prefix)
			require.NoError(t, err)
			plan, err := pgx.CollectRows(rows, pgx.RowTo[string])
			require.NoError(t, err)
			planText := strings.Join(plan, "\n")
			// Count the lookup table's own chunks in the plan; the compressed relation of a
			// chunk carries a name of its own that must not count as a second chunk.
			planned := 0
			for _, chunk := range lookupChunkNames {
				if regexp.MustCompile(`\b` + regexp.QuoteMeta(chunk) + `\b`).MatchString(planText) {
					planned++
				}
			}
			assert.Equal(t, 1, planned, "one chunk planned (compressed=%v):\n%s", compressed, planText)
			assert.NotContains(t, planText, "Append", "no Append over chunks (compressed=%v):\n%s", compressed, planText)

			found, err := lookupCloseTimes(ctx, apiPool, bucket, prefix)
			require.NoError(t, err)
			assert.Equal(t, []int64{closeTimes[h].Unix()}, found)
		}
	}
	_ = m
}

// partialLookupRanges returns the prefix range index (range start / 2^30) of each transaction_hashes
// chunk that holds rows in its rowstore part next to compressed data, in order.
func partialLookupRanges(ctx context.Context, t *testing.T, pool *pgxpool.Pool) []int64 {
	t.Helper()
	rows, err := pool.Query(ctx, `
		SELECT ds.range_start >> 30 FROM _timescaledb_catalog.chunk c
		JOIN _timescaledb_catalog.hypertable h ON h.id = c.hypertable_id
		JOIN _timescaledb_catalog.chunk_constraint cc ON cc.chunk_id = c.id
		JOIN _timescaledb_catalog.dimension_slice ds ON ds.id = cc.dimension_slice_id
		WHERE h.table_name = 'transaction_hashes' AND c.status & 8 <> 0
		ORDER BY 1`)
	require.NoError(t, err)
	ranges, err := pgx.CollectRows(rows, pgx.RowTo[int64])
	require.NoError(t, err)
	return ranges
}

// callLookupMaintenance runs transaction_hashes_maintenance the way a TimescaleDB job does: outside
// a transaction block, so its per-chunk COMMITs apply, through the simple protocol.
func callLookupMaintenance(ctx context.Context, t *testing.T, pool *pgxpool.Pool, jobID int, config string) {
	t.Helper()
	_, err := pool.Exec(ctx, `CALL transaction_hashes_maintenance($1, $2)`, pgx.QueryExecModeSimpleProtocol, jobID, config)
	require.NoError(t, err)
}

// lookupMaintenanceJob returns the id and config of the maintenance job the migration creates.
func lookupMaintenanceJob(ctx context.Context, t *testing.T, pool *pgxpool.Pool) (int, string) {
	t.Helper()
	var jobID int
	var config string
	err := pool.QueryRow(ctx,
		`SELECT job_id, config::text FROM timescaledb_information.jobs WHERE proc_name = 'transaction_hashes_maintenance'`,
	).Scan(&jobID, &config)
	require.NoError(t, err, "the migration creates one maintenance job")
	return jobID, config
}

// insertLookupRow writes a transaction_hashes row for hash with the given close time and no
// transactions row: the lookup row of a transaction that retention dropped.
func insertLookupRow(ctx context.Context, t *testing.T, pool *pgxpool.Pool, hash types.HashBytea, closeEpoch int64) {
	t.Helper()
	bucket, prefix, err := hash.LookupKey()
	require.NoError(t, err)
	_, err = pool.Exec(ctx,
		`INSERT INTO transaction_hashes (hash_bucket, hash_prefix, ledger_close_epoch) VALUES ($1, $2, $3)`,
		bucket, prefix, closeEpoch)
	require.NoError(t, err)
}

// TestTransactionHashesMaintenance runs the table's maintenance procedure: recompression of a
// worker's share of the chunks, and the retention pass that rebuilds each chunk without the lookup
// rows older than the oldest transactions row.
func TestTransactionHashesMaintenance(t *testing.T) {
	const day = 24 * time.Hour
	floor := time.Unix(1_790_000_000, 0).UTC()
	// One hash per lookup chunk, in chunks of prefix range 0x004, 0x2af and 0x3fb: one even range
	// and two odd ones, so two workers each get a share.
	chunkHashes := func(t *testing.T) []types.HashBytea {
		return []types.HashBytea{
			types.HashBytea("0123456789" + string(randomHash(t))[10:]),
			types.HashBytea("abcdef0123" + string(randomHash(t))[10:]),
			types.HashBytea("fedcba9876" + string(randomHash(t))[10:]),
		}
	}
	// seed writes one transaction per hash, a week apart from floor on, with its lookup row, and
	// compresses every lookup chunk.
	seed := func(ctx context.Context, t *testing.T, pool *pgxpool.Pool, hashes []types.HashBytea) {
		for i, h := range hashes {
			insertTestTransaction(ctx, t, pool, h, toid.New(int32(1000*(i+1)), 1, 0).ToInt64(), floor.Add(time.Duration(i)*7*day))
		}
		insertTransactionHashesFromTransactions(ctx, t, pool)
		_, err := pool.Exec(ctx, `SELECT compress_chunk(c) FROM show_chunks('transaction_hashes') c`)
		require.NoError(t, err)
	}
	// writeLate writes one more transaction into the compressed lookup chunk of each hash, through
	// ingestion's COPY, and returns the new hashes.
	writeLate := func(ctx context.Context, t *testing.T, m *TransactionModel, pool *pgxpool.Pool, like []types.HashBytea) []types.HashBytea {
		late := make([]types.HashBytea, len(like))
		txs := make([]*types.Transaction, len(like))
		closeTime := floor.Add(30 * day)
		for i, h := range like {
			late[i] = sameChunkHash(t, h)
			insertTestTransaction(ctx, t, pool, late[i], toid.New(int32(5000+i), 1, 0).ToInt64(), closeTime)
			txs[i] = &types.Transaction{Hash: late[i], LedgerCreatedAt: closeTime}
		}
		err := db.RunInTransaction(ctx, pool, func(tx pgx.Tx) error {
			_, copyErr := m.BatchCopyHashes(ctx, tx, txs)
			return copyErr
		})
		require.NoError(t, err)
		return late
	}
	assertFound := func(ctx context.Context, t *testing.T, m *TransactionModel, hashes []types.HashBytea) {
		for _, h := range hashes {
			tx, err := m.GetByHash(ctx, h.String(), "")
			require.NoError(t, err)
			require.NotNil(t, tx, h)
			assert.Equal(t, h, tx.Hash)
		}
	}
	countLookup := func(ctx context.Context, t *testing.T, pool *pgxpool.Pool, query string) int {
		n, err := db.QueryOne[int](ctx, pool, query)
		require.NoError(t, err)
		return n
	}
	const (
		countRows         = `SELECT count(*) FROM transaction_hashes`
		countChunks       = `SELECT count(*) FROM timescaledb_information.chunks WHERE hypertable_name = 'transaction_hashes'`
		countUncompressed = `SELECT count(*) FROM timescaledb_information.chunks WHERE hypertable_name = 'transaction_hashes' AND NOT is_compressed`
	)

	t.Run("recompresses the chunks of its share", func(t *testing.T) {
		m, pool, ctx := newTestTransactionModel(t)
		hashes := chunkHashes(t)
		seed(ctx, t, pool, hashes)
		late := writeLate(ctx, t, m, pool, hashes[:1])
		require.Equal(t, 1, partialLookupChunks(ctx, t, pool), "the late row sits in the rowstore part of a compressed chunk")

		callLookupMaintenance(ctx, t, pool, 0, `{"workers": 1, "worker": 0}`)

		assert.Zero(t, partialLookupChunks(ctx, t, pool))
		assert.Zero(t, countLookup(ctx, t, pool, countUncompressed))
		assert.Equal(t, 4, countLookup(ctx, t, pool, countRows))
		assertFound(ctx, t, m, append(hashes, late...))
	})

	t.Run("two workers split the chunks", func(t *testing.T) {
		m, pool, ctx := newTestTransactionModel(t)
		hashes := chunkHashes(t)
		seed(ctx, t, pool, hashes)
		late := writeLate(ctx, t, m, pool, hashes)
		require.Equal(t, []int64{0x004, 0x2af, 0x3fb}, partialLookupRanges(ctx, t, pool))

		callLookupMaintenance(ctx, t, pool, 0, `{"workers": 2, "worker": 0}`)
		assert.Equal(t, []int64{0x2af, 0x3fb}, partialLookupRanges(ctx, t, pool), "worker 0 of 2 takes the even ranges only")

		callLookupMaintenance(ctx, t, pool, 0, `{"workers": 2, "worker": 1}`)
		assert.Empty(t, partialLookupRanges(ctx, t, pool), "worker 1 of 2 takes the odd ranges")
		assertFound(ctx, t, m, append(hashes, late...))
	})

	t.Run("job_id 0 never deletes", func(t *testing.T) {
		_, pool, ctx := newTestTransactionModel(t)
		seed(ctx, t, pool, chunkHashes(t))
		insertLookupRow(ctx, t, pool, randomHash(t), floor.Add(-100*day).Unix())

		callLookupMaintenance(ctx, t, pool, 0, `{"workers": 1, "worker": 0}`)

		assert.Equal(t, 4, countLookup(ctx, t, pool, countRows), "the row below the floor stays")
	})

	// addTransactionsRetention gives transactions a retention policy whose cutoff lies after the
	// floor's period, unscheduled so the test alone decides what runs. The maintenance floor is
	// then the oldest transactions row's period.
	addTransactionsRetention := func(ctx context.Context, t *testing.T, pool *pgxpool.Pool) {
		t.Helper()
		_, err := pool.Exec(ctx, `SELECT alter_job(add_retention_policy('transactions', INTERVAL '30 days', initial_start => now() + INTERVAL '10 years'), scheduled => false)`)
		require.NoError(t, err)
	}

	t.Run("without a retention policy on transactions a scheduled run deletes nothing", func(t *testing.T) {
		m, pool, ctx := newTestTransactionModel(t)
		hashes := chunkHashes(t)
		seed(ctx, t, pool, hashes)
		insertLookupRow(ctx, t, pool, sameChunkHash(t, hashes[0]), floor.Add(-100*day).Unix())
		_, err := pool.Exec(ctx, `SELECT compress_chunk(c, if_not_compressed => true) FROM show_chunks('transaction_hashes') c`)
		require.NoError(t, err)
		require.Equal(t, 4, countLookup(ctx, t, pool, countRows))
		jobID, config := lookupMaintenanceJob(ctx, t, pool)

		callLookupMaintenance(ctx, t, pool, jobID, config)

		assert.Equal(t, 4, countLookup(ctx, t, pool, countRows), "nothing is ever dropped from transactions, so no lookup row is orphaned")
		assert.Zero(t, partialLookupChunks(ctx, t, pool))
		assertFound(ctx, t, m, hashes)
	})

	t.Run("a scheduled run deletes the periods that closed before the oldest transaction", func(t *testing.T) {
		m, pool, ctx := newTestTransactionModel(t)
		hashes := chunkHashes(t)
		seed(ctx, t, pool, hashes)
		addTransactionsRetention(ctx, t, pool)
		// Lookup rows of dropped history: one in the chunk of hashes[0], one alone in a chunk of its
		// own (prefix range 0x155), both in periods before the floor's; and one a day before the
		// floor, in the floor's own period, which the run keeps.
		insertLookupRow(ctx, t, pool, sameChunkHash(t, hashes[0]), floor.Add(-100*day).Unix())
		insertLookupRow(ctx, t, pool, types.HashBytea("5555555555"+string(randomHash(t))[10:]), floor.Add(-100*day).Unix())
		insertLookupRow(ctx, t, pool, sameChunkHash(t, hashes[1]), floor.Add(-day).Unix())
		_, err := pool.Exec(ctx, `SELECT compress_chunk(c, if_not_compressed => true) FROM show_chunks('transaction_hashes') c`)
		require.NoError(t, err)
		require.Equal(t, 6, countLookup(ctx, t, pool, countRows))
		require.Equal(t, 4, countLookup(ctx, t, pool, countChunks))
		jobID, config := lookupMaintenanceJob(ctx, t, pool)

		callLookupMaintenance(ctx, t, pool, jobID, config)

		assert.Equal(t, 4, countLookup(ctx, t, pool, countRows), "the rows of earlier periods are gone, the floor's period stays whole")
		floorPeriod := floor.Unix() / 2592000
		below, err := db.QueryOne[int](ctx, pool, `SELECT count(*) FROM transaction_hashes WHERE close_period < $1`, floorPeriod)
		require.NoError(t, err)
		assert.Zero(t, below)
		kept, err := db.QueryOne[int](ctx, pool, `SELECT count(*) FROM transaction_hashes WHERE ledger_close_epoch < $1`, floor.Unix())
		require.NoError(t, err)
		assert.Equal(t, 1, kept, "the row a day before the floor shares its period and stays")
		assert.Equal(t, 4, countLookup(ctx, t, pool, countChunks), "no chunk is dropped or rebuilt")
		assert.Zero(t, countLookup(ctx, t, pool, countUncompressed))
		assert.Zero(t, partialLookupChunks(ctx, t, pool), "whole batches are deleted; nothing is decompressed")
		assertFound(ctx, t, m, hashes)
	})

	t.Run("with no transactions the run is a no-op", func(t *testing.T) {
		_, pool, ctx := newTestTransactionModel(t)
		insertLookupRow(ctx, t, pool, randomHash(t), floor.Add(-100*day).Unix())
		jobID, config := lookupMaintenanceJob(ctx, t, pool)

		callLookupMaintenance(ctx, t, pool, jobID, config)

		assert.Equal(t, 1, countLookup(ctx, t, pool, countRows))
	})
}

// fillScriptStatements returns the statements of the one-off fill script in
// docs/deploy/upgrading.md, split the way psql sends them, so the procedure's COMMITs run outside
// a transaction block.
func fillScriptStatements(t *testing.T) []string {
	t.Helper()
	doc, err := os.ReadFile(filepath.Join("..", "..", "docs", "deploy", "upgrading.md"))
	require.NoError(t, err)
	_, rest, found := strings.Cut(string(doc), "## Filling `transaction_hashes`")
	require.True(t, found)
	_, rest, found = strings.Cut(rest, "```sql\n")
	require.True(t, found)
	script, _, found := strings.Cut(rest, "```")
	require.True(t, found)

	var statements []string
	var current strings.Builder
	inBody := false
	for _, line := range strings.Split(script, "\n") {
		current.WriteString(line)
		current.WriteString("\n")
		if strings.Count(line, "$$")%2 == 1 {
			inBody = !inBody
		}
		if !inBody && strings.HasSuffix(strings.TrimSpace(line), ";") {
			statements = append(statements, current.String())
			current.Reset()
		}
	}
	require.NotEmpty(t, statements)
	return statements
}

// TestFillTransactionHashesScript runs the fill script from docs/deploy/upgrading.md against a
// database whose history has no lookup rows: every transaction gets its row, the chunks end up
// compressed, a run under the row cadence recompresses once, at the end, and a second run writes
// nothing.
func TestFillTransactionHashesScript(t *testing.T) {
	m, pool, ctx := newTestTransactionModel(t)

	closeTime := time.Now().Add(-30 * 24 * time.Hour).Truncate(time.Second).UTC()
	hashes := []types.HashBytea{randomHash(t), randomHash(t), randomHash(t)}
	for i, h := range hashes {
		insertTestTransaction(ctx, t, pool, h, toid.New(int32(1000*(i+1)), 1, 0).ToInt64(), closeTime.Add(time.Duration(i)*8*24*time.Hour))
	}
	countRows := func(t *testing.T) int {
		n, err := db.QueryOne[int](ctx, pool, `SELECT count(*) FROM transaction_hashes`)
		require.NoError(t, err)
		return n
	}
	require.Zero(t, countRows(t))

	// The script runs on one connection that counts its recompression notices.
	var recompressions int
	connConfig, err := pgx.ParseConfig(pool.Config().ConnString())
	require.NoError(t, err)
	connConfig.OnNotice = func(_ *pgconn.PgConn, n *pgconn.Notice) {
		if strings.HasPrefix(n.Message, "transaction_hashes: recompressing") {
			recompressions++
		}
	}
	conn, err := pgx.ConnectConfig(ctx, connConfig)
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close(ctx) })

	run := func(t *testing.T) {
		recompressions = 0
		for _, statement := range fillScriptStatements(t) {
			_, err := conn.Exec(ctx, statement, pgx.QueryExecModeSimpleProtocol)
			require.NoError(t, err, statement)
		}
	}
	run(t)
	assert.Equal(t, len(hashes), countRows(t))
	assert.Equal(t, 1, recompressions, "under 500,000,000 rows the only recompression is the final one")
	assert.Zero(t, partialLookupChunks(ctx, t, pool))
	uncompressed, err := db.QueryOne[int](ctx, pool, `SELECT count(*) FROM timescaledb_information.chunks WHERE hypertable_name = 'transaction_hashes' AND NOT is_compressed`)
	require.NoError(t, err)
	assert.Zero(t, uncompressed, "the final maintenance run compresses every chunk")
	for _, h := range hashes {
		tx, err := m.GetByHash(ctx, h.String(), "")
		require.NoError(t, err)
		require.NotNil(t, tx, h)
		assert.Equal(t, h, tx.Hash)
	}

	run(t)
	assert.Equal(t, len(hashes), countRows(t), "a run after a complete one writes nothing")
	assert.Equal(t, 1, recompressions, "a run after a complete one still recompresses at the end")
}
