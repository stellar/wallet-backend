package services

import (
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stellar/go-stellar-sdk/ingest/ledgerbackend"
	"github.com/stellar/go-stellar-sdk/xdr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/wallet-backend/internal/data"
	"github.com/stellar/wallet-backend/internal/indexer"
	"github.com/stellar/wallet-backend/internal/metrics"
)

const (
	pubnetPassphrase = "Public Global Stellar Network ; September 2015"
	// backfillTestLedgerFixture is a real pubnet ledger with 264 transactions.
	backfillTestLedgerFixture = "../indexer/testdata/ledger-57500008.xdr.gz"
)

// backfillTestChain is a synthetic ledger history: one real ledger's
// contents cloned under consecutive sequence numbers, closing every cadence
// from base. Hourly chunks and a one-minute cadence give 60 ledgers per
// chunk window, so a test covers several windows with a few hundred ledgers.
type backfillTestChain struct {
	ledgers map[uint32]xdr.LedgerCloseMeta
	base    time.Time
	cadence time.Duration
	first   uint32
}

// backfillTestFirstLedger is the first ledger every test chain starts at.
const backfillTestFirstLedger = 1000

func newBackfillTestChain(t *testing.T, last uint32) *backfillTestChain {
	first := uint32(backfillTestFirstLedger)
	t.Helper()
	f, err := os.Open(backfillTestLedgerFixture)
	require.NoError(t, err)
	defer f.Close()
	gz, err := gzip.NewReader(f)
	require.NoError(t, err)
	raw, err := io.ReadAll(gz)
	require.NoError(t, err)

	chain := &backfillTestChain{
		ledgers: make(map[uint32]xdr.LedgerCloseMeta, last-first+1),
		base:    time.Date(2026, 10, 27, 0, 0, 0, 0, time.UTC),
		cadence: time.Minute,
		first:   first,
	}
	for seq := first; seq <= last; seq++ {
		var lcm xdr.LedgerCloseMeta
		require.NoError(t, lcm.UnmarshalBinary(raw))
		require.Equal(t, int32(1), lcm.V, "fixture is a V1 ledger close meta")
		lcm.V1.LedgerHeader.Header.LedgerSeq = xdr.Uint32(seq)
		lcm.V1.LedgerHeader.Header.ScpValue.CloseTime = xdr.TimePoint(chain.closeTime(seq).Unix())
		chain.ledgers[seq] = lcm
	}
	return chain
}

func (c *backfillTestChain) closeTime(seq uint32) time.Time {
	return c.base.Add(time.Duration(int64(seq)-int64(c.first)) * c.cadence)
}

// factory returns a backend over the chain; failures injects a permanent
// error for the given ledgers.
func (c *backfillTestChain) factory(failures map[uint32]error) LedgerBackendFactory {
	return func(context.Context) (ledgerbackend.LedgerBackend, error) {
		return &failingLedgerBackend{multiLedgerBackend: multiLedgerBackend{ledgers: c.ledgers}, failures: failures}, nil
	}
}

type failingLedgerBackend struct {
	multiLedgerBackend
	failures map[uint32]error
}

func (b *failingLedgerBackend) GetLedger(ctx context.Context, seq uint32) (xdr.LedgerCloseMeta, error) {
	if err, ok := b.failures[seq]; ok {
		return xdr.LedgerCloseMeta{}, err
	}
	return b.multiLedgerBackend.GetLedger(ctx, seq)
}

var errLedgerSourceBroken = errors.New("ledger source broken")

func newBackfillTestService(t *testing.T, models *data.Models, factory LedgerBackendFactory) *ingestService {
	t.Helper()
	svc, err := NewIngestService(IngestServiceConfig{
		IngestionMode:               IngestionModeBackfill,
		Models:                      models,
		Metrics:                     metrics.NewMetrics(prometheus.NewRegistry()),
		NetworkPassphrase:           pubnetPassphrase,
		LedgerBackendFactory:        factory,
		IsPermanentFetchError:       func(err error) bool { return errors.Is(err, errLedgerSourceBroken) },
		BackfillWorkers:             3,
		BackfillFlushSize:           7,
		BackfillCompressConcurrency: 2,
		DatastoreBufferSize:         10,
	})
	require.NoError(t, err)
	t.Cleanup(svc.Close)
	return svc
}

// writeLiveLedgers writes ledgers the way live ingestion would, one commit
// per ledger through the shared insert path, and sets the cursors around them.
func writeLiveLedgers(t *testing.T, ctx context.Context, svc *ingestService, pool *pgxpool.Pool, chain *backfillTestChain, first, last uint32) {
	t.Helper()
	buffer := indexer.NewIndexerBuffer()
	for seq := first; seq <= last; seq++ {
		buffer.Clear()
		_, err := svc.processLedger(ctx, chain.ledgers[seq], buffer)
		require.NoError(t, err)
		require.NoError(t, pgx.BeginFunc(ctx, pool, func(tx pgx.Tx) error {
			return svc.insertIntoDB(ctx, tx, buffer)
		}))
	}
	setupDBCursors(t, ctx, pool, last, first)
}

func setHourlyChunks(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {
	t.Helper()
	for _, table := range data.BulkCopyTableNames() {
		_, err := pool.Exec(ctx, fmt.Sprintf(`SELECT set_chunk_time_interval('%s', INTERVAL '1 hour')`, table))
		require.NoError(t, err)
	}
}

func distinctLedgers(t *testing.T, ctx context.Context, pool *pgxpool.Pool, first, last uint32) int {
	t.Helper()
	var n int
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT count(DISTINCT ledger_number) FROM transactions WHERE ledger_number BETWEEN $1 AND $2`, first, last).Scan(&n))
	return n
}

func cursorValue(t *testing.T, ctx context.Context, pool *pgxpool.Pool, name string) uint32 {
	t.Helper()
	var v int64
	require.NoError(t, pool.QueryRow(ctx, `SELECT value::bigint FROM ingest_store WHERE key = $1`, name).Scan(&v))
	return uint32(v)
}

func compressedChunksByStart(t *testing.T, ctx context.Context, pool *pgxpool.Pool, table string) map[time.Time]bool {
	t.Helper()
	rows, err := pool.Query(ctx, `SELECT COALESCE(range_start, to_timestamp(range_start_integer)), is_compressed FROM timescaledb_information.chunks WHERE hypertable_name = $1`, table)
	require.NoError(t, err)
	defer rows.Close()
	out := map[time.Time]bool{}
	for rows.Next() {
		var start time.Time
		var compressed bool
		require.NoError(t, rows.Scan(&start, &compressed))
		out[start.UTC()] = compressed
	}
	require.NoError(t, rows.Err())
	return out
}

func Test_startBackfilling_extendsHistoryBelowTheCursor(t *testing.T) {
	pool, models := openBackfillTestDB(t)
	ctx := context.Background()
	setHourlyChunks(t, ctx, pool)

	// Ledgers 1000-1130 close one minute apart from 2026-10-27 00:00 UTC:
	// hour 0 = 1000-1059, hour 1 = 1060-1119, hour 2 = 1120-1130. Live
	// ingested 1100-1130, so hour 1's chunk exists and is shared.
	chain := newBackfillTestChain(t, 1130)
	svc := newBackfillTestService(t, models, chain.factory(nil))
	writeLiveLedgers(t, ctx, svc, pool, chain, 1100, 1130)
	liveLedgers := distinctLedgers(t, ctx, pool, 1100, 1130)
	require.Equal(t, 31, liveLedgers)

	require.NoError(t, svc.startBackfilling(ctx, 1010))

	assert.Equal(t, uint32(1010), cursorValue(t, ctx, pool, data.OldestLedgerCursorName), "cursor is exact: the requested start")
	assert.Equal(t, uint32(1130), cursorValue(t, ctx, pool, data.LatestLedgerCursorName))
	assert.Equal(t, 121, distinctLedgers(t, ctx, pool, 1010, 1130), "every ledger from the start to the tip is present")
	assert.Equal(t, 0, distinctLedgers(t, ctx, pool, 1000, 1009), "nothing below the start")

	var duplicates int
	require.NoError(t, pool.QueryRow(ctx, `SELECT count(*) FROM (SELECT to_id FROM transactions GROUP BY 1 HAVING count(*) > 1) d`).Scan(&duplicates))
	assert.Zero(t, duplicates)
	var lookupRows, txRows int
	require.NoError(t, pool.QueryRow(ctx, `SELECT count(*) FROM transaction_hashes`).Scan(&lookupRows))
	require.NoError(t, pool.QueryRow(ctx, `SELECT count(*) FROM transactions`).Scan(&txRows))
	assert.Equal(t, txRows, lookupRows, "one lookup row per transaction")

	// Hour 0 ends before the first live ledger (01:40), so the backfill
	// compressed it; hour 1 holds live rows and is left to the live policy.
	hour0 := chain.base
	hour1 := chain.base.Add(time.Hour)
	for _, table := range data.BulkCopyTableNames() {
		state := compressedChunksByStart(t, ctx, pool, table)
		assert.True(t, state[hour0], "%s hour 0 compressed: %v", table, state)
		assert.False(t, state[hour1], "%s hour 1 left uncompressed: %v", table, state)
	}
	lookupState := compressedChunksByStart(t, ctx, pool, transactionHashesTable)
	for start, compressed := range lookupState {
		assert.False(t, compressed, "lookup chunk %s holds live rows and stays uncompressed", start)
	}

	var scheduled int
	require.NoError(t, pool.QueryRow(ctx, `SELECT count(*) FROM timescaledb_information.jobs WHERE proc_name = 'policy_compression' AND scheduled`).Scan(&scheduled))
	assert.Equal(t, len(data.BulkCopyTableNames())+1, scheduled, "policies are scheduled again after the run")
	assert.Zero(t, testutil.ToFloat64(svc.appMetrics.Ingestion.BackfillWindowsRemaining))
	assert.Equal(t, float64(1010), testutil.ToFloat64(svc.appMetrics.Ingestion.OldestLedger))

	// A second run has nothing to do and changes nothing.
	require.NoError(t, svc.startBackfilling(ctx, 1010))
	assert.Equal(t, 121, distinctLedgers(t, ctx, pool, 1010, 1130))
	assert.Equal(t, uint32(1010), cursorValue(t, ctx, pool, data.OldestLedgerCursorName))
}

func Test_startBackfilling_failsFastAndARerunConverges(t *testing.T) {
	pool, models := openBackfillTestDB(t)
	ctx := context.Background()
	setHourlyChunks(t, ctx, pool)
	chain := newBackfillTestChain(t, 1130)

	// Ledger 1030 cannot be fetched: hour 1 (1060-1099 below live) fills and
	// lowers the cursor, hour 0 fails after some flushes committed.
	broken := newBackfillTestService(t, models, chain.factory(map[uint32]error{1030: errLedgerSourceBroken}))
	writeLiveLedgers(t, ctx, broken, pool, chain, 1100, 1130)

	err := broken.startBackfilling(ctx, 1010)
	require.Error(t, err)
	assert.ErrorIs(t, err, errLedgerSourceBroken)
	assert.Equal(t, uint32(1060), cursorValue(t, ctx, pool, data.OldestLedgerCursorName), "cursor stops at the last complete window")
	assert.Equal(t, 71, distinctLedgers(t, ctx, pool, 1060, 1130))
	partial := distinctLedgers(t, ctx, pool, 1010, 1059)
	assert.Greater(t, partial, 0, "the failed window committed some flushes before failing")
	assert.Less(t, partial, 50)
	assert.Equal(t, float64(1), testutil.ToFloat64(broken.appMetrics.Ingestion.ErrorsTotal.WithLabelValues("backfill_window")))
	assert.Zero(t, testutil.ToFloat64(broken.appMetrics.Ingestion.RetriesTotal.WithLabelValues("backfill_window")), "a permanent error is not retried")

	var scheduled int
	require.NoError(t, pool.QueryRow(ctx, `SELECT count(*) FROM timescaledb_information.jobs WHERE proc_name = 'policy_compression' AND scheduled`).Scan(&scheduled))
	assert.Equal(t, len(data.BulkCopyTableNames())+1, scheduled, "policies are scheduled again even after a failure")

	// With the source fixed, the rerun deletes the partial window and refills it.
	fixed := newBackfillTestService(t, models, chain.factory(nil))
	require.NoError(t, fixed.startBackfilling(ctx, 1010))
	assert.Equal(t, uint32(1010), cursorValue(t, ctx, pool, data.OldestLedgerCursorName))
	assert.Equal(t, 121, distinctLedgers(t, ctx, pool, 1010, 1130))
	var duplicates int
	require.NoError(t, pool.QueryRow(ctx, `SELECT count(*) FROM (SELECT to_id FROM transactions GROUP BY 1 HAVING count(*) > 1) d`).Scan(&duplicates))
	assert.Zero(t, duplicates)
}

func Test_startBackfilling_fillsGapsAboveTheCursor(t *testing.T) {
	pool, models := openBackfillTestDB(t)
	ctx := context.Background()
	setHourlyChunks(t, ctx, pool)
	chain := newBackfillTestChain(t, 1130)
	svc := newBackfillTestService(t, models, chain.factory(nil))
	writeLiveLedgers(t, ctx, svc, pool, chain, 1100, 1130)

	// Punch a hole in live history.
	for _, table := range data.BulkCopyTables {
		_, err := pool.Exec(ctx, fmt.Sprintf(`DELETE FROM %s WHERE %s BETWEEN ($1::bigint << 32) AND (($2::bigint << 32) - 1)`, table.Table, table.TOIDColumn), 1110, 1113)
		require.NoError(t, err)
	}
	require.Equal(t, 28, distinctLedgers(t, ctx, pool, 1100, 1130))

	// A start at or above the cursor fills only the gap.
	require.NoError(t, svc.startBackfilling(ctx, 1100))
	assert.Equal(t, 31, distinctLedgers(t, ctx, pool, 1100, 1130))
	assert.Equal(t, uint32(1100), cursorValue(t, ctx, pool, data.OldestLedgerCursorName), "the cursor does not move for gap fills")
	var duplicates int
	require.NoError(t, pool.QueryRow(ctx, `SELECT count(*) FROM (SELECT to_id FROM transactions GROUP BY 1 HAVING count(*) > 1) d`).Scan(&duplicates))
	assert.Zero(t, duplicates)
}

func Test_startBackfilling_preflight(t *testing.T) {
	pool, models := openBackfillTestDB(t)
	ctx := context.Background()
	chain := newBackfillTestChain(t, 1001)

	t.Run("refuses an empty database", func(t *testing.T) {
		svc := newBackfillTestService(t, models, chain.factory(nil))
		err := svc.startBackfilling(ctx, 1000)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "run live ingestion once")
	})

	t.Run("refuses a pool below the connection floor", func(t *testing.T) {
		svc := newBackfillTestService(t, models, chain.factory(nil))
		svc.backfillCompressConcurrency = int(pool.Config().MaxConns) // 5 + K + 1 > MaxConns
		err := svc.startBackfilling(ctx, 1000)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "db-max-conns")
	})

	t.Run("refuses a zero start ledger", func(t *testing.T) {
		svc := newBackfillTestService(t, models, chain.factory(nil))
		require.Error(t, svc.startBackfilling(ctx, 0))
	})
}
