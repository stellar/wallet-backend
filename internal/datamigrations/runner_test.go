package datamigrations

import (
	"context"
	"crypto/rand"
	"fmt"
	"testing"
	"time"

	"github.com/alitto/pond/v2"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stellar/go-stellar-sdk/ingest/ledgerbackend"
	"github.com/stellar/go-stellar-sdk/keypair"
	"github.com/stellar/go-stellar-sdk/network"
	"github.com/stellar/go-stellar-sdk/toid"
	"github.com/stellar/go-stellar-sdk/xdr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/wallet-backend/internal/data"
	"github.com/stellar/wallet-backend/internal/db"
	"github.com/stellar/wallet-backend/internal/db/dbtest"
	"github.com/stellar/wallet-backend/internal/metrics"
	"github.com/stellar/wallet-backend/internal/utils"
)

const memoMigration = "transactions-memo"

// fakeBackend serves the ledgers in its map and fails GetLedger for any other one, as an
// RPC past its retention window would.
type fakeBackend struct {
	ledgers map[uint32]xdr.LedgerCloseMeta
}

func (b *fakeBackend) GetLatestLedgerSequence(context.Context) (uint32, error) { return 0, nil }

func (b *fakeBackend) GetLedger(_ context.Context, seq uint32) (xdr.LedgerCloseMeta, error) {
	if meta, ok := b.ledgers[seq]; ok {
		return meta, nil
	}
	return xdr.LedgerCloseMeta{}, fmt.Errorf("ledger %d is not served", seq)
}

func (b *fakeBackend) PrepareRange(context.Context, ledgerbackend.Range) error { return nil }

func (b *fakeBackend) IsPrepared(context.Context, ledgerbackend.Range) (bool, error) {
	return true, nil
}

func (b *fakeBackend) Close() error { return nil }

// memoLedger builds a V0 ledger close meta whose transactions carry the given memos, in
// order, readable by the SDK's LedgerTransactionReader (results paired to envelopes by hash).
func memoLedger(t *testing.T, seq uint32, closedAt time.Time, memos ...xdr.Memo) xdr.LedgerCloseMeta {
	t.Helper()
	source := keypair.MustRandom()
	var muxed xdr.MuxedAccount
	require.NoError(t, muxed.SetAddress(source.Address()))

	envs := make([]xdr.TransactionEnvelope, 0, len(memos))
	procs := make([]xdr.TransactionResultMeta, 0, len(memos))
	for i, memo := range memos {
		env := xdr.TransactionEnvelope{
			Type: xdr.EnvelopeTypeEnvelopeTypeTx,
			V1: &xdr.TransactionV1Envelope{Tx: xdr.Transaction{
				SourceAccount: muxed,
				Fee:           100,
				SeqNum:        xdr.SequenceNumber(i + 1),
				Memo:          memo,
				Operations: []xdr.Operation{{Body: xdr.OperationBody{
					Type:           xdr.OperationTypeBumpSequence,
					BumpSequenceOp: &xdr.BumpSequenceOp{BumpTo: xdr.SequenceNumber(i + 2)},
				}}},
			}},
		}
		hash, err := network.HashTransactionInEnvelope(env, network.TestNetworkPassphrase)
		require.NoError(t, err)
		results := []xdr.OperationResult{}
		envs = append(envs, env)
		procs = append(procs, xdr.TransactionResultMeta{
			Result: xdr.TransactionResultPair{
				TransactionHash: xdr.Hash(hash),
				Result:          xdr.TransactionResult{Result: xdr.TransactionResultResult{Code: xdr.TransactionResultCodeTxSuccess, Results: &results}},
			},
		})
	}
	return xdr.LedgerCloseMeta{V: 0, V0: &xdr.LedgerCloseMetaV0{
		LedgerHeader: xdr.LedgerHeaderHistoryEntry{Header: xdr.LedgerHeader{
			LedgerSeq: xdr.Uint32(seq),
			ScpValue:  xdr.StellarValue{CloseTime: xdr.TimePoint(closedAt.Unix())},
		}},
		TxSet:        xdr.TransactionSet{Txs: envs},
		TxProcessing: procs,
	}}
}

type runnerFixture struct {
	pool    *pgxpool.Pool
	models  *data.Models
	metrics *metrics.DataMigrationMetrics
	runner  *Runner
	ledgers map[uint32]xdr.LedgerCloseMeta
	// want maps to_id to the expected (memo_type, memo) after the migration.
	want map[int64]data.TransactionMemo
	// factoryCalls counts backends created: one for the preflight read, one per slice attempt.
	factoryCalls int
	// onFactory, when set, runs on every backend creation with the call count.
	onFactory func(calls int)
	oldest    uint32
	latest    uint32
	day1      time.Time
	day2      time.Time
}

// newRunnerFixture seeds ledgers [oldest, latest] of transactions without memos across two
// chunk days, compresses the older day's chunk (the production shape: history compressed
// before the memo columns existed), records the migration as pending with boundary
// latest+1, and wires a Runner whose backend factory serves the matching ledger metas.
func newRunnerFixture(t *testing.T, ctx context.Context) *runnerFixture {
	t.Helper()
	const oldest, latest uint32 = 100, 111
	dbt := dbtest.Open(t)
	t.Cleanup(func() { dbt.Close() })
	pool, err := db.OpenDBConnectionPool(ctx, dbt.DSN)
	require.NoError(t, err)
	t.Cleanup(pool.Close)
	m := metrics.NewMetrics(prometheus.NewRegistry())
	models, err := data.NewModels(pool, m.DB)
	require.NoError(t, err)

	day1 := time.Date(2026, time.March, 1, 12, 0, 0, 0, time.UTC)
	day2 := day1.Add(24 * time.Hour)
	mid := (oldest + latest) / 2

	var hash xdr.Hash
	for i := range hash {
		hash[i] = byte(i)
	}
	memosByLedger := func(seq uint32) []xdr.Memo {
		switch seq % 4 {
		case 0:
			return []xdr.Memo{xdr.MemoText(fmt.Sprintf("memo %d", seq)), {Type: xdr.MemoTypeMemoNone}}
		case 1:
			return []xdr.Memo{xdr.MemoID(uint64(seq))}
		case 2:
			return []xdr.Memo{xdr.MemoHash(hash), xdr.MemoText("\xff\xfe"), xdr.MemoRetHash(hash)}
		default:
			return nil // an empty ledger
		}
	}

	f := &runnerFixture{pool: pool, models: models, metrics: m.DataMigration, ledgers: map[uint32]xdr.LedgerCloseMeta{}, want: map[int64]data.TransactionMemo{}, oldest: oldest, latest: latest, day1: day1, day2: day2}
	for seq := oldest; seq <= latest; seq++ {
		closedAt := day1.Add(time.Duration(seq-oldest) * time.Minute)
		if seq > mid {
			closedAt = day2.Add(time.Duration(seq-mid) * time.Minute)
		}
		memos := memosByLedger(seq)
		f.ledgers[seq] = memoLedger(t, seq, closedAt, memos...)
		for i, memo := range memos {
			toID := toid.New(int32(seq), int32(i+1), 0).ToInt64()
			var h [32]byte
			_, err := rand.Read(h[:])
			require.NoError(t, err)
			_, err = pool.Exec(ctx, `INSERT INTO transactions (hash, to_id, fee_charged, result_code, ledger_number, ledger_created_at, is_fee_bump)
				VALUES ($1, $2, 100, 'TransactionResultCodeTxSuccess', $3, $4, false)`, h[:], toID, seq, closedAt)
			require.NoError(t, err)
			mt, mv := utils.EncodeMemo(memo)
			f.want[toID] = data.TransactionMemo{ToID: toID, LedgerCreatedAt: closedAt, MemoType: mt, Memo: mv}
		}
	}
	_, err = pool.Exec(ctx, `INSERT INTO ingest_store (key, value) VALUES ($1, $2), ($3, $4)`,
		data.OldestLedgerCursorName, fmt.Sprint(oldest), data.LatestLedgerCursorName, fmt.Sprint(latest))
	require.NoError(t, err)
	require.NoError(t, models.DataMigrations.InsertIfMissing(ctx, pool, memoMigration, data.DataMigrationPending, latest+1))

	// Compress day 1 only; day 2 plays the live chunk.
	_, err = pool.Exec(ctx, `SELECT compress_chunk(c) FROM show_chunks('transactions', older_than => $1::timestamptz) c`, day2)
	require.NoError(t, err)
	require.True(t, f.chunkCompressed(t, ctx, day1), "day-1 chunk should start compressed")
	require.False(t, f.chunkCompressed(t, ctx, day2), "day-2 chunk should start uncompressed")

	workers := pond.NewPool(2)
	t.Cleanup(workers.StopAndWait)
	f.runner = &Runner{
		Deps:    Deps{Models: models, NetworkPassphrase: network.TestNetworkPassphrase, Pool: workers},
		Metrics: m.DataMigration,
		BackendFactory: func(context.Context) (ledgerbackend.LedgerBackend, error) {
			f.factoryCalls++
			if f.onFactory != nil {
				f.onFactory(f.factoryCalls)
			}
			return &fakeBackend{ledgers: f.ledgers}, nil
		},
	}
	return f
}

// chunkCompressed reports whether the chunk holding closedAt is fully compressed: listed
// as compressed and without the partial flag (status bit 8) DML on a compressed chunk sets.
func (f *runnerFixture) chunkCompressed(t *testing.T, ctx context.Context, closedAt time.Time) bool {
	t.Helper()
	var compressed, partial bool
	err := f.pool.QueryRow(ctx, `
		SELECT i.is_compressed, (c.status & 8) <> 0
		FROM timescaledb_information.chunks i
		JOIN _timescaledb_catalog.chunk c ON c.schema_name = i.chunk_schema AND c.table_name = i.chunk_name
		WHERE i.hypertable_name = 'transactions' AND i.range_start <= $1 AND i.range_end > $1`, closedAt).Scan(&compressed, &partial)
	require.NoError(t, err)
	return compressed && !partial
}

// memos reads every row's memo columns keyed by to_id.
func (f *runnerFixture) memos(t *testing.T, ctx context.Context) map[int64]data.TransactionMemo {
	t.Helper()
	type row struct {
		ToID            int64     `db:"to_id"`
		LedgerCreatedAt time.Time `db:"ledger_created_at"`
		MemoType        *int16    `db:"memo_type"`
		Memo            []byte    `db:"memo"`
	}
	rows, err := db.QueryMany[row](ctx, f.pool, `SELECT to_id, ledger_created_at, memo_type, memo FROM transactions`)
	require.NoError(t, err)
	got := make(map[int64]data.TransactionMemo, len(rows))
	for _, r := range rows {
		tm := data.TransactionMemo{ToID: r.ToID, LedgerCreatedAt: r.LedgerCreatedAt.UTC(), Memo: r.Memo}
		if r.MemoType == nil {
			tm.MemoType = -1 // marks a row the migration has not reached
		} else {
			tm.MemoType = *r.MemoType
		}
		got[r.ToID] = tm
	}
	return got
}

func (f *runnerFixture) row(t *testing.T, ctx context.Context) *data.DataMigration {
	t.Helper()
	row, err := f.models.DataMigrations.Get(ctx, memoMigration)
	require.NoError(t, err)
	require.NotNil(t, row)
	return row
}

func withRunnerSizes(t *testing.T, slice, flush uint32) {
	t.Helper()
	prevSlice, prevFlush, prevWait := sliceLedgers, flushLedgers, retryWait
	sliceLedgers, flushLedgers, retryWait = slice, flush, 10*time.Millisecond
	t.Cleanup(func() { sliceLedgers, flushLedgers, retryWait = prevSlice, prevFlush, prevWait })
}

func TestRunner_FillsHistoryNewestFirstAndRecompresses(t *testing.T) {
	ctx := context.Background()
	withRunnerSizes(t, 3, 2) // several slices per chunk, several flushes per slice
	f := newRunnerFixture(t, ctx)

	require.NoError(t, f.runner.Run(ctx, memoMigration))

	assert.Equal(t, f.want, f.memos(t, ctx), "every row carries the memo its ledger says")
	row := f.row(t, ctx)
	assert.Equal(t, data.DataMigrationDone, row.Status)
	assert.Equal(t, f.oldest, row.CursorLedger)
	assert.Equal(t, f.latest+1, row.BoundaryLedger)
	assert.NotNil(t, row.StartedAt)
	assert.NotNil(t, row.FinishedAt)
	assert.Nil(t, row.LastError)
	assert.Equal(t, 5, f.factoryCalls, "one preflight read, then 12 ledgers in slices of 3")
	assert.Equal(t, float64(f.oldest), testutil.ToFloat64(f.metrics.Cursor.WithLabelValues(memoMigration)))
	assert.True(t, f.chunkCompressed(t, ctx, f.day1), "the rewritten compressed chunk is compressed again")
	assert.False(t, f.chunkCompressed(t, ctx, f.day2), "the live chunk is left uncompressed")
}

func TestRunner_ResumesFromCursor(t *testing.T) {
	ctx := context.Background()
	withRunnerSizes(t, 4, 4)
	f := newRunnerFixture(t, ctx)

	// A previous run got down to ledger 108: rows at or above it are already filled.
	_, err := f.pool.Exec(ctx, `UPDATE data_migrations SET status = 'in_progress', cursor_ledger = 108 WHERE name = $1`, memoMigration)
	require.NoError(t, err)
	require.NoError(t, f.runner.Run(ctx, memoMigration))

	got := f.memos(t, ctx)
	for toID, want := range f.want {
		seq := uint32(toid.Parse(toID).LedgerSequence)
		if seq >= 108 {
			assert.Equal(t, int16(-1), got[toID].MemoType, "ledger %d above the cursor is not touched", seq)
		} else {
			assert.Equal(t, want, got[toID], "ledger %d below the cursor is filled", seq)
		}
	}
	row := f.row(t, ctx)
	assert.Equal(t, data.DataMigrationDone, row.Status)
	assert.Equal(t, f.oldest, row.CursorLedger)
}

func TestRunner_FailingSliceRecordsErrorAndResumes(t *testing.T) {
	ctx := context.Background()
	withRunnerSizes(t, 4, 4)
	f := newRunnerFixture(t, ctx)

	// The backend cannot serve ledger 105.
	missing := f.ledgers[105]
	delete(f.ledgers, 105)
	err := f.runner.Run(ctx, memoMigration)
	require.ErrorContains(t, err, "ledgers [104, 107] failed 3 times")

	row := f.row(t, ctx)
	assert.Equal(t, data.DataMigrationInProgress, row.Status)
	assert.Equal(t, uint32(108), row.CursorLedger, "the first slice committed, the failed one did not")
	require.NotNil(t, row.LastError)
	assert.Contains(t, *row.LastError, "ledger 105 is not served")
	assert.Equal(t, 5, f.factoryCalls, "one preflight read, one good slice, three attempts of the failing one")
	assert.Equal(t, float64(3), testutil.ToFloat64(f.metrics.ErrorsTotal.WithLabelValues(memoMigration)))
	got := f.memos(t, ctx)
	assert.Equal(t, int16(-1), got[toid.New(100, 1, 0).ToInt64()].MemoType, "ledgers below the failed slice are untouched")
	assert.Equal(t, f.want[toid.New(108, 1, 0).ToInt64()], got[toid.New(108, 1, 0).ToInt64()])

	// Once the ledger is served again, the next run finishes and clears the error.
	f.ledgers[105] = missing
	require.NoError(t, f.runner.Run(ctx, memoMigration))
	assert.Equal(t, f.want, f.memos(t, ctx))
	row = f.row(t, ctx)
	assert.Equal(t, data.DataMigrationDone, row.Status)
	assert.Nil(t, row.LastError)
}

func TestRunner_StopsOnContextCancel(t *testing.T) {
	ctx := context.Background()
	withRunnerSizes(t, 4, 4)
	f := newRunnerFixture(t, ctx)

	// Cancel while the second slice [104, 107] is being read (factory call 3, after the
	// preflight read and the first slice).
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	f.onFactory = func(calls int) {
		if calls == 3 {
			cancel()
		}
	}
	err := f.runner.Run(runCtx, memoMigration)
	require.ErrorIs(t, err, context.Canceled)
	assert.Equal(t, 3, f.factoryCalls, "a cancelled slice is not retried")

	row := f.row(t, ctx)
	assert.Equal(t, data.DataMigrationInProgress, row.Status)
	assert.Equal(t, uint32(108), row.CursorLedger, "only the first slice committed")
	assert.Nil(t, row.LastError)

	f.onFactory = nil
	require.NoError(t, f.runner.Run(ctx, memoMigration))
	assert.Equal(t, f.want, f.memos(t, ctx))
	assert.Equal(t, data.DataMigrationDone, f.row(t, ctx).Status)
}

func TestRunner_DoesNotRun(t *testing.T) {
	ctx := context.Background()
	withRunnerSizes(t, 4, 4)
	f := newRunnerFixture(t, ctx)

	testCases := []struct {
		name    string
		migName string
		setup   func(t *testing.T) (cleanup func())
		wantErr string
		// wantStatus is the row's status after the run; empty when the row is absent.
		wantStatus string
	}{
		{
			name:       "unknown_name",
			migName:    "no-such-migration",
			wantErr:    `unknown data migration "no-such-migration"`,
			wantStatus: data.DataMigrationPending,
		},
		{
			name:    "lock_held_by_another_session",
			migName: memoMigration,
			setup: func(t *testing.T) func() {
				conn, err := f.pool.Acquire(ctx)
				require.NoError(t, err)
				locked, err := db.AcquireAdvisoryLock(ctx, conn, lockID())
				require.NoError(t, err)
				require.True(t, locked)
				return func() {
					require.NoError(t, db.ReleaseAdvisoryLock(ctx, conn, lockID()))
					conn.Release()
				}
			},
			wantErr:    "another data migration is running",
			wantStatus: data.DataMigrationPending,
		},
		{
			name:    "already_done",
			migName: memoMigration,
			setup: func(t *testing.T) func() {
				_, err := f.pool.Exec(ctx, `UPDATE data_migrations SET status = 'done' WHERE name = $1`, memoMigration)
				require.NoError(t, err)
				return func() {}
			},
			wantStatus: data.DataMigrationDone,
		},
		{
			name:    "not_recorded",
			migName: memoMigration,
			setup: func(t *testing.T) func() {
				_, err := f.pool.Exec(ctx, `DELETE FROM data_migrations`)
				require.NoError(t, err)
				return func() {}
			},
			wantErr: ErrNotRecorded.Error(),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			if tc.setup != nil {
				cleanup := tc.setup(t)
				defer cleanup()
			}

			err := f.runner.Run(ctx, tc.migName)
			if tc.wantErr == "" {
				require.NoError(t, err)
			} else {
				require.EqualError(t, err, tc.wantErr)
			}

			assert.Equal(t, 0, f.factoryCalls, "no backend is created")
			for toID, got := range f.memos(t, ctx) {
				assert.Equal(t, int16(-1), got.MemoType, "row %d is not rewritten", toID)
			}
			row, err := f.models.DataMigrations.Get(ctx, memoMigration)
			require.NoError(t, err)
			if tc.wantStatus == "" {
				assert.Nil(t, row)
				return
			}
			require.NotNil(t, row)
			assert.Equal(t, tc.wantStatus, row.Status)
			assert.Equal(t, f.latest+1, row.CursorLedger)
			assert.Nil(t, row.StartedAt)
		})
	}
}
