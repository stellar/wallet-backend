package services

import (
	"context"
	"crypto/rand"
	"fmt"
	"testing"
	"time"

	"github.com/alitto/pond/v2"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
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

type memoBackfillFixture struct {
	pool    *pgxpool.Pool
	svc     *ingestService
	ledgers map[uint32]xdr.LedgerCloseMeta
	// want maps to_id to the expected (memo_type, memo) after the backfill.
	want map[int64]data.TransactionMemo
	// factoryCalls counts backends created, one per slice.
	factoryCalls int
	oldest       uint32
	latest       uint32
	day1         time.Time
	day2         time.Time
}

// newMemoBackfillFixture seeds ledgers [oldest, latest] of transactions without memos
// across two chunk days, compresses the older day's chunk (the production shape: history
// compressed before the memo columns existed), and wires an ingestService whose backend
// factory serves the matching ledger metas.
func newMemoBackfillFixture(t *testing.T, ctx context.Context, oldest, latest uint32) *memoBackfillFixture {
	t.Helper()
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

	f := &memoBackfillFixture{pool: pool, ledgers: map[uint32]xdr.LedgerCloseMeta{}, want: map[int64]data.TransactionMemo{}, oldest: oldest, latest: latest, day1: day1, day2: day2}
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
	setIngestStoreValue(t, ctx, pool, data.OldestLedgerCursorName, oldest)
	setIngestStoreValue(t, ctx, pool, data.LatestLedgerCursorName, latest)
	setIngestStoreValue(t, ctx, pool, data.MemoBackfillCursorName, latest+1)

	// Compress day 1 only; day 2 plays the live chunk.
	_, err = pool.Exec(ctx, `SELECT compress_chunk(c) FROM show_chunks('transactions', older_than => $1::timestamptz) c`, day2)
	require.NoError(t, err)
	require.True(t, f.chunkCompressed(t, ctx, day1), "day-1 chunk should start compressed")
	require.False(t, f.chunkCompressed(t, ctx, day2), "day-2 chunk should start uncompressed")

	workers := pond.NewPool(2)
	t.Cleanup(workers.StopAndWait)
	f.svc = &ingestService{
		models:            models,
		appMetrics:        m,
		networkPassphrase: network.TestNetworkPassphrase,
		ledgerIndexerPool: workers,
		ledgerBackendFactory: func(context.Context) (ledgerbackend.LedgerBackend, error) {
			f.factoryCalls++
			return &multiLedgerBackend{ledgers: f.ledgers}, nil
		},
	}
	return f
}

// chunkCompressed reports whether the chunk holding closedAt is fully compressed: listed
// as compressed and without the partial flag DML on a compressed chunk sets.
func (f *memoBackfillFixture) chunkCompressed(t *testing.T, ctx context.Context, closedAt time.Time) bool {
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
func (f *memoBackfillFixture) memos(t *testing.T, ctx context.Context) map[int64]data.TransactionMemo {
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
			tm.MemoType = -1 // marks a row the backfill has not reached
		} else {
			tm.MemoType = *r.MemoType
		}
		got[r.ToID] = tm
	}
	return got
}

func withMemoBackfillSizes(t *testing.T, slice, flush uint32) {
	t.Helper()
	prevSlice, prevFlush := memoBackfillSliceLedgers, memoBackfillFlushLedgers
	memoBackfillSliceLedgers, memoBackfillFlushLedgers = slice, flush
	t.Cleanup(func() { memoBackfillSliceLedgers, memoBackfillFlushLedgers = prevSlice, prevFlush })
}

func TestMemoBackfill_FillsHistoryNewestFirstAndRecompresses(t *testing.T) {
	ctx := context.Background()
	withMemoBackfillSizes(t, 3, 2) // several slices per chunk, several flushes per slice
	f := newMemoBackfillFixture(t, ctx, 100, 111)

	require.NoError(t, f.svc.memoBackfillPass(ctx))

	assert.Equal(t, f.want, f.memos(t, ctx), "every row carries the memo its ledger says")
	assert.Equal(t, f.oldest, getIngestStoreValue(t, ctx, f.pool, data.MemoBackfillCursorName))
	assert.Equal(t, 4, f.factoryCalls, "12 ledgers in slices of 3 = one backend per slice")
	assert.True(t, f.chunkCompressed(t, ctx, f.day1), "the rewritten compressed chunk is compressed again")
	assert.False(t, f.chunkCompressed(t, ctx, f.day2), "the live chunk is left uncompressed")

	// Done: a second pass touches nothing.
	calls := f.factoryCalls
	require.NoError(t, f.svc.memoBackfillPass(ctx))
	assert.Equal(t, calls, f.factoryCalls)
}

func TestMemoBackfill_ResumesFromCursor(t *testing.T) {
	ctx := context.Background()
	withMemoBackfillSizes(t, 4, 4)
	f := newMemoBackfillFixture(t, ctx, 100, 111)

	// A previous run got down to ledger 108: rows at or above it are already filled.
	setIngestStoreValue(t, ctx, f.pool, data.MemoBackfillCursorName, 108)
	require.NoError(t, f.svc.memoBackfillPass(ctx))

	got := f.memos(t, ctx)
	for toID, want := range f.want {
		seq := uint32(toid.Parse(toID).LedgerSequence)
		if seq >= 108 {
			assert.Equal(t, int16(-1), got[toID].MemoType, "ledger %d above the cursor is not touched", seq)
		} else {
			assert.Equal(t, want, got[toID], "ledger %d below the cursor is filled", seq)
		}
	}
	assert.Equal(t, f.oldest, getIngestStoreValue(t, ctx, f.pool, data.MemoBackfillCursorName))
}

func TestMemoBackfill_FailedSliceLeavesCursorAndRetries(t *testing.T) {
	ctx := context.Background()
	withMemoBackfillSizes(t, 4, 4)
	f := newMemoBackfillFixture(t, ctx, 100, 111)

	// The backend cannot serve ledger 105 (as an RPC past its retention window would).
	delete(f.ledgers, 105)
	cancelCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	err := f.svc.memoBackfillPass(cancelCtx)
	require.ErrorContains(t, err, "backfilling memos for ledgers [104, 107]")

	assert.Equal(t, uint32(108), getIngestStoreValue(t, ctx, f.pool, data.MemoBackfillCursorName), "the first slice committed, the failed one did not")
	got := f.memos(t, ctx)
	assert.Equal(t, int16(-1), got[toid.New(100, 1, 0).ToInt64()].MemoType, "ledgers below the failed slice are untouched")
	assert.Equal(t, f.want[toid.New(108, 1, 0).ToInt64()], got[toid.New(108, 1, 0).ToInt64()])

	// runMemoBackfill retries and finishes once the ledger is served again.
	f.ledgers[105] = memoLedger(t, 105, f.day1.Add(5*time.Minute), xdr.MemoID(105))
	prev := memoBackfillRetryWait
	memoBackfillRetryWait = 10 * time.Millisecond
	t.Cleanup(func() { memoBackfillRetryWait = prev })
	f.svc.runMemoBackfill(ctx)
	assert.Equal(t, f.want, f.memos(t, ctx))
}

func TestMemoBackfill_StopsOnContextCancel(t *testing.T) {
	ctx := context.Background()
	withMemoBackfillSizes(t, 4, 4)
	f := newMemoBackfillFixture(t, ctx, 100, 111)
	delete(f.ledgers, 110) // the backend blocks on a missing ledger until ctx is done

	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		f.svc.runMemoBackfill(runCtx)
	}()
	time.Sleep(100 * time.Millisecond)
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("runMemoBackfill did not stop on cancel")
	}
	assert.Equal(t, f.latest+1, getIngestStoreValue(t, ctx, f.pool, data.MemoBackfillCursorName), "nothing committed")
}
