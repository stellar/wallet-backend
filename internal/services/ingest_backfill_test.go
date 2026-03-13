package services

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stellar/go-stellar-sdk/ingest/ledgerbackend"
	"github.com/stellar/go-stellar-sdk/xdr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/stellar/wallet-backend/internal/data"
	"github.com/stellar/wallet-backend/internal/db"
	"github.com/stellar/wallet-backend/internal/db/dbtest"
	"github.com/stellar/wallet-backend/internal/indexer"
	"github.com/stellar/wallet-backend/internal/indexer/types"
	"github.com/stellar/wallet-backend/internal/metrics"
)

// ===========================================================================
// Group 1: Pure Function Tests (no DB, no service construction)
// ===========================================================================

func Test_analyzeBatchResults(t *testing.T) {
	ctx := context.Background()
	testCases := []struct {
		name       string
		results    []BackfillResult
		wantFailed int
	}{
		{
			name:       "empty_results",
			results:    []BackfillResult{},
			wantFailed: 0,
		},
		{
			name: "all_success",
			results: []BackfillResult{
				{Batch: BackfillBatch{StartLedger: 1, EndLedger: 10}},
				{Batch: BackfillBatch{StartLedger: 11, EndLedger: 20}},
				{Batch: BackfillBatch{StartLedger: 21, EndLedger: 30}},
			},
			wantFailed: 0,
		},
		{
			name: "one_failure",
			results: []BackfillResult{
				{Batch: BackfillBatch{StartLedger: 1, EndLedger: 10}},
				{Batch: BackfillBatch{StartLedger: 11, EndLedger: 20}, Error: fmt.Errorf("network error")},
				{Batch: BackfillBatch{StartLedger: 21, EndLedger: 30}},
			},
			wantFailed: 1,
		},
		{
			name: "all_failures",
			results: []BackfillResult{
				{Batch: BackfillBatch{StartLedger: 1, EndLedger: 10}, Error: fmt.Errorf("error 1")},
				{Batch: BackfillBatch{StartLedger: 11, EndLedger: 20}, Error: fmt.Errorf("error 2")},
				{Batch: BackfillBatch{StartLedger: 21, EndLedger: 30}, Error: fmt.Errorf("error 3")},
			},
			wantFailed: 3,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got := analyzeBatchResults(ctx, tc.results)
			assert.Equal(t, tc.wantFailed, got)
		})
	}
}

func Test_timeRangesOverlap(t *testing.T) {
	base := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	testCases := []struct {
		name   string
		aStart time.Time
		aEnd   time.Time
		bStart time.Time
		bEnd   time.Time
		want   bool
	}{
		{
			name:   "no_overlap_a_before_b",
			aStart: base, aEnd: base.Add(1 * time.Hour),
			bStart: base.Add(2 * time.Hour), bEnd: base.Add(3 * time.Hour),
			want: false,
		},
		{
			name:   "no_overlap_b_before_a",
			aStart: base.Add(4 * time.Hour), aEnd: base.Add(5 * time.Hour),
			bStart: base.Add(1 * time.Hour), bEnd: base.Add(2 * time.Hour),
			want: false,
		},
		{
			name:   "partial_overlap",
			aStart: base, aEnd: base.Add(2 * time.Hour),
			bStart: base.Add(1 * time.Hour), bEnd: base.Add(3 * time.Hour),
			want: true,
		},
		{
			name:   "a_contains_b",
			aStart: base, aEnd: base.Add(4 * time.Hour),
			bStart: base.Add(1 * time.Hour), bEnd: base.Add(2 * time.Hour),
			want: true,
		},
		{
			name:   "b_contains_a",
			aStart: base.Add(1 * time.Hour), aEnd: base.Add(2 * time.Hour),
			bStart: base, bEnd: base.Add(4 * time.Hour),
			want: true,
		},
		{
			name:   "exact_match",
			aStart: base, aEnd: base.Add(2 * time.Hour),
			bStart: base, bEnd: base.Add(2 * time.Hour),
			want: true,
		},
		{
			name:   "touching_at_boundary",
			aStart: base, aEnd: base.Add(1 * time.Hour),
			bStart: base.Add(1 * time.Hour), bEnd: base.Add(2 * time.Hour),
			want: true,
		},
		{
			name:   "zero_width_same_point",
			aStart: base, aEnd: base,
			bStart: base, bEnd: base,
			want: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got := timeRangesOverlap(tc.aStart, tc.aEnd, tc.bStart, tc.bEnd)
			assert.Equal(t, tc.want, got)
		})
	}
}

func Test_collectPipelineErrors(t *testing.T) {
	testCases := []struct {
		name      string
		readerErr error
		workerErr error
		wantNil   bool
		wantMsg   string
	}{
		{
			name:    "no_errors",
			wantNil: true,
		},
		{
			name:      "reader_error_only",
			readerErr: fmt.Errorf("reader failed"),
			wantMsg:   "reader failed",
		},
		{
			name:      "worker_error_only",
			workerErr: fmt.Errorf("worker failed"),
			wantMsg:   "worker failed",
		},
		{
			name:      "both_errors_returns_reader",
			readerErr: fmt.Errorf("reader failed"),
			workerErr: fmt.Errorf("worker failed"),
			wantMsg:   "reader failed",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			readerCh := make(chan error, 1)
			workerCh := make(chan error, 1)
			if tc.readerErr != nil {
				readerCh <- tc.readerErr
			}
			if tc.workerErr != nil {
				workerCh <- tc.workerErr
			}

			got := collectPipelineErrors(readerCh, workerCh)
			if tc.wantNil {
				assert.NoError(t, got)
			} else {
				require.Error(t, got)
				assert.Contains(t, got.Error(), tc.wantMsg)
			}
		})
	}
}

func Test_pipelineMetrics_deltas(t *testing.T) {
	testCases := []struct {
		name        string
		setup       func(pm *pipelineMetrics)
		callTwice   bool
		wantProcess time.Duration
		wantBlocked time.Duration
		wantLedgers int64
	}{
		{
			name: "initial_deltas",
			setup: func(pm *pipelineMetrics) {
				pm.processDuration.Store(int64(100 * time.Millisecond))
				pm.workerBlockedTime.Store(int64(50 * time.Millisecond))
				pm.workerLedgers.Store(10)
			},
			wantProcess: 100 * time.Millisecond,
			wantBlocked: 50 * time.Millisecond,
			wantLedgers: 10,
		},
		{
			name: "subsequent_call_returns_delta",
			setup: func(pm *pipelineMetrics) {
				pm.processDuration.Store(int64(100 * time.Millisecond))
				pm.workerBlockedTime.Store(int64(50 * time.Millisecond))
				pm.workerLedgers.Store(10)
			},
			callTwice:   true,
			wantProcess: 0, // No change between first and second call
			wantBlocked: 0,
			wantLedgers: 0,
		},
		{
			name:        "no_change_returns_zero",
			setup:       func(_ *pipelineMetrics) {},
			wantProcess: 0,
			wantBlocked: 0,
			wantLedgers: 0,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var pm pipelineMetrics
			tc.setup(&pm)

			if tc.callTwice {
				// First call consumes the initial values
				pm.deltas()
			}

			process, blocked, ledgers := pm.deltas()
			assert.Equal(t, tc.wantProcess, process, "process duration mismatch")
			assert.Equal(t, tc.wantBlocked, blocked, "blocked duration mismatch")
			assert.Equal(t, tc.wantLedgers, ledgers, "ledgers count mismatch")
		})
	}
}

func Test_watermarkTracker_advance(t *testing.T) {
	base := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)

	testCases := []struct {
		name            string
		batch           BackfillBatch
		flushes         [][]*LedgerBuffer
		wantWatermark   uint32
		wantSignalCount int
		wantClosePruned bool
	}{
		{
			name:  "sequential_advance",
			batch: BackfillBatch{StartLedger: 10, EndLedger: 12},
			flushes: [][]*LedgerBuffer{
				{makeLedgerBuffer(10, base), makeLedgerBuffer(11, base.Add(time.Second)), makeLedgerBuffer(12, base.Add(2*time.Second))},
			},
			wantWatermark:   13,
			wantSignalCount: 1,
		},
		{
			name:  "out_of_order_advance",
			batch: BackfillBatch{StartLedger: 10, EndLedger: 12},
			flushes: [][]*LedgerBuffer{
				{makeLedgerBuffer(12, base.Add(2*time.Second))},
				{makeLedgerBuffer(10, base)},
				{makeLedgerBuffer(11, base.Add(time.Second))},
			},
			wantWatermark:   13,
			wantSignalCount: 2, // No signal after 12 (gap at 10), signal after 10+11 fills gap
		},
		{
			name:  "single_ledger_batch",
			batch: BackfillBatch{StartLedger: 5, EndLedger: 5},
			flushes: [][]*LedgerBuffer{
				{makeLedgerBuffer(5, base)},
			},
			wantWatermark:   6,
			wantSignalCount: 1,
		},
		{
			name:  "no_advance_gap_at_start",
			batch: BackfillBatch{StartLedger: 10, EndLedger: 12},
			flushes: [][]*LedgerBuffer{
				{makeLedgerBuffer(11, base.Add(time.Second)), makeLedgerBuffer(12, base.Add(2*time.Second))},
			},
			wantWatermark:   10, // Can't advance past the gap at 10
			wantSignalCount: 0,
		},
		{
			name:  "prunes_close_times",
			batch: BackfillBatch{StartLedger: 10, EndLedger: 12},
			flushes: [][]*LedgerBuffer{
				{makeLedgerBuffer(10, base), makeLedgerBuffer(11, base.Add(time.Second)), makeLedgerBuffer(12, base.Add(2*time.Second))},
			},
			wantWatermark:   13,
			wantSignalCount: 1,
			wantClosePruned: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			compressorCh := make(chan *CompressBatch, 100)
			wt := newWatermarkTracker(&tc.batch, compressorCh)

			for _, buffers := range tc.flushes {
				wt.advance(buffers)
			}

			assert.Equal(t, tc.wantWatermark, wt.watermark)
			assert.Len(t, compressorCh, tc.wantSignalCount)

			if tc.wantClosePruned {
				// After full advance, all entries below watermark should be pruned
				assert.Empty(t, wt.closeTimes)
			}
		})
	}
}

func Test_executeFlush(t *testing.T) {
	base := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)

	testCases := []struct {
		name        string
		flushErr    error
		wantErr     bool
		wantAdvance bool
	}{
		{
			name:        "successful_flush",
			wantAdvance: true,
		},
		{
			name:     "flush_error",
			flushErr: fmt.Errorf("db connection lost"),
			wantErr:  true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			batch := &BackfillBatch{StartLedger: 10, EndLedger: 12}
			compressorCh := make(chan *CompressBatch, 100)
			wt := newWatermarkTracker(batch, compressorCh)

			buffers := []*LedgerBuffer{
				makeLedgerBuffer(10, base),
				makeLedgerBuffer(11, base.Add(time.Second)),
				makeLedgerBuffer(12, base.Add(2*time.Second)),
			}

			flush := func(_ context.Context, _ []*LedgerBuffer) error {
				return tc.flushErr
			}

			duration, err := executeFlush(context.Background(), buffers, flush, wt)

			if tc.wantErr {
				require.Error(t, err)
				assert.Zero(t, duration)
				assert.Equal(t, uint32(10), wt.watermark, "watermark should NOT advance on error")
			} else {
				require.NoError(t, err)
				assert.Greater(t, int64(duration), int64(0), "duration should be positive")
				assert.Equal(t, uint32(13), wt.watermark, "watermark should advance past flushed ledgers")
			}
		})
	}
}

// ===========================================================================
// Group 2: Mock-Only Tests (service with mocked factory, no DB)
// ===========================================================================

func Test_ingestService_fetchLedgerCloseTime(t *testing.T) {
	closeTime := time.Date(2024, 6, 15, 12, 0, 0, 0, time.UTC)

	testCases := []struct {
		name     string
		factory  LedgerBackendFactory
		wantTime time.Time
		wantErr  string
	}{
		{
			name: "success",
			factory: func(ctx context.Context) (ledgerbackend.LedgerBackend, error) {
				b := &LedgerBackendMock{}
				b.On("PrepareRange", mock.Anything, mock.Anything).Return(nil)
				b.On("GetLedger", mock.Anything, uint32(100)).Return(makeLedgerCloseMeta(100, closeTime), nil)
				b.On("Close").Return(nil)
				return b, nil
			},
			wantTime: time.Unix(closeTime.Unix(), 0).UTC(),
		},
		{
			name: "factory_error",
			factory: func(_ context.Context) (ledgerbackend.LedgerBackend, error) {
				return nil, fmt.Errorf("connection refused")
			},
			wantErr: "creating backend for boundary timestamps",
		},
		{
			name: "prepare_range_error",
			factory: func(_ context.Context) (ledgerbackend.LedgerBackend, error) {
				b := &LedgerBackendMock{}
				b.On("PrepareRange", mock.Anything, mock.Anything).Return(fmt.Errorf("range unavailable"))
				b.On("Close").Return(nil)
				return b, nil
			},
			wantErr: "preparing range for start ledger",
		},
		{
			name: "get_ledger_error",
			factory: func(_ context.Context) (ledgerbackend.LedgerBackend, error) {
				b := &LedgerBackendMock{}
				b.On("PrepareRange", mock.Anything, mock.Anything).Return(nil)
				b.On("GetLedger", mock.Anything, mock.Anything).Return(xdr.LedgerCloseMeta{}, fmt.Errorf("ledger not found"))
				b.On("Close").Return(nil)
				return b, nil
			},
			wantErr: "getting start ledger",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			svc := newTestBackfillService(t, testBackfillOpts{factory: tc.factory})

			got, err := svc.fetchLedgerCloseTime(context.Background(), 100)
			if tc.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tc.wantErr)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tc.wantTime, got)
			}
		})
	}
}

func Test_ingestService_setupBatchBackend(t *testing.T) {
	testCases := []struct {
		name    string
		factory LedgerBackendFactory
		wantErr string
	}{
		{
			name: "success",
			factory: func(_ context.Context) (ledgerbackend.LedgerBackend, error) {
				b := &LedgerBackendMock{}
				b.On("PrepareRange", mock.Anything, mock.Anything).Return(nil)
				return b, nil
			},
		},
		{
			name: "factory_error",
			factory: func(_ context.Context) (ledgerbackend.LedgerBackend, error) {
				return nil, fmt.Errorf("connection refused")
			},
			wantErr: "creating ledger backend",
		},
		{
			name: "prepare_range_error",
			factory: func(_ context.Context) (ledgerbackend.LedgerBackend, error) {
				b := &LedgerBackendMock{}
				b.On("PrepareRange", mock.Anything, mock.Anything).Return(fmt.Errorf("range unavailable"))
				return b, nil
			},
			wantErr: "preparing backend range",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			svc := newTestBackfillService(t, testBackfillOpts{factory: tc.factory})
			batch := BackfillBatch{StartLedger: 100, EndLedger: 200}

			backend, err := svc.setupBatchBackend(context.Background(), batch)
			if tc.wantErr != "" {
				require.Error(t, err)
				assert.Nil(t, backend)
				assert.Contains(t, err.Error(), tc.wantErr)
			} else {
				require.NoError(t, err)
				assert.NotNil(t, backend)
			}
		})
	}
}

func Test_ingestService_mapBatchesToChunks(t *testing.T) {
	base := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)

	testCases := []struct {
		name       string
		gaps       []data.LedgerRange
		chunks     []*db.Chunk
		factory    LedgerBackendFactory
		wantErr    string
		wantBatch  int
		wantChunks map[int]int // batch index -> chunk count
	}{
		{
			name: "single_gap_one_chunk",
			gaps: []data.LedgerRange{{GapStart: 100, GapEnd: 200}},
			chunks: []*db.Chunk{
				{Name: "chunk_1", Start: base, End: base.Add(48 * time.Hour)},
			},
			factory: func(_ context.Context) (ledgerbackend.LedgerBackend, error) {
				b := &LedgerBackendMock{}
				b.On("PrepareRange", mock.Anything, mock.Anything).Return(nil)
				b.On("GetLedger", mock.Anything, mock.Anything).Return(makeLedgerCloseMeta(100, base.Add(time.Hour)), nil)
				b.On("Close").Return(nil)
				return b, nil
			},
			wantBatch:  1,
			wantChunks: map[int]int{0: 1},
		},
		{
			name: "single_gap_multiple_chunks",
			gaps: []data.LedgerRange{{GapStart: 100, GapEnd: 200}},
			chunks: []*db.Chunk{
				{Name: "chunk_1", Start: base, End: base.Add(24 * time.Hour)},
				{Name: "chunk_2", Start: base.Add(24 * time.Hour), End: base.Add(48 * time.Hour)},
				{Name: "chunk_3", Start: base.Add(96 * time.Hour), End: base.Add(120 * time.Hour)},
			},
			factory: func(_ context.Context) (ledgerbackend.LedgerBackend, error) {
				b := &LedgerBackendMock{}
				b.On("PrepareRange", mock.Anything, mock.Anything).Return(nil)
				// Start at hour 1, end at hour 25 → overlaps chunk_1 and chunk_2 but not chunk_3
				b.On("GetLedger", mock.Anything, uint32(100)).Return(makeLedgerCloseMeta(100, base.Add(time.Hour)), nil)
				b.On("GetLedger", mock.Anything, uint32(200)).Return(makeLedgerCloseMeta(200, base.Add(25*time.Hour)), nil)
				b.On("Close").Return(nil)
				return b, nil
			},
			wantBatch:  1,
			wantChunks: map[int]int{0: 2},
		},
		{
			name: "no_overlapping_chunks",
			gaps: []data.LedgerRange{{GapStart: 100, GapEnd: 200}},
			chunks: []*db.Chunk{
				{Name: "chunk_far", Start: base.Add(96 * time.Hour), End: base.Add(120 * time.Hour)},
			},
			factory: func(_ context.Context) (ledgerbackend.LedgerBackend, error) {
				b := &LedgerBackendMock{}
				b.On("PrepareRange", mock.Anything, mock.Anything).Return(nil)
				// Both start and end are before chunk_far
				b.On("GetLedger", mock.Anything, mock.Anything).Return(makeLedgerCloseMeta(100, base.Add(time.Hour)), nil)
				b.On("Close").Return(nil)
				return b, nil
			},
			wantBatch:  1,
			wantChunks: map[int]int{0: 0},
		},
		{
			name: "fetch_error",
			gaps: []data.LedgerRange{{GapStart: 100, GapEnd: 200}},
			chunks: []*db.Chunk{
				{Name: "chunk_1", Start: base, End: base.Add(48 * time.Hour)},
			},
			factory: func(_ context.Context) (ledgerbackend.LedgerBackend, error) {
				return nil, fmt.Errorf("connection refused")
			},
			wantErr: "fetching start ledger",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			svc := newTestBackfillService(t, testBackfillOpts{factory: tc.factory})

			batches, chunksByBatch, err := svc.mapBatchesToChunks(context.Background(), tc.gaps, tc.chunks)
			if tc.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tc.wantErr)
			} else {
				require.NoError(t, err)
				assert.Len(t, batches, tc.wantBatch)
				for idx, wantCount := range tc.wantChunks {
					assert.Len(t, chunksByBatch[batches[idx]], wantCount, "chunk count for batch %d", idx)
				}
			}
		})
	}
}

// ===========================================================================
// Group 3: DB Tests (require dbtest.Open)
// ===========================================================================

func Test_ingestService_calculateBackfillGaps(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()
	ctx := context.Background()
	dbConnectionPool, err := db.OpenDBConnectionPool(ctx, dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	testCases := []struct {
		name         string
		startLedger  uint32
		endLedger    uint32
		setupDB      func(t *testing.T)
		expectedGaps []data.LedgerRange
	}{
		{
			name:        "entirely_before_oldest_ingested_ledger",
			startLedger: 50,
			endLedger:   80,
			setupDB: func(t *testing.T) {
				_, err := dbConnectionPool.Exec(ctx,
					`INSERT INTO transactions (hash, to_id, envelope_xdr, fee_charged, result_code, meta_xdr, ledger_number, ledger_created_at)
					VALUES ('anchor_hash', 1, 'env', 100, 'TransactionResultCodeTxSuccess', 'meta', 100, NOW())`)
				require.NoError(t, err)
			},
			expectedGaps: []data.LedgerRange{
				{GapStart: 50, GapEnd: 80},
			},
		},
		{
			name:        "overlaps_with_oldest_ingested_ledger",
			startLedger: 50,
			endLedger:   150,
			setupDB: func(t *testing.T) {
				for ledger := uint32(100); ledger <= 200; ledger++ {
					_, err := dbConnectionPool.Exec(ctx,
						`INSERT INTO transactions (hash, to_id, envelope_xdr, fee_charged, result_code, meta_xdr, ledger_number, ledger_created_at)
						VALUES ($1, $2, 'env', 100, 'TransactionResultCodeTxSuccess', 'meta', $3, NOW())`,
						"hash"+string(rune(ledger)), ledger, ledger)
					require.NoError(t, err)
				}
			},
			expectedGaps: []data.LedgerRange{
				{GapStart: 50, GapEnd: 99},
			},
		},
		{
			name:        "entirely_within_ingested_range_no_gaps",
			startLedger: 110,
			endLedger:   150,
			setupDB: func(t *testing.T) {
				for ledger := uint32(100); ledger <= 200; ledger++ {
					_, err := dbConnectionPool.Exec(ctx,
						`INSERT INTO transactions (hash, to_id, envelope_xdr, fee_charged, result_code, meta_xdr, ledger_number, ledger_created_at)
						VALUES ($1, $2, 'env', 100, 'TransactionResultCodeTxSuccess', 'meta', $3, NOW())`,
						"hash"+string(rune(ledger)), ledger, ledger)
					require.NoError(t, err)
				}
			},
			expectedGaps: []data.LedgerRange{},
		},
		{
			name:        "entirely_within_ingested_range_with_gaps",
			startLedger: 110,
			endLedger:   180,
			setupDB: func(t *testing.T) {
				for ledger := uint32(100); ledger <= 120; ledger++ {
					_, err := dbConnectionPool.Exec(ctx,
						`INSERT INTO transactions (hash, to_id, envelope_xdr, fee_charged, result_code, meta_xdr, ledger_number, ledger_created_at)
						VALUES ($1, $2, 'env', 100, 'TransactionResultCodeTxSuccess', 'meta', $3, NOW())`,
						"hash"+string(rune(ledger)), ledger, ledger)
					require.NoError(t, err)
				}
				for ledger := uint32(150); ledger <= 200; ledger++ {
					_, err := dbConnectionPool.Exec(ctx,
						`INSERT INTO transactions (hash, to_id, envelope_xdr, fee_charged, result_code, meta_xdr, ledger_number, ledger_created_at)
						VALUES ($1, $2, 'env', 100, 'TransactionResultCodeTxSuccess', 'meta', $3, NOW())`,
						"hash"+string(rune(ledger)), ledger, ledger)
					require.NoError(t, err)
				}
			},
			expectedGaps: []data.LedgerRange{
				{GapStart: 121, GapEnd: 149},
			},
		},
		{
			name:        "stale_cursor_is_ignored_uses_actual_oldest_from_transactions",
			startLedger: 50,
			endLedger:   150,
			setupDB: func(t *testing.T) {
				_, err := dbConnectionPool.Exec(ctx, `INSERT INTO ingest_store (key, value) VALUES ('oldest_ledger_cursor', 50)`)
				require.NoError(t, err)
				for ledger := uint32(100); ledger <= 200; ledger++ {
					_, err := dbConnectionPool.Exec(ctx,
						`INSERT INTO transactions (hash, to_id, envelope_xdr, fee_charged, result_code, meta_xdr, ledger_number, ledger_created_at)
						VALUES ($1, $2, 'env', 100, 'TransactionResultCodeTxSuccess', 'meta', $3, NOW())`,
						"hash"+string(rune(ledger)), ledger, ledger)
					require.NoError(t, err)
				}
			},
			expectedGaps: []data.LedgerRange{
				{GapStart: 50, GapEnd: 99},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := dbConnectionPool.Exec(ctx, "DELETE FROM transactions")
			require.NoError(t, err)
			_, err = dbConnectionPool.Exec(ctx, "DELETE FROM ingest_store")
			require.NoError(t, err)

			mockMetricsService := metrics.NewMockMetricsService()
			mockMetricsService.On("RegisterPoolMetrics", mock.Anything, mock.Anything).Return().Maybe()
			mockMetricsService.On("ObserveDBQueryDuration", mock.Anything, mock.Anything, mock.Anything).Return().Maybe()
			mockMetricsService.On("IncDBQuery", mock.Anything, mock.Anything).Return().Maybe()
			defer mockMetricsService.AssertExpectations(t)

			models, err := data.NewModels(dbConnectionPool, mockMetricsService)
			require.NoError(t, err)

			if tc.setupDB != nil {
				tc.setupDB(t)
			}

			svc := newTestBackfillService(t, testBackfillOpts{models: models})

			gaps, err := svc.calculateBackfillGaps(ctx, tc.startLedger, tc.endLedger)
			require.NoError(t, err)
			assert.Equal(t, tc.expectedGaps, gaps)
		})
	}
}

func Test_ingestService_updateOldestCursor(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()
	ctx := context.Background()
	dbConnectionPool, err := db.OpenDBConnectionPool(ctx, dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	testCases := []struct {
		name          string
		initialCursor uint32
		updateTo      uint32
		wantCursor    uint32
	}{
		{
			name:          "updates_to_lower_value",
			initialCursor: 100,
			updateTo:      50,
			wantCursor:    50,
		},
		{
			name:          "keeps_existing_lower_value",
			initialCursor: 50,
			updateTo:      100,
			wantCursor:    50,
		},
		{
			name:          "same_value_no_change",
			initialCursor: 100,
			updateTo:      100,
			wantCursor:    100,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			setupDBCursors(t, ctx, dbConnectionPool, 200, tc.initialCursor)

			mockMetricsService := metrics.NewMockMetricsService()
			mockMetricsService.On("RegisterPoolMetrics", mock.Anything, mock.Anything).Return().Maybe()
			mockMetricsService.On("ObserveDBQueryDuration", mock.Anything, mock.Anything, mock.Anything).Return().Maybe()
			mockMetricsService.On("IncDBQuery", mock.Anything, mock.Anything).Return().Maybe()
			mockMetricsService.On("IncDBTransaction", mock.Anything).Return().Maybe()
			mockMetricsService.On("ObserveDBTransactionDuration", mock.Anything, mock.Anything).Return().Maybe()
			defer mockMetricsService.AssertExpectations(t)

			models, err := data.NewModels(dbConnectionPool, mockMetricsService)
			require.NoError(t, err)

			svc := newTestBackfillService(t, testBackfillOpts{models: models})

			err = svc.updateOldestCursor(ctx, tc.updateTo)
			require.NoError(t, err)

			cursor, err := models.IngestStore.Get(ctx, "oldest_ledger_cursor")
			require.NoError(t, err)
			assert.Equal(t, tc.wantCursor, cursor)
		})
	}
}

func Test_ingestService_flushHistoricalBatch(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()
	ctx := context.Background()
	dbConnectionPool, err := db.OpenDBConnectionPool(ctx, dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	testCases := []struct {
		name                 string
		setupBuffers         func() []*LedgerBuffer
		wantTxCount          int
		wantOpCount          int
		wantStateChangeCount int
		txHashes             []string
	}{
		{
			name: "flush_empty_buffer",
			setupBuffers: func() []*LedgerBuffer {
				return []*LedgerBuffer{makeLedgerBuffer(100, time.Now())}
			},
			wantTxCount:          0,
			wantOpCount:          0,
			wantStateChangeCount: 0,
		},
		{
			name: "flush_with_data",
			setupBuffers: func() []*LedgerBuffer {
				buf := indexer.NewIndexerBuffer()
				tx1 := createTestTransaction(flushTxHash1, 1)
				tx2 := createTestTransaction(flushTxHash2, 2)
				op1 := createTestOperation(200)
				op2 := createTestOperation(201)
				sc1 := createTestStateChange(1, testAddr1, 200)
				sc2 := createTestStateChange(2, testAddr2, 201)
				buf.PushTransaction(testAddr1, tx1)
				buf.PushTransaction(testAddr2, tx2)
				buf.PushOperation(testAddr1, op1, tx1)
				buf.PushOperation(testAddr2, op2, tx2)
				buf.PushStateChange(tx1, op1, sc1)
				buf.PushStateChange(tx2, op2, sc2)
				return []*LedgerBuffer{{buffer: buf, ledgerSeq: 100, closeTime: time.Now()}}
			},
			wantTxCount:          2,
			wantOpCount:          2,
			wantStateChangeCount: 2,
			txHashes:             []string{flushTxHash1, flushTxHash2},
		},
		{
			name: "flush_single_transaction",
			setupBuffers: func() []*LedgerBuffer {
				buf := indexer.NewIndexerBuffer()
				tx := createTestTransaction(flushTxHash3, 3)
				buf.PushTransaction(testAddr1, tx)
				return []*LedgerBuffer{{buffer: buf, ledgerSeq: 101, closeTime: time.Now()}}
			},
			wantTxCount: 1,
			txHashes:    []string{flushTxHash3},
		},
		{
			name: "flush_multiple_ledger_buffers",
			setupBuffers: func() []*LedgerBuffer {
				buf1 := indexer.NewIndexerBuffer()
				tx1 := createTestTransaction(flushTxHash1, 1)
				buf1.PushTransaction(testAddr1, tx1)

				buf2 := indexer.NewIndexerBuffer()
				tx2 := createTestTransaction(flushTxHash2, 2)
				buf2.PushTransaction(testAddr2, tx2)

				return []*LedgerBuffer{
					{buffer: buf1, ledgerSeq: 100, closeTime: time.Now()},
					{buffer: buf2, ledgerSeq: 101, closeTime: time.Now()},
				}
			},
			wantTxCount: 2,
			txHashes:    []string{flushTxHash1, flushTxHash2},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Clean up
			for _, hash := range []string{flushTxHash1, flushTxHash2, flushTxHash3, flushTxHash4} {
				_, err = dbConnectionPool.Exec(ctx, `DELETE FROM state_changes WHERE to_id IN (SELECT to_id FROM transactions WHERE hash = $1)`, types.HashBytea(hash))
				require.NoError(t, err)
				_, err = dbConnectionPool.Exec(ctx, `DELETE FROM transactions WHERE hash = $1`, types.HashBytea(hash))
				require.NoError(t, err)
			}
			_, err = dbConnectionPool.Exec(ctx, `TRUNCATE operations, operations_accounts CASCADE`)
			require.NoError(t, err)
			setupDBCursors(t, ctx, dbConnectionPool, 200, 100)

			mockMetricsService := metrics.NewMockMetricsService()
			mockMetricsService.On("RegisterPoolMetrics", mock.Anything, mock.Anything).Return().Maybe()
			mockMetricsService.On("ObserveDBQueryDuration", mock.Anything, mock.Anything, mock.Anything).Return().Maybe()
			mockMetricsService.On("IncDBQuery", mock.Anything, mock.Anything).Return().Maybe()
			mockMetricsService.On("IncDBTransaction", mock.Anything).Return().Maybe()
			mockMetricsService.On("ObserveDBTransactionDuration", mock.Anything, mock.Anything).Return().Maybe()
			mockMetricsService.On("ObserveDBBatchSize", mock.Anything, mock.Anything, mock.Anything).Return().Maybe()
			mockMetricsService.On("IncStateChanges", mock.Anything, mock.Anything, mock.Anything).Return().Maybe()
			defer mockMetricsService.AssertExpectations(t)

			models, err := data.NewModels(dbConnectionPool, mockMetricsService)
			require.NoError(t, err)

			svc := newTestBackfillService(t, testBackfillOpts{models: models})

			buffers := tc.setupBuffers()
			err = svc.flushHistoricalBatch(ctx, buffers)
			require.NoError(t, err)

			// Verify transaction count
			if len(tc.txHashes) > 0 {
				hashBytes := make([][]byte, len(tc.txHashes))
				for i, h := range tc.txHashes {
					val, err := types.HashBytea(h).Value()
					require.NoError(t, err)
					hashBytes[i] = val.([]byte)
				}
				var txCount int
				err = dbConnectionPool.QueryRow(ctx,
					`SELECT COUNT(*) FROM transactions WHERE hash = ANY($1)`,
					hashBytes).Scan(&txCount)
				require.NoError(t, err)
				assert.Equal(t, tc.wantTxCount, txCount, "transaction count mismatch")
			}

			// Verify operation count
			if tc.wantOpCount > 0 {
				var opCount int
				err = dbConnectionPool.QueryRow(ctx,
					`SELECT COUNT(*) FROM operations`).Scan(&opCount)
				require.NoError(t, err)
				assert.Equal(t, tc.wantOpCount, opCount, "operation count mismatch")
			}

			// Verify state change count
			if tc.wantStateChangeCount > 0 {
				scHashBytes := make([][]byte, len(tc.txHashes))
				for i, h := range tc.txHashes {
					val, err := types.HashBytea(h).Value()
					require.NoError(t, err)
					scHashBytes[i] = val.([]byte)
				}
				var scCount int
				err = dbConnectionPool.QueryRow(ctx,
					`SELECT COUNT(*) FROM state_changes WHERE to_id IN (SELECT to_id FROM transactions WHERE hash = ANY($1))`,
					scHashBytes).Scan(&scCount)
				require.NoError(t, err)
				assert.Equal(t, tc.wantStateChangeCount, scCount, "state change count mismatch")
			}
		})
	}
}

// ===========================================================================
// Group 4: Integration Tests (DB + mocks, test wiring)
// ===========================================================================

func Test_ingestService_consumeAndFlush(t *testing.T) {
	base := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)

	testCases := []struct {
		name           string
		batchSize      int
		sendCount      int
		flushErr       error
		wantFlushCalls int
		wantErr        bool
	}{
		{
			name:           "flushes_at_batch_size",
			batchSize:      3,
			sendCount:      3,
			wantFlushCalls: 1,
		},
		{
			name:           "final_flush_for_remainder",
			batchSize:      10,
			sendCount:      3,
			wantFlushCalls: 1,
		},
		{
			name:           "multiple_flushes",
			batchSize:      2,
			sendCount:      5,
			wantFlushCalls: 3, // 2 full + 1 final (1 item)
		},
		{
			name:           "flush_error_returns",
			batchSize:      2,
			sendCount:      3,
			flushErr:       fmt.Errorf("db connection lost"),
			wantFlushCalls: 1,
			wantErr:        true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			svc := newTestBackfillService(t, testBackfillOpts{
				batchSize:         100,
				dbInsertBatchSize: tc.batchSize,
			})

			gapBatch := &BackfillBatch{StartLedger: 10, EndLedger: uint32(10 + tc.sendCount - 1)}
			pipeCtx, pipeCancel := context.WithCancel(context.Background())
			defer pipeCancel()

			flushCh := make(chan *LedgerBuffer, tc.sendCount)
			compressorCh := make(chan *CompressBatch, 100)
			var pm pipelineMetrics

			flushCalls := 0
			flush := func(_ context.Context, buffers []*LedgerBuffer) error {
				flushCalls++
				return tc.flushErr
			}

			// Send items
			for i := 0; i < tc.sendCount; i++ {
				flushCh <- makeLedgerBuffer(uint32(10+i), base.Add(time.Duration(i)*time.Second))
			}
			close(flushCh)

			ledgersCount, _, err := svc.consumeAndFlush(context.Background(), gapBatch, pipeCtx, flushCh, flush, &pm, pipeCancel, compressorCh)

			if tc.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tc.sendCount, ledgersCount)
			}
			assert.Equal(t, tc.wantFlushCalls, flushCalls)
		})
	}
}

func Test_ingestService_processBackfillBatchesParallel(t *testing.T) {
	testCases := []struct {
		name        string
		batches     []BackfillBatch
		failIndex   int // -1 means no failure
		wantResults int
		wantFailed  int
	}{
		{
			name: "all_batches_succeed",
			batches: []BackfillBatch{
				{StartLedger: 100, EndLedger: 109},
				{StartLedger: 110, EndLedger: 119},
				{StartLedger: 120, EndLedger: 129},
			},
			failIndex:   -1,
			wantResults: 3,
			wantFailed:  0,
		},
		{
			name: "one_batch_fails",
			batches: []BackfillBatch{
				{StartLedger: 100, EndLedger: 109},
				{StartLedger: 110, EndLedger: 119},
				{StartLedger: 120, EndLedger: 129},
			},
			failIndex:   1,
			wantResults: 3,
			wantFailed:  1,
		},
		{
			name:        "empty_batches",
			batches:     []BackfillBatch{},
			failIndex:   -1,
			wantResults: 0,
			wantFailed:  0,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			factory := func(_ context.Context) (ledgerbackend.LedgerBackend, error) {
				b := &LedgerBackendMock{}
				if tc.failIndex >= 0 {
					failBatch := tc.batches[tc.failIndex]
					b.On("PrepareRange", mock.Anything, mock.MatchedBy(func(r ledgerbackend.Range) bool {
						return r.From() == failBatch.StartLedger
					})).Return(fmt.Errorf("simulated failure"))
				}
				b.On("PrepareRange", mock.Anything, mock.Anything).Return(nil).Maybe()
				b.On("Close").Return(nil).Maybe()
				return b, nil
			}

			svc := newTestBackfillService(t, testBackfillOpts{
				factory:   factory,
				batchSize: 10,
			})

			processor := func(_ context.Context, backend ledgerbackend.LedgerBackend, batch BackfillBatch) BackfillResult {
				// Simply test setupBatchBackend behavior through processSingleBatch
				if err := backend.PrepareRange(context.Background(), ledgerbackend.BoundedRange(batch.StartLedger, batch.EndLedger)); err != nil {
					return BackfillResult{Batch: batch, Error: fmt.Errorf("preparing backend range: %w", err)}
				}
				return BackfillResult{Batch: batch, LedgersCount: int(batch.EndLedger - batch.StartLedger + 1)}
			}

			results := svc.processBackfillBatchesParallel(context.Background(), tc.batches, processor)

			require.Len(t, results, tc.wantResults)

			failedCount := 0
			for _, r := range results {
				if r.Error != nil {
					failedCount++
				}
			}
			assert.Equal(t, tc.wantFailed, failedCount)
		})
	}
}

func Test_ingestService_startHistoricalBackfill(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()
	ctx := context.Background()
	dbConnectionPool, err := db.OpenDBConnectionPool(ctx, dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	testCases := []struct {
		name           string
		startLedger    uint32
		endLedger      uint32
		latestIngested uint32
		setupDB        func(t *testing.T)
		wantErr        string
	}{
		{
			name:        "start_greater_than_end",
			startLedger: 200,
			endLedger:   100,
			wantErr:     "start ledger cannot be greater than end ledger",
		},
		{
			name:           "end_exceeds_latest_ingested",
			startLedger:    50,
			endLedger:      150,
			latestIngested: 100,
			wantErr:        "end ledger 150 cannot be greater than latest ingested ledger 100",
		},
		{
			name:           "no_gaps_returns_nil",
			startLedger:    110,
			endLedger:      150,
			latestIngested: 200,
			setupDB: func(t *testing.T) {
				// Insert contiguous transactions 100-200 so no gaps exist
				for ledger := uint32(100); ledger <= 200; ledger++ {
					_, err := dbConnectionPool.Exec(ctx,
						`INSERT INTO transactions (hash, to_id, envelope_xdr, fee_charged, result_code, meta_xdr, ledger_number, ledger_created_at)
						VALUES ($1, $2, 'env', 100, 'TransactionResultCodeTxSuccess', 'meta', $3, NOW())`,
						fmt.Sprintf("hash_validate_%d", ledger), ledger, ledger)
					require.NoError(t, err)
				}
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := dbConnectionPool.Exec(ctx, "DELETE FROM transactions")
			require.NoError(t, err)
			_, err = dbConnectionPool.Exec(ctx, "DELETE FROM ingest_store")
			require.NoError(t, err)

			if tc.latestIngested > 0 {
				_, err = dbConnectionPool.Exec(ctx,
					`INSERT INTO ingest_store (key, value) VALUES ('latest_ledger_cursor', $1)`,
					fmt.Sprintf("%d", tc.latestIngested))
				require.NoError(t, err)
				_, err = dbConnectionPool.Exec(ctx,
					`INSERT INTO ingest_store (key, value) VALUES ('oldest_ledger_cursor', $1)`,
					fmt.Sprintf("%d", tc.latestIngested))
				require.NoError(t, err)
			}

			mockMetricsService := metrics.NewMockMetricsService()
			mockMetricsService.On("RegisterPoolMetrics", mock.Anything, mock.Anything).Return().Maybe()
			mockMetricsService.On("ObserveDBQueryDuration", mock.Anything, mock.Anything, mock.Anything).Return().Maybe()
			mockMetricsService.On("IncDBQuery", mock.Anything, mock.Anything).Return().Maybe()
			defer mockMetricsService.AssertExpectations(t)

			models, err := data.NewModels(dbConnectionPool, mockMetricsService)
			require.NoError(t, err)

			if tc.setupDB != nil {
				tc.setupDB(t)
			}

			mockBackendFactory := func(_ context.Context) (ledgerbackend.LedgerBackend, error) {
				return nil, fmt.Errorf("mock backend factory error")
			}

			svc := newTestBackfillService(t, testBackfillOpts{
				models:    models,
				factory:   mockBackendFactory,
				batchSize: 100,
			})

			err = svc.startHistoricalBackfill(ctx, tc.startLedger, tc.endLedger)
			if tc.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tc.wantErr)
			} else {
				require.NoError(t, err)
			}
		})
	}
}
