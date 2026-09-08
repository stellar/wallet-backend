// Tests for progressive recompression watermark logic during historical backfill.
package services

import (
	"context"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stellar/go-stellar-sdk/keypair"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/wallet-backend/internal/data"
	"github.com/stellar/wallet-backend/internal/db"
	"github.com/stellar/wallet-backend/internal/db/dbtest"
	"github.com/stellar/wallet-backend/internal/indexer"
	"github.com/stellar/wallet-backend/internal/indexer/types"
	"github.com/stellar/wallet-backend/internal/metrics"
	"github.com/stellar/wallet-backend/internal/utils"
)

// newTestRecompressor creates a progressiveRecompressor for testing watermark logic.
// No background goroutine is started; triggerCh is buffered for direct inspection.
func newTestRecompressor(totalBatches int) *progressiveRecompressor {
	return &progressiveRecompressor{
		completed:    make([]bool, totalBatches),
		endTimes:     make([]time.Time, totalBatches),
		watermarkIdx: -1,
		triggerCh:    make(chan time.Time, totalBatches),
	}
}

// drainWindows reads all available safeEnd values from triggerCh without blocking.
func drainWindows(r *progressiveRecompressor) []time.Time {
	var windows []time.Time
	for {
		select {
		case w := <-r.triggerCh:
			windows = append(windows, w)
		default:
			return windows
		}
	}
}

func Test_progressiveRecompressor_MarkDone_sequential(t *testing.T) {
	r := newTestRecompressor(5)
	base := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)

	// Complete batches 0-4 in order
	for i := 0; i < 5; i++ {
		startTime := base.Add(time.Duration(i) * time.Hour)
		endTime := base.Add(time.Duration(i+1) * time.Hour)
		r.MarkDone(i, startTime, endTime)
	}

	windows := drainWindows(r)
	// Each call advances the watermark — expect 5 windows
	require.Len(t, windows, 5)

	// First window: safeEnd = batch 0 endTime
	assert.Equal(t, base.Add(1*time.Hour), windows[0])

	// Second window: safeEnd = batch 1 endTime
	assert.Equal(t, base.Add(2*time.Hour), windows[1])

	// Last window: safeEnd = batch 4 endTime
	assert.Equal(t, base.Add(5*time.Hour), windows[4])

	// Verify globalStart set from batch 0
	assert.Equal(t, base, r.globalStart)
}

func Test_progressiveRecompressor_MarkDone_outOfOrder(t *testing.T) {
	r := newTestRecompressor(5)
	base := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	endTime := func(i int) time.Time { return base.Add(time.Duration(i+1) * time.Hour) }
	startTime := func(i int) time.Time { return base.Add(time.Duration(i) * time.Hour) }

	// Complete batch 2 first — watermark can't advance (batch 0 not done)
	r.MarkDone(2, startTime(2), endTime(2))
	windows := drainWindows(r)
	assert.Empty(t, windows)
	assert.Equal(t, -1, r.watermarkIdx)

	// Complete batch 4 — still no advancement
	r.MarkDone(4, startTime(4), endTime(4))
	windows = drainWindows(r)
	assert.Empty(t, windows)

	// Complete batch 0 — watermark advances to 0 only (batch 1 missing)
	r.MarkDone(0, startTime(0), endTime(0))
	windows = drainWindows(r)
	require.Len(t, windows, 1)
	assert.Equal(t, endTime(0), windows[0])
	assert.Equal(t, 0, r.watermarkIdx)

	// Complete batch 1 — watermark jumps from 0 to 2 (batch 2 was already done)
	r.MarkDone(1, startTime(1), endTime(1))
	windows = drainWindows(r)
	require.Len(t, windows, 1)
	assert.Equal(t, endTime(2), windows[0]) // safeEnd = batch 2's endTime
	assert.Equal(t, 2, r.watermarkIdx)

	// Complete batch 3 — watermark jumps from 2 to 4 (batch 4 was already done)
	r.MarkDone(3, startTime(3), endTime(3))
	windows = drainWindows(r)
	require.Len(t, windows, 1)
	assert.Equal(t, endTime(4), windows[0])
	assert.Equal(t, 4, r.watermarkIdx)
}

func Test_progressiveRecompressor_MarkDone_failedBatchBlocksWatermark(t *testing.T) {
	r := newTestRecompressor(5)
	base := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	endTime := func(i int) time.Time { return base.Add(time.Duration(i+1) * time.Hour) }
	startTime := func(i int) time.Time { return base.Add(time.Duration(i) * time.Hour) }

	// Complete batches 0 and 1
	r.MarkDone(0, startTime(0), endTime(0))
	r.MarkDone(1, startTime(1), endTime(1))
	_ = drainWindows(r) // consume windows

	// Batch 2 fails (never call MarkDone for it)

	// Complete batches 3 and 4
	r.MarkDone(3, startTime(3), endTime(3))
	r.MarkDone(4, startTime(4), endTime(4))

	// No new windows — watermark stuck at 1
	windows := drainWindows(r)
	assert.Empty(t, windows)
	assert.Equal(t, 1, r.watermarkIdx)
}

func Test_progressiveRecompressor_MarkDone_singleBatch(t *testing.T) {
	r := newTestRecompressor(1)
	start := time.Date(2025, 6, 1, 0, 0, 0, 0, time.UTC)
	end := time.Date(2025, 6, 1, 1, 0, 0, 0, time.UTC)

	r.MarkDone(0, start, end)

	windows := drainWindows(r)
	require.Len(t, windows, 1)
	assert.Equal(t, end, windows[0])
	assert.Equal(t, 0, r.watermarkIdx)
	assert.Equal(t, start, r.globalStart)
}

func Test_progressiveRecompressor_MarkDone_globalStartSetFromBatchZero(t *testing.T) {
	r := newTestRecompressor(3)
	base := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)

	// Complete batch 1 first — globalStart should NOT be set
	r.MarkDone(1, base.Add(1*time.Hour), base.Add(2*time.Hour))
	assert.True(t, r.globalStart.IsZero())

	// Complete batch 0 — globalStart should be set to batch 0's startTime
	r.MarkDone(0, base, base.Add(1*time.Hour))
	assert.Equal(t, base, r.globalStart)

	// Complete batch 2 — globalStart unchanged
	r.MarkDone(2, base.Add(2*time.Hour), base.Add(3*time.Hour))
	assert.Equal(t, base, r.globalStart)
}

func Test_progressiveRecompressor_MarkDone_allSimultaneous(t *testing.T) {
	r := newTestRecompressor(4)
	base := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	endTime := func(i int) time.Time { return base.Add(time.Duration(i+1) * time.Hour) }
	startTime := func(i int) time.Time { return base.Add(time.Duration(i) * time.Hour) }

	// Complete all batches in reverse order except batch 0
	r.MarkDone(3, startTime(3), endTime(3))
	r.MarkDone(2, startTime(2), endTime(2))
	r.MarkDone(1, startTime(1), endTime(1))
	windows := drainWindows(r)
	assert.Empty(t, windows) // Nothing yet — batch 0 missing

	// Complete batch 0 — watermark jumps from -1 to 3 in one step
	r.MarkDone(0, startTime(0), endTime(0))
	windows = drainWindows(r)
	require.Len(t, windows, 1)
	assert.Equal(t, endTime(3), windows[0]) // safeEnd = last batch
	assert.Equal(t, 3, r.watermarkIdx)
}

// Test_flushBatchBufferWithRetry_PermanentErrorFailsFast verifies that a buffer the
// database can never accept — here a state change whose signer key does not encode —
// is abandoned on the first attempt instead of consuming the retry budget.
func Test_flushBatchBufferWithRetry_PermanentErrorFailsFast(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()
	ctx := context.Background()

	pool, err := db.OpenDBConnectionPool(ctx, dbt.DSN)
	require.NoError(t, err)
	defer pool.Close()

	appMetrics := metrics.NewMetrics(prometheus.NewRegistry())
	models, err := data.NewModels(pool, appMetrics.DB)
	require.NoError(t, err)

	// flushBatchBufferWithRetry only touches the models and the ingestion metrics when
	// no cursor update is requested, so a struct literal with those two is the whole
	// service surface under test.
	m := &ingestService{models: models, appMetrics: appMetrics}

	now := time.Now()
	account := keypair.MustRandom().Address()
	transaction := &types.Transaction{
		Hash:            "f176b7b0133690fbfb2de8fa9ca2273cb4f2e29447e0cf0e14a5f82d0daa4877",
		ToID:            1,
		FeeCharged:      100,
		ResultCode:      "TransactionResultCodeTxSuccess",
		LedgerNumber:    1,
		LedgerCreatedAt: now,
	}

	buffer := indexer.NewIndexerBuffer()
	buffer.PushStateChange(transaction, nil, types.StateChange{
		ToID:                1,
		StateChangeID:       1,
		StateChangeCategory: types.StateChangeCategorySigner,
		StateChangeReason:   types.StateChangeReasonAdd,
		LedgerCreatedAt:     now,
		LedgerNumber:        1,
		AccountID:           types.AddressBytea(account),
		SignerAccountID:     utils.NullSignerKeyBytea("not-a-strkey"),
	})

	err = m.flushBatchBufferWithRetry(ctx, buffer, nil)

	require.Error(t, err)
	require.ErrorIs(t, err, data.ErrRowEncoding)

	// No retry ran: the retry counters never moved and the permanent-error counter did.
	assert.Equal(t, 0.0, testutil.ToFloat64(
		appMetrics.Ingestion.RetriesTotal.WithLabelValues("batch_flush")))
	assert.Equal(t, 0.0, testutil.ToFloat64(
		appMetrics.Ingestion.RetryExhaustionsTotal.WithLabelValues("batch_flush")))
	assert.Equal(t, 1.0, testutil.ToFloat64(
		appMetrics.Ingestion.ErrorsTotal.WithLabelValues("batch_flush")))
}
