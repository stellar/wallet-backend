package ingest

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stellar/go-stellar-sdk/xdr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestBackfillFetcher_DeliversAllLedgers(t *testing.T) {
	ledgersPerFile := uint32(10)
	schema := testSchema(ledgersPerFile)
	ds := &mockDataStore{}

	// Register 3 files covering ledgers 100-129
	for fileStart := uint32(100); fileStart < 130; fileStart += ledgersPerFile {
		objectKey := schema.GetObjectKeyFromSequenceNumber(fileStart)
		ds.On("GetFile", mock.Anything, objectKey).Return(
			encodeBatch(t, fileStart, fileStart+ledgersPerFile-1), nil,
		).Once()
	}

	ledgerCh := make(chan xdr.LedgerCloseMeta, 100)

	fetcher := NewBackfillFetcher(BackfillFetcherConfig{
		NumWorkers: 3,
		RetryLimit: 1,
		RetryWait:  10 * time.Millisecond,
		GapStart:   100,
		GapEnd:     129,
	}, ds, schema, ledgerCh)

	ctx := context.Background()
	fetchCtx, fetchCancel := context.WithCancelCause(ctx)
	defer fetchCancel(nil)

	stats := fetcher.Run(fetchCtx, fetchCancel)

	// ledgerCh should be closed by Run
	var received []uint32
	for lcm := range ledgerCh {
		received = append(received, lcm.LedgerSequence())
	}

	// All 30 ledgers delivered (order doesn't matter)
	assert.Len(t, received, 30)
	assert.Equal(t, 3, stats.FetchCount) // 3 files fetched, not 30 ledgers

	// Verify all sequences present
	seqSet := make(map[uint32]bool)
	for _, seq := range received {
		seqSet[seq] = true
	}
	for seq := uint32(100); seq <= 129; seq++ {
		assert.True(t, seqSet[seq], "missing ledger %d", seq)
	}
}

func TestBackfillFetcher_PartialFileFiltering(t *testing.T) {
	// Gap 105-124 with LedgersPerFile=10 spans files 100-109, 110-119, 120-129.
	// Only ledgers 105-124 should be delivered.
	ledgersPerFile := uint32(10)
	schema := testSchema(ledgersPerFile)
	ds := &mockDataStore{}

	for fileStart := uint32(100); fileStart < 130; fileStart += ledgersPerFile {
		objectKey := schema.GetObjectKeyFromSequenceNumber(fileStart)
		ds.On("GetFile", mock.Anything, objectKey).Return(
			encodeBatch(t, fileStart, fileStart+ledgersPerFile-1), nil,
		).Once()
	}

	ledgerCh := make(chan xdr.LedgerCloseMeta, 100)

	fetcher := NewBackfillFetcher(BackfillFetcherConfig{
		NumWorkers: 2,
		RetryLimit: 1,
		RetryWait:  10 * time.Millisecond,
		GapStart:   105,
		GapEnd:     124,
	}, ds, schema, ledgerCh)

	ctx := context.Background()
	fetchCtx, fetchCancel := context.WithCancelCause(ctx)
	defer fetchCancel(nil)

	fetcher.Run(fetchCtx, fetchCancel)

	var received []uint32
	for lcm := range ledgerCh {
		received = append(received, lcm.LedgerSequence())
	}

	assert.Len(t, received, 20)
	seqSet := make(map[uint32]bool)
	for _, seq := range received {
		seqSet[seq] = true
	}
	for seq := uint32(105); seq <= 124; seq++ {
		assert.True(t, seqSet[seq], "missing ledger %d", seq)
	}
	// Verify out-of-range ledgers are NOT delivered
	for _, seq := range []uint32{100, 101, 102, 103, 104, 125, 126, 127, 128, 129} {
		assert.False(t, seqSet[seq], "unexpected ledger %d", seq)
	}
}

func TestBackfillFetcher_ContextCancellation(t *testing.T) {
	ledgersPerFile := uint32(10)
	schema := testSchema(ledgersPerFile)
	ds := &mockDataStore{}

	// Use a small ledgerCh buffer so workers will block on send
	ledgerCh := make(chan xdr.LedgerCloseMeta, 1)

	// Register many files so workers have work to do
	for fileStart := uint32(0); fileStart < 100; fileStart += ledgersPerFile {
		objectKey := schema.GetObjectKeyFromSequenceNumber(fileStart)
		ds.On("GetFile", mock.Anything, objectKey).Return(
			encodeBatch(t, fileStart, fileStart+ledgersPerFile-1), nil,
		).Maybe()
	}

	fetcher := NewBackfillFetcher(BackfillFetcherConfig{
		NumWorkers: 3,
		RetryLimit: 1,
		RetryWait:  10 * time.Millisecond,
		GapStart:   0,
		GapEnd:     99,
	}, ds, schema, ledgerCh)

	ctx := context.Background()
	fetchCtx, fetchCancel := context.WithCancelCause(ctx)

	// Cancel after a short delay
	go func() {
		time.Sleep(50 * time.Millisecond)
		fetchCancel(fmt.Errorf("test cancellation"))
	}()

	done := make(chan struct{})
	go func() {
		fetcher.Run(fetchCtx, fetchCancel)
		close(done)
	}()

	// Drain ledgerCh so workers can make progress until cancellation
	go func() {
		for range ledgerCh { //nolint:revive
		}
	}()

	// Run should complete without deadlock
	select {
	case <-done:
		// Success
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not exit after context cancellation — possible deadlock")
	}
}

func TestBackfillFetcher_RetryOnTransientError(t *testing.T) {
	ledgersPerFile := uint32(10)
	schema := testSchema(ledgersPerFile)
	ds := &mockDataStore{}

	objectKey := schema.GetObjectKeyFromSequenceNumber(100)
	// First call fails, second succeeds
	ds.On("GetFile", mock.Anything, objectKey).
		Return(nil, fmt.Errorf("transient S3 error")).Once()
	ds.On("GetFile", mock.Anything, objectKey).
		Return(encodeBatch(t, 100, 109), nil).Once()

	ledgerCh := make(chan xdr.LedgerCloseMeta, 100)

	fetcher := NewBackfillFetcher(BackfillFetcherConfig{
		NumWorkers: 1,
		RetryLimit: 2,
		RetryWait:  10 * time.Millisecond,
		GapStart:   100,
		GapEnd:     109,
	}, ds, schema, ledgerCh)

	ctx := context.Background()
	fetchCtx, fetchCancel := context.WithCancelCause(ctx)
	defer fetchCancel(nil)

	stats := fetcher.Run(fetchCtx, fetchCancel)

	var received []uint32
	for lcm := range ledgerCh {
		received = append(received, lcm.LedgerSequence())
	}

	assert.Len(t, received, 10)
	assert.Equal(t, 1, stats.FetchCount)
	assert.Nil(t, context.Cause(fetchCtx), "context should not be cancelled")
}

func TestBackfillFetcher_MissingFile(t *testing.T) {
	ledgersPerFile := uint32(10)
	schema := testSchema(ledgersPerFile)
	ds := &mockDataStore{}

	objectKey := schema.GetObjectKeyFromSequenceNumber(100)
	ds.On("GetFile", mock.Anything, objectKey).
		Return(nil, os.ErrNotExist)

	ledgerCh := make(chan xdr.LedgerCloseMeta, 100)

	fetcher := NewBackfillFetcher(BackfillFetcherConfig{
		NumWorkers: 1,
		RetryLimit: 1,
		RetryWait:  10 * time.Millisecond,
		GapStart:   100,
		GapEnd:     109,
	}, ds, schema, ledgerCh)

	ctx := context.Background()
	fetchCtx, fetchCancel := context.WithCancelCause(ctx)
	defer fetchCancel(nil)

	fetcher.Run(fetchCtx, fetchCancel)

	// Context should be cancelled with cause
	cause := context.Cause(fetchCtx)
	require.NotNil(t, cause)
	assert.Contains(t, cause.Error(), "not found")
}
