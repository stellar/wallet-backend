package ingest

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"testing"
	"time"

	"github.com/stellar/go-stellar-sdk/ingest/ledgerbackend"
	"github.com/stellar/go-stellar-sdk/support/compressxdr"
	"github.com/stellar/go-stellar-sdk/support/datastore"
	"github.com/stellar/go-stellar-sdk/xdr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// mockDataStore implements datastore.DataStore for testing.
type mockDataStore struct {
	mock.Mock
}

func (m *mockDataStore) GetFile(ctx context.Context, path string) (io.ReadCloser, int64, error) {
	args := m.Called(ctx, path)
	if args.Get(0) == nil {
		return nil, 0, args.Error(1)
	}
	// Size (second return) is unused by the streaming decode path, so it is always 0 here.
	return args.Get(0).(io.ReadCloser), 0, args.Error(1)
}

func (m *mockDataStore) GetFileMetadata(ctx context.Context, path string) (map[string]string, error) {
	args := m.Called(ctx, path)
	return args.Get(0).(map[string]string), args.Error(1)
}

func (m *mockDataStore) GetFileLastModified(ctx context.Context, path string) (time.Time, error) {
	args := m.Called(ctx, path)
	return args.Get(0).(time.Time), args.Error(1)
}

func (m *mockDataStore) PutFile(ctx context.Context, path string, in io.WriterTo, metaData map[string]string) error {
	args := m.Called(ctx, path, in, metaData)
	return args.Error(0)
}

func (m *mockDataStore) PutFileIfNotExists(ctx context.Context, path string, in io.WriterTo, metaData map[string]string) (bool, error) {
	args := m.Called(ctx, path, in, metaData)
	return args.Bool(0), args.Error(1)
}

func (m *mockDataStore) Exists(ctx context.Context, path string) (bool, error) {
	args := m.Called(ctx, path)
	return args.Bool(0), args.Error(1)
}

func (m *mockDataStore) Size(ctx context.Context, path string) (int64, error) {
	args := m.Called(ctx, path)
	return args.Get(0).(int64), args.Error(1)
}

func (m *mockDataStore) ListFilePaths(ctx context.Context, options datastore.ListFileOptions) ([]string, error) {
	args := m.Called(ctx, options)
	return args.Get(0).([]string), args.Error(1)
}

func (m *mockDataStore) Close() error {
	args := m.Called()
	return args.Error(0)
}

// encodeBatch creates a compressed XDR-encoded LedgerCloseMetaBatch as an io.ReadCloser.
// Each ledger in [startSeq, endSeq] gets a minimal LedgerCloseMeta.
func encodeBatch(t *testing.T, startSeq, endSeq uint32) io.ReadCloser {
	t.Helper()

	batch := buildBatch(t, startSeq, endSeq)
	var buf bytes.Buffer
	encoder := compressxdr.NewXDREncoder(compressxdr.DefaultCompressor, &batch)
	_, err := encoder.WriteTo(&buf)
	require.NoError(t, err)
	return io.NopCloser(bytes.NewReader(buf.Bytes()))
}

// buildBatch creates a LedgerCloseMetaBatch for ledgers [startSeq, endSeq].
func buildBatch(t *testing.T, startSeq, endSeq uint32) xdr.LedgerCloseMetaBatch {
	t.Helper()

	batch := xdr.LedgerCloseMetaBatch{
		StartSequence: xdr.Uint32(startSeq),
		EndSequence:   xdr.Uint32(endSeq),
	}
	for seq := startSeq; seq <= endSeq; seq++ {
		batch.LedgerCloseMetas = append(batch.LedgerCloseMetas, xdr.LedgerCloseMeta{
			V: 0,
			V0: &xdr.LedgerCloseMetaV0{
				LedgerHeader: xdr.LedgerHeaderHistoryEntry{
					Header: xdr.LedgerHeader{
						LedgerSeq: xdr.Uint32(seq),
					},
				},
			},
		})
	}
	return batch
}

// testSchema returns a DataStoreSchema with the given ledgersPerFile.
func testSchema(ledgersPerFile uint32) datastore.DataStoreSchema {
	return datastore.DataStoreSchema{
		LedgersPerFile:    ledgersPerFile,
		FilesPerPartition: 1,
	}
}

func TestDatastoreBackend_Defaults(t *testing.T) {
	ds := &mockDataStore{}
	schema := testSchema(10)

	backend, err := newDatastoreBackend(ledgerbackend.BufferedStorageBackendConfig{}, ds, schema)
	require.NoError(t, err)

	assert.Equal(t, uint32(100), backend.config.BufferSize, "default BufferSize should be 100")
	assert.Equal(t, uint32(10), backend.config.NumWorkers, "default NumWorkers should be 10")
}

func TestDatastoreBackend_Validation(t *testing.T) {
	ds := &mockDataStore{}

	t.Run("workers > buffer errors", func(t *testing.T) {
		_, err := newDatastoreBackend(ledgerbackend.BufferedStorageBackendConfig{
			BufferSize: 3,
			NumWorkers: 5,
		}, ds, testSchema(10))
		require.Error(t, err)
		assert.Contains(t, err.Error(), "NumWorkers (5) must be <= BufferSize (3)")
	})

	t.Run("ledgersPerFile=0 errors", func(t *testing.T) {
		_, err := newDatastoreBackend(ledgerbackend.BufferedStorageBackendConfig{}, ds, testSchema(0))
		require.Error(t, err)
		assert.Contains(t, err.Error(), "LedgersPerFile must be > 0")
	})
}

func TestDatastoreBackend_SequentialGetLedger(t *testing.T) {
	ds := &mockDataStore{}
	schema := testSchema(10)

	backend, err := newDatastoreBackend(ledgerbackend.BufferedStorageBackendConfig{
		BufferSize: 3,
		NumWorkers: 2,
	}, ds, schema)
	require.NoError(t, err)

	// Two files: ledgers 0-9 and 10-19.
	objectKey0 := schema.GetObjectKeyFromSequenceNumber(0)
	objectKey10 := schema.GetObjectKeyFromSequenceNumber(10)
	ds.On("GetFile", mock.Anything, objectKey0).Return(encodeBatch(t, 0, 9), nil)
	ds.On("GetFile", mock.Anything, objectKey10).Return(encodeBatch(t, 10, 19), nil)
	// Additional files that may be prefetched by the buffer.
	objectKey20 := schema.GetObjectKeyFromSequenceNumber(20)
	objectKey30 := schema.GetObjectKeyFromSequenceNumber(30)
	ds.On("GetFile", mock.Anything, objectKey20).Return(encodeBatch(t, 20, 29), nil)
	ds.On("GetFile", mock.Anything, objectKey30).Return(encodeBatch(t, 30, 39), nil)

	ctx := context.Background()
	require.NoError(t, backend.PrepareRange(ctx, ledgerbackend.BoundedRange(0, 19)))

	// Fetch all 20 ledgers sequentially.
	for seq := uint32(0); seq <= 19; seq++ {
		lcm, err := backend.GetLedger(ctx, seq)
		require.NoError(t, err, "GetLedger(%d)", seq)
		assert.Equal(t, xdr.Uint32(seq), lcm.V0.LedgerHeader.Header.LedgerSeq)
	}

	require.NoError(t, backend.Close())
}

func TestDatastoreBackend_CachedBatchHit(t *testing.T) {
	ds := &mockDataStore{}
	schema := testSchema(5)

	backend, err := newDatastoreBackend(ledgerbackend.BufferedStorageBackendConfig{
		BufferSize: 2,
		NumWorkers: 1,
	}, ds, schema)
	require.NoError(t, err)

	objectKey0 := schema.GetObjectKeyFromSequenceNumber(0)
	// Only expect one GetFile call — subsequent ledgers in the same batch are cached.
	ds.On("GetFile", mock.Anything, objectKey0).Return(encodeBatch(t, 0, 4), nil).Once()
	objectKey5 := schema.GetObjectKeyFromSequenceNumber(5)
	ds.On("GetFile", mock.Anything, objectKey5).Return(encodeBatch(t, 5, 9), nil)
	objectKey10 := schema.GetObjectKeyFromSequenceNumber(10)
	ds.On("GetFile", mock.Anything, objectKey10).Return(encodeBatch(t, 10, 14), nil)

	ctx := context.Background()
	require.NoError(t, backend.PrepareRange(ctx, ledgerbackend.BoundedRange(0, 4)))

	// Fetch all 5 ledgers — all from the same cached batch.
	for seq := uint32(0); seq <= 4; seq++ {
		lcm, err := backend.GetLedger(ctx, seq)
		require.NoError(t, err)
		assert.Equal(t, xdr.Uint32(seq), lcm.V0.LedgerHeader.Header.LedgerSeq)
	}

	// Verify GetFile was called exactly once for file 0.
	ds.AssertNumberOfCalls(t, "GetFile", 1)

	require.NoError(t, backend.Close())
}

func TestDatastoreBackend_CrossFileBoundary(t *testing.T) {
	ds := &mockDataStore{}
	schema := testSchema(5)

	backend, err := newDatastoreBackend(ledgerbackend.BufferedStorageBackendConfig{
		BufferSize: 3,
		NumWorkers: 2,
	}, ds, schema)
	require.NoError(t, err)

	// Range spans two files: [3, 7] covers files at boundaries 0 and 5.
	objectKey0 := schema.GetObjectKeyFromSequenceNumber(0)
	objectKey5 := schema.GetObjectKeyFromSequenceNumber(5)
	ds.On("GetFile", mock.Anything, objectKey0).Return(encodeBatch(t, 0, 4), nil)
	ds.On("GetFile", mock.Anything, objectKey5).Return(encodeBatch(t, 5, 9), nil)

	ctx := context.Background()
	require.NoError(t, backend.PrepareRange(ctx, ledgerbackend.BoundedRange(3, 7)))

	// Fetch ledgers across the file boundary.
	for seq := uint32(3); seq <= 7; seq++ {
		lcm, err := backend.GetLedger(ctx, seq)
		require.NoError(t, err, "GetLedger(%d)", seq)
		assert.Equal(t, xdr.Uint32(seq), lcm.V0.LedgerHeader.Header.LedgerSeq)
	}

	require.NoError(t, backend.Close())
}

func TestDatastoreBackend_NotPrepared(t *testing.T) {
	ds := &mockDataStore{}
	backend, err := newDatastoreBackend(ledgerbackend.BufferedStorageBackendConfig{}, ds, testSchema(10))
	require.NoError(t, err)

	ctx := context.Background()

	_, err = backend.GetLedger(ctx, 0)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "must be prepared")

	_, err = backend.GetLatestLedgerSequence(ctx)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "must be prepared")
}

func TestDatastoreBackend_OutOfOrder(t *testing.T) {
	ds := &mockDataStore{}
	schema := testSchema(10)

	backend, err := newDatastoreBackend(ledgerbackend.BufferedStorageBackendConfig{
		BufferSize: 2,
		NumWorkers: 1,
	}, ds, schema)
	require.NoError(t, err)

	objectKey0 := schema.GetObjectKeyFromSequenceNumber(0)
	ds.On("GetFile", mock.Anything, objectKey0).Return(encodeBatch(t, 0, 9), nil)
	objectKey10 := schema.GetObjectKeyFromSequenceNumber(10)
	ds.On("GetFile", mock.Anything, objectKey10).Return(encodeBatch(t, 10, 19), nil)
	objectKey20 := schema.GetObjectKeyFromSequenceNumber(20)
	ds.On("GetFile", mock.Anything, objectKey20).Return(encodeBatch(t, 20, 29), nil)

	ctx := context.Background()
	require.NoError(t, backend.PrepareRange(ctx, ledgerbackend.BoundedRange(0, 19)))

	// Requesting ledger 5 when nextLedger is 0 should error.
	_, err = backend.GetLedger(ctx, 5)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not the expected next ledger")

	require.NoError(t, backend.Close())
}

func TestDatastoreBackend_OutsideRange(t *testing.T) {
	ds := &mockDataStore{}
	schema := testSchema(10)

	backend, err := newDatastoreBackend(ledgerbackend.BufferedStorageBackendConfig{
		BufferSize: 2,
		NumWorkers: 1,
	}, ds, schema)
	require.NoError(t, err)

	objectKey10 := schema.GetObjectKeyFromSequenceNumber(10)
	ds.On("GetFile", mock.Anything, objectKey10).Return(encodeBatch(t, 10, 19), nil)
	objectKey20 := schema.GetObjectKeyFromSequenceNumber(20)
	ds.On("GetFile", mock.Anything, objectKey20).Return(encodeBatch(t, 20, 29), nil)

	ctx := context.Background()
	require.NoError(t, backend.PrepareRange(ctx, ledgerbackend.BoundedRange(10, 19)))

	_, err = backend.GetLedger(ctx, 5)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "outside prepared range")

	_, err = backend.GetLedger(ctx, 25)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "outside prepared range")

	require.NoError(t, backend.Close())
}

func TestDatastoreBackend_RePrepare(t *testing.T) {
	ds := &mockDataStore{}
	schema := testSchema(10)

	backend, err := newDatastoreBackend(ledgerbackend.BufferedStorageBackendConfig{
		BufferSize: 2,
		NumWorkers: 1,
	}, ds, schema)
	require.NoError(t, err)

	objectKey0 := schema.GetObjectKeyFromSequenceNumber(0)
	objectKey10 := schema.GetObjectKeyFromSequenceNumber(10)
	objectKey20 := schema.GetObjectKeyFromSequenceNumber(20)

	// First preparation.
	ds.On("GetFile", mock.Anything, objectKey0).Return(encodeBatch(t, 0, 9), nil)
	ds.On("GetFile", mock.Anything, objectKey10).Return(encodeBatch(t, 10, 19), nil)
	ds.On("GetFile", mock.Anything, objectKey20).Return(encodeBatch(t, 20, 29), nil)

	ctx := context.Background()
	require.NoError(t, backend.PrepareRange(ctx, ledgerbackend.BoundedRange(0, 9)))

	// Read one ledger from first preparation.
	lcm, err := backend.GetLedger(ctx, 0)
	require.NoError(t, err)
	assert.Equal(t, xdr.Uint32(0), lcm.V0.LedgerHeader.Header.LedgerSeq)

	// Re-prepare with a different range.
	require.NoError(t, backend.PrepareRange(ctx, ledgerbackend.BoundedRange(10, 19)))

	lcm, err = backend.GetLedger(ctx, 10)
	require.NoError(t, err)
	assert.Equal(t, xdr.Uint32(10), lcm.V0.LedgerHeader.Header.LedgerSeq)

	require.NoError(t, backend.Close())
}

func TestDatastoreBackend_Close(t *testing.T) {
	ds := &mockDataStore{}
	schema := testSchema(10)

	backend, err := newDatastoreBackend(ledgerbackend.BufferedStorageBackendConfig{
		BufferSize: 2,
		NumWorkers: 1,
	}, ds, schema)
	require.NoError(t, err)

	objectKey0 := schema.GetObjectKeyFromSequenceNumber(0)
	ds.On("GetFile", mock.Anything, objectKey0).Return(encodeBatch(t, 0, 9), nil)
	objectKey10 := schema.GetObjectKeyFromSequenceNumber(10)
	ds.On("GetFile", mock.Anything, objectKey10).Return(encodeBatch(t, 10, 19), nil)

	ctx := context.Background()
	require.NoError(t, backend.PrepareRange(ctx, ledgerbackend.BoundedRange(0, 9)))
	require.NoError(t, backend.Close())

	// All methods should error after close.
	_, err = backend.GetLedger(ctx, 0)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "closed")

	_, err = backend.GetLatestLedgerSequence(ctx)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "closed")

	_, err = backend.IsPrepared(ctx, ledgerbackend.BoundedRange(0, 9))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "closed")

	err = backend.PrepareRange(ctx, ledgerbackend.BoundedRange(0, 9))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "closed")
}

func TestDatastoreBackend_WorkerRetry(t *testing.T) {
	ds := &mockDataStore{}
	schema := testSchema(10)

	backend, err := newDatastoreBackend(ledgerbackend.BufferedStorageBackendConfig{
		BufferSize: 2,
		NumWorkers: 1,
		RetryLimit: 2,
		RetryWait:  10 * time.Millisecond,
	}, ds, schema)
	require.NoError(t, err)

	objectKey0 := schema.GetObjectKeyFromSequenceNumber(0)

	// First call fails, second succeeds.
	ds.On("GetFile", mock.Anything, objectKey0).
		Return(nil, fmt.Errorf("transient network error")).Once()
	ds.On("GetFile", mock.Anything, objectKey0).
		Return(encodeBatch(t, 0, 9), nil).Once()

	objectKey10 := schema.GetObjectKeyFromSequenceNumber(10)
	ds.On("GetFile", mock.Anything, objectKey10).Return(encodeBatch(t, 10, 19), nil)
	objectKey20 := schema.GetObjectKeyFromSequenceNumber(20)
	ds.On("GetFile", mock.Anything, objectKey20).Return(encodeBatch(t, 20, 29), nil)

	ctx := context.Background()
	require.NoError(t, backend.PrepareRange(ctx, ledgerbackend.BoundedRange(0, 9)))

	// Should succeed after retry.
	lcm, err := backend.GetLedger(ctx, 0)
	require.NoError(t, err)
	assert.Equal(t, xdr.Uint32(0), lcm.V0.LedgerHeader.Header.LedgerSeq)

	require.NoError(t, backend.Close())
}

func TestDatastoreBackend_WorkerRetryNotExist(t *testing.T) {
	ds := &mockDataStore{}
	schema := testSchema(10)

	backend, err := newDatastoreBackend(ledgerbackend.BufferedStorageBackendConfig{
		BufferSize: 2,
		NumWorkers: 1,
		RetryLimit: 1,
		RetryWait:  10 * time.Millisecond,
	}, ds, schema)
	require.NoError(t, err)

	objectKey0 := schema.GetObjectKeyFromSequenceNumber(0)
	// Bounded range + file not found = hard error (cancel context).
	ds.On("GetFile", mock.Anything, objectKey0).Return(nil, os.ErrNotExist)
	objectKey10 := schema.GetObjectKeyFromSequenceNumber(10)
	ds.On("GetFile", mock.Anything, objectKey10).Return(encodeBatch(t, 10, 19), nil)

	ctx := context.Background()
	require.NoError(t, backend.PrepareRange(ctx, ledgerbackend.BoundedRange(0, 9)))

	// GetLedger should fail because the file doesn't exist.
	_, err = backend.GetLedger(ctx, 0)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not found")
	assert.ErrorIs(t, err, ErrBufferDead, "a hard NotExist on a bounded range permanently kills the buffer")

	require.NoError(t, backend.Close())
}

// TestDatastoreBackend_WorkerExhaustsRetries_BufferDies covers the other path
// that self-cancels the buffer: a transient error that never clears within
// RetryLimit attempts. GetLedger must surface ErrBufferDead so callers (the
// live-ingestion ledger-fetch retry ladder) know retrying is pointless — this
// buffer is dead and will return the same error forever.
func TestDatastoreBackend_WorkerExhaustsRetries_BufferDies(t *testing.T) {
	ds := &mockDataStore{}
	schema := testSchema(10)

	backend, err := newDatastoreBackend(ledgerbackend.BufferedStorageBackendConfig{
		BufferSize: 2,
		NumWorkers: 1,
		RetryLimit: 2,
		RetryWait:  10 * time.Millisecond,
	}, ds, schema)
	require.NoError(t, err)

	objectKey0 := schema.GetObjectKeyFromSequenceNumber(0)
	ds.On("GetFile", mock.Anything, objectKey0).
		Return(nil, fmt.Errorf("transient network error"))

	ctx := context.Background()
	require.NoError(t, backend.PrepareRange(ctx, ledgerbackend.BoundedRange(0, 9)))

	_, err = backend.GetLedger(ctx, 0)
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrBufferDead)
	assert.NotErrorIs(t, err, context.Canceled, "a dead buffer must be distinguishable from a caller-driven shutdown")

	// A second call against the same (now-dead) backend returns the same
	// permanent error rather than hanging or succeeding.
	_, err = backend.GetLedger(ctx, 0)
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrBufferDead)

	require.NoError(t, backend.Close())
}

func TestDatastoreBackend_ParentContextCancelled_IsNotBufferDead(t *testing.T) {
	// The buffer's ctx is derived from the ctx passed to PrepareRange, so
	// cancelling that parent (a shutdown signal, not a worker giving up) also
	// fires buf.ctx.Done() with cause context.Canceled. GetLedger's error must
	// not claim ErrBufferDead in that case, or a graceful shutdown would be
	// misclassified as a permanent ledger-fetch failure worth failing fast on.
	//
	// GetFile always returns a transient error with a long RetryWait, so the
	// worker is reliably still sleeping between attempts (never exhausting
	// RetryLimit itself, which would call buf.cancel with a real cause) when
	// the test cancels the parent context.
	ds := &mockDataStore{}
	schema := testSchema(10)

	backend, err := newDatastoreBackend(ledgerbackend.BufferedStorageBackendConfig{
		BufferSize: 2,
		NumWorkers: 1,
		RetryLimit: 5,
		RetryWait:  5 * time.Second,
	}, ds, schema)
	require.NoError(t, err)

	// mock.Anything for the path too: with BufferSize 2 and NumWorkers 1, the
	// construction seed loop queues more than one task (sequences 0, 10, 20),
	// and which one the single worker is retrying when the parent context is
	// cancelled is not deterministic.
	ds.On("GetFile", mock.Anything, mock.Anything).Return(nil, fmt.Errorf("transient network error"))

	parentCtx, cancel := context.WithCancel(context.Background())
	require.NoError(t, backend.PrepareRange(parentCtx, ledgerbackend.UnboundedRange(0)))
	cancel()

	require.Eventually(t, func() bool {
		_, getErr := backend.GetLedger(context.Background(), 0)
		return getErr != nil
	}, time.Second, time.Millisecond, "GetLedger should error once the parent context cancellation propagates")

	_, err = backend.GetLedger(context.Background(), 0)
	require.Error(t, err)
	assert.ErrorIs(t, err, context.Canceled)
	assert.NotErrorIs(t, err, ErrBufferDead)
}

func TestDatastoreBackend_IsPrepared(t *testing.T) {
	ds := &mockDataStore{}
	schema := testSchema(10)

	backend, err := newDatastoreBackend(ledgerbackend.BufferedStorageBackendConfig{
		BufferSize: 2,
		NumWorkers: 1,
	}, ds, schema)
	require.NoError(t, err)

	ctx := context.Background()

	// Not prepared yet.
	ok, err := backend.IsPrepared(ctx, ledgerbackend.BoundedRange(0, 9))
	require.NoError(t, err)
	assert.False(t, ok)

	objectKey0 := schema.GetObjectKeyFromSequenceNumber(0)
	ds.On("GetFile", mock.Anything, objectKey0).Return(encodeBatch(t, 0, 9), nil)
	objectKey10 := schema.GetObjectKeyFromSequenceNumber(10)
	ds.On("GetFile", mock.Anything, objectKey10).Return(encodeBatch(t, 10, 19), nil)

	require.NoError(t, backend.PrepareRange(ctx, ledgerbackend.BoundedRange(0, 19)))

	// Subrange should be contained.
	ok, err = backend.IsPrepared(ctx, ledgerbackend.BoundedRange(5, 10))
	require.NoError(t, err)
	assert.True(t, ok)

	// Larger range should not be contained.
	ok, err = backend.IsPrepared(ctx, ledgerbackend.BoundedRange(0, 25))
	require.NoError(t, err)
	assert.False(t, ok)

	require.NoError(t, backend.Close())
}

// TestDatastoreBackend_UnboundedPrepareNoDeadlock guards the seeding path for
// unbounded ranges. The seed loop enqueues bufferSize+1 tasks; a bounded range stops
// early at its end boundary, but an unbounded range has no boundary, so the workers must
// already be draining the task queue or the final push blocks forever on a full channel.
// PrepareRange must therefore return promptly. The migration's UnboundedRange is the only
// unbounded consumer, so this case is not covered by the bounded tests above.
func TestDatastoreBackend_UnboundedPrepareNoDeadlock(t *testing.T) {
	ds := &mockDataStore{}
	schema := testSchema(1)
	// Once draining, a worker tries to download the tip file, which does not exist yet, so it
	// sleeps and retries. Optional: before the fix the workers never start, so GetFile is never called.
	ds.On("GetFile", mock.Anything, mock.Anything).Return(nil, os.ErrNotExist).Maybe()

	backend, err := newDatastoreBackend(ledgerbackend.BufferedStorageBackendConfig{
		BufferSize: 4,
		NumWorkers: 1,
		RetryWait:  10 * time.Millisecond,
	}, ds, schema)
	require.NoError(t, err)

	done := make(chan error, 1)
	go func() {
		done <- backend.PrepareRange(context.Background(), ledgerbackend.UnboundedRange(100))
	}()

	select {
	case prepErr := <-done:
		require.NoError(t, prepErr)
	case <-time.After(3 * time.Second):
		t.Fatal("PrepareRange deadlocked on an unbounded range: seed loop blocked before workers started")
	}

	require.NoError(t, backend.Close())
}

// TestDatastoreBackend_UnboundedNotExistRetriesIndefinitely guards the live-tip wait:
// on an unbounded range the next ledger file is legitimately absent until the exporter publishes
// it, which can take longer than RetryLimit polls. The worker must keep awaiting that file rather
// than giving up after RetryLimit — otherwise it falls out without delivering or re-enqueuing, and
// the drainer (plus every later GetLedger) stalls forever. Here the file is missing for far more
// polls than RetryLimit, then appears; GetLedger must still return it. RetryLimit bounds only
// transient (non-NotExist) errors, exercised by TestDatastoreBackend_WorkerRetry.
func TestDatastoreBackend_UnboundedNotExistRetriesIndefinitely(t *testing.T) {
	ds := &mockDataStore{}
	schema := testSchema(1) // 1 ledger per file

	backend, err := newDatastoreBackend(ledgerbackend.BufferedStorageBackendConfig{
		BufferSize: 2,
		NumWorkers: 1,
		RetryLimit: 1, // far fewer than the NotExist polls below; must not bound them
		RetryWait:  5 * time.Millisecond,
	}, ds, schema)
	require.NoError(t, err)

	objectKey100 := schema.GetObjectKeyFromSequenceNumber(100)
	// Absent for 5 polls (>> RetryLimit+1), then published.
	ds.On("GetFile", mock.Anything, objectKey100).Return(nil, os.ErrNotExist).Times(5)
	ds.On("GetFile", mock.Anything, objectKey100).Return(encodeBatch(t, 100, 100), nil)
	// Prefetched later files (101, 102, …) never arrive — the consumer only needs 100.
	ds.On("GetFile", mock.Anything, mock.Anything).Return(nil, os.ErrNotExist).Maybe()

	ctx := context.Background()
	require.NoError(t, backend.PrepareRange(ctx, ledgerbackend.UnboundedRange(100)))

	type result struct {
		lcm xdr.LedgerCloseMeta
		err error
	}
	done := make(chan result, 1)
	go func() {
		lcm, getErr := backend.GetLedger(ctx, 100)
		done <- result{lcm, getErr}
	}()

	select {
	case r := <-done:
		require.NoError(t, r.err)
		assert.Equal(t, xdr.Uint32(100), r.lcm.V0.LedgerHeader.Header.LedgerSeq)
	case <-time.After(3 * time.Second):
		t.Fatal("GetLedger stalled: worker gave up on a not-yet-published file instead of awaiting it")
	}

	require.NoError(t, backend.Close())
}

// delayedReadCloser sleeps once before yielding its bytes, simulating a slow download.
type delayedReadCloser struct {
	delay time.Duration
	r     io.Reader
	slept bool
}

func (d *delayedReadCloser) Read(p []byte) (int, error) {
	if !d.slept {
		time.Sleep(d.delay)
		d.slept = true
	}
	return d.r.Read(p)
}
func (d *delayedReadCloser) Close() error { return nil }

// TestDatastoreBackend_OutOfOrderDownloadsDeliverInSequence stresses delivery ordering:
// many workers download files whose completion order is deliberately reversed (later files
// finish first). A single drainer must still hand GetLedger a strictly increasing sequence.
// Run with -race -count to flush scheduler-dependent reordering.
func TestDatastoreBackend_OutOfOrderDownloadsDeliverInSequence(t *testing.T) {
	const n = 200
	ds := &mockDataStore{}
	schema := testSchema(1) // 1 ledger per file → consecutive file-starts 0..n-1

	for seq := uint32(0); seq < n; seq++ {
		s := seq
		var buf bytes.Buffer
		batch := buildBatch(t, s, s)
		enc := compressxdr.NewXDREncoder(compressxdr.DefaultCompressor, &batch)
		_, err := enc.WriteTo(&buf)
		require.NoError(t, err)
		raw := buf.Bytes()
		// Later sequences finish sooner → out-of-order completion vs the required delivery order.
		delay := time.Duration(n-s) * 50 * time.Microsecond
		ds.On("GetFile", mock.Anything, schema.GetObjectKeyFromSequenceNumber(s)).
			Return(&delayedReadCloser{delay: delay, r: bytes.NewReader(raw)}, nil)
	}

	backend, err := newDatastoreBackend(ledgerbackend.BufferedStorageBackendConfig{
		BufferSize: 64,
		NumWorkers: 16,
	}, ds, schema)
	require.NoError(t, err)

	ctx := context.Background()
	require.NoError(t, backend.PrepareRange(ctx, ledgerbackend.BoundedRange(0, n-1)))

	for seq := uint32(0); seq < n; seq++ {
		lcm, err := backend.GetLedger(ctx, seq)
		require.NoError(t, err, "GetLedger(%d)", seq)
		require.Equal(t, xdr.Uint32(seq), lcm.V0.LedgerHeader.Header.LedgerSeq,
			"out-of-order delivery at seq %d", seq)
	}
	require.NoError(t, backend.Close())
}

// TestDatastoreBackend_TipReportsFrontier verifies that for an unbounded range the tip is
// the growing delivery frontier (last available ledger), NOT a constant 0. This is what lets the
// migration's flushWindowsAtTip coalesce in bulk instead of flushing every ledger.
func TestDatastoreBackend_TipReportsFrontier(t *testing.T) {
	ds := &mockDataStore{}
	schema := testSchema(1)

	// Ledgers 100..104 exist; 105 is the (not-yet-closed) tip → NotExist, unbounded waits.
	for seq := uint32(100); seq <= 104; seq++ {
		ds.On("GetFile", mock.Anything, schema.GetObjectKeyFromSequenceNumber(seq)).
			Return(encodeBatch(t, seq, seq), nil)
	}
	ds.On("GetFile", mock.Anything, mock.Anything).Return(nil, os.ErrNotExist).Maybe()

	backend, err := newDatastoreBackend(ledgerbackend.BufferedStorageBackendConfig{
		BufferSize: 8,
		NumWorkers: 4,
		RetryWait:  5 * time.Millisecond,
	}, ds, schema)
	require.NoError(t, err)

	ctx := context.Background()
	require.NoError(t, backend.PrepareRange(ctx, ledgerbackend.UnboundedRange(100)))

	// Poll until the frontier reaches the last available ledger (104). The key assertion is that
	// it is non-zero and tracks delivered ledgers — the old code returned prepared.To() == 0.
	var tip uint32
	require.Eventually(t, func() bool {
		tip, err = backend.GetLatestLedgerSequence(ctx)
		require.NoError(t, err)
		return tip == 104
	}, 2*time.Second, 10*time.Millisecond, "frontier should advance to last available ledger; got %d", tip)

	require.NoError(t, backend.Close())
}

// TestDatastoreBackend_TipZeroBeforeDelivery verifies the frontier is 0 until something is
// delivered (mirrors the SDK's "currentLedger == from → 0").
func TestDatastoreBackend_TipZeroBeforeDelivery(t *testing.T) {
	ds := &mockDataStore{}
	schema := testSchema(1)
	// No file ever exists → nothing is delivered.
	ds.On("GetFile", mock.Anything, mock.Anything).Return(nil, os.ErrNotExist).Maybe()

	backend, err := newDatastoreBackend(ledgerbackend.BufferedStorageBackendConfig{
		BufferSize: 4,
		NumWorkers: 1,
		RetryWait:  5 * time.Millisecond,
	}, ds, schema)
	require.NoError(t, err)

	ctx := context.Background()
	require.NoError(t, backend.PrepareRange(ctx, ledgerbackend.UnboundedRange(100)))

	tip, err := backend.GetLatestLedgerSequence(ctx)
	require.NoError(t, err)
	require.Equal(t, uint32(0), tip)

	require.NoError(t, backend.Close())
}
