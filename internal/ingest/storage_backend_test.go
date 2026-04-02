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

func (m *mockDataStore) GetFile(ctx context.Context, path string) (io.ReadCloser, error) {
	args := m.Called(ctx, path)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(io.ReadCloser), args.Error(1)
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

func TestOptimizedStorageBackend_Defaults(t *testing.T) {
	ds := &mockDataStore{}
	schema := testSchema(10)

	backend, err := newOptimizedStorageBackend(ledgerbackend.BufferedStorageBackendConfig{}, ds, schema)
	require.NoError(t, err)

	assert.Equal(t, uint32(100), backend.config.BufferSize, "default BufferSize should be 100")
	assert.Equal(t, uint32(10), backend.config.NumWorkers, "default NumWorkers should be 10")
}

func TestOptimizedStorageBackend_Validation(t *testing.T) {
	ds := &mockDataStore{}

	t.Run("workers > buffer errors", func(t *testing.T) {
		_, err := newOptimizedStorageBackend(ledgerbackend.BufferedStorageBackendConfig{
			BufferSize: 3,
			NumWorkers: 5,
		}, ds, testSchema(10))
		require.Error(t, err)
		assert.Contains(t, err.Error(), "NumWorkers (5) must be <= BufferSize (3)")
	})

	t.Run("ledgersPerFile=0 errors", func(t *testing.T) {
		_, err := newOptimizedStorageBackend(ledgerbackend.BufferedStorageBackendConfig{}, ds, testSchema(0))
		require.Error(t, err)
		assert.Contains(t, err.Error(), "LedgersPerFile must be > 0")
	})
}

func TestOptimizedStorageBackend_SequentialGetLedger(t *testing.T) {
	ds := &mockDataStore{}
	schema := testSchema(10)

	backend, err := newOptimizedStorageBackend(ledgerbackend.BufferedStorageBackendConfig{
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

func TestOptimizedStorageBackend_CachedBatchHit(t *testing.T) {
	ds := &mockDataStore{}
	schema := testSchema(5)

	backend, err := newOptimizedStorageBackend(ledgerbackend.BufferedStorageBackendConfig{
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

func TestOptimizedStorageBackend_CrossFileBoundary(t *testing.T) {
	ds := &mockDataStore{}
	schema := testSchema(5)

	backend, err := newOptimizedStorageBackend(ledgerbackend.BufferedStorageBackendConfig{
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

func TestOptimizedStorageBackend_NotPrepared(t *testing.T) {
	ds := &mockDataStore{}
	backend, err := newOptimizedStorageBackend(ledgerbackend.BufferedStorageBackendConfig{}, ds, testSchema(10))
	require.NoError(t, err)

	ctx := context.Background()

	_, err = backend.GetLedger(ctx, 0)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "must be prepared")

	_, err = backend.GetLatestLedgerSequence(ctx)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "must be prepared")
}

func TestOptimizedStorageBackend_OutOfOrder(t *testing.T) {
	ds := &mockDataStore{}
	schema := testSchema(10)

	backend, err := newOptimizedStorageBackend(ledgerbackend.BufferedStorageBackendConfig{
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

func TestOptimizedStorageBackend_OutsideRange(t *testing.T) {
	ds := &mockDataStore{}
	schema := testSchema(10)

	backend, err := newOptimizedStorageBackend(ledgerbackend.BufferedStorageBackendConfig{
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

func TestOptimizedStorageBackend_RePrepare(t *testing.T) {
	ds := &mockDataStore{}
	schema := testSchema(10)

	backend, err := newOptimizedStorageBackend(ledgerbackend.BufferedStorageBackendConfig{
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

func TestOptimizedStorageBackend_Close(t *testing.T) {
	ds := &mockDataStore{}
	schema := testSchema(10)

	backend, err := newOptimizedStorageBackend(ledgerbackend.BufferedStorageBackendConfig{
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

func TestOptimizedStorageBackend_WorkerRetry(t *testing.T) {
	ds := &mockDataStore{}
	schema := testSchema(10)

	backend, err := newOptimizedStorageBackend(ledgerbackend.BufferedStorageBackendConfig{
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

func TestOptimizedStorageBackend_WorkerRetryNotExist(t *testing.T) {
	ds := &mockDataStore{}
	schema := testSchema(10)

	backend, err := newOptimizedStorageBackend(ledgerbackend.BufferedStorageBackendConfig{
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

	require.NoError(t, backend.Close())
}

func TestOptimizedStorageBackend_IsPrepared(t *testing.T) {
	ds := &mockDataStore{}
	schema := testSchema(10)

	backend, err := newOptimizedStorageBackend(ledgerbackend.BufferedStorageBackendConfig{
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
