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

// encodeBatch compresses a LedgerCloseMetaBatch into zstd+XDR bytes for test mocking.
func encodeBatch(t *testing.T, batch xdr.LedgerCloseMetaBatch) []byte {
	t.Helper()
	var buf bytes.Buffer
	encoder := compressxdr.NewXDREncoder(compressxdr.DefaultCompressor, batch)
	_, err := encoder.WriteTo(&buf)
	require.NoError(t, err)
	return buf.Bytes()
}

// makeBatch creates a LedgerCloseMetaBatch spanning [start, end] with dummy LedgerCloseMeta entries.
func makeBatch(t *testing.T, start, end uint32) xdr.LedgerCloseMetaBatch {
	t.Helper()
	batch := xdr.LedgerCloseMetaBatch{
		StartSequence: xdr.Uint32(start),
		EndSequence:   xdr.Uint32(end),
	}
	for seq := start; seq <= end; seq++ {
		lcm := xdr.LedgerCloseMeta{
			V: 0,
			V0: &xdr.LedgerCloseMetaV0{
				LedgerHeader: xdr.LedgerHeaderHistoryEntry{
					Header: xdr.LedgerHeader{
						LedgerSeq: xdr.Uint32(seq),
					},
				},
			},
		}
		require.NoError(t, batch.AddLedger(lcm))
	}
	return batch
}

// mockDataStore is a mock for datastore.DataStore.
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

func (m *mockDataStore) GetFileLastModified(ctx context.Context, filePath string) (time.Time, error) {
	args := m.Called(ctx, filePath)
	return args.Get(0).(time.Time), args.Error(1)
}

func (m *mockDataStore) PutFile(ctx context.Context, path string, payload io.WriterTo, metadata map[string]string) error {
	args := m.Called(ctx, path, payload, metadata)
	return args.Error(0)
}

func (m *mockDataStore) PutFileIfNotExists(ctx context.Context, path string, payload io.WriterTo, metadata map[string]string) (bool, error) {
	args := m.Called(ctx, path, payload, metadata)
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

// testSchema returns a DataStoreSchema suitable for testing with 10 ledgers per file.
func testSchema() datastore.DataStoreSchema {
	return datastore.DataStoreSchema{
		LedgersPerFile:    10,
		FilesPerPartition: 1,
	}
}

// setupMockDataStore configures the mock to return encoded batches for the given file-start sequences.
// Each file covers [fileStart, fileStart+ledgersPerFile-1].
func setupMockDataStore(t *testing.T, ds *mockDataStore, schema datastore.DataStoreSchema, fileStarts ...uint32) {
	t.Helper()
	for _, start := range fileStarts {
		end := start + schema.LedgersPerFile - 1
		batch := makeBatch(t, start, end)
		encoded := encodeBatch(t, batch)
		objectKey := schema.GetObjectKeyFromSequenceNumber(start)
		ds.On("GetFile", mock.Anything, objectKey).Return(
			io.NopCloser(bytes.NewReader(encoded)), nil,
		)
	}
}

func TestNewBackfillLedgerBackend_Defaults(t *testing.T) {
	ds := &mockDataStore{}
	schema := testSchema()

	backend, err := NewBackfillLedgerBackend(BackfillLedgerBackendConfig{}, ds, schema)
	require.NoError(t, err)
	require.NotNil(t, backend)

	assert.Equal(t, uint32(defaultBackfillBufferSize), backend.config.BufferSize)
	assert.Equal(t, uint32(defaultBackfillNumWorkers), backend.config.NumWorkers)
	assert.Equal(t, uint32(defaultBackfillRetryLimit), backend.config.RetryLimit)
	assert.Equal(t, defaultBackfillRetryWait, backend.config.RetryWait)
}

func TestNewBackfillLedgerBackend_WorkersClamped(t *testing.T) {
	ds := &mockDataStore{}
	schema := testSchema()

	backend, err := NewBackfillLedgerBackend(BackfillLedgerBackendConfig{
		BufferSize: 2,
		NumWorkers: 10, // Should be clamped to 2.
	}, ds, schema)
	require.NoError(t, err)
	assert.Equal(t, uint32(2), backend.config.NumWorkers)
}

func TestNewBackfillLedgerBackend_InvalidSchema(t *testing.T) {
	ds := &mockDataStore{}
	schema := datastore.DataStoreSchema{LedgersPerFile: 0}

	_, err := NewBackfillLedgerBackend(BackfillLedgerBackendConfig{}, ds, schema)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "ledgersPerFile must be > 0")
}

func TestBackfillLedgerBackend_GetLedger_NotPrepared(t *testing.T) {
	ds := &mockDataStore{}
	schema := testSchema()

	backend, err := NewBackfillLedgerBackend(BackfillLedgerBackendConfig{}, ds, schema)
	require.NoError(t, err)

	_, err = backend.GetLedger(context.Background(), 100)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not prepared")
}

func TestBackfillLedgerBackend_GetLedger_Sequential(t *testing.T) {
	ds := &mockDataStore{}
	schema := testSchema()

	// Range: ledgers 0-19 across 2 files (0-9, 10-19).
	setupMockDataStore(t, ds, schema, 0, 10)

	backend, err := NewBackfillLedgerBackend(BackfillLedgerBackendConfig{
		BufferSize: 3,
		NumWorkers: 2,
		RetryLimit: 1,
		RetryWait:  10 * time.Millisecond,
	}, ds, schema)
	require.NoError(t, err)

	err = backend.PrepareRange(context.Background(), ledgerbackend.BoundedRange(0, 19))
	require.NoError(t, err)

	// Fetch all 20 ledgers sequentially.
	for seq := uint32(0); seq < 20; seq++ {
		lcm, err := backend.GetLedger(context.Background(), seq)
		require.NoError(t, err, "failed at sequence %d", seq)
		assert.Equal(t, seq, lcm.LedgerSequence())
	}

	require.NoError(t, backend.Close())
	ds.AssertExpectations(t)
}

func TestBackfillLedgerBackend_GetLedger_CacheBoundary(t *testing.T) {
	ds := &mockDataStore{}
	schema := testSchema()

	setupMockDataStore(t, ds, schema, 0, 10)

	backend, err := NewBackfillLedgerBackend(BackfillLedgerBackendConfig{
		BufferSize: 3,
		NumWorkers: 2,
		RetryLimit: 1,
		RetryWait:  10 * time.Millisecond,
	}, ds, schema)
	require.NoError(t, err)

	err = backend.PrepareRange(context.Background(), ledgerbackend.BoundedRange(0, 19))
	require.NoError(t, err)

	// Fetch last ledger of first file.
	lcm, err := backend.GetLedger(context.Background(), 9)
	require.NoError(t, err)
	assert.Equal(t, uint32(9), lcm.LedgerSequence())

	// Fetch first ledger of second file — should trigger batch transition.
	lcm, err = backend.GetLedger(context.Background(), 10)
	require.NoError(t, err)
	assert.Equal(t, uint32(10), lcm.LedgerSequence())

	require.NoError(t, backend.Close())
	ds.AssertExpectations(t)
}

func TestBackfillLedgerBackend_PrepareRange_Twice(t *testing.T) {
	ds := &mockDataStore{}
	schema := testSchema()

	setupMockDataStore(t, ds, schema, 0, 10, 90, 100)

	backend, err := NewBackfillLedgerBackend(BackfillLedgerBackendConfig{
		BufferSize: 3,
		NumWorkers: 2,
		RetryLimit: 1,
		RetryWait:  10 * time.Millisecond,
	}, ds, schema)
	require.NoError(t, err)

	// First prepare.
	err = backend.PrepareRange(context.Background(), ledgerbackend.BoundedRange(0, 19))
	require.NoError(t, err)

	lcm, err := backend.GetLedger(context.Background(), 0)
	require.NoError(t, err)
	assert.Equal(t, uint32(0), lcm.LedgerSequence())

	// Re-prepare with a different range (simulates fetchBoundaryTimestamps).
	err = backend.PrepareRange(context.Background(), ledgerbackend.BoundedRange(90, 109))
	require.NoError(t, err)

	lcm, err = backend.GetLedger(context.Background(), 90)
	require.NoError(t, err)
	assert.Equal(t, uint32(90), lcm.LedgerSequence())

	require.NoError(t, backend.Close())
}

func TestBackfillLedgerBackend_PrepareRange_Idempotent(t *testing.T) {
	ds := &mockDataStore{}
	schema := testSchema()

	setupMockDataStore(t, ds, schema, 0, 10)

	backend, err := NewBackfillLedgerBackend(BackfillLedgerBackendConfig{
		BufferSize: 3,
		NumWorkers: 2,
		RetryLimit: 1,
		RetryWait:  10 * time.Millisecond,
	}, ds, schema)
	require.NoError(t, err)

	err = backend.PrepareRange(context.Background(), ledgerbackend.BoundedRange(0, 19))
	require.NoError(t, err)

	// Preparing a sub-range of the already-prepared range should be a no-op.
	err = backend.PrepareRange(context.Background(), ledgerbackend.BoundedRange(5, 15))
	require.NoError(t, err)

	// Should still be able to get ledger 0 (buffer wasn't reset).
	lcm, err := backend.GetLedger(context.Background(), 0)
	require.NoError(t, err)
	assert.Equal(t, uint32(0), lcm.LedgerSequence())

	require.NoError(t, backend.Close())
}

func TestBackfillLedgerBackend_Close(t *testing.T) {
	ds := &mockDataStore{}
	schema := testSchema()

	setupMockDataStore(t, ds, schema, 0)

	backend, err := NewBackfillLedgerBackend(BackfillLedgerBackendConfig{
		BufferSize: 2,
		NumWorkers: 1,
		RetryLimit: 1,
		RetryWait:  10 * time.Millisecond,
	}, ds, schema)
	require.NoError(t, err)

	err = backend.PrepareRange(context.Background(), ledgerbackend.BoundedRange(0, 9))
	require.NoError(t, err)

	require.NoError(t, backend.Close())

	// After close, all operations should fail.
	_, err = backend.GetLedger(context.Background(), 0)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "closed")

	_, err = backend.GetLatestLedgerSequence(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "closed")

	err = backend.PrepareRange(context.Background(), ledgerbackend.BoundedRange(0, 9))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "closed")
}

func TestBackfillLedgerBackend_WorkerRetry(t *testing.T) {
	ds := &mockDataStore{}
	schema := testSchema()

	batch := makeBatch(t, 0, 9)
	encoded := encodeBatch(t, batch)
	objectKey := schema.GetObjectKeyFromSequenceNumber(0)

	// First call fails, second succeeds.
	ds.On("GetFile", mock.Anything, objectKey).Return(nil, fmt.Errorf("transient S3 error")).Once()
	ds.On("GetFile", mock.Anything, objectKey).Return(
		io.NopCloser(bytes.NewReader(encoded)), nil,
	).Once()

	backend, err := NewBackfillLedgerBackend(BackfillLedgerBackendConfig{
		BufferSize: 2,
		NumWorkers: 1,
		RetryLimit: 3,
		RetryWait:  10 * time.Millisecond,
	}, ds, schema)
	require.NoError(t, err)

	err = backend.PrepareRange(context.Background(), ledgerbackend.BoundedRange(0, 9))
	require.NoError(t, err)

	lcm, err := backend.GetLedger(context.Background(), 0)
	require.NoError(t, err)
	assert.Equal(t, uint32(0), lcm.LedgerSequence())

	require.NoError(t, backend.Close())
	ds.AssertExpectations(t)
}

func TestBackfillLedgerBackend_WorkerRetry_MaxExceeded(t *testing.T) {
	ds := &mockDataStore{}
	schema := testSchema()

	objectKey := schema.GetObjectKeyFromSequenceNumber(0)

	// All calls fail.
	ds.On("GetFile", mock.Anything, objectKey).Return(nil, fmt.Errorf("persistent S3 error"))

	backend, err := NewBackfillLedgerBackend(BackfillLedgerBackendConfig{
		BufferSize: 2,
		NumWorkers: 1,
		RetryLimit: 2,
		RetryWait:  10 * time.Millisecond,
	}, ds, schema)
	require.NoError(t, err)

	err = backend.PrepareRange(context.Background(), ledgerbackend.BoundedRange(0, 9))
	require.NoError(t, err)

	_, err = backend.GetLedger(context.Background(), 0)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "maximum retries exceeded")

	require.NoError(t, backend.Close())
}

func TestBackfillLedgerBackend_IsPrepared(t *testing.T) {
	ds := &mockDataStore{}
	schema := testSchema()

	setupMockDataStore(t, ds, schema, 0, 10)

	backend, err := NewBackfillLedgerBackend(BackfillLedgerBackendConfig{
		BufferSize: 3,
		NumWorkers: 2,
		RetryLimit: 1,
		RetryWait:  10 * time.Millisecond,
	}, ds, schema)
	require.NoError(t, err)

	// Not prepared yet.
	prepared, err := backend.IsPrepared(context.Background(), ledgerbackend.BoundedRange(0, 19))
	require.NoError(t, err)
	assert.False(t, prepared)

	err = backend.PrepareRange(context.Background(), ledgerbackend.BoundedRange(0, 19))
	require.NoError(t, err)

	// Should be prepared for sub-range.
	prepared, err = backend.IsPrepared(context.Background(), ledgerbackend.BoundedRange(5, 15))
	require.NoError(t, err)
	assert.True(t, prepared)

	// Should NOT be prepared for wider range.
	prepared, err = backend.IsPrepared(context.Background(), ledgerbackend.BoundedRange(0, 30))
	require.NoError(t, err)
	assert.False(t, prepared)

	require.NoError(t, backend.Close())
}

func TestBackfillLedgerBackend_MissingObject_BoundedRange(t *testing.T) {
	ds := &mockDataStore{}
	schema := testSchema()

	objectKey := schema.GetObjectKeyFromSequenceNumber(0)
	ds.On("GetFile", mock.Anything, objectKey).Return(nil, os.ErrNotExist)

	backend, err := NewBackfillLedgerBackend(BackfillLedgerBackendConfig{
		BufferSize: 2,
		NumWorkers: 1,
		RetryLimit: 1,
		RetryWait:  10 * time.Millisecond,
	}, ds, schema)
	require.NoError(t, err)

	err = backend.PrepareRange(context.Background(), ledgerbackend.BoundedRange(0, 9))
	require.NoError(t, err)

	// GetLedger should propagate the missing object error.
	_, err = backend.GetLedger(context.Background(), 0)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "missing")

	require.NoError(t, backend.Close())
}
