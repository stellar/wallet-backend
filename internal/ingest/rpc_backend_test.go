package ingest

import (
	"context"
	"testing"

	"github.com/stellar/go-stellar-sdk/ingest/ledgerbackend"
	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"
	"github.com/stellar/go-stellar-sdk/xdr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// mockRPCClient implements rpcClient for testing.
type mockRPCClient struct {
	mock.Mock
}

func (m *mockRPCClient) GetLedgers(ctx context.Context, req protocol.GetLedgersRequest) (protocol.GetLedgersResponse, error) {
	args := m.Called(ctx, req)
	return args.Get(0).(protocol.GetLedgersResponse), args.Error(1)
}

func (m *mockRPCClient) GetHealth(ctx context.Context) (protocol.GetHealthResponse, error) {
	args := m.Called(ctx)
	return args.Get(0).(protocol.GetHealthResponse), args.Error(1)
}

// buildLedgerInfo creates a LedgerInfo with the given sequence and a valid LedgerCloseMeta XDR.
func buildLedgerInfo(t *testing.T, seq uint32) protocol.LedgerInfo {
	t.Helper()
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
	b64, err := xdr.MarshalBase64(lcm)
	require.NoError(t, err)
	return protocol.LedgerInfo{
		Sequence:       seq,
		LedgerMetadata: b64,
	}
}

func TestOptimizedRPCBackend_GetLedger_BufferHit(t *testing.T) {
	client := &mockRPCClient{}
	backend := newOptimizedRPCBackend(client, 5)

	// PrepareRange fetches starting ledger and populates buffer.
	client.On("GetLedgers", mock.Anything, protocol.GetLedgersRequest{
		StartLedger: uint32(100),
		Pagination:  &protocol.LedgerPaginationOptions{Limit: 5},
	}).Return(protocol.GetLedgersResponse{
		Ledgers:      []protocol.LedgerInfo{buildLedgerInfo(t, 100), buildLedgerInfo(t, 101), buildLedgerInfo(t, 102)},
		LatestLedger: 200,
	}, nil).Once()

	ctx := context.Background()
	require.NoError(t, backend.PrepareRange(ctx, ledgerbackend.UnboundedRange(100)))

	// First GetLedger should be a buffer hit — no additional RPC call.
	lcm, err := backend.GetLedger(ctx, 100)
	require.NoError(t, err)
	assert.Equal(t, xdr.Uint32(100), lcm.V0.LedgerHeader.Header.LedgerSeq)

	// Second and third should also be buffer hits.
	lcm, err = backend.GetLedger(ctx, 101)
	require.NoError(t, err)
	assert.Equal(t, xdr.Uint32(101), lcm.V0.LedgerHeader.Header.LedgerSeq)

	lcm, err = backend.GetLedger(ctx, 102)
	require.NoError(t, err)
	assert.Equal(t, xdr.Uint32(102), lcm.V0.LedgerHeader.Header.LedgerSeq)

	// Only one GetLedgers call was made (during PrepareRange). No GetHealth calls at all.
	client.AssertNumberOfCalls(t, "GetLedgers", 1)
	client.AssertNotCalled(t, "GetHealth")
}

func TestOptimizedRPCBackend_GetLedger_BufferMiss(t *testing.T) {
	client := &mockRPCClient{}
	backend := newOptimizedRPCBackend(client, 3)

	// PrepareRange returns ledgers 100-102.
	client.On("GetLedgers", mock.Anything, protocol.GetLedgersRequest{
		StartLedger: uint32(100),
		Pagination:  &protocol.LedgerPaginationOptions{Limit: 3},
	}).Return(protocol.GetLedgersResponse{
		Ledgers:      []protocol.LedgerInfo{buildLedgerInfo(t, 100), buildLedgerInfo(t, 101), buildLedgerInfo(t, 102)},
		LatestLedger: 200,
	}, nil).Once()

	// Fetch for 103 returns new batch.
	client.On("GetLedgers", mock.Anything, protocol.GetLedgersRequest{
		StartLedger: uint32(103),
		Pagination:  &protocol.LedgerPaginationOptions{Limit: 3},
	}).Return(protocol.GetLedgersResponse{
		Ledgers:      []protocol.LedgerInfo{buildLedgerInfo(t, 103), buildLedgerInfo(t, 104)},
		LatestLedger: 200,
	}, nil).Once()

	ctx := context.Background()
	require.NoError(t, backend.PrepareRange(ctx, ledgerbackend.UnboundedRange(100)))

	// Consume 100-102 from buffer.
	for seq := uint32(100); seq <= 102; seq++ {
		_, err := backend.GetLedger(ctx, seq)
		require.NoError(t, err)
	}

	// Ledger 103 triggers a new RPC fetch.
	lcm, err := backend.GetLedger(ctx, 103)
	require.NoError(t, err)
	assert.Equal(t, xdr.Uint32(103), lcm.V0.LedgerHeader.Header.LedgerSeq)

	client.AssertNumberOfCalls(t, "GetLedgers", 2)
	client.AssertNotCalled(t, "GetHealth")
}

func TestOptimizedRPCBackend_GetLedger_BeyondLatestRetry(t *testing.T) {
	client := &mockRPCClient{}
	backend := newOptimizedRPCBackend(client, 5)

	// PrepareRange: ledger 100 is beyond latest (empty response, latestLedger=99).
	client.On("GetLedgers", mock.Anything, protocol.GetLedgersRequest{
		StartLedger: uint32(100),
		Pagination:  &protocol.LedgerPaginationOptions{Limit: 5},
	}).Return(protocol.GetLedgersResponse{
		Ledgers:      []protocol.LedgerInfo{},
		LatestLedger: 99,
	}, nil).Once()

	ctx := context.Background()
	require.NoError(t, backend.PrepareRange(ctx, ledgerbackend.UnboundedRange(100)))

	// GetLedger(100): first attempt returns empty (still at 99), second attempt has it.
	client.On("GetLedgers", mock.Anything, protocol.GetLedgersRequest{
		StartLedger: uint32(100),
		Pagination:  &protocol.LedgerPaginationOptions{Limit: 5},
	}).Return(protocol.GetLedgersResponse{
		Ledgers:      []protocol.LedgerInfo{},
		LatestLedger: 99,
	}, nil).Once()

	client.On("GetLedgers", mock.Anything, protocol.GetLedgersRequest{
		StartLedger: uint32(100),
		Pagination:  &protocol.LedgerPaginationOptions{Limit: 5},
	}).Return(protocol.GetLedgersResponse{
		Ledgers:      []protocol.LedgerInfo{buildLedgerInfo(t, 100)},
		LatestLedger: 100,
	}, nil).Once()

	lcm, err := backend.GetLedger(ctx, 100)
	require.NoError(t, err)
	assert.Equal(t, xdr.Uint32(100), lcm.V0.LedgerHeader.Header.LedgerSeq)

	// 3 total GetLedgers calls: 1 from PrepareRange + 2 from GetLedger retries.
	client.AssertNumberOfCalls(t, "GetLedgers", 3)
}

func TestOptimizedRPCBackend_GetLedger_ContextCancellation(t *testing.T) {
	client := &mockRPCClient{}
	backend := newOptimizedRPCBackend(client, 5)

	// PrepareRange: beyond latest.
	client.On("GetLedgers", mock.Anything, mock.Anything).Return(protocol.GetLedgersResponse{
		Ledgers:      []protocol.LedgerInfo{},
		LatestLedger: 99,
	}, nil)

	ctx := context.Background()
	require.NoError(t, backend.PrepareRange(ctx, ledgerbackend.UnboundedRange(100)))

	// GetLedger with cancelled context should return immediately.
	cancelCtx, cancel := context.WithCancel(ctx)
	cancel()

	_, err := backend.GetLedger(cancelCtx, 100)
	assert.ErrorIs(t, err, context.Canceled)
}

func TestOptimizedRPCBackend_GetLatestLedgerSequence(t *testing.T) {
	client := &mockRPCClient{}
	backend := newOptimizedRPCBackend(client, 5)

	client.On("GetLedgers", mock.Anything, mock.Anything).Return(protocol.GetLedgersResponse{
		Ledgers:      []protocol.LedgerInfo{buildLedgerInfo(t, 100)},
		LatestLedger: 500,
	}, nil).Once()

	ctx := context.Background()
	require.NoError(t, backend.PrepareRange(ctx, ledgerbackend.UnboundedRange(100)))

	// latestLedger should be updated from the GetLedgers response.
	latest, err := backend.GetLatestLedgerSequence(ctx)
	require.NoError(t, err)
	assert.Equal(t, uint32(500), latest)
}

func TestOptimizedRPCBackend_GetLatestLedgerSequence_NotPrepared(t *testing.T) {
	client := &mockRPCClient{}
	backend := newOptimizedRPCBackend(client, 5)

	_, err := backend.GetLatestLedgerSequence(context.Background())
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "must be prepared")
}

func TestOptimizedRPCBackend_BufferEviction(t *testing.T) {
	client := &mockRPCClient{}
	backend := newOptimizedRPCBackend(client, 5)

	client.On("GetLedgers", mock.Anything, protocol.GetLedgersRequest{
		StartLedger: uint32(100),
		Pagination:  &protocol.LedgerPaginationOptions{Limit: 5},
	}).Return(protocol.GetLedgersResponse{
		Ledgers:      []protocol.LedgerInfo{buildLedgerInfo(t, 100), buildLedgerInfo(t, 101), buildLedgerInfo(t, 102)},
		LatestLedger: 200,
	}, nil).Once()

	ctx := context.Background()
	require.NoError(t, backend.PrepareRange(ctx, ledgerbackend.UnboundedRange(100)))

	// Consume ledger 100 — this should evict it from buffer.
	_, err := backend.GetLedger(ctx, 100)
	require.NoError(t, err)

	// Verify buffer state: 100 should be evicted, 101 and 102 should remain.
	_, exists100 := backend.buffer[100]
	_, exists101 := backend.buffer[101]
	_, exists102 := backend.buffer[102]
	assert.False(t, exists100, "ledger 100 should be evicted after consumption")
	assert.True(t, exists101, "ledger 101 should still be in buffer")
	assert.True(t, exists102, "ledger 102 should still be in buffer")
}

func TestOptimizedRPCBackend_BoundedRange(t *testing.T) {
	client := &mockRPCClient{}
	backend := newOptimizedRPCBackend(client, 5)

	client.On("GetLedgers", mock.Anything, mock.Anything).Return(protocol.GetLedgersResponse{
		Ledgers:      []protocol.LedgerInfo{buildLedgerInfo(t, 100), buildLedgerInfo(t, 101)},
		LatestLedger: 200,
	}, nil).Once()

	ctx := context.Background()
	require.NoError(t, backend.PrepareRange(ctx, ledgerbackend.BoundedRange(100, 101)))

	// Consume 100 and 101.
	_, err := backend.GetLedger(ctx, 100)
	require.NoError(t, err)
	_, err = backend.GetLedger(ctx, 101)
	require.NoError(t, err)

	// Requesting 102 should fail — outside bounded range.
	_, err = backend.GetLedger(ctx, 102)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "outside prepared range")
}

func TestOptimizedRPCBackend_SequentialEnforcement(t *testing.T) {
	client := &mockRPCClient{}
	backend := newOptimizedRPCBackend(client, 5)

	client.On("GetLedgers", mock.Anything, mock.Anything).Return(protocol.GetLedgersResponse{
		Ledgers:      []protocol.LedgerInfo{buildLedgerInfo(t, 100), buildLedgerInfo(t, 101)},
		LatestLedger: 200,
	}, nil).Once()

	ctx := context.Background()
	require.NoError(t, backend.PrepareRange(ctx, ledgerbackend.UnboundedRange(100)))

	// Requesting 101 before 100 should fail.
	_, err := backend.GetLedger(ctx, 101)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not the expected ledger")
}

func TestOptimizedRPCBackend_Close(t *testing.T) {
	client := &mockRPCClient{}
	backend := newOptimizedRPCBackend(client, 5)

	client.On("GetLedgers", mock.Anything, mock.Anything).Return(protocol.GetLedgersResponse{
		Ledgers:      []protocol.LedgerInfo{buildLedgerInfo(t, 100)},
		LatestLedger: 200,
	}, nil).Once()

	ctx := context.Background()
	require.NoError(t, backend.PrepareRange(ctx, ledgerbackend.UnboundedRange(100)))

	require.NoError(t, backend.Close())

	// All methods should error after close.
	_, err := backend.GetLedger(ctx, 100)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "closed")

	_, err = backend.GetLatestLedgerSequence(ctx)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "closed")

	err = backend.PrepareRange(ctx, ledgerbackend.UnboundedRange(100))
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "closed")
}

func TestOptimizedRPCBackend_DefaultBufferSize(t *testing.T) {
	client := &mockRPCClient{}
	backend := newOptimizedRPCBackend(client, 0)
	assert.Equal(t, defaultRPCBufferSize, backend.bufferSize)
}

func TestOptimizedRPCBackend_IsPrepared(t *testing.T) {
	client := &mockRPCClient{}
	backend := newOptimizedRPCBackend(client, 5)

	ctx := context.Background()

	// Not prepared initially.
	ok, err := backend.IsPrepared(ctx, ledgerbackend.UnboundedRange(100))
	require.NoError(t, err)
	assert.False(t, ok)

	client.On("GetLedgers", mock.Anything, mock.Anything).Return(protocol.GetLedgersResponse{
		Ledgers:      []protocol.LedgerInfo{buildLedgerInfo(t, 100)},
		LatestLedger: 200,
	}, nil).Once()

	require.NoError(t, backend.PrepareRange(ctx, ledgerbackend.UnboundedRange(100)))

	// Unbounded range from 100 contains bounded range [100, 150].
	ok, err = backend.IsPrepared(ctx, ledgerbackend.BoundedRange(100, 150))
	require.NoError(t, err)
	assert.True(t, ok)

	// Unbounded range from 100 does not contain unbounded range from 50.
	ok, err = backend.IsPrepared(ctx, ledgerbackend.UnboundedRange(50))
	require.NoError(t, err)
	assert.False(t, ok)
}

func TestOptimizedRPCBackend_PrepareRange_AlreadyPrepared(t *testing.T) {
	client := &mockRPCClient{}
	backend := newOptimizedRPCBackend(client, 5)

	client.On("GetLedgers", mock.Anything, mock.Anything).Return(protocol.GetLedgersResponse{
		Ledgers:      []protocol.LedgerInfo{buildLedgerInfo(t, 100)},
		LatestLedger: 200,
	}, nil).Once()

	ctx := context.Background()
	require.NoError(t, backend.PrepareRange(ctx, ledgerbackend.UnboundedRange(100)))

	// Second prepare should fail.
	err := backend.PrepareRange(ctx, ledgerbackend.UnboundedRange(200))
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "already prepared")
}

func TestOptimizedRPCBackend_GetLedger_RPCLedgerMissing(t *testing.T) {
	client := &mockRPCClient{}
	backend := newOptimizedRPCBackend(client, 5)

	// PrepareRange returns ledger 100.
	client.On("GetLedgers", mock.Anything, protocol.GetLedgersRequest{
		StartLedger: uint32(100),
		Pagination:  &protocol.LedgerPaginationOptions{Limit: 5},
	}).Return(protocol.GetLedgersResponse{
		Ledgers:      []protocol.LedgerInfo{buildLedgerInfo(t, 100)},
		LatestLedger: 200,
	}, nil).Once()

	ctx := context.Background()
	require.NoError(t, backend.PrepareRange(ctx, ledgerbackend.UnboundedRange(100)))

	// Consume 100.
	_, err := backend.GetLedger(ctx, 100)
	require.NoError(t, err)

	// Fetch for 101 returns empty but latestLedger=200 (not beyond latest — it's just missing).
	client.On("GetLedgers", mock.Anything, protocol.GetLedgersRequest{
		StartLedger: uint32(101),
		Pagination:  &protocol.LedgerPaginationOptions{Limit: 5},
	}).Return(protocol.GetLedgersResponse{
		Ledgers:      []protocol.LedgerInfo{},
		LatestLedger: 200,
	}, nil).Once()

	_, err = backend.GetLedger(ctx, 101)
	assert.Error(t, err)
	var missingErr *ledgerbackend.RPCLedgerMissingError
	assert.ErrorAs(t, err, &missingErr)
	assert.Equal(t, uint32(101), missingErr.Sequence)
}
