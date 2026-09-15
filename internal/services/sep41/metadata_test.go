package sep41

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/alitto/pond/v2"
	"github.com/stellar/go-stellar-sdk/xdr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/stellar/wallet-backend/internal/data"
	"github.com/stellar/wallet-backend/internal/services"
)

const (
	testContractA = "CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG2FB2RMQQVU2HHGCYSC"
	testContractB = "CAS3J7GYLGXMF6TDJBBYYSE3HQ6BBSMLNUQ34T6TZMYMW2EVH34XOWMA"
)

// expectMetadataFetch wires up the three FetchSingleField calls (name, symbol,
// decimals) that fetchOne makes for a single contract. It returns the mock so
// callers can chain additional expectations.
func expectMetadataFetch(m *services.ContractMetadataServiceMock, contractID, name, symbol string, decimals uint32) {
	m.On("FetchSingleField", mock.Anything, contractID, "name", mock.Anything).Return(strScVal(name), nil).Once()
	m.On("FetchSingleField", mock.Anything, contractID, "symbol", mock.Anything).Return(strScVal(symbol), nil).Once()
	m.On("FetchSingleField", mock.Anything, contractID, "decimals", mock.Anything).Return(u32ScVal(decimals), nil).Once()
}

func TestValidateTokenString(t *testing.T) {
	t.Run("accepts a short ASCII value well under the cap", func(t *testing.T) {
		require.NoError(t, validateTokenString("name", "USDC", maxTokenNameLength))
	})

	t.Run("accepts an empty value", func(t *testing.T) {
		require.NoError(t, validateTokenString("symbol", "", maxTokenSymbolLength))
	})

	t.Run("accepts a value exactly at the byte cap", func(t *testing.T) {
		require.NoError(t, validateTokenString("name", strings.Repeat("a", maxTokenNameLength), maxTokenNameLength))
	})

	t.Run("rejects a value that exceeds the byte cap by one", func(t *testing.T) {
		err := validateTokenString("name", strings.Repeat("a", maxTokenNameLength+1), maxTokenNameLength)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "exceeds")
	})

	t.Run("rejects invalid UTF-8", func(t *testing.T) {
		err := validateTokenString("name", "\xff\xfe\xfd", maxTokenNameLength)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "UTF-8")
	})

	t.Run("rejects a value containing a NUL byte", func(t *testing.T) {
		err := validateTokenString("name", "USD\x00C", maxTokenNameLength)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "NUL")
	})
}

func TestNewMetadataFetcher(t *testing.T) {
	t.Run("returns nil when the rpc service is nil", func(t *testing.T) {
		assert.Nil(t, newMetadataFetcher(nil, pond.NewPool(0)))
	})

	t.Run("returns nil when the worker pool is nil", func(t *testing.T) {
		rpc := services.NewContractMetadataServiceMock(t)
		assert.Nil(t, newMetadataFetcher(rpc, nil))
	})

	t.Run("returns a fetcher when both dependencies are provided", func(t *testing.T) {
		rpc := services.NewContractMetadataServiceMock(t)
		got := newMetadataFetcher(rpc, pond.NewPool(0))
		require.NotNil(t, got)
		assert.Same(t, rpc, got.rpc)
	})
}

func TestMetadataFetcher_FetchMetadata(t *testing.T) {
	ctx := context.Background()

	t.Run("returns an empty map when the fetcher is nil", func(t *testing.T) {
		var f *metadataFetcher
		out, err := f.FetchMetadata(ctx, []string{testContractA})
		require.NoError(t, err)
		assert.Empty(t, out)
	})

	t.Run("returns an empty map when no contracts are provided", func(t *testing.T) {
		rpc := services.NewContractMetadataServiceMock(t)
		f := newMetadataFetcher(rpc, pond.NewPool(0))
		out, err := f.FetchMetadata(ctx, nil)
		require.NoError(t, err)
		assert.Empty(t, out)
	})

	t.Run("skips contracts marked as fetched, even after a recorded failure", func(t *testing.T) {
		rpc := services.NewContractMetadataServiceMock(t)
		f := newMetadataFetcher(rpc, pond.NewPool(2))

		// A contract in its back-off window whose metadata then shows up in the
		// database must not reach RPC. The mock has no expectations, so it would.
		f.recordFailure(testContractA)
		f.markFetched([]string{testContractA})

		out, err := f.FetchMetadata(ctx, []string{testContractA})
		require.NoError(t, err)
		assert.Empty(t, out)
	})

	t.Run("returns a populated Contract for a successful single-contract fetch", func(t *testing.T) {
		rpc := services.NewContractMetadataServiceMock(t)
		expectMetadataFetch(rpc, testContractA, "USD Coin", "USDC", 7)

		f := newMetadataFetcher(rpc, pond.NewPool(2))
		out, err := f.FetchMetadata(ctx, []string{testContractA})
		require.NoError(t, err)
		require.Len(t, out, 1)

		got := out[testContractA]
		require.NotNil(t, got)
		assert.Equal(t, data.DeterministicContractID(testContractA), got.ID)
		assert.Equal(t, testContractA, got.ContractID)
		assert.Equal(t, contractTokenType, got.Type)
		require.NotNil(t, got.Name)
		assert.Equal(t, "USD Coin", *got.Name)
		require.NotNil(t, got.Symbol)
		assert.Equal(t, "USDC", *got.Symbol)
		assert.Equal(t, uint32(7), got.Decimals)
	})

	t.Run("tolerates a per-contract RPC failure by omitting that contract from the map", func(t *testing.T) {
		rpc := services.NewContractMetadataServiceMock(t)
		// A succeeds.
		expectMetadataFetch(rpc, testContractA, "USD Coin", "USDC", 7)
		// B fails on the first hop (name); symbol/decimals are never called.
		rpc.On("FetchSingleField", mock.Anything, testContractB, "name", mock.Anything).
			Return(xdr.ScVal{}, errors.New("simulate boom")).Once()

		f := newMetadataFetcher(rpc, pond.NewPool(2))
		out, err := f.FetchMetadata(ctx, []string{testContractA, testContractB})
		require.NoError(t, err)
		assert.Len(t, out, 1)
		assert.Contains(t, out, testContractA)
		assert.NotContains(t, out, testContractB)
	})

	t.Run("drops a contract whose name field is not a string", func(t *testing.T) {
		rpc := services.NewContractMetadataServiceMock(t)
		rpc.On("FetchSingleField", mock.Anything, testContractA, "name", mock.Anything).
			Return(u32ScVal(123), nil).Once() // wrong type

		f := newMetadataFetcher(rpc, pond.NewPool(2))
		out, err := f.FetchMetadata(ctx, []string{testContractA})
		require.NoError(t, err)
		assert.Empty(t, out)
	})

	t.Run("drops a contract whose name fails validation (NUL byte)", func(t *testing.T) {
		rpc := services.NewContractMetadataServiceMock(t)
		rpc.On("FetchSingleField", mock.Anything, testContractA, "name", mock.Anything).
			Return(strScVal("bad\x00name"), nil).Once()

		f := newMetadataFetcher(rpc, pond.NewPool(2))
		out, err := f.FetchMetadata(ctx, []string{testContractA})
		require.NoError(t, err)
		assert.Empty(t, out)
	})

	t.Run("drops a contract whose symbol exceeds the byte cap", func(t *testing.T) {
		rpc := services.NewContractMetadataServiceMock(t)
		rpc.On("FetchSingleField", mock.Anything, testContractA, "name", mock.Anything).
			Return(strScVal("OK"), nil).Once()
		rpc.On("FetchSingleField", mock.Anything, testContractA, "symbol", mock.Anything).
			Return(strScVal(strings.Repeat("S", maxTokenSymbolLength+1)), nil).Once()

		f := newMetadataFetcher(rpc, pond.NewPool(2))
		out, err := f.FetchMetadata(ctx, []string{testContractA})
		require.NoError(t, err)
		assert.Empty(t, out)
	})

	t.Run("drops a contract whose decimals is not a u32", func(t *testing.T) {
		rpc := services.NewContractMetadataServiceMock(t)
		rpc.On("FetchSingleField", mock.Anything, testContractA, "name", mock.Anything).
			Return(strScVal("OK"), nil).Once()
		rpc.On("FetchSingleField", mock.Anything, testContractA, "symbol", mock.Anything).
			Return(strScVal("OK"), nil).Once()
		rpc.On("FetchSingleField", mock.Anything, testContractA, "decimals", mock.Anything).
			Return(strScVal("seven"), nil).Once() // wrong type

		f := newMetadataFetcher(rpc, pond.NewPool(2))
		out, err := f.FetchMetadata(ctx, []string{testContractA})
		require.NoError(t, err)
		assert.Empty(t, out)
	})

	t.Run("drops a contract whose decimals exceeds the cap", func(t *testing.T) {
		rpc := services.NewContractMetadataServiceMock(t)
		rpc.On("FetchSingleField", mock.Anything, testContractA, "name", mock.Anything).
			Return(strScVal("OK"), nil).Once()
		rpc.On("FetchSingleField", mock.Anything, testContractA, "symbol", mock.Anything).
			Return(strScVal("OK"), nil).Once()
		rpc.On("FetchSingleField", mock.Anything, testContractA, "decimals", mock.Anything).
			Return(u32ScVal(maxTokenDecimals+1), nil).Once()

		f := newMetadataFetcher(rpc, pond.NewPool(2))
		out, err := f.FetchMetadata(ctx, []string{testContractA})
		require.NoError(t, err)
		assert.Empty(t, out)
	})

	t.Run("skips a contract that is still inside its failure-backoff window", func(t *testing.T) {
		rpc := services.NewContractMetadataServiceMock(t)
		// One failing name() simulation is all the RPC is allowed to see: the
		// backoff entry recorded by the first call must suppress the second.
		rpc.On("FetchSingleField", mock.Anything, testContractA, "name", mock.Anything).
			Return(xdr.ScVal{}, errors.New("simulate boom")).Once()

		f := newMetadataFetcher(rpc, pond.NewPool(2))
		out, err := f.FetchMetadata(ctx, []string{testContractA})
		require.NoError(t, err)
		assert.Empty(t, out)

		out, err = f.FetchMetadata(ctx, []string{testContractA})
		require.NoError(t, err)
		assert.Empty(t, out)
		rpc.AssertNumberOfCalls(t, "FetchSingleField", 1)
	})

	t.Run("refetches a contract once its failure-backoff window expires", func(t *testing.T) {
		// The back-off deadline is absolute, so shorten it before recording the
		// failure. Shortening it afterwards leaves the original five minutes.
		orig := metadataFailureBackoff
		metadataFailureBackoff = 20 * time.Millisecond
		t.Cleanup(func() { metadataFailureBackoff = orig })

		rpc := services.NewContractMetadataServiceMock(t)
		rpc.On("FetchSingleField", mock.Anything, testContractA, "name", mock.Anything).
			Return(xdr.ScVal{}, errors.New("simulate boom")).Once()
		expectMetadataFetch(rpc, testContractA, "USD Coin", "USDC", 7)

		f := newMetadataFetcher(rpc, pond.NewPool(2))
		out, err := f.FetchMetadata(ctx, []string{testContractA})
		require.NoError(t, err)
		assert.Empty(t, out)

		// Calls inside the window return nothing. The first call past the deadline
		// reaches the now-healthy RPC.
		require.Eventually(t, func() bool {
			out, err := f.FetchMetadata(ctx, []string{testContractA})
			return err == nil && len(out) == 1
		}, time.Second, 5*time.Millisecond)

		// The successful fetch replaced the back-off entry, so it never reaches
		// RPC again.
		out, err = f.FetchMetadata(ctx, []string{testContractA})
		require.NoError(t, err)
		assert.Empty(t, out)
		rpc.AssertNumberOfCalls(t, "FetchSingleField", 4) // 1 failed name + name/symbol/decimals
	})

	t.Run("fetches a contract only once per process", func(t *testing.T) {
		rpc := services.NewContractMetadataServiceMock(t)
		expectMetadataFetch(rpc, testContractA, "USD Coin", "USDC", 7)

		f := newMetadataFetcher(rpc, pond.NewPool(2))
		out, err := f.FetchMetadata(ctx, []string{testContractA})
		require.NoError(t, err)
		require.Len(t, out, 1)

		// We already have this contract's metadata, so the second call returns
		// nothing rather than refetching.
		out, err = f.FetchMetadata(ctx, []string{testContractA})
		require.NoError(t, err)
		assert.Empty(t, out)
		rpc.AssertNumberOfCalls(t, "FetchSingleField", 3)
	})

	t.Run("fetches only the uncached contract of a mixed batch", func(t *testing.T) {
		rpc := services.NewContractMetadataServiceMock(t)
		expectMetadataFetch(rpc, testContractA, "USD Coin", "USDC", 7)
		expectMetadataFetch(rpc, testContractB, "Euro Coin", "EURC", 6)

		f := newMetadataFetcher(rpc, pond.NewPool(2))
		out, err := f.FetchMetadata(ctx, []string{testContractA})
		require.NoError(t, err)
		require.Len(t, out, 1)

		out, err = f.FetchMetadata(ctx, []string{testContractA, testContractB})
		require.NoError(t, err)
		require.Len(t, out, 1)
		assert.NotContains(t, out, testContractA)
		require.NotNil(t, out[testContractB])
		require.NotNil(t, out[testContractB].Symbol)
		assert.Equal(t, "EURC", *out[testContractB].Symbol)
		rpc.AssertNumberOfCalls(t, "FetchSingleField", 6)
	})

	t.Run("processes contracts in multiple batches when input exceeds the batch size", func(t *testing.T) {
		// Shrink batch knobs so we exercise the multi-batch loop without sleeping
		// for real seconds.
		origSize, origSleep := metadataBatchSize, metadataBatchSleep
		metadataBatchSize, metadataBatchSleep = 2, time.Millisecond
		t.Cleanup(func() { metadataBatchSize, metadataBatchSleep = origSize, origSleep })

		// 5 contracts → ceil(5/2) = 3 batches.
		contractIDs := []string{
			"CA111111111111111111111111111111111111111111111111111A",
			"CA222222222222222222222222222222222222222222222222222B",
			"CA333333333333333333333333333333333333333333333333333C",
			"CA444444444444444444444444444444444444444444444444444D",
			"CA555555555555555555555555555555555555555555555555555E",
		}
		rpc := services.NewContractMetadataServiceMock(t)
		for _, cid := range contractIDs {
			expectMetadataFetch(rpc, cid, "name-"+cid, "SYM", 6)
		}

		f := newMetadataFetcher(rpc, pond.NewPool(4))
		out, err := f.FetchMetadata(ctx, contractIDs)
		require.NoError(t, err)
		assert.Len(t, out, len(contractIDs))
		for _, cid := range contractIDs {
			assert.Contains(t, out, cid)
		}
	})
}
