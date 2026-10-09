package prices

import (
	"context"
	"fmt"
	"testing"

	"github.com/stellar/go-stellar-sdk/strkey"
	"github.com/stellar/go-stellar-sdk/xdr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/wallet-backend/internal/indexer/types"
)

type seedFetcher struct {
	vals map[string]xdr.ScVal
}

func seedKey(contract, fn string, args []xdr.ScVal) string {
	if len(args) > 0 && args[0].U32 != nil {
		return fmt.Sprintf("%s/%s/%d", contract, fn, *args[0].U32)
	}
	return contract + "/" + fn
}

func (f *seedFetcher) FetchSingleField(_ context.Context, contract, fn string, args ...xdr.ScVal) (xdr.ScVal, error) {
	v, ok := f.vals[seedKey(contract, fn, args)]
	if !ok {
		return xdr.ScVal{}, fmt.Errorf("no mock for %s", seedKey(contract, fn, args))
	}
	return v, nil
}

func contractAddr(b byte) string {
	var id [32]byte
	id[0] = b
	return strkey.MustEncode(strkey.VersionByteContract, id[:])
}

func addrVal(t *testing.T, addr string) xdr.ScVal {
	t.Helper()
	raw, err := strkey.Decode(strkey.VersionByteContract, addr)
	require.NoError(t, err)
	var cid xdr.ContractId
	copy(cid[:], raw)
	return xdr.ScVal{Type: xdr.ScValTypeScvAddress, Address: &xdr.ScAddress{Type: xdr.ScAddressTypeScAddressTypeContract, ContractId: &cid}}
}

func u32Val(n uint32) xdr.ScVal {
	v := xdr.Uint32(n)
	return xdr.ScVal{Type: xdr.ScValTypeScvU32, U32: &v}
}

func TestSeedPoolsSoroswap(t *testing.T) {
	factory := contractAddr(1)
	build := func(t *testing.T, n int, badToken1At int) map[string]xdr.ScVal {
		vals := map[string]xdr.ScVal{factory + "/all_pairs_length": u32Val(uint32(n))}
		for i := 0; i < n; i++ {
			pair := contractAddr(byte(10 + i))
			vals[fmt.Sprintf("%s/all_pairs/%d", factory, i)] = addrVal(t, pair)
			vals[pair+"/token_0"] = addrVal(t, contractAddr(byte(100+i)))
			if i == badToken1At {
				vals[pair+"/token_1"] = u32Val(7)
			} else {
				vals[pair+"/token_1"] = addrVal(t, contractAddr(byte(200+i)))
			}
		}
		return vals
	}

	t.Run("happy path", func(t *testing.T) {
		var done []int
		s := &PoolSeeder{Fetcher: &seedFetcher{vals: build(t, 3, -1)}}
		pools, err := s.SoroswapPools(context.Background(), factory, func(d, total int) {
			assert.Equal(t, 3, total)
			done = append(done, d)
		})
		require.NoError(t, err)
		require.Len(t, pools, 3)
		assert.Equal(t, []int{1, 2, 3}, done)
		for i, p := range pools {
			assert.Equal(t, contractAddr(byte(10+i)), p.Pool)
			assert.Equal(t, contractAddr(byte(100+i)), p.Token0)
			assert.Equal(t, contractAddr(byte(200+i)), p.Token1)
			assert.Equal(t, types.TradeVenueSoroswap, p.Venue)
			assert.Zero(t, p.CreatedLedger)
		}
	})

	t.Run("decode failure names the index", func(t *testing.T) {
		s := &PoolSeeder{Fetcher: &seedFetcher{vals: build(t, 3, 2)}}
		pools, err := s.SoroswapPools(context.Background(), factory, nil)
		require.Error(t, err)
		assert.Nil(t, pools)
		assert.Contains(t, err.Error(), "token_1 of pair 2")
	})

	t.Run("zero pairs", func(t *testing.T) {
		s := &PoolSeeder{Fetcher: &seedFetcher{vals: build(t, 0, -1)}}
		pools, err := s.SoroswapPools(context.Background(), factory, nil)
		require.NoError(t, err)
		assert.Empty(t, pools)
	})

	t.Run("fetch error", func(t *testing.T) {
		s := &PoolSeeder{Fetcher: &seedFetcher{vals: map[string]xdr.ScVal{}}}
		_, err := s.SoroswapPools(context.Background(), factory, nil)
		require.ErrorContains(t, err, "all_pairs_length")
	})
}
