package prices

import (
	"context"
	"fmt"

	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/wallet-backend/internal/indexer/types"
)

// PoolSeeder reads the pool list of an AMM factory through contract simulation.
type PoolSeeder struct {
	Fetcher ContractFieldFetcher
}

// SoroswapPools lists every pair the Soroswap factory has created, with its two tokens. The
// created ledger is not available from the factory, so it is left at 0. Any failure aborts the run.
func (s *PoolSeeder) SoroswapPools(ctx context.Context, factory string, progress func(done, total int)) ([]types.AMMPool, error) {
	lengthVal, err := s.Fetcher.FetchSingleField(ctx, factory, "all_pairs_length")
	if err != nil {
		return nil, fmt.Errorf("reading all_pairs_length: %w", err)
	}
	n, ok := lengthVal.GetU32()
	if !ok {
		return nil, fmt.Errorf("all_pairs_length returned %s, want u32", lengthVal.Type)
	}
	total := uint32(n)

	pools := make([]types.AMMPool, 0, total)
	for i := uint32(0); i < total; i++ {
		idx := i
		pairVal, err := s.Fetcher.FetchSingleField(ctx, factory, "all_pairs", xdr.ScVal{Type: xdr.ScValTypeScvU32, U32: (*xdr.Uint32)(&idx)})
		if err != nil {
			return nil, fmt.Errorf("reading all_pairs(%d): %w", i, err)
		}
		pair, err := addressString(pairVal)
		if err != nil {
			return nil, fmt.Errorf("decoding all_pairs(%d): %w", i, err)
		}
		token0, err := s.token(ctx, pair, "token_0", i)
		if err != nil {
			return nil, err
		}
		token1, err := s.token(ctx, pair, "token_1", i)
		if err != nil {
			return nil, err
		}
		pools = append(pools, types.AMMPool{
			Pool:   pair,
			Venue:  types.TradeVenueSoroswap,
			Token0: token0,
			Token1: token1,
		})
		if progress != nil {
			progress(int(i)+1, int(total))
		}
	}
	return pools, nil
}

func (s *PoolSeeder) token(ctx context.Context, pair, fn string, index uint32) (string, error) {
	v, err := s.Fetcher.FetchSingleField(ctx, pair, fn)
	if err != nil {
		return "", fmt.Errorf("reading %s of pair %d (%s): %w", fn, index, pair, err)
	}
	addr, err := addressString(v)
	if err != nil {
		return "", fmt.Errorf("decoding %s of pair %d (%s): %w", fn, index, pair, err)
	}
	return addr, nil
}

func addressString(v xdr.ScVal) (string, error) {
	addr, ok := v.GetAddress()
	if !ok {
		return "", fmt.Errorf("value is %s, want address", v.Type)
	}
	s, err := addr.String()
	if err != nil {
		return "", fmt.Errorf("encoding address: %w", err)
	}
	return s, nil
}
