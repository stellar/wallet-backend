package blend

import (
	"context"
	"math/big"
	"testing"

	"github.com/stellar/go-stellar-sdk/xdr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/stellar/wallet-backend/internal/services"
)

// bigFromStroop scales a float amount into a 7-decimal ("STROOP") fixed-point
// big.Int, matching how Comet balances, get_normalized_weight, and
// get_total_supply are all denominated on-chain (see comet.go's verification
// note). Test-only: real callers get their big.Ints from i128ToBigInt.
func bigFromStroop(amount float64) *big.Int {
	scaled := new(big.Rat).Mul(new(big.Rat).SetFloat64(amount), new(big.Rat).SetInt64(1e7))
	// amount is always an exact multiple of 1e-7 in these tests, so this is exact.
	return new(big.Int).Quo(scaled.Num(), scaled.Denom())
}

func TestRatToFixedString(t *testing.T) {
	t.Run("exact integer", func(t *testing.T) {
		assert.Equal(t, "10000000", ratToFixedString(big.NewRat(1, 1), 7))
	})

	t.Run("rounds down below half", func(t *testing.T) {
		// 1/3 at 0 decimals = 0.333... -> 0
		assert.Equal(t, "0", ratToFixedString(big.NewRat(1, 3), 0))
	})

	t.Run("rounds up at exactly half, away from zero", func(t *testing.T) {
		assert.Equal(t, "1", ratToFixedString(big.NewRat(1, 2), 0))
		assert.Equal(t, "-1", ratToFixedString(big.NewRat(-1, 2), 0))
	})

	t.Run("rounds up above half", func(t *testing.T) {
		// 2/3 at 0 decimals = 0.666... -> 1
		assert.Equal(t, "1", ratToFixedString(big.NewRat(2, 3), 0))
	})

	t.Run("zero", func(t *testing.T) {
		assert.Equal(t, "0", ratToFixedString(big.NewRat(0, 1), 7))
	})
}

// TestCometValuation is pure math — no mocks. Expected values were
// cross-checked with exact rational arithmetic (Python's fractions module)
// independently of the Go implementation.
func TestCometValuation(t *testing.T) {
	t.Run("canonical 80/20 pool", func(t *testing.T) {
		// blndBal=4,000,000, usdcBal=200,000, weights 0.8/0.2, lpSupply=1,000,000.
		// blndPrice = (200k/0.2)/(4M/0.8) = 1,000,000/5,000,000 = 0.2 USD.
		// lpPrice = (4M*0.2 + 200k)/1M = (800k+200k)/1M = 1.0 USD.
		blndPrice, lpPrice, err := cometValuation(
			bigFromStroop(4_000_000), bigFromStroop(200_000),
			bigFromStroop(0.8), bigFromStroop(0.2),
			bigFromStroop(1_000_000),
		)
		require.NoError(t, err)
		assert.Equal(t, "2000000", blndPrice)
		assert.Equal(t, "10000000", lpPrice)
	})

	t.Run("uneven supply, non-terminating spot price", func(t *testing.T) {
		// blndBal=3,000,000, usdcBal=100,000, weights 0.8/0.2, lpSupply=700,000.
		// blndPrice = (100k*0.8)/(0.2*3M) = 80,000/600,000 = 2/15 = 0.1333333...
		// -> rounds to 1333333 at 7 decimals.
		// poolValue = 3M*(2/15) + 100k = 400,000 + 100,000 = 500,000.
		// lpPrice = 500,000/700,000 = 5/7 = 0.7142857142... -> rounds to 7142857.
		blndPrice, lpPrice, err := cometValuation(
			bigFromStroop(3_000_000), bigFromStroop(100_000),
			bigFromStroop(0.8), bigFromStroop(0.2),
			bigFromStroop(700_000),
		)
		require.NoError(t, err)
		assert.Equal(t, "1333333", blndPrice)
		assert.Equal(t, "7142857", lpPrice)
	})

	t.Run("tiny balances still round correctly", func(t *testing.T) {
		// Smallest possible non-zero 7-decimal amounts (raw i128 == 3, 7, 5).
		blndBal := big.NewInt(3)
		usdcBal := big.NewInt(7)
		blndWeight := bigFromStroop(0.8)
		usdcWeight := bigFromStroop(0.2)
		lpSupply := big.NewInt(5)

		// blndPrice = (7/0.2)/(3/0.8) = (7*0.8)/(0.2*3) = 5.6/0.6 = 28/3 = 9.3333... -> 93333333.
		// poolValue = 3*(28/3) + 7 = 28+7 = 35 (raw). lpPrice = 35/5 = 7.0 -> 70000000.
		blndPrice, lpPrice, err := cometValuation(blndBal, usdcBal, blndWeight, usdcWeight, lpSupply)
		require.NoError(t, err)
		assert.Equal(t, "93333333", blndPrice)
		assert.Equal(t, "70000000", lpPrice)
	})

	t.Run("rejects a positive price that rounds to zero", func(t *testing.T) {
		// usdcBal=1 raw unit vs blndBal=10^20 raw units at equal weights puts
		// the BLND spot at 10^-20 — far below half of one 7-decimal unit, so
		// it would round to "0", which the snapshot invariant forbids.
		huge := new(big.Int).Exp(big.NewInt(10), big.NewInt(20), nil)
		_, _, err := cometValuation(huge, big.NewInt(1), big.NewInt(5_000_000), big.NewInt(5_000_000), huge)
		assert.ErrorContains(t, err, "rounds to zero")
	})

	t.Run("rejects nil and non-positive inputs without dividing by zero", func(t *testing.T) {
		one := bigFromStroop(1)
		zero := big.NewInt(0)
		neg := big.NewInt(-1)

		cases := map[string]struct {
			blndBal, usdcBal, blndWeight, usdcWeight, lpSupply *big.Int
		}{
			"nil blndBal":       {nil, one, one, one, one},
			"nil usdcBal":       {one, nil, one, one, one},
			"nil blndWeight":    {one, one, nil, one, one},
			"nil usdcWeight":    {one, one, one, nil, one},
			"nil lpSupply":      {one, one, one, one, nil},
			"zero blndBal":      {zero, one, one, one, one},
			"zero usdcBal":      {one, zero, one, one, one},
			"zero blndWeight":   {one, one, zero, one, one},
			"zero usdcWeight":   {one, one, one, zero, one},
			"zero lpSupply":     {one, one, one, one, zero},
			"negative blndBal":  {neg, one, one, one, one},
			"negative lpSupply": {one, one, one, one, neg},
		}

		for name, c := range cases {
			t.Run(name, func(t *testing.T) {
				_, _, err := cometValuation(c.blndBal, c.usdcBal, c.blndWeight, c.usdcWeight, c.lpSupply)
				assert.Error(t, err)
			})
		}
	})
}

// TestFetchCometState mocks services.ContractMetadataService — no real RPC.
func TestFetchCometState(t *testing.T) {
	ctx := context.Background()
	// cometID (mainnet) / cometIDTestnet exercise fetchCometState against two
	// distinct pool addresses — the price snapshot task (a later change) will
	// call it once per network, so the tests deliberately don't hardcode a
	// single address throughout.
	const (
		cometID        = "CAS3FL6TLZKDGGSISDBWGGPXT3NRR4DYTZD7YOD3HMYO6LTJUVGRVEAM"
		cometIDTestnet = "CA5UTUUPHYL5K22UBRUVC37EARZUGYOSGK3IKIXG2JLCC5ZZLI4BDWDM"
	)

	// setupHappyMock wires up get_tokens/get_balance/get_normalized_weight/
	// get_total_supply against the pool at poolID so the token at index
	// blndIdx is BLND (weight 0.8) and the other is USDC (weight 0.2),
	// proving fetchCometState identifies BLND by weight rather than by
	// get_tokens() position.
	setupHappyMock := func(t *testing.T, poolID string, blndIdx int) *services.ContractMetadataServiceMock {
		t.Helper()
		tokenA := randomContractAddr(t)
		tokenB := randomContractAddr(t)
		tokens := []string{tokenA, tokenB}
		blndAddr, usdcAddr := tokens[blndIdx], tokens[1-blndIdx]

		m := services.NewContractMetadataServiceMock(t)
		m.On("FetchSingleField", mock.Anything, poolID, "get_tokens", []xdr.ScVal(nil)).
			Return(vecScVal(contractAddrScVal(t, tokenA), contractAddrScVal(t, tokenB)), nil).Once()

		blndArg, err := contractAddressScVal(blndAddr)
		require.NoError(t, err)
		usdcArg, err := contractAddressScVal(usdcAddr)
		require.NoError(t, err)

		// Each numeric getter is fetched twice: fetchCometState's torn-read
		// guard requires two agreeing consistency reads.
		m.On("FetchSingleField", mock.Anything, poolID, "get_balance", []xdr.ScVal{blndArg}).
			Return(i128ScVal(40_000_000_000_000), nil).Times(2) // 4,000,000.0000000
		m.On("FetchSingleField", mock.Anything, poolID, "get_balance", []xdr.ScVal{usdcArg}).
			Return(i128ScVal(2_000_000_000_000), nil).Times(2) // 200,000.0000000
		m.On("FetchSingleField", mock.Anything, poolID, "get_normalized_weight", []xdr.ScVal{blndArg}).
			Return(i128ScVal(8_000_000), nil).Times(2) // 0.8
		m.On("FetchSingleField", mock.Anything, poolID, "get_normalized_weight", []xdr.ScVal{usdcArg}).
			Return(i128ScVal(2_000_000), nil).Times(2) // 0.2
		m.On("FetchSingleField", mock.Anything, poolID, "get_total_supply", []xdr.ScVal(nil)).
			Return(i128ScVal(10_000_000_000_000), nil).Times(2) // 1,000,000.0000000

		return m
	}

	t.Run("identifies BLND leg by weight when it is get_tokens()[0]", func(t *testing.T) {
		m := setupHappyMock(t, cometID, 0)

		state, err := fetchCometState(ctx, m, cometID)
		require.NoError(t, err)
		require.NotNil(t, state)
		assert.Equal(t, big.NewInt(40_000_000_000_000), state.BLNDBalance)
		assert.Equal(t, big.NewInt(2_000_000_000_000), state.USDCBalance)
		assert.Equal(t, big.NewInt(8_000_000), state.BLNDWeight)
		assert.Equal(t, big.NewInt(2_000_000), state.USDCWeight)
		assert.Equal(t, big.NewInt(10_000_000_000_000), state.LPSupply)
	})

	t.Run("identifies BLND leg by weight when it is get_tokens()[1], against a different pool address", func(t *testing.T) {
		m := setupHappyMock(t, cometIDTestnet, 1)

		state, err := fetchCometState(ctx, m, cometIDTestnet)
		require.NoError(t, err)
		require.NotNil(t, state)
		assert.Equal(t, big.NewInt(40_000_000_000_000), state.BLNDBalance)
		assert.Equal(t, big.NewInt(2_000_000_000_000), state.USDCBalance)
	})

	t.Run("nil metadata service degrades with error", func(t *testing.T) {
		_, err := fetchCometState(ctx, nil, cometID)
		assert.Error(t, err)
	})

	t.Run("get_tokens RPC error propagates", func(t *testing.T) {
		m := services.NewContractMetadataServiceMock(t)
		m.On("FetchSingleField", mock.Anything, cometID, "get_tokens", []xdr.ScVal(nil)).
			Return(xdr.ScVal{}, assert.AnError).Once()

		_, err := fetchCometState(ctx, m, cometID)
		assert.Error(t, err)
	})

	t.Run("get_tokens wrong shape errors", func(t *testing.T) {
		m := services.NewContractMetadataServiceMock(t)
		m.On("FetchSingleField", mock.Anything, cometID, "get_tokens", []xdr.ScVal(nil)).
			Return(u32ScVal(1), nil).Once()

		_, err := fetchCometState(ctx, m, cometID)
		assert.Error(t, err)
	})

	t.Run("get_tokens wrong count errors", func(t *testing.T) {
		m := services.NewContractMetadataServiceMock(t)
		m.On("FetchSingleField", mock.Anything, cometID, "get_tokens", []xdr.ScVal(nil)).
			Return(vecScVal(contractAddrScVal(t, randomContractAddr(t))), nil).Once()

		_, err := fetchCometState(ctx, m, cometID)
		assert.Error(t, err)
	})

	t.Run("get_balance RPC error propagates", func(t *testing.T) {
		tokenA := randomContractAddr(t)
		tokenB := randomContractAddr(t)

		m := services.NewContractMetadataServiceMock(t)
		m.On("FetchSingleField", mock.Anything, cometID, "get_tokens", []xdr.ScVal(nil)).
			Return(vecScVal(contractAddrScVal(t, tokenA), contractAddrScVal(t, tokenB)), nil).Once()
		m.On("FetchSingleField", mock.Anything, cometID, "get_balance", mock.Anything).
			Return(xdr.ScVal{}, assert.AnError).Once()

		_, err := fetchCometState(ctx, m, cometID)
		assert.Error(t, err)
	})

	t.Run("get_normalized_weight wrong type errors", func(t *testing.T) {
		tokenA := randomContractAddr(t)
		tokenB := randomContractAddr(t)

		m := services.NewContractMetadataServiceMock(t)
		m.On("FetchSingleField", mock.Anything, cometID, "get_tokens", []xdr.ScVal(nil)).
			Return(vecScVal(contractAddrScVal(t, tokenA), contractAddrScVal(t, tokenB)), nil).Once()
		m.On("FetchSingleField", mock.Anything, cometID, "get_balance", mock.Anything).
			Return(i128ScVal(1), nil)
		m.On("FetchSingleField", mock.Anything, cometID, "get_normalized_weight", mock.Anything).
			Return(u32ScVal(1), nil).Once()

		_, err := fetchCometState(ctx, m, cometID)
		assert.Error(t, err)
	})

	t.Run("get_total_supply RPC error propagates", func(t *testing.T) {
		tokenA := randomContractAddr(t)
		tokenB := randomContractAddr(t)

		m := services.NewContractMetadataServiceMock(t)
		m.On("FetchSingleField", mock.Anything, cometID, "get_tokens", []xdr.ScVal(nil)).
			Return(vecScVal(contractAddrScVal(t, tokenA), contractAddrScVal(t, tokenB)), nil).Once()
		m.On("FetchSingleField", mock.Anything, cometID, "get_balance", mock.Anything).
			Return(i128ScVal(1), nil)
		m.On("FetchSingleField", mock.Anything, cometID, "get_normalized_weight", mock.Anything).
			Return(i128ScVal(8_000_000), nil).Once().
			On("FetchSingleField", mock.Anything, cometID, "get_normalized_weight", mock.Anything).
			Return(i128ScVal(2_000_000), nil).Once()
		m.On("FetchSingleField", mock.Anything, cometID, "get_total_supply", []xdr.ScVal(nil)).
			Return(xdr.ScVal{}, assert.AnError).Once()

		_, err := fetchCometState(ctx, m, cometID)
		assert.Error(t, err)
	})

	t.Run("state changed between consistency reads errors", func(t *testing.T) {
		tokenA := randomContractAddr(t)
		tokenB := randomContractAddr(t)
		blndArg, err := contractAddressScVal(tokenA)
		require.NoError(t, err)

		m := services.NewContractMetadataServiceMock(t)
		m.On("FetchSingleField", mock.Anything, cometID, "get_tokens", []xdr.ScVal(nil)).
			Return(vecScVal(contractAddrScVal(t, tokenA), contractAddrScVal(t, tokenB)), nil).Once()
		// tokenA's balance moves between the two consistency reads — a swap
		// landing mid-read — while everything else stays put.
		m.On("FetchSingleField", mock.Anything, cometID, "get_balance", []xdr.ScVal{blndArg}).
			Return(i128ScVal(40_000_000_000_000), nil).Once().
			On("FetchSingleField", mock.Anything, cometID, "get_balance", []xdr.ScVal{blndArg}).
			Return(i128ScVal(41_000_000_000_000), nil).Once()
		m.On("FetchSingleField", mock.Anything, cometID, "get_balance", mock.Anything).
			Return(i128ScVal(2_000_000_000_000), nil)
		m.On("FetchSingleField", mock.Anything, cometID, "get_normalized_weight", []xdr.ScVal{blndArg}).
			Return(i128ScVal(8_000_000), nil)
		m.On("FetchSingleField", mock.Anything, cometID, "get_normalized_weight", mock.Anything).
			Return(i128ScVal(2_000_000), nil)
		m.On("FetchSingleField", mock.Anything, cometID, "get_total_supply", []xdr.ScVal(nil)).
			Return(i128ScVal(10_000_000_000_000), nil)

		_, err = fetchCometState(ctx, m, cometID)
		assert.ErrorContains(t, err, "state changed between consistency reads")
	})

	t.Run("equal weights are ambiguous and error", func(t *testing.T) {
		tokenA := randomContractAddr(t)
		tokenB := randomContractAddr(t)

		m := services.NewContractMetadataServiceMock(t)
		m.On("FetchSingleField", mock.Anything, cometID, "get_tokens", []xdr.ScVal(nil)).
			Return(vecScVal(contractAddrScVal(t, tokenA), contractAddrScVal(t, tokenB)), nil).Once()
		m.On("FetchSingleField", mock.Anything, cometID, "get_balance", mock.Anything).
			Return(i128ScVal(1), nil)
		m.On("FetchSingleField", mock.Anything, cometID, "get_normalized_weight", mock.Anything).
			Return(i128ScVal(5_000_000), nil)
		m.On("FetchSingleField", mock.Anything, cometID, "get_total_supply", []xdr.ScVal(nil)).
			Return(i128ScVal(1), nil)

		_, err := fetchCometState(ctx, m, cometID)
		assert.Error(t, err)
	})
}
