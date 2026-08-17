package blend

import (
	"context"
	"encoding/base64"
	"encoding/hex"
	"math/big"
	"testing"

	"github.com/stellar/go-stellar-sdk/xdr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/stellar/wallet-backend/internal/entities"
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

// cometEntryResult builds one entities.LedgerEntryResult for the Comet pool
// at poolID whose ContractData key/value are key/val — the shape
// fetchCometState decodes out of a getLedgerEntries response.
func cometEntryResult(t *testing.T, poolID string, key, val xdr.ScVal) entities.LedgerEntryResult {
	t.Helper()
	keyB64, err := cometLedgerKey(poolID, key)
	require.NoError(t, err)

	addrVal, err := contractAddressScVal(poolID)
	require.NoError(t, err)
	data := xdr.LedgerEntryData{
		Type: xdr.LedgerEntryTypeContractData,
		ContractData: &xdr.ContractDataEntry{
			Contract:   *addrVal.Address,
			Key:        key,
			Durability: xdr.ContractDataDurabilityPersistent,
			Val:        val,
		},
	}
	raw, err := data.MarshalBinary()
	require.NoError(t, err)
	return entities.LedgerEntryResult{KeyXDR: keyB64, DataXDR: base64.StdEncoding.EncodeToString(raw)}
}

// cometInstanceVal builds the ScvContractInstance value for a WASM contract
// whose executable hash is wasmHashHex.
func cometInstanceVal(t *testing.T, wasmHashHex string) xdr.ScVal {
	t.Helper()
	raw, err := hex.DecodeString(wasmHashHex)
	require.NoError(t, err)
	var hash xdr.Hash
	copy(hash[:], raw)
	return xdr.ScVal{
		Type: xdr.ScValTypeScvContractInstance,
		Instance: &xdr.ScContractInstance{
			Executable: xdr.ContractExecutable{
				Type:     xdr.ContractExecutableTypeContractExecutableWasm,
				WasmHash: &hash,
			},
		},
	}
}

// cometRecordVal builds one Comet Record struct value ({balance, index,
// scalar, weight} keyed by field-name Symbols, matching the live entry dump
// in comet.go's verification note).
func cometRecordVal(t *testing.T, balance *big.Int, weight *big.Int, scalar int64, index uint32) xdr.ScVal {
	t.Helper()
	v := xdr.ScVal{Type: xdr.ScValTypeScvMap}
	m := mapScVal(
		xdr.ScMapEntry{Key: symScVal("balance"), Val: i128ScVal(balance.Int64())},
		xdr.ScMapEntry{Key: symScVal("index"), Val: u32ScVal(index)},
		xdr.ScMapEntry{Key: symScVal("scalar"), Val: i128ScVal(scalar)},
		xdr.ScMapEntry{Key: symScVal("weight"), Val: i128ScVal(weight.Int64())},
	)
	v.Map = &m
	return v
}

// cometRecordMapVal builds the AllRecordData Map<Address, Record> value for
// the given token records, in the order supplied.
func cometRecordMapVal(t *testing.T, tokens []string, records []xdr.ScVal) xdr.ScVal {
	t.Helper()
	entries := make([]xdr.ScMapEntry, len(tokens))
	for i := range tokens {
		entries[i] = xdr.ScMapEntry{Key: contractAddrScVal(t, tokens[i]), Val: records[i]}
	}
	v := xdr.ScVal{Type: xdr.ScValTypeScvMap}
	m := mapScVal(entries...)
	v.Map = &m
	return v
}

// TestFetchCometState mocks services.RPCService — no real RPC. The happy
// mocks mirror the live mainnet entry dump recorded in comet.go: an 80/20
// BLND:USDC record map, a TotalShares i128, and a contract instance pinned
// to cometWasmHash.
func TestFetchCometState(t *testing.T) {
	ctx := context.Background()
	// cometID (mainnet) / cometIDTestnet exercise fetchCometState against two
	// distinct pool addresses — the price snapshot task calls it with whatever
	// pool the deployment configures, so the tests deliberately don't
	// hardcode a single address throughout.
	const (
		cometID        = "CAS3FL6TLZKDGGSISDBWGGPXT3NRR4DYTZD7YOD3HMYO6LTJUVGRVEAM"
		cometIDTestnet = "CA5UTUUPHYL5K22UBRUVC37EARZUGYOSGK3IKIXG2JLCC5ZZLI4BDWDM"
	)
	const sevenDecScalar = 100_000_000_000 // 10^(18-7), both legs 7-decimals

	// happyEntries wires up the three ledger entries for the pool at poolID so
	// the token at index blndIdx is BLND (weight 0.8) and the other is USDC
	// (weight 0.2), proving fetchCometState identifies BLND by weight rather
	// than by record-map position.
	happyEntries := func(t *testing.T, poolID string, blndIdx int) ([]string, []entities.LedgerEntryResult) {
		t.Helper()
		tokens := []string{randomContractAddr(t), randomContractAddr(t)}
		records := make([]xdr.ScVal, 2)
		records[blndIdx] = cometRecordVal(t, big.NewInt(40_000_000_000_000), big.NewInt(8_000_000), sevenDecScalar, uint32(blndIdx))    // 4,000,000.0 BLND, weight 0.8
		records[1-blndIdx] = cometRecordVal(t, big.NewInt(2_000_000_000_000), big.NewInt(2_000_000), sevenDecScalar, uint32(1-blndIdx)) // 200,000.0 USDC, weight 0.2

		return tokens, []entities.LedgerEntryResult{
			cometEntryResult(t, poolID, xdr.ScVal{Type: xdr.ScValTypeScvLedgerKeyContractInstance}, cometInstanceVal(t, cometWasmHash)),
			cometEntryResult(t, poolID, cometUnitKeyScVal("AllRecordData"), cometRecordMapVal(t, tokens, records)),
			cometEntryResult(t, poolID, cometUnitKeyScVal("TotalShares"), i128ScVal(10_000_000_000_000)), // 1,000,000.0 shares
		}
	}

	mockRPC := func(t *testing.T, entries []entities.LedgerEntryResult, err error) *services.RPCServiceMock {
		t.Helper()
		m := services.NewRPCServiceMock(t)
		m.On("GetLedgerEntries", mock.MatchedBy(func(keys []string) bool { return len(keys) == 3 })).
			Return(entities.RPCGetLedgerEntriesResult{LatestLedger: 1, Entries: entries}, err).Once()
		return m
	}

	t.Run("identifies BLND leg by weight when it is the first record", func(t *testing.T) {
		tokens, entries := happyEntries(t, cometID, 0)
		m := mockRPC(t, entries, nil)

		state, err := fetchCometState(ctx, m, cometID)
		require.NoError(t, err)
		require.NotNil(t, state)
		assert.Equal(t, tokens[0], state.BLNDAddress)
		assert.Equal(t, big.NewInt(40_000_000_000_000), state.BLNDBalance)
		assert.Equal(t, big.NewInt(2_000_000_000_000), state.USDCBalance)
		assert.Equal(t, big.NewInt(8_000_000), state.BLNDWeight)
		assert.Equal(t, big.NewInt(2_000_000), state.USDCWeight)
		assert.Equal(t, big.NewInt(10_000_000_000_000), state.LPSupply)
	})

	t.Run("identifies BLND leg by weight when it is the second record, against a different pool address", func(t *testing.T) {
		tokens, entries := happyEntries(t, cometIDTestnet, 1)
		m := mockRPC(t, entries, nil)

		state, err := fetchCometState(ctx, m, cometIDTestnet)
		require.NoError(t, err)
		require.NotNil(t, state)
		assert.Equal(t, tokens[1], state.BLNDAddress)
		assert.Equal(t, big.NewInt(40_000_000_000_000), state.BLNDBalance)
		assert.Equal(t, big.NewInt(2_000_000_000_000), state.USDCBalance)
	})

	t.Run("nil RPC service degrades with error", func(t *testing.T) {
		_, err := fetchCometState(ctx, nil, cometID)
		assert.Error(t, err)
	})

	t.Run("RPC error propagates", func(t *testing.T) {
		m := mockRPC(t, nil, assert.AnError)

		_, err := fetchCometState(ctx, m, cometID)
		assert.Error(t, err)
	})

	t.Run("missing entry errors", func(t *testing.T) {
		_, entries := happyEntries(t, cometID, 0)
		m := mockRPC(t, entries[:2], nil) // TotalShares absent

		_, err := fetchCometState(ctx, m, cometID)
		assert.ErrorContains(t, err, "TotalShares entry not found")
	})

	t.Run("WASM hash mismatch refuses to decode", func(t *testing.T) {
		_, entries := happyEntries(t, cometID, 0)
		otherHash := "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
		entries[0] = cometEntryResult(t, cometID, xdr.ScVal{Type: xdr.ScValTypeScvLedgerKeyContractInstance}, cometInstanceVal(t, otherHash))
		m := mockRPC(t, entries, nil)

		_, err := fetchCometState(ctx, m, cometID)
		assert.ErrorContains(t, err, "does not match the pinned Comet hash")
	})

	t.Run("wrong token count errors", func(t *testing.T) {
		_, entries := happyEntries(t, cometID, 0)
		oneToken := []string{randomContractAddr(t)}
		oneRecord := []xdr.ScVal{cometRecordVal(t, big.NewInt(1), big.NewInt(10_000_000), sevenDecScalar, 0)}
		entries[1] = cometEntryResult(t, cometID, cometUnitKeyScVal("AllRecordData"), cometRecordMapVal(t, oneToken, oneRecord))
		m := mockRPC(t, entries, nil)

		_, err := fetchCometState(ctx, m, cometID)
		assert.ErrorContains(t, err, "expected 2 tokens")
	})

	t.Run("differing token scalars error", func(t *testing.T) {
		_, entries := happyEntries(t, cometID, 0)
		tokens := []string{randomContractAddr(t), randomContractAddr(t)}
		records := []xdr.ScVal{
			cometRecordVal(t, big.NewInt(1000), big.NewInt(8_000_000), sevenDecScalar, 0),
			cometRecordVal(t, big.NewInt(1000), big.NewInt(2_000_000), 1_000_000_000_000, 1), // 6-decimal token
		}
		entries[1] = cometEntryResult(t, cometID, cometUnitKeyScVal("AllRecordData"), cometRecordMapVal(t, tokens, records))
		m := mockRPC(t, entries, nil)

		_, err := fetchCometState(ctx, m, cometID)
		assert.ErrorContains(t, err, "token scalars differ")
	})

	t.Run("record missing a field errors", func(t *testing.T) {
		_, entries := happyEntries(t, cometID, 0)
		tokens := []string{randomContractAddr(t), randomContractAddr(t)}
		noWeight := xdr.ScVal{Type: xdr.ScValTypeScvMap}
		nm := mapScVal(
			xdr.ScMapEntry{Key: symScVal("balance"), Val: i128ScVal(1000)},
			xdr.ScMapEntry{Key: symScVal("scalar"), Val: i128ScVal(sevenDecScalar)},
		)
		noWeight.Map = &nm
		records := []xdr.ScVal{noWeight, cometRecordVal(t, big.NewInt(1000), big.NewInt(2_000_000), sevenDecScalar, 1)}
		entries[1] = cometEntryResult(t, cometID, cometUnitKeyScVal("AllRecordData"), cometRecordMapVal(t, tokens, records))
		m := mockRPC(t, entries, nil)

		_, err := fetchCometState(ctx, m, cometID)
		assert.ErrorContains(t, err, `missing "weight" field`)
	})

	t.Run("TotalShares wrong type errors", func(t *testing.T) {
		_, entries := happyEntries(t, cometID, 0)
		entries[2] = cometEntryResult(t, cometID, cometUnitKeyScVal("TotalShares"), u32ScVal(1))
		m := mockRPC(t, entries, nil)

		_, err := fetchCometState(ctx, m, cometID)
		assert.ErrorContains(t, err, "TotalShares: expected i128")
	})

	t.Run("equal weights are ambiguous and error", func(t *testing.T) {
		_, entries := happyEntries(t, cometID, 0)
		tokens := []string{randomContractAddr(t), randomContractAddr(t)}
		records := []xdr.ScVal{
			cometRecordVal(t, big.NewInt(1000), big.NewInt(5_000_000), sevenDecScalar, 0),
			cometRecordVal(t, big.NewInt(1000), big.NewInt(5_000_000), sevenDecScalar, 1),
		}
		entries[1] = cometEntryResult(t, cometID, cometUnitKeyScVal("AllRecordData"), cometRecordMapVal(t, tokens, records))
		m := mockRPC(t, entries, nil)

		_, err := fetchCometState(ctx, m, cometID)
		assert.ErrorContains(t, err, "cannot identify BLND leg")
	})
}
