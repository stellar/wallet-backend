// Package blend — comet.go derives the BLND/USD spot price and the Comet
// backstop-deposit LP token's USD price from a Comet weighted pool's raw
// on-chain state. The Blend v2 backstop's deposit token is a Comet BLND:USDC
// weighted LP (mainnet
// CAS3FL6TLZKDGGSISDBWGGPXT3NRR4DYTZD7YOD3HMYO6LTJUVGRVEAM, testnet
// CA5UTUUPHYL5K22UBRUVC37EARZUGYOSGK3IKIXG2JLCC5ZZLI4BDWDM); no on-chain
// oracle prices it directly, so the price snapshot task derives it from the
// pool's own balances/weights instead.
//
// The pool state is read straight from the contract's ledger entries rather
// than by simulating its getter functions: everything the valuation needs
// lives in two persistent ContractData entries (CometDEX/comet-contracts-v1,
// contracts/src/c_pool/storage_types.rs `DataKey` and metadata.rs accessors):
//
//   - DataKey::AllRecordData — Map<Address, Record{balance, weight, scalar,
//     index}>, the same fields get_balance/get_normalized_weight return
//   - DataKey::TotalShares — i128, what get_total_supply returns
//
// One getLedgerEntries request returns both entries plus the contract
// instance from a single RPC snapshot (the response carries one
// latestLedger), so the read is atomic by construction — a Comet transaction
// can never interleave with it — where per-getter simulations would each
// execute against whatever ledger is latest at that moment. The instance
// entry's executable WASM hash is asserted against cometWasmHash before any
// storage is decoded: the deployed contract has no upgrade entrypoint (its
// layout cannot change at this address), so a mismatch means the configured
// address is not the pinned Comet pool.
//
// Verification (2026-07-22): the key encodings (unit enum variants as
// single-element ScvVec[Symbol]), the Record field shapes, both legs'
// 7-decimal scalars, and the instance WASM hash were all confirmed against
// the live mainnet pool via getLedgerEntries — a bare-Symbol key encoding
// returns no entry. Comet's own doc comment on get_normalized_weight
// ("decimal form with 7 decimals") and c_consts.rs `STROOP: i128 = 10^7`
// pin the weight scale; init.rs asserts total_weight == STROOP.
package blend

import (
	"context"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"math/big"

	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/wallet-backend/internal/services"
)

// cometPriceDecimals is the fixed-point precision cometValuation's outputs
// use — 7 decimals, matching Comet's own STROOP scale (see package doc) so
// callers can treat blndPrice/lpPrice the same way they treat any other
// on-chain 7-decimal amount (i128String's raw-integer-string convention,
// also used by blend_oracle_prices.Price).
const cometPriceDecimals = 7

// cometTokenCount is the number of legs a Comet BLND:USDC weighted pool
// has. fetchCometState errors on any other count rather than guessing which
// tokens to treat as BLND/USDC.
const cometTokenCount = 2

// cometWasmHash is the executable WASM hash of the deployed Comet pool
// contract (identical on mainnet and testnet; pinned 2026-07-08, re-confirmed
// live 2026-07-22). fetchCometState refuses to decode storage whose contract
// instance reports any other hash — the storage layout this file relies on
// is defined by exactly this code.
const cometWasmHash = "8abc28913035c07411ed5d134e6bfeab4723d97ddd4d1a22a0605d35c94d1a36"

// cometState is a Comet weighted pool's raw on-chain balances, weights, and
// LP total supply, already split into BLND and USDC legs. Balance/weight/
// supply fields are 7-decimal fixed-point big.Ints (Comet's STROOP scale) —
// the same shape cometValuation consumes. BLNDAddress is the strkey
// C-address of the BLND leg's token contract — the price snapshot task
// (prices.go) needs it to key the derived BLND price row by the real BLND
// token address rather than the Comet pool's own address.
type cometState struct {
	BLNDAddress string
	BLNDBalance *big.Int
	USDCBalance *big.Int
	BLNDWeight  *big.Int
	USDCWeight  *big.Int
	LPSupply    *big.Int
}

// cometValuation derives the BLND spot price (denominated in USDC, taken as
// USD by convention — this pool has no on-chain USDC/USD leg of its own) and
// the Comet LP token's USD price from a weighted two-token Comet pool's raw
// balances/weights/supply.
//
// Spot price (Balancer-style weighted pool; see comet-contracts-v1's
// calc_spot_price, and the identical formula in the Balancer V1 whitepaper):
// for legs with balances B and normalized weights W,
// spot_price(quote, base) = (B_quote/W_quote) / (B_base/W_base). Here the
// quote leg is USDC and the base leg is BLND, so:
//
//	blndPrice = (usdcBal/usdcWeight) / (blndBal/blndWeight)
//
// LP token USD price is the pool's total USD value divided by lpSupply,
// where pool value = blndBal*blndPrice + usdcBal (USDC contributes at its
// own balance since USDC ≡ $1). This NAV-per-share definition holds for any
// weight split — it does not depend on blndWeight/usdcWeight directly, only
// through blndPrice.
//
// Inputs are 7-decimal fixed-point big.Ints (Comet's STROOP scale); outputs
// are 7-decimal fixed-point decimal strings (e.g. "2000000" for 0.2, not
// "0.2000000" — see i128String). All arithmetic is done with big.Rat to
// avoid intermediate overflow/precision loss, matching real-world Comet
// balances that run into the trillions of raw units.
//
// Returns an error — never a panic — for a nil, zero, or negative balance,
// weight, or lpSupply: valuing a pool with no liquidity, no weight, or no
// outstanding shares is meaningless, not just numerically undefined.
func cometValuation(blndBal, usdcBal, blndWeight, usdcWeight, lpSupply *big.Int) (blndPrice, lpPrice string, err error) {
	inputs := []struct {
		name string
		val  *big.Int
	}{
		{"blndBal", blndBal},
		{"usdcBal", usdcBal},
		{"blndWeight", blndWeight},
		{"usdcWeight", usdcWeight},
		{"lpSupply", lpSupply},
	}
	for _, in := range inputs {
		if in.val == nil {
			return "", "", fmt.Errorf("blend: cometValuation: %s is nil", in.name)
		}
		if in.val.Sign() <= 0 {
			return "", "", fmt.Errorf("blend: cometValuation: %s must be positive, got %s", in.name, in.val)
		}
	}

	// blndPriceRat = (usdcBal/usdcWeight) / (blndBal/blndWeight)
	//              = (usdcBal * blndWeight) / (usdcWeight * blndBal)
	num := new(big.Int).Mul(usdcBal, blndWeight)
	den := new(big.Int).Mul(usdcWeight, blndBal)
	blndPriceRat := new(big.Rat).SetFrac(num, den)

	blndValue := new(big.Rat).Mul(new(big.Rat).SetInt(blndBal), blndPriceRat)
	poolValue := new(big.Rat).Add(blndValue, new(big.Rat).SetInt(usdcBal))
	lpPriceRat := new(big.Rat).Quo(poolValue, new(big.Rat).SetInt(lpSupply))

	blndPrice = ratToFixedString(blndPriceRat, cometPriceDecimals)
	lpPrice = ratToFixedString(lpPriceRat, cometPriceDecimals)
	// A positive rational below half of one 7-decimal unit rounds to "0". A
	// zero price violates the snapshot invariant that non-positive prices are
	// never persisted (snapshotOracle skips them as invalid), so reject it
	// here rather than let downstream valuation report $0 for a live asset.
	if blndPrice == "0" || lpPrice == "0" {
		return "", "", fmt.Errorf("blend: cometValuation: price rounds to zero at %d decimals (blnd=%s, lp=%s)", cometPriceDecimals, blndPrice, lpPrice)
	}
	return blndPrice, lpPrice, nil
}

// ratToFixedString converts r into a base-10 fixed-point integer string at
// decimals digits of precision — e.g. 0.2 at 7 decimals renders as
// "2000000", the same "raw integer, no decimal point" convention i128String
// and blend_oracle_prices.Price use elsewhere in this codebase. Rounds to
// the nearest representable value, with exact halves rounding away from
// zero.
func ratToFixedString(r *big.Rat, decimals int) string {
	scale := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(decimals)), nil)
	scaled := new(big.Rat).Mul(r, new(big.Rat).SetInt(scale))

	// big.Rat always normalizes with a positive denominator, so num's sign
	// is scaled's sign and QuoRem (truncating/T-division) leaves rem with
	// that same sign.
	num := scaled.Num()
	den := scaled.Denom()
	q, rem := new(big.Int).QuoRem(num, den, new(big.Int))

	twiceRem := new(big.Int).Lsh(new(big.Int).Abs(rem), 1)
	if twiceRem.Cmp(den) >= 0 {
		if num.Sign() < 0 {
			q.Sub(q, big.NewInt(1))
		} else {
			q.Add(q, big.NewInt(1))
		}
	}
	return q.String()
}

// cometUnitKeyScVal builds the ScVal encoding of a Comet DataKey unit enum
// variant as it appears in ContractData ledger-entry keys: a single-element
// ScVec holding the variant-name Symbol (verified live — see package doc).
func cometUnitKeyScVal(variant string) xdr.ScVal {
	sym := xdr.ScSymbol(variant)
	vec := &xdr.ScVec{{Type: xdr.ScValTypeScvSymbol, Sym: &sym}}
	return xdr.ScVal{Type: xdr.ScValTypeScvVec, Vec: &vec}
}

// cometLedgerKey builds the base64-encoded LedgerKey for one of the Comet
// pool's persistent ContractData entries at cometID.
func cometLedgerKey(cometID string, key xdr.ScVal) (string, error) {
	addrVal, err := contractAddressScVal(cometID)
	if err != nil {
		return "", fmt.Errorf("blend: encoding comet pool address: %w", err)
	}
	lk := xdr.LedgerKey{
		Type: xdr.LedgerEntryTypeContractData,
		ContractData: &xdr.LedgerKeyContractData{
			Contract:   *addrVal.Address,
			Key:        key,
			Durability: xdr.ContractDataDurabilityPersistent,
		},
	}
	b64, err := lk.MarshalBinaryBase64()
	if err != nil {
		return "", fmt.Errorf("blend: marshaling comet ledger key: %w", err)
	}
	return b64, nil
}

// recordI128Field pulls one named i128 field out of a decoded Comet Record
// struct (an ScMap keyed by field-name Symbols — see mapGet).
func recordI128Field(record *xdr.ScMap, token, field string) (*big.Int, error) {
	fieldVal, found := mapGet(record, field)
	if !found {
		return nil, fmt.Errorf("blend: fetchCometState: record for %s: missing %q field", token, field)
	}
	parts, ok := fieldVal.GetI128()
	if !ok {
		return nil, fmt.Errorf("blend: fetchCometState: record for %s: %q is not an i128 (got %v)", token, field, fieldVal.Type)
	}
	return i128ToBigInt(parts), nil
}

// decodeContractDataVal unmarshals one getLedgerEntries result's base64 XDR
// payload and returns the ContractData entry's value ScVal.
func decodeContractDataVal(dataXDR string) (xdr.ScVal, error) {
	raw, err := base64.StdEncoding.DecodeString(dataXDR)
	if err != nil {
		return xdr.ScVal{}, fmt.Errorf("decoding entry base64: %w", err)
	}
	var data xdr.LedgerEntryData
	if err := data.UnmarshalBinary(raw); err != nil {
		return xdr.ScVal{}, fmt.Errorf("unmarshaling entry XDR: %w", err)
	}
	if data.Type != xdr.LedgerEntryTypeContractData || data.ContractData == nil {
		return xdr.ScVal{}, fmt.Errorf("expected ContractData entry, got %v", data.Type)
	}
	return data.ContractData.Val, nil
}

// fetchCometState reads the Comet pool's state at cometID — token balances,
// normalized weights, and LP total supply — from its ContractData ledger
// entries in a single getLedgerEntries call (atomic; see package doc), after
// asserting the contract instance's WASM hash matches cometWasmHash. The two
// legs are split into BLND and USDC by weight: the higher-weighted leg is
// BLND (the pinned Comet pool is an 80/20 BLND:USDC split). Any RPC failure,
// missing entry, WASM hash mismatch, unexpected shape, token count other
// than 2, differing token scalars (the valuation's raw-balance ratio assumes
// both legs share decimals), or a tie between the two weights (which would
// make the BLND/USDC split ambiguous) is reported as an error.
func fetchCometState(ctx context.Context, rpc services.RPCService, cometID string) (*cometState, error) {
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("blend: fetchCometState: context error: %w", err)
	}
	if rpc == nil {
		return nil, fmt.Errorf("blend: fetchCometState: nil RPCService")
	}

	instanceKey, err := cometLedgerKey(cometID, xdr.ScVal{Type: xdr.ScValTypeScvLedgerKeyContractInstance})
	if err != nil {
		return nil, fmt.Errorf("blend: fetchCometState: %w", err)
	}
	recordKey, err := cometLedgerKey(cometID, cometUnitKeyScVal("AllRecordData"))
	if err != nil {
		return nil, fmt.Errorf("blend: fetchCometState: %w", err)
	}
	sharesKey, err := cometLedgerKey(cometID, cometUnitKeyScVal("TotalShares"))
	if err != nil {
		return nil, fmt.Errorf("blend: fetchCometState: %w", err)
	}

	result, err := rpc.GetLedgerEntries([]string{instanceKey, recordKey, sharesKey})
	if err != nil {
		return nil, fmt.Errorf("blend: fetchCometState: fetching ledger entries: %w", err)
	}
	byKey := make(map[string]string, len(result.Entries))
	for _, entry := range result.Entries {
		byKey[entry.KeyXDR] = entry.DataXDR
	}
	vals := make(map[string]xdr.ScVal, 3)
	for name, key := range map[string]string{"instance": instanceKey, "AllRecordData": recordKey, "TotalShares": sharesKey} {
		dataXDR, found := byKey[key]
		if !found {
			return nil, fmt.Errorf("blend: fetchCometState: pool %s: %s entry not found on chain", cometID, name)
		}
		val, err := decodeContractDataVal(dataXDR)
		if err != nil {
			return nil, fmt.Errorf("blend: fetchCometState: pool %s: %s entry: %w", cometID, name, err)
		}
		vals[name] = val
	}

	instance, ok := vals["instance"].GetInstance()
	if !ok {
		return nil, fmt.Errorf("blend: fetchCometState: pool %s: instance entry is not a ContractInstance (got %v)", cometID, vals["instance"].Type)
	}
	wasmHash, ok := instance.Executable.GetWasmHash()
	if !ok {
		return nil, fmt.Errorf("blend: fetchCometState: pool %s: contract executable is not WASM (got %v)", cometID, instance.Executable.Type)
	}
	if gotHash := hex.EncodeToString(wasmHash[:]); gotHash != cometWasmHash {
		return nil, fmt.Errorf("blend: fetchCometState: pool %s: WASM hash %s does not match the pinned Comet hash %s — refusing to decode its storage", cometID, gotHash, cometWasmHash)
	}

	recordMap, ok := vals["AllRecordData"].GetMap()
	if !ok {
		return nil, fmt.Errorf("blend: fetchCometState: pool %s: AllRecordData is not a Map (got %v)", cometID, vals["AllRecordData"].Type)
	}
	if recordMap == nil || len(*recordMap) != cometTokenCount {
		got := 0
		if recordMap != nil {
			got = len(*recordMap)
		}
		return nil, fmt.Errorf("blend: fetchCometState: expected %d tokens in Comet pool %s, got %d", cometTokenCount, cometID, got)
	}

	tokens := make([]string, cometTokenCount)
	balances := make([]*big.Int, cometTokenCount)
	weights := make([]*big.Int, cometTokenCount)
	scalars := make([]*big.Int, cometTokenCount)
	for i, entry := range *recordMap {
		addr, ok := addrString(entry.Key)
		if !ok {
			return nil, fmt.Errorf("blend: fetchCometState: AllRecordData key[%d]: expected Address, got %v", i, entry.Key.Type)
		}
		tokens[i] = addr

		record, ok := entry.Val.GetMap()
		if !ok {
			return nil, fmt.Errorf("blend: fetchCometState: record for %s: expected Map, got %v", addr, entry.Val.Type)
		}
		if balances[i], err = recordI128Field(record, addr, "balance"); err != nil {
			return nil, err
		}
		if weights[i], err = recordI128Field(record, addr, "weight"); err != nil {
			return nil, err
		}
		if scalars[i], err = recordI128Field(record, addr, "scalar"); err != nil {
			return nil, err
		}
	}
	if scalars[0].Cmp(scalars[1]) != 0 {
		return nil, fmt.Errorf("blend: fetchCometState: pool %s: token scalars differ (%s vs %s) — the valuation's raw-balance ratio requires both legs to share decimals", cometID, scalars[0], scalars[1])
	}

	sharesParts, ok := vals["TotalShares"].GetI128()
	if !ok {
		return nil, fmt.Errorf("blend: fetchCometState: TotalShares: expected i128, got %v", vals["TotalShares"].Type)
	}
	lpSupply := i128ToBigInt(sharesParts)

	blndIdx := 0
	switch weights[0].Cmp(weights[1]) {
	case 0:
		return nil, fmt.Errorf("blend: fetchCometState: cannot identify BLND leg: both tokens have equal weight %s", weights[0])
	case -1:
		blndIdx = 1
	}
	usdcIdx := 1 - blndIdx

	return &cometState{
		BLNDAddress: tokens[blndIdx],
		BLNDBalance: balances[blndIdx],
		USDCBalance: balances[usdcIdx],
		BLNDWeight:  weights[blndIdx],
		USDCWeight:  weights[usdcIdx],
		LPSupply:    lpSupply,
	}, nil
}
