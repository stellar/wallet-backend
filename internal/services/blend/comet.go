// Package blend — comet.go derives the BLND/USD spot price and the Comet
// backstop-deposit LP token's USD price from a Comet weighted pool's raw
// on-chain state. The Blend v2 backstop's deposit token is a Comet BLND:USDC
// weighted LP (mainnet
// CAS3FL6TLZKDGGSISDBWGGPXT3NRR4DYTZD7YOD3HMYO6LTJUVGRVEAM, testnet
// CA5UTUUPHYL5K22UBRUVC37EARZUGYOSGK3IKIXG2JLCC5ZZLI4BDWDM, wasm hash
// 8abc28913035c07411ed5d134e6bfeab4723d97ddd4d1a22a0605d35c94d1a36 — pinned
// 2026-07-08); no on-chain oracle prices it directly, so the price snapshot
// task derives it from the pool's own balances/weights instead.
//
// Verification (2026-07-08): `stellar contract info interface --contract-id
// CAS3FL6TLZKDGGSISDBWGGPXT3NRR4DYTZD7YOD3HMYO6LTJUVGRVEAM --rpc-url
// https://mainnet.sorobanrpc.com --network-passphrase "Public Global Stellar
// Network ; September 2015"` (stellar-cli 27.0.0) against the live mainnet
// contract, cross-checked against CometDEX/comet-contracts-v1 (GitHub,
// contracts/src/c_pool/comet.rs and contracts/src/c_consts.rs) confirms:
//
//   - get_tokens(env: Env) -> Vec<Address>            — no-arg
//   - get_balance(env: Env, token: Address) -> i128    — 1-arg
//   - get_normalized_weight(env: Env, token: Address) -> i128 — 1-arg;
//     comet.rs's own doc comment: "Get the weight of the token in decimal
//     form with 7 decimals". c_consts.rs defines `STROOP: i128 = 10^7` as
//     Comet's fixed-point scale, confirming the weight scalar is 7 decimals
//     — the same scale Comet uses for balances and get_total_supply (the LP
//     token's total shares), and the scale this file's own inputs/outputs use.
//   - get_total_supply(env: Env) -> i128                — no-arg
//
// services.ContractMetadataService.FetchSingleField already accepts
// variadic xdr.ScVal arguments, so the two 1-arg getters need no plumbing
// changes — fetchCometState calls them directly with a single Address
// argument built by contractAddressScVal (scval.go).
package blend

import (
	"context"
	"fmt"
	"math/big"

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

// cometState is a Comet weighted pool's raw on-chain balances, weights, and
// LP total supply, already split into BLND and USDC legs. Balance/weight/
// supply fields are 7-decimal fixed-point big.Ints (Comet's STROOP scale) —
// the same shape cometValuation consumes. BLNDAddress is the strkey
// C-address of the BLND leg's token contract (get_tokens()[blndIdx]) — the
// price snapshot task (prices.go) needs it to key the derived BLND price row
// by the real BLND token address rather than the Comet pool's own address.
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

// fetchCometState calls the Comet getters (get_tokens, get_balance,
// get_normalized_weight, get_total_supply — see package doc for verified
// signatures) via metadata against the Comet pool at cometID, and splits the
// two legs into BLND and USDC by weight: the higher-weighted leg is BLND
// (the pinned Comet pool is an 80/20 BLND:USDC split), regardless of which
// index get_tokens() happens to return it at. The mutable numeric state is
// read twice and must match (see the consistency comment inline). Any RPC
// failure, unexpected return shape, token count other than 2, a mismatch
// between the two consistency reads, or a tie between the two weights (which
// would make the BLND/USDC split ambiguous) is reported as an error.
func fetchCometState(ctx context.Context, metadata services.ContractMetadataService, cometID string) (*cometState, error) {
	if metadata == nil {
		return nil, fmt.Errorf("blend: fetchCometState: nil ContractMetadataService")
	}

	tokensVal, err := metadata.FetchSingleField(ctx, cometID, "get_tokens")
	if err != nil {
		return nil, fmt.Errorf("blend: fetchCometState: fetching get_tokens: %w", err)
	}
	tokenVals, ok := vecVal(tokensVal)
	if !ok {
		return nil, fmt.Errorf("blend: fetchCometState: get_tokens: expected Vec, got %v", tokensVal.Type)
	}
	if len(tokenVals) != cometTokenCount {
		return nil, fmt.Errorf("blend: fetchCometState: expected %d tokens in Comet pool %s, got %d", cometTokenCount, cometID, len(tokenVals))
	}

	tokens := make([]string, cometTokenCount)
	for i, tv := range tokenVals {
		addr, ok := addrString(tv)
		if !ok {
			return nil, fmt.Errorf("blend: fetchCometState: get_tokens[%d]: expected Address, got %v", i, tv.Type)
		}
		tokens[i] = addr
	}

	// Each getter call is its own latest-ledger RPC simulation, so a Comet
	// transaction (swap, join, exit) landing between calls would mix pre- and
	// post-trade values into a pool state that never existed on-chain. Soroban
	// allows only one contract invocation per simulated transaction, so the
	// getters cannot be batched into a single consistent read; instead the
	// numeric state is read twice and both readings must agree. Any pool
	// mutation changes at least one balance or the LP supply, so agreement
	// means both readings observed the same state. A mismatch errors out —
	// the snapshot task's next pass retries.
	first, err := readCometNumericState(ctx, metadata, cometID, tokens)
	if err != nil {
		return nil, err
	}
	second, err := readCometNumericState(ctx, metadata, cometID, tokens)
	if err != nil {
		return nil, err
	}
	if !first.equal(second) {
		return nil, fmt.Errorf("blend: fetchCometState: pool %s state changed between consistency reads", cometID)
	}

	blndIdx := 0
	switch first.weights[0].Cmp(first.weights[1]) {
	case 0:
		return nil, fmt.Errorf("blend: fetchCometState: cannot identify BLND leg: both tokens have equal weight %s", first.weights[0])
	case -1:
		blndIdx = 1
	}
	usdcIdx := 1 - blndIdx

	return &cometState{
		BLNDAddress: tokens[blndIdx],
		BLNDBalance: first.balances[blndIdx],
		USDCBalance: first.balances[usdcIdx],
		BLNDWeight:  first.weights[blndIdx],
		USDCWeight:  first.weights[usdcIdx],
		LPSupply:    first.lpSupply,
	}, nil
}

// cometNumericReading is one read of a Comet pool's mutable numeric state:
// per-token balances and normalized weights (parallel to the token list it
// was read with), and the LP token total supply.
type cometNumericReading struct {
	balances []*big.Int
	weights  []*big.Int
	lpSupply *big.Int
}

// equal reports whether two readings observed identical pool state. Both
// readings must come from the same token list.
func (r *cometNumericReading) equal(o *cometNumericReading) bool {
	for i := range r.balances {
		if r.balances[i].Cmp(o.balances[i]) != 0 || r.weights[i].Cmp(o.weights[i]) != 0 {
			return false
		}
	}
	return r.lpSupply.Cmp(o.lpSupply) == 0
}

// readCometNumericState performs one read of the Comet pool's numeric getters
// — get_balance and get_normalized_weight per token, plus get_total_supply —
// see fetchCometState for the double-read consistency contract built on it.
func readCometNumericState(ctx context.Context, metadata services.ContractMetadataService, cometID string, tokens []string) (*cometNumericReading, error) {
	reading := &cometNumericReading{
		balances: make([]*big.Int, len(tokens)),
		weights:  make([]*big.Int, len(tokens)),
	}
	for i, tok := range tokens {
		argVal, err := contractAddressScVal(tok)
		if err != nil {
			return nil, fmt.Errorf("blend: fetchCometState: encoding token %s argument: %w", tok, err)
		}

		balVal, err := metadata.FetchSingleField(ctx, cometID, "get_balance", argVal)
		if err != nil {
			return nil, fmt.Errorf("blend: fetchCometState: fetching get_balance(%s): %w", tok, err)
		}
		balParts, ok := balVal.GetI128()
		if !ok {
			return nil, fmt.Errorf("blend: fetchCometState: get_balance(%s): expected i128, got %v", tok, balVal.Type)
		}
		reading.balances[i] = i128ToBigInt(balParts)

		weightVal, err := metadata.FetchSingleField(ctx, cometID, "get_normalized_weight", argVal)
		if err != nil {
			return nil, fmt.Errorf("blend: fetchCometState: fetching get_normalized_weight(%s): %w", tok, err)
		}
		weightParts, ok := weightVal.GetI128()
		if !ok {
			return nil, fmt.Errorf("blend: fetchCometState: get_normalized_weight(%s): expected i128, got %v", tok, weightVal.Type)
		}
		reading.weights[i] = i128ToBigInt(weightParts)
	}

	supplyVal, err := metadata.FetchSingleField(ctx, cometID, "get_total_supply")
	if err != nil {
		return nil, fmt.Errorf("blend: fetchCometState: fetching get_total_supply: %w", err)
	}
	supplyParts, ok := supplyVal.GetI128()
	if !ok {
		return nil, fmt.Errorf("blend: fetchCometState: get_total_supply: expected i128, got %v", supplyVal.Type)
	}
	reading.lpSupply = i128ToBigInt(supplyParts)

	return reading, nil
}
