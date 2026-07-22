// Package blend — rates.go implements Blend v2's three-slope reactive
// interest-rate curve, APR/APY conversion, rate projection, and USD
// valuation as pure math: no I/O, no side effects. big.Rat/big.Int carry
// all intermediate precision; float64 only appears at ToAPY's boundary and
// decimal-string parsing only at ParseDecimalToBigInt's boundary — both
// documented, deliberate edge conversions for callers (PR5's GraphQL
// resolvers) that need a primitive rather than an arbitrary-precision type.
//
// Scalars and conventions confirmed against blend-contracts-v2 @ ba22b487
// and blend-sdk-js @ af62659397f36c4dd0c25c76f340eacac1b957bc (2026-07-08:
// pool/src/pool/reserve.rs, pool/src/emissions/distributor.rs, pool/src/
// emissions/manager.rs, backstop/src/emissions/distributor.rs; src/pool/
// reserve.ts, src/emissions.ts, src/math.ts):
//
//   - Curve formula (reserve.rs's set_rates / reserve.ts's setRates): three
//     slopes, boundaries closed (<=) at util==target and util==95%; r_three
//     is NOT ir_mod-scaled in the util>95% branch. Matches this package's
//     pinned formula exactly — no curve-shape deviation.
//   - Supply-side capture: supplyApr = borrowApr*util*(1-bstop_rate),
//     confirmed verbatim in setRates's supplyCapture calculation.
//   - APR->APY: blend-sdk-js's Reserve.setRates uses two DIFFERENT
//     compounding periods, not one: estBorrowApy=(1+borrowApr/365)^365-1
//     (daily) and estSupplyApy=(1+supplyApr/52)^52-1 (weekly) — "est borrow
//     apy at a higher compounding rate than supply such that each is a
//     'safer' estimate for the user" (sdk comment). DEVIATION from this
//     task's literal single-formula spec: ToAPY takes an explicit `periods`
//     argument instead of hardcoding 52; BorrowAPYCompoundingPeriods and
//     SupplyAPYCompoundingPeriods document which to pass.
//   - Rate projection: Reserve.accrue applies simple (non-compounding)
//     per-call growth: new_dRate = dRate*(1+borrowApr*Δt/year); new_bRate
//     grows by the accrued interest's supplier share over total supply,
//     which reduces algebraically to bRate*(1+borrowApr*rawUtil*
//     (1-bstopRate)*Δt/year) with rawUtil the UNCLAMPED liabilities/supply
//     ratio — see ProjectRates for why the clamp applies only to the curve,
//     not the accrual split. Verified by hand and against a real mainnet
//     reserve — see rates_test.go. ir_mod is an input here, not itself
//     projected: the contract, too, applies the stored ir_mod across the
//     whole elapsed window and re-derives it only for the next one.
//   - Emissions: eps (ReserveEmissionData/BackstopEmissionData) is
//     uniformly SCALAR_14 (1e14) fixed-point "BLND per second" for BOTH
//     backstop and pool-reserve emissions (manager.rs: "Eps is scaled by 14
//     decimals"; confirmed against a real mainnet eps value — see
//     TestEmissionsAPR). What DOES vary per-reserve is the claim-conversion
//     divisor: distributor.rs's update_user_emissions divides by
//     `supply_scalar * SCALAR_7`, where supply_scalar is
//     10^ReserveConfig.decimals for a pool reserve, or the backstop's own
//     fixed SCALAR_7 (its LP-share balances are always 7-decimal) —
//     SCALAR_7*SCALAR_7 = SCALAR_14 is why the backstop's divisor happens
//     to look like one fixed constant. DEVIATION: ClaimableEmissions takes
//     this divisor as an explicit `scalar` argument — the task's literal
//     signature omitted it, which is only correct for exactly-7-decimal
//     reserves.
//   - reserve_token_id parity (dToken=index*2, bToken=index*2+1) and
//     SECONDS_PER_YEAR=31536000 confirmed as pinned; unused by this file
//     directly (decoding lives in entries.go) but SecondsPerYear is
//     re-exported here since ProjectRates/EmissionsAPR need it.
//   - BackstopLPTokens deliberately returns 0 when poolShares==0, per this
//     task's explicit spec — DEVIATION from blend-sdk-js's
//     BackstopPool.sharesToBackstopTokens, which returns shares unchanged
//     in that case (a 1:1 pre-deposit bootstrap assumption).
package blend

import (
	"fmt"
	"math"
	"math/big"
	"strings"
)

// SecondsPerYear is Blend v2's fixed year length for APR<->per-second rate
// conversions (pool/src/constants.rs's SECONDS_PER_YEAR) — a fixed
// 365-day year, not the Julian or calendar year.
const SecondsPerYear = 31536000

// BorrowAPYCompoundingPeriods and SupplyAPYCompoundingPeriods are the
// compounding periods blend-sdk-js's Reserve.setRates passes to its own
// APR->APY estimate for each side — daily for borrow, weekly for supply,
// deliberately asymmetric so each estimate is conservative ("safer") for
// the user it is shown to (more frequent compounding always yields a
// higher APY for the same APR). Pass one of these to ToAPY.
const (
	BorrowAPYCompoundingPeriods = 365
	SupplyAPYCompoundingPeriods = 52
)

// scalar7Int is SCALAR_7 (1e7): the fixed-point scale of Curve's fields,
// PoolConfig.bstop_rate, and ir_mod (all ReserveConfig 7-decimal fields).
const scalar7Int = 10_000_000

// emissionsEpsScalar is SCALAR_14 (1e14): the fixed native scale of
// ReserveEmissionData.eps and BackstopEmissionData.eps, uniform for both
// pool-reserve and backstop emissions — see package doc.
const emissionsEpsScalar = 100_000_000_000_000

// Curve is a reserve's three-slope reactive interest-rate curve
// (ReserveConfig's util/r_base/r_one/r_two/r_three fields), each a
// 7-decimal ("SCALAR_7") fixed-point integer exactly as stored on-chain.
type Curve struct {
	Util, RBase, ROne, RTwo, RThree int32
}

// ratFromFixed7 converts a 7-decimal ("SCALAR_7") fixed-point on-chain
// integer — a Curve field, PoolConfig.bstop_rate, or ir_mod — into its
// real-number big.Rat value.
func ratFromFixed7(v int32) *big.Rat {
	return big.NewRat(int64(v), scalar7Int)
}

// Utilization returns a reserve's current utilization — the fraction of its
// underlying supply currently borrowed — as (dSupply*dRate)/(bSupply*
// bRate). dRate and bRate share the same fixed-point scale (SCALAR_12 for
// v2), so that scale cancels in the ratio and needs no explicit descaling.
// The contract's own path descales the two sides first with asymmetric
// rounding — ceil on liabilities, floor on supply (reserve.rs's
// to_asset_from_d_token / to_asset_from_b_token) — and then ceils the
// resulting 7-decimal ratio, whereas this function keeps the exact rational.
// Go's result is therefore always <= the contract's, by less than 1e-7 for
// any funded reserve.
//
// Branch order mirrors reserve.rs's utilization exactly: zero liabilities
// is 0, and liabilities >= supply clamps to exactly 1 ("This is capped at
// 100% to ensure interest calculations are fair") — a reachable bad-debt
// state; unclamped, it would push BorrowAPR's >95% slope past its [0,1]
// domain and display an unbounded rate. The clamp also covers zero supply
// with outstanding debt, so there is no divide-by-zero path.
func Utilization(bSupply, bRate, dSupply, dRate *big.Int) *big.Rat {
	liabilities := new(big.Int).Mul(dSupply, dRate)
	if liabilities.Sign() == 0 {
		return new(big.Rat)
	}
	supply := new(big.Int).Mul(bSupply, bRate)
	if liabilities.Cmp(supply) >= 0 {
		return big.NewRat(1, 1)
	}
	return new(big.Rat).SetFrac(liabilities, supply)
}

// BorrowAPR computes a reserve's current borrow APR from its utilization,
// curve, and interest-rate modifier — pool/src/pool/reserve.rs's
// set_rates, reproduced in exact rational arithmetic (no intermediate
// fixed-point rounding, unlike the on-chain execution). Three slopes:
// util<=target scales r_one across [0,target]; target<util<=95% scales
// r_two across (target,95%]; util>95% scales r_three (NOT ir_mod-scaled —
// matches the contract's own asymmetry) across (95%,100%]. Boundaries are
// closed at target and at 95% (<=, matching the contract). If target is
// exactly 0, the util<=target branch's util/target scalar is taken as 0
// (matching blend-sdk-js's explicit target==0 guard) rather than dividing
// by zero.
func BorrowAPR(util *big.Rat, c Curve, irMod *big.Rat) *big.Rat {
	target := ratFromFixed7(c.Util)
	rBase := ratFromFixed7(c.RBase)
	rOne := ratFromFixed7(c.ROne)
	rTwo := ratFromFixed7(c.RTwo)
	rThree := ratFromFixed7(c.RThree)
	ninetyFive := big.NewRat(95, 100)

	switch {
	case util.Cmp(target) <= 0:
		utilScalar := new(big.Rat)
		if target.Sign() != 0 {
			utilScalar.Quo(util, target)
		}
		base := new(big.Rat).Add(rBase, new(big.Rat).Mul(utilScalar, rOne))
		return new(big.Rat).Mul(irMod, base)

	case util.Cmp(ninetyFive) <= 0:
		utilScalar := new(big.Rat).Quo(new(big.Rat).Sub(util, target), new(big.Rat).Sub(ninetyFive, target))
		base := new(big.Rat).Add(new(big.Rat).Add(rBase, rOne), new(big.Rat).Mul(utilScalar, rTwo))
		return new(big.Rat).Mul(irMod, base)

	default:
		utilScalar := new(big.Rat).Quo(new(big.Rat).Sub(util, ninetyFive), big.NewRat(5, 100))
		intersection := new(big.Rat).Mul(irMod, new(big.Rat).Add(new(big.Rat).Add(rBase, rOne), rTwo))
		extra := new(big.Rat).Mul(utilScalar, rThree)
		return new(big.Rat).Add(intersection, extra)
	}
}

// SupplyAPR is the supply-side capture of borrowAPR: the fraction of
// borrower interest kept by suppliers after the pool's backstop take rate
// (bstopRate, 7-decimal fixed point) claims its share, weighted by
// utilization — reserve.rs/reserve.ts's supplyCapture calculation:
// borrowAPR*util*(1-bstopRate).
func SupplyAPR(borrowAPR, util *big.Rat, bstopRate int32) *big.Rat {
	capture := new(big.Rat).Sub(big.NewRat(1, 1), ratFromFixed7(bstopRate))
	return new(big.Rat).Mul(new(big.Rat).Mul(borrowAPR, util), capture)
}

// ToAPY compounds an APR at periods compounding periods per year:
// (1+apr/periods)^periods - 1. Pass BorrowAPYCompoundingPeriods or
// SupplyAPYCompoundingPeriods per blend-sdk-js's asymmetric convention
// (see package doc). float64 is precise enough here: this is a display
// estimate, not a settlement amount. If apr is negative enough that the
// per-period base (1+apr/periods) is non-positive — more than a full
// period's worth of loss compounded, which has no sane percentage-return
// interpretation and can even flip sign depending on whether periods is
// odd or even — ToAPY clamps to -1 (total loss) rather than propagate that
// nonsensical result.
func ToAPY(apr *big.Rat, periods int64) float64 {
	aprFloat, _ := apr.Float64()
	base := 1 + aprFloat/float64(periods)
	if base <= 0 {
		return -1
	}
	return math.Pow(base, float64(periods)) - 1
}

// roundRat rounds r to the nearest big.Int, ties rounding away from zero —
// the same convention comet.go's ratToFixedString uses at 0 decimals.
func roundRat(r *big.Rat) *big.Int {
	num := r.Num()
	den := r.Denom()
	q, rem := new(big.Int).QuoRem(num, den, new(big.Int))

	twiceRem := new(big.Int).Lsh(new(big.Int).Abs(rem), 1)
	if twiceRem.Cmp(den) >= 0 {
		if num.Sign() < 0 {
			q.Sub(q, big.NewInt(1))
		} else {
			q.Add(q, big.NewInt(1))
		}
	}
	return q
}

// ProjectRates projects bRate/dRate forward from lastTime to now given the
// reserve's current borrowAPR (derived from the CLAMPED utilization curve —
// see Utilization/BorrowAPR) and raw token supplies. It reproduces
// Reserve::load's accrual (pool/src/pool/reserve.rs) in exact rational form:
//
//	pD = dRate·(1 + borrowAPR·Δt/year)
//	pB = bRate·(1 + borrowAPR·rawUtil·(1−bstopRate)·Δt/year)
//
// where rawUtil is the UNCLAMPED liabilities/supply ratio
// (dSupply·dRate)/(bSupply·bRate). The contract grows bRate by
// accruedInterest·(1−bstopRate)/totalSupply, which reduces to exactly that —
// the clamp only shapes the curve's borrowAPR, not the accrual split, so in
// a bad-debt reserve (liabilities ≥ supply) rawUtil > 1 keeps bRate growing
// faster than a clamped ratio would (suppliers accrue the full borrower
// interest). The contract's on-load guards are mirrored exactly: Δt<=0, zero
// bSupply, or zero liabilities (utilization()==0, i.e. no borrowers) all
// return bRate/dRate unchanged — the contract bumps last_time and skips the
// rate update in those states.
//
// The real update also re-derives ir_mod from time-weighted reactivity, but
// only for the NEXT interval: calc_accrual applies the stored ir_mod across
// the whole elapsed window, which is exactly what holding ir_mod fixed here
// does. Results are rounded to the nearest raw integer, ties away from zero;
// the contract's fixed-point path (ceil on dRate accrual, floor on bRate)
// can differ from this exact-rational form by a few 1e-12 rate units over
// long idle windows — rates_test.go's mainnet vector pins the exact-rational
// value (the on-chain fixed-point result is 1 unit higher on pD).
func ProjectRates(bRate, dRate, bSupply, dSupply *big.Int, borrowAPR *big.Rat, bstopRate int32, lastTime, now int64) (pB, pD *big.Int) {
	deltaT := now - lastTime
	liabilities := new(big.Int).Mul(dSupply, dRate)
	supply := new(big.Int).Mul(bSupply, bRate)
	if deltaT <= 0 || bSupply.Sign() == 0 || liabilities.Sign() == 0 || supply.Sign() == 0 {
		return new(big.Int).Set(bRate), new(big.Int).Set(dRate)
	}

	elapsedFraction := big.NewRat(deltaT, SecondsPerYear)
	growthD := new(big.Rat).Mul(borrowAPR, elapsedFraction)
	rawUtil := new(big.Rat).SetFrac(liabilities, supply)
	capture := new(big.Rat).Sub(big.NewRat(1, 1), ratFromFixed7(bstopRate))
	growthB := new(big.Rat).Mul(new(big.Rat).Mul(growthD, rawUtil), capture)

	newD := new(big.Rat).Mul(new(big.Rat).SetInt(dRate), new(big.Rat).Add(big.NewRat(1, 1), growthD))
	newB := new(big.Rat).Mul(new(big.Rat).SetInt(bRate), new(big.Rat).Add(big.NewRat(1, 1), growthB))

	return roundRat(newB), roundRat(newD)
}

// Underlying converts a raw b/dToken amount into its underlying-asset value
// via the reserve's b/dRate (SCALAR_12 fixed point): tokens*rate/1e12,
// truncating any remainder toward zero. This doesn't replicate the
// contract's asymmetric rounding (ceiling for dTokens/debt, floor for
// bTokens/supply, each conservative in the protocol's favor) — for a
// display/valuation helper the sub-stroop difference is immaterial.
func Underlying(tokens, rate *big.Int) *big.Int {
	product := new(big.Int).Mul(tokens, rate)
	return product.Quo(product, big.NewInt(1_000_000_000_000))
}

// EmissionsAPR annualizes a raw on-chain eps (BackstopEmissionData.eps or
// ReserveEmissionData.eps — both always SCALAR_14=1e14 fixed-point "BLND
// per second", see package doc) into an APR against sideUSD, the USD value
// of whatever is being emitted to (a reserve's total b/dToken supply, or
// the backstop's LP-token pool). Returns 0 (not an error) if the emission
// has expired (now>=expiration) or sideUSD is non-positive (nothing
// meaningful to annualize against).
func EmissionsAPR(eps int64, expiration, now int64, blndUSD, sideUSD *big.Rat) *big.Rat {
	if now >= expiration || sideUSD.Sign() <= 0 {
		return new(big.Rat)
	}
	epsPerSecond := big.NewRat(eps, emissionsEpsScalar)
	annualBLND := new(big.Rat).Mul(epsPerSecond, big.NewRat(SecondsPerYear, 1))
	annualUSD := new(big.Rat).Mul(annualBLND, blndUSD)
	return new(big.Rat).Quo(annualUSD, sideUSD)
}

// ProjectEmissionIndex advances a stream's stored emission index from
// lastTime to now — the contract distributors' update_emission_data step
// (pool/src/emissions/distributor.rs, backstop/src/emissions/distributor.rs,
// mirrored by blend-sdk-js's Emissions.accrue), which every on-chain claim
// runs BEFORE applying a user's balance. A stored index is only as fresh as
// the stream's last on-chain touch, so claimable amounts computed against it
// alone under-report until someone else interacts; projecting closes that gap.
//
//	index + floor(Δt·eps·scalar/supply), Δt = min(now, expiration) − lastTime
//
// supply is the total balance the stream emits to — a reserve's raw b/dToken
// supply, or the backstop's UNQUEUED shares (shares − q4w; queued shares earn
// no emissions). scalar is the supply's own fixed-point unit, 10^decimals for
// a pool reserve or 1e7 for backstop shares — NOT the claim divisor (that
// stays scalar·1e7, see ClaimableEmissions): the index carries BLND-per-unit
// at eps's 1e14 scale divided by 1e7, so advance-by-10^decimals and
// claim-by-10^decimals·1e7 cancel exactly to raw 7-decimal BLND.
//
// Matching the contract's guards, the index is returned unchanged (a copy)
// when the stream is already fully accrued or can't accrue: lastTime at or
// past expiration or now, zero eps, or zero/negative supply.
func ProjectEmissionIndex(index *big.Int, eps int64, lastTime, expiration, now int64, supply, scalar *big.Int) *big.Int {
	out := new(big.Int).Set(index)
	if lastTime >= expiration || lastTime >= now || eps == 0 || supply.Sign() <= 0 {
		return out
	}
	t := now
	if t > expiration {
		t = expiration
	}
	additional := new(big.Int).Mul(big.NewInt(t-lastTime), big.NewInt(eps))
	additional.Mul(additional, scalar)
	additional.Quo(additional, supply)
	return out.Add(out, additional)
}

// ClaimableEmissions is the contract distributor's claim-conversion step
// (pool/src/emissions/distributor.rs's update_user_emissions,
// backstop/src/emissions/distributor.rs's mirror): accrued +
// floor(tokenBalance*(emisIndex-userIndex)/scalar). scalar is the
// caller's job to supply correctly for the context — see package doc: pass
// `10^ReserveConfig.decimals * 1e7` for a pool reserve emission (b/dToken),
// or `1e7 * 1e7` (=1e14) for a backstop emission (LP shares are always
// 7-decimal). A negative index delta is clamped to 0 — on-chain it cannot
// happen (the distributor advances the stream index before any user write,
// and traps via require_nonnegative as defense-in-depth), so off-chain it
// would only reflect an ingestion gap between the stream row and the user
// row; clamping mirrors the contract's guard instead of surfacing a
// negative claimable.
func ClaimableEmissions(accrued, userIndex, emisIndex, tokenBalance, scalar *big.Int) *big.Int {
	delta := new(big.Int).Sub(emisIndex, userIndex)
	if delta.Sign() < 0 {
		return new(big.Int).Set(accrued)
	}
	toAccrue := new(big.Int).Mul(tokenBalance, delta)
	toAccrue.Quo(toAccrue, scalar)
	return new(big.Int).Add(accrued, toAccrue)
}

// EarnedInterest is a user's floating interest earned to date on a
// reserve: the current underlying value of their supply+collateral
// bTokens, minus netSupplied (their net principal contributed, tracked by
// blend_positions — see ParseDecimalToBigInt for how that column reaches a
// *big.Int). May be negative after a full exit (e.g. bRate advanced, or
// netSupplied dust truncation, between the position closing and this being
// computed).
func EarnedInterest(supplyB, collatB, bRate, netSupplied *big.Int) *big.Int {
	total := new(big.Int).Add(supplyB, collatB)
	return new(big.Int).Sub(Underlying(total, bRate), netSupplied)
}

// BackstopLPTokens converts a user's backstop shares into Comet LP tokens:
// shares*poolTokens/poolShares, truncating toward zero. Returns 0 when
// poolShares is zero (an empty/pre-deposit backstop pool) — DEVIATION from
// blend-sdk-js's BackstopPool.sharesToBackstopTokens, which returns shares
// unchanged in that case (a 1:1 bootstrap assumption); this package follows
// this task's explicit "0 when poolShares==0" contract instead (see
// package doc).
func BackstopLPTokens(shares, poolShares, poolTokens *big.Int) *big.Int {
	if poolShares.Sign() == 0 {
		return big.NewInt(0)
	}
	product := new(big.Int).Mul(shares, poolTokens)
	return product.Quo(product, poolShares)
}

// USDValue is amount/10^tokenDecimals * price/10^priceDecimals — a token
// amount's USD value given a price already scaled to priceDecimals (the
// same raw-price convention blend_oracle_prices.Price and this package's
// own cometValuation use). Kept as an exact big.Rat; callers needing a
// primitive convert at their own boundary (e.g. ratToFixedString-style
// rounding, or Float64 for a GraphQL Float field).
func USDValue(amount *big.Int, tokenDecimals int32, price *big.Int, priceDecimals int32) *big.Rat {
	num := new(big.Int).Mul(amount, price)
	den := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(tokenDecimals+priceDecimals)), nil)
	return new(big.Rat).SetFrac(num, den)
}

// ParseDecimalToBigInt parses a base-10 decimal string — as stored in
// blend_positions.net_supplied/net_borrowed (Postgres NUMERIC columns;
// migration-time auction adjustments divide by 1e12 server-side and can
// leave a fractional remainder, e.g. "1100.0000000000000000") — into a
// *big.Int, truncating any fractional part toward zero. Truncating (not
// rounding) is deliberate: b/dToken raw amounts have no sub-stroop
// precision, so a fractional remainder here is unrepresentable dust, not a
// real quantity to round into existence. Returns an error for an empty
// string or anything that doesn't parse as a decimal number — callers must
// decide what "no value" means for their own column rather than have this
// function silently assume 0.
func ParseDecimalToBigInt(s string) (*big.Int, error) {
	if s == "" {
		return nil, fmt.Errorf("blend: ParseDecimalToBigInt: empty string")
	}

	intPart := s
	if idx := strings.IndexByte(s, '.'); idx >= 0 {
		intPart = s[:idx]
	}

	neg := false
	switch {
	case strings.HasPrefix(intPart, "-"):
		neg = true
		intPart = intPart[1:]
	case strings.HasPrefix(intPart, "+"):
		intPart = intPart[1:]
	}
	if intPart == "" {
		intPart = "0"
	}

	n, ok := new(big.Int).SetString(intPart, 10)
	if !ok {
		return nil, fmt.Errorf("blend: ParseDecimalToBigInt: invalid decimal string %q", s)
	}
	if neg {
		n.Neg(n)
	}
	return n, nil
}
