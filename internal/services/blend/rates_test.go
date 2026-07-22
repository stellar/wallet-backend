package blend

import (
	"math/big"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// assertRatEqual compares two *big.Rat by value (Cmp), not by internal
// representation — big.Rat always auto-reduces, so Cmp==0 is the correct
// equality check regardless of how each side was constructed.
func assertRatEqual(t *testing.T, expected, actual *big.Rat, msgAndArgs ...interface{}) {
	t.Helper()
	if expected.Cmp(actual) != 0 {
		t.Errorf("expected rat %s, got %s: %v", expected.RatString(), actual.RatString(), msgAndArgs)
	}
}

// bigRatFromDecimalString builds an exact *big.Rat from a pair of decimal
// numerator/denominator strings too large for int64 literals (big.NewRat
// only accepts int64) — used to hardcode Python-fractions-verified exact
// expected values for the real mainnet vector without re-deriving them from
// the same formula under test.
func bigRatFromDecimalString(t *testing.T, num, den string) *big.Rat {
	t.Helper()
	n, ok := new(big.Int).SetString(num, 10)
	require.True(t, ok, "bad numerator %q", num)
	d, ok := new(big.Int).SetString(den, 10)
	require.True(t, ok, "bad denominator %q", den)
	return new(big.Rat).SetFrac(n, d)
}

// ---------------------------------------------------------------------
// Real mainnet vector, fetched 2026-07-08 via:
//
//	stellar contract invoke --id CAJJZSGMMM3PD7N33TAPHGBUGTB43OC73HVIK2L2G6BNGGGYOSSYBXBD \
//	  --source-account GDT5YVPSELT2FIPUW4ZRGGKH3SVEX4EEHGCBWUKPR33AR5P7TIOVKXEM \
//	  --rpc-url https://mainnet.sorobanrpc.com \
//	  --network-passphrase "Public Global Stellar Network ; September 2015" \
//	  --send no -- get_reserve --asset CCW67TSZV3SSS2HXMBQ5JFGCKJNXKZM7UQUWUZPUTHXSTZLEO7SJMI75
//	  (USDC reserve on Blend v2's mainnet pool) plus `-- get_config` for
//	  bstop_rate. stellar-cli 27.0.0, simulation only (--send no), no signing
//	  key needed. Exact expected fractions were cross-checked independently
//	  with Python's fractions module (not derived from this package's Go
//	  implementation) — see the per-test comments below for the arithmetic.
//
// config: c_factor=9500000 l_factor=9500000 max_util=9000000 r_base=300000
// r_one=400000 r_two=1200000 r_three=50000000 reactivity=20 util=8000000
// decimals=7 scalar="10000000" (=10^decimals, confirming the pinned
// per-reserve supply_scalar convention)
// data: b_rate=1130417963074 b_supply=534472729796550
// backstop_credit=1777399400 d_rate=1206619920020 d_supply=403614364767661
// ir_mod=15759132 last_time=1783543369
// pool config: bstop_rate=2000000 (20%)
// ---------------------------------------------------------------------
var (
	realBSupply = mustBigInt("534472729796550")
	realBRate   = mustBigInt("1130417963074")
	realDSupply = mustBigInt("403614364767661")
	realDRate   = mustBigInt("1206619920020")
	realCurve   = Curve{Util: 8000000, RBase: 300000, ROne: 400000, RTwo: 1200000, RThree: 50000000}
	realIRMod   = big.NewRat(15759132, 10_000_000)
	realBstop   = int32(2000000)
)

func mustBigInt(s string) *big.Int {
	n, ok := new(big.Int).SetString(s, 10)
	if !ok {
		panic("bad big.Int literal: " + s)
	}
	return n
}

func TestUtilization(t *testing.T) {
	t.Run("clamped to 1 when bSupply is zero but debt is outstanding", func(t *testing.T) {
		got := Utilization(big.NewInt(0), big.NewInt(999), big.NewInt(500), big.NewInt(999))
		assertRatEqual(t, big.NewRat(1, 1), got)
	})

	t.Run("clamped to 1 when bRate is zero but debt is outstanding", func(t *testing.T) {
		got := Utilization(big.NewInt(500), big.NewInt(0), big.NewInt(500), big.NewInt(999))
		assertRatEqual(t, big.NewRat(1, 1), got)
	})

	t.Run("clean 2/3 ratio", func(t *testing.T) {
		// liabilities = 100*2e12 = 2e14; supply = 200*1.5e12 = 3e14; util=2/3.
		got := Utilization(big.NewInt(200), mustBigInt("1500000000000"), big.NewInt(100), mustBigInt("2000000000000"))
		assertRatEqual(t, big.NewRat(2, 3), got)
	})

	t.Run("real mainnet vector", func(t *testing.T) {
		// util = (dSupply*dRate)/(bSupply*bRate)
		//      = 24350456626743911085123661/30208878726760821871629735
		// (Python: Fraction(d_supply*d_rate, b_supply*b_rate)) ≈ 0.8060695283328351
		want := bigRatFromDecimalString(t, "24350456626743911085123661", "30208878726760821871629735")
		got := Utilization(realBSupply, realBRate, realDSupply, realDRate)
		assertRatEqual(t, want, got)
	})

	// The contract clamps utilization at exactly 100% ("This is capped at
	// 100% to ensure interest calculations are fair", reserve.rs). Without
	// the clamp, a bad-debt reserve (liabilities > supply) would push
	// BorrowAPR's >95% branch past its [0,1] domain and display an
	// unbounded rate.
	t.Run("clamped to 1 when liabilities exceed supply", func(t *testing.T) {
		got := Utilization(big.NewInt(100), mustBigInt("1000000000000"), big.NewInt(150), mustBigInt("1000000000000"))
		assertRatEqual(t, big.NewRat(1, 1), got)
	})

	t.Run("clamped to 1 when liabilities equal supply", func(t *testing.T) {
		got := Utilization(big.NewInt(100), mustBigInt("1000000000000"), big.NewInt(100), mustBigInt("1000000000000"))
		assertRatEqual(t, big.NewRat(1, 1), got)
	})

	t.Run("clamped to 1 when supply is zero but liabilities are not", func(t *testing.T) {
		// The contract's branch order: liabilities==0 wins first, then
		// liabilities >= supply clamps — so zero supply with outstanding
		// debt is 100% utilization, not 0.
		got := Utilization(big.NewInt(0), big.NewInt(0), big.NewInt(100), mustBigInt("1000000000000"))
		assertRatEqual(t, big.NewRat(1, 1), got)
	})

	t.Run("zero when liabilities are zero regardless of supply", func(t *testing.T) {
		got := Utilization(big.NewInt(100), mustBigInt("1000000000000"), big.NewInt(0), big.NewInt(999))
		assertRatEqual(t, big.NewRat(0, 1), got)
	})
}

func TestBorrowAPR(t *testing.T) {
	// Synthetic curve: target=0.8, rBase=0.1, rOne=0.2, rTwo=0.3, rThree=0.4.
	// Expected values computed independently via Python fractions (see
	// verification sub-step's script); this table exercises all three
	// slopes plus the closed boundaries at util==target and util==0.95.
	c := Curve{Util: 8_000_000, RBase: 1_000_000, ROne: 2_000_000, RTwo: 3_000_000, RThree: 4_000_000}

	tests := []struct {
		name  string
		util  *big.Rat
		irMod *big.Rat
		want  *big.Rat
	}{
		{"util=0, irMod=1: base only", big.NewRat(0, 1), big.NewRat(1, 1), big.NewRat(1, 10)},
		{"util=target(0.8) boundary, closed: full r_one", big.NewRat(8, 10), big.NewRat(1, 1), big.NewRat(3, 10)},
		{"util=0.4 (half of target): half r_one", big.NewRat(4, 10), big.NewRat(1, 1), big.NewRat(1, 5)},
		{"target<util<=0.95: util=0.875 half r_two", big.NewRat(875, 1000), big.NewRat(1, 1), big.NewRat(45, 100)},
		{"util=0.95 boundary, closed: full r_two", big.NewRat(95, 100), big.NewRat(1, 1), big.NewRat(6, 10)},
		{"util>0.95: util=0.975 half r_three, irMod=1", big.NewRat(975, 1000), big.NewRat(1, 1), big.NewRat(8, 10)},
		{"util=1.0 (full r_three), irMod=1", big.NewRat(1, 1), big.NewRat(1, 1), big.NewRat(1, 1)},
		{"irMod=2 scales util<=target branch", big.NewRat(4, 10), big.NewRat(2, 1), big.NewRat(2, 5)},
		{
			// r_three must NOT be ir_mod-scaled: with irMod=2, expected =
			// 2*(0.1+0.2+0.3) + 0.5*0.4 = 1.2+0.2 = 1.4, NOT 2*(0.6+0.2)=1.6.
			"irMod=2 does NOT scale r_three in util>0.95 branch",
			big.NewRat(975, 1000), big.NewRat(2, 1), big.NewRat(7, 5),
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := BorrowAPR(tc.util, c, tc.irMod)
			assertRatEqual(t, tc.want, got, tc.name)
		})
	}

	t.Run("zero-target edge, reproduced from blend-sdk-js reserve.test.ts", func(t *testing.T) {
		// blend-sdk-js test "reserve v2 zero utilization applies interest
		// rate modifier with zero target utilization": config.util=0,
		// r_base=100000 (0.01), ir_mod=toFixed(1.5,7)=15000000 (1.5),
		// reserve has d_supply=0 so utilization is 0. Expected curIr=150000
		// (0.015) i.e. borrowApr := irMod*rBase = 1.5*0.01 = 0.015.
		zeroTargetCurve := Curve{Util: 0, RBase: 100000, ROne: 500000, RTwo: 5000000, RThree: 15000000}
		irMod := big.NewRat(15000000, 10_000_000)
		got := BorrowAPR(big.NewRat(0, 1), zeroTargetCurve, irMod)
		assertRatEqual(t, big.NewRat(15, 1000), got)
	})

	t.Run("real mainnet vector: target<util<=0.95 branch, irMod!=1", func(t *testing.T) {
		util := bigRatFromDecimalString(t, "24350456626743911085123661", "30208878726760821871629735")
		// borrow_apr = irmod*(r_base+r_one+(util-target)/(0.95-target)*r_two)
		//   = 59393658265844428560644300155817/503481312112680364527162250000000
		//   ≈ 0.11796596385399104 (Python fractions, independent of BorrowAPR).
		want := bigRatFromDecimalString(t, "59393658265844428560644300155817", "503481312112680364527162250000000")
		got := BorrowAPR(util, realCurve, realIRMod)
		assertRatEqual(t, want, got)
	})
}

func TestSupplyAPR(t *testing.T) {
	borrowAPR := big.NewRat(2, 10) // 0.2
	util := big.NewRat(1, 2)       // 0.5

	tests := []struct {
		name      string
		bstopRate int32
		want      *big.Rat
	}{
		{"0% take rate: full capture", 0, big.NewRat(1, 10)},
		{"10% take rate", 1_000_000, big.NewRat(9, 100)},
		{"100% take rate: nothing left for suppliers", 10_000_000, big.NewRat(0, 1)},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := SupplyAPR(borrowAPR, util, tc.bstopRate)
			assertRatEqual(t, tc.want, got)
		})
	}

	t.Run("real mainnet vector", func(t *testing.T) {
		borrowAPR := bigRatFromDecimalString(t, "59393658265844428560644300155817", "503481312112680364527162250000000")
		util := bigRatFromDecimalString(t, "24350456626743911085123661", "30208878726760821871629735")
		// supply_apr = borrow_apr*util*(1-0.2)
		//   ≈ 0.07607101507449186 (Python fractions, independent).
		want := bigRatFromDecimalString(t,
			"1446262699506094735699732868024913377601659328333013486037",
			"19012007373502969466268351231278862353401352836879687500000")
		got := SupplyAPR(borrowAPR, util, realBstop)
		assertRatEqual(t, want, got)
	})
}

func TestToAPY(t *testing.T) {
	t.Run("zero APR", func(t *testing.T) {
		assert.InDelta(t, 0.0, ToAPY(big.NewRat(0, 1), SupplyAPYCompoundingPeriods), 1e-12)
	})

	t.Run("small APR, weekly compounding (supply convention)", func(t *testing.T) {
		// (1+0.05/52)^52 - 1 = 0.051245841927200164 (python math).
		assert.InDelta(t, 0.051245841927200164, ToAPY(big.NewRat(5, 100), SupplyAPYCompoundingPeriods), 1e-9)
	})

	t.Run("large APR, daily compounding (borrow convention)", func(t *testing.T) {
		// (1+1.0/365)^365 - 1 = 1.7145674820219727 (python math).
		assert.InDelta(t, 1.7145674820219727, ToAPY(big.NewRat(1, 1), BorrowAPYCompoundingPeriods), 1e-9)
	})

	t.Run("negative guard: base exactly zero clamps to -1", func(t *testing.T) {
		// apr = -periods makes base = 1 + (-periods)/periods = 0.
		assert.Equal(t, -1.0, ToAPY(big.NewRat(-365, 1), 365))
	})

	t.Run("negative guard: base negative clamps to -1 instead of flipping sign", func(t *testing.T) {
		// apr=-1000, periods=52: base = 1-1000/52 = -18.23... < 0. Without the
		// guard, math.Pow(-18.23, 52) is a huge positive number (even
		// exponent) — a nonsensical "APY" for a >1800% per-period loss.
		assert.Equal(t, -1.0, ToAPY(big.NewRat(-1000, 1), 52))
	})
}

func TestProjectRates(t *testing.T) {
	rateOne := mustBigInt("1000000000000") // rate exactly 1.0 at SCALAR_12

	t.Run("zero elapsed returns unchanged rates", func(t *testing.T) {
		bRate := mustBigInt("1130417963074")
		dRate := mustBigInt("1206619920020")
		pB, pD := ProjectRates(bRate, dRate, big.NewInt(2), big.NewInt(1), big.NewRat(1, 10), 2_000_000, 1000, 1000)
		assert.Equal(t, bRate, pB)
		assert.Equal(t, dRate, pD)
	})

	t.Run("negative elapsed (stale request) returns unchanged rates", func(t *testing.T) {
		bRate := mustBigInt("1130417963074")
		dRate := mustBigInt("1206619920020")
		pB, pD := ProjectRates(bRate, dRate, big.NewInt(2), big.NewInt(1), big.NewRat(1, 10), 2_000_000, 1000, 900)
		assert.Equal(t, bRate, pB)
		assert.Equal(t, dRate, pD)
	})

	t.Run("zero liabilities: contract's util==0 short-circuit, rates unchanged", func(t *testing.T) {
		// Reserve::load bumps last_time and skips the rate update entirely
		// when utilization()==0 (reserve.rs) — without this branch dRate
		// would wrongly grow by irMod·rBase while nobody borrows.
		pB, pD := ProjectRates(rateOne, rateOne, big.NewInt(1_000_000), big.NewInt(0), big.NewRat(1, 100), 2_000_000, 0, SecondsPerYear)
		assert.Equal(t, rateOne, pB)
		assert.Equal(t, rateOne, pD)
	})

	t.Run("zero bSupply: contract's on-load guard, rates unchanged", func(t *testing.T) {
		pB, pD := ProjectRates(rateOne, rateOne, big.NewInt(0), big.NewInt(1_000_000), big.NewRat(1, 100), 2_000_000, 0, SecondsPerYear)
		assert.Equal(t, rateOne, pB)
		assert.Equal(t, rateOne, pD)
	})

	t.Run("1 year at known APR: clean round-number growth", func(t *testing.T) {
		// bRate=dRate=1e12 (rate 1.0), borrowAPR=0.1, bstop=0.2;
		// supplies give rawUtil = (1·1e12)/(2·1e12) = 0.5 ->
		// growthB = 0.1*0.5*0.8 = 0.04. Over exactly one year:
		// newD = 1e12*(1+0.1) = 1.1e12; newB = 1e12*(1+0.04) = 1.04e12.
		pB, pD := ProjectRates(rateOne, rateOne, big.NewInt(2), big.NewInt(1), big.NewRat(1, 10), 2_000_000, 0, SecondsPerYear)
		assert.Equal(t, mustBigInt("1040000000000"), pB)
		assert.Equal(t, mustBigInt("1100000000000"), pD)
	})

	t.Run("bad debt (liabilities > supply): bRate grows by the UNCLAMPED ratio", func(t *testing.T) {
		// rawUtil = 102/100 = 1.02 — past the clamp. The contract's accrual
		// split still applies the raw liabilities/supply ratio to bRate
		// (accrued·(1−bstop)/totalSupply), so with borrowAPR=0.5, bstop=0,
		// 1 year: newD = 1.5e12; newB = 1e12·(1+0.5·1.02) = 1.51e12.
		// A clamped ratio would understate newB at 1.5e12.
		pB, pD := ProjectRates(rateOne, rateOne, big.NewInt(100), big.NewInt(102), big.NewRat(1, 2), 0, 0, SecondsPerYear)
		assert.Equal(t, mustBigInt("1510000000000"), pB)
		assert.Equal(t, mustBigInt("1500000000000"), pD)
	})

	t.Run("real mainnet vector projected 6 seconds forward", func(t *testing.T) {
		// borrowAPR as computed in TestBorrowAPR's real vector; rawUtil is
		// derived internally from the same realB/DSupply·realB/DRate products
		// TestUtilization checks. lastTime=1783543369, now=1783543375 (Δt=6s,
		// the gap observed between the `get_reserve` and `get_config` RPC
		// calls made back-to-back while gathering this vector). Expected
		// pB/pD computed independently in Python using exact
		// fractions.Fraction arithmetic on the SAME formula, then rounded
		// half-away-from-zero: pD=1206619947101, pB=1130417979435. (The
		// contract's own fixed-point ceil/floor path lands 1 unit higher on
		// pD — this pins the exact-rational form, not the on-chain rounding.)
		borrowAPR := bigRatFromDecimalString(t, "59393658265844428560644300155817", "503481312112680364527162250000000")
		pB, pD := ProjectRates(realBRate, realDRate, realBSupply, realDSupply, borrowAPR, realBstop, 1783543369, 1783543375)
		assert.Equal(t, mustBigInt("1130417979435"), pB)
		assert.Equal(t, mustBigInt("1206619947101"), pD)
	})
}

func TestUnderlying(t *testing.T) {
	t.Run("clean conversion", func(t *testing.T) {
		got := Underlying(big.NewInt(1_000_000), mustBigInt("2000000000000"))
		assert.Equal(t, big.NewInt(2_000_000), got)
	})

	t.Run("truncates toward zero, does not round", func(t *testing.T) {
		// 3 * 1500000000001 = 4500000000003; /1e12 = 4.500000000003 -> 4.
		got := Underlying(big.NewInt(3), mustBigInt("1500000000001"))
		assert.Equal(t, big.NewInt(4), got)
	})

	t.Run("zero tokens", func(t *testing.T) {
		got := Underlying(big.NewInt(0), mustBigInt("1500000000001"))
		assert.Equal(t, big.NewInt(0), got)
	})
}

func TestEmissionsAPR(t *testing.T) {
	t.Run("expired (now == expiration) is zero", func(t *testing.T) {
		got := EmissionsAPR(10589234129794, 1000, 1000, big.NewRat(8, 100), big.NewRat(1_000_000, 1))
		assertRatEqual(t, big.NewRat(0, 1), got)
	})

	t.Run("expired (now > expiration) is zero", func(t *testing.T) {
		got := EmissionsAPR(10589234129794, 1000, 1001, big.NewRat(8, 100), big.NewRat(1_000_000, 1))
		assertRatEqual(t, big.NewRat(0, 1), got)
	})

	t.Run("sideUSD zero is zero", func(t *testing.T) {
		got := EmissionsAPR(10589234129794, 2000, 1000, big.NewRat(8, 100), big.NewRat(0, 1))
		assertRatEqual(t, big.NewRat(0, 1), got)
	})

	t.Run("sideUSD negative is zero", func(t *testing.T) {
		got := EmissionsAPR(10589234129794, 2000, 1000, big.NewRat(8, 100), big.NewRat(-1, 1))
		assertRatEqual(t, big.NewRat(0, 1), got)
	})

	t.Run("real eps/expiration vector (mainnet USDC dToken emission) with illustrative USD figures", func(t *testing.T) {
		// eps=10589234129794 and expiration=1784095601 fetched 2026-07-08 via
		// `stellar contract invoke ... get_reserve_emissions
		// --reserve_token_index 2` against the same mainnet pool/reserve as
		// TestUtilization's vector (reserve index 1 -> dToken index 2).
		// blndUSD=$0.08 and sideUSD=$1,000,000 are illustrative round numbers
		// (not fetched on-chain) chosen so the expected fraction is easy to
		// state; the eps/expiration values are real.
		// annualBLND = (10589234129794/1e14)*31536000 = 10435690234911987/3125000000
		// apr = annualBLND*0.08/1000000 = 10435690234911987/39062500000000000
		//     ≈ 0.26715367001374685 (Python fractions, independent).
		want := bigRatFromDecimalString(t, "10435690234911987", "39062500000000000")
		got := EmissionsAPR(10589234129794, 1784095601, 1783543375, big.NewRat(8, 100), big.NewRat(1_000_000, 1))
		assertRatEqual(t, want, got)
	})
}

func TestProjectEmissionIndex(t *testing.T) {
	scalar7Big := big.NewInt(10_000_000)

	t.Run("lastTime at expiration: unchanged", func(t *testing.T) {
		got := ProjectEmissionIndex(big.NewInt(12345), 1_000_000_000_000, 1600000000, 1600000000, 1700000000, big.NewInt(1_000_000_000), scalar7Big)
		assert.Equal(t, big.NewInt(12345), got)
	})

	t.Run("lastTime at or past now: unchanged", func(t *testing.T) {
		got := ProjectEmissionIndex(big.NewInt(12345), 1_000_000_000_000, 1500000000, 1600000000, 1500000000, big.NewInt(1_000_000_000), scalar7Big)
		assert.Equal(t, big.NewInt(12345), got)
	})

	t.Run("zero eps: unchanged", func(t *testing.T) {
		got := ProjectEmissionIndex(big.NewInt(12345), 0, 1500000000, 1600000000, 1500000005, big.NewInt(1_000_000_000), scalar7Big)
		assert.Equal(t, big.NewInt(12345), got)
	})

	t.Run("zero supply: unchanged (all backstop shares queued)", func(t *testing.T) {
		got := ProjectEmissionIndex(big.NewInt(12345), 1_000_000_000_000, 1500000000, 1600000000, 1500000005, big.NewInt(0), scalar7Big)
		assert.Equal(t, big.NewInt(12345), got)
	})

	t.Run("result is a copy, input index untouched", func(t *testing.T) {
		index := big.NewInt(12345)
		got := ProjectEmissionIndex(index, 1_000_000_000_000, 1500000000, 1600000000, 1500000005, big.NewInt(1_000_000_000), scalar7Big)
		assert.NotEqual(t, index, got)
		assert.Equal(t, big.NewInt(12345), index)
	})

	t.Run("real contract-test vector: projection clamps at expiration (test_update_emission_data_past_exp)", func(t *testing.T) {
		// blend-contracts-v2 pool/src/emissions/distributor.rs: now=1700000000
		// is past expiration=1600000001, so Δt=100000001 from
		// last_time=1500000000; eps=0_01000000000000, supply=100_0000000,
		// supply_scalar=1e7. Contract asserts index == 10012_34577890000000.
		got := ProjectEmissionIndex(
			mustBigInt("1234567890000000"),
			1_000_000_000_000,
			1500000000, 1600000001, 1700000000,
			big.NewInt(100_0000000),
			scalar7Big,
		)
		assert.Equal(t, mustBigInt("1001234577890000000"), got)
	})

	t.Run("real contract-test vector: floor division (test_update_emission_data_rounds_down)", func(t *testing.T) {
		// Δt=5, eps=0_01000000000000, supply=100_0001111 (indivisible),
		// supply_scalar=1e7. Contract asserts index == 1234617889944450.
		got := ProjectEmissionIndex(
			mustBigInt("1234567890000000"),
			1_000_000_000_000,
			1500000000, 1600000000, 1500000005,
			big.NewInt(100_0001111),
			scalar7Big,
		)
		assert.Equal(t, mustBigInt("1234617889944450"), got)
	})
}

func TestClaimableEmissions(t *testing.T) {
	t.Run("zero balance: no accrual regardless of index delta", func(t *testing.T) {
		got := ClaimableEmissions(big.NewInt(500), big.NewInt(0), big.NewInt(999_999_999), big.NewInt(0), mustBigInt("100000000000000"))
		assert.Equal(t, big.NewInt(500), got)
	})

	t.Run("zero index delta: no accrual", func(t *testing.T) {
		got := ClaimableEmissions(big.NewInt(500), big.NewInt(12345), big.NewInt(12345), big.NewInt(999), mustBigInt("100000000000000"))
		assert.Equal(t, big.NewInt(500), got)
	})

	t.Run("real contract-test vector (blend-contracts-v2 pool/src/emissions/distributor.rs, test_update_user_emissions_accrues)", func(t *testing.T) {
		// accrued=0_1000000 (1000000), user index=567890000000,
		// reserve index=1234567890000000, balance=0_5000000 (5000000),
		// supply_scalar=1_0000000 (1e7 reserve decimals=7) so
		// scalar=supply_scalar*SCALAR_7=1e14. Contract asserts
		// new_user_emission_data.accrued == 6_2700000 (62700000).
		got := ClaimableEmissions(
			big.NewInt(1_000_000),
			mustBigInt("567890000000"),
			mustBigInt("1234567890000000"),
			big.NewInt(5_000_000),
			mustBigInt("100000000000000"),
		)
		assert.Equal(t, big.NewInt(62_700_000), got)
	})

	t.Run("negative index delta clamps to accrued only", func(t *testing.T) {
		// A user index ahead of the stream index can only mean an ingestion
		// gap (impossible on-chain); the claimable must not go negative.
		got := ClaimableEmissions(big.NewInt(500), big.NewInt(2000), big.NewInt(1000), big.NewInt(999_999), mustBigInt("100000000000000"))
		assert.Equal(t, big.NewInt(500), got)
	})

	t.Run("varying per-reserve scalar (non-7-decimal reserve)", func(t *testing.T) {
		// A 5-decimal reserve: supply_scalar=1e5, scalar=1e5*1e7=1e12.
		// toAccrue = floor(4e12*500/1e12) = 2000.
		got := ClaimableEmissions(
			big.NewInt(0),
			big.NewInt(0),
			big.NewInt(500),
			mustBigInt("4000000000000"),
			mustBigInt("1000000000000"),
		)
		assert.Equal(t, big.NewInt(2000), got)
	})
}

func TestEarnedInterest(t *testing.T) {
	t.Run("positive earned interest", func(t *testing.T) {
		// underlying = (1000000+500000)*2e12/1e12 = 3000000; earned =
		// 3000000-2000000 = 1000000.
		got := EarnedInterest(big.NewInt(1_000_000), big.NewInt(500_000), mustBigInt("2000000000000"), big.NewInt(2_000_000))
		assert.Equal(t, big.NewInt(1_000_000), got)
	})

	t.Run("negative after full exit", func(t *testing.T) {
		// No remaining b/dToken balance but netSupplied still carries a
		// stale principal figure (e.g. dust or timing skew) -> underlying=0,
		// earned = 0-500000 = -500000.
		got := EarnedInterest(big.NewInt(0), big.NewInt(0), mustBigInt("2000000000000"), big.NewInt(500_000))
		assert.Equal(t, big.NewInt(-500_000), got)
	})
}

func TestBackstopLPTokens(t *testing.T) {
	t.Run("clean conversion", func(t *testing.T) {
		got := BackstopLPTokens(big.NewInt(100), big.NewInt(1000), big.NewInt(5000))
		assert.Equal(t, big.NewInt(500), got)
	})

	t.Run("zero when poolShares is zero", func(t *testing.T) {
		got := BackstopLPTokens(big.NewInt(100), big.NewInt(0), big.NewInt(5000))
		assert.Equal(t, big.NewInt(0), got)
	})

	t.Run("truncates toward zero", func(t *testing.T) {
		// 7*10/3 = 23.33... -> 23.
		got := BackstopLPTokens(big.NewInt(7), big.NewInt(3), big.NewInt(10))
		assert.Equal(t, big.NewInt(23), got)
	})
}

func TestUSDValue(t *testing.T) {
	t.Run("7-decimal token, 14-decimal price", func(t *testing.T) {
		// 1.0 token (1e7 raw) at $2.0 (2e14 raw, 14-dec) = $2.0.
		got := USDValue(mustBigInt("10000000"), 7, mustBigInt("200000000000000"), 14)
		assertRatEqual(t, big.NewRat(2, 1), got)
	})

	t.Run("6-decimal token, 7-decimal price", func(t *testing.T) {
		// 1.5 USDC (1500000 raw, 6-dec) at $3.0 (30000000 raw, 7-dec) = $4.5.
		got := USDValue(big.NewInt(1_500_000), 6, big.NewInt(30_000_000), 7)
		assertRatEqual(t, big.NewRat(9, 2), got)
	})

	t.Run("exact non-terminating-decimal fraction stays exact", func(t *testing.T) {
		// 1 raw unit of a 7-decimal token ($3, 0-decimal price) = 3/1e7 —
		// not representable exactly as a terminating decimal at every
		// intermediate step if computed via floats, but exact as a Rat.
		got := USDValue(big.NewInt(1), 7, big.NewInt(3), 0)
		assertRatEqual(t, big.NewRat(3, 10_000_000), got)
	})

	t.Run("zero amount", func(t *testing.T) {
		got := USDValue(big.NewInt(0), 7, big.NewInt(30_000_000), 7)
		assertRatEqual(t, big.NewRat(0, 1), got)
	})
}

func TestParseDecimalToBigInt(t *testing.T) {
	tests := []struct {
		name    string
		in      string
		want    *big.Int
		wantErr bool
	}{
		{"plain integer", "42", big.NewInt(42), false},
		{"decimal with trailing zeros (postgres numeric shape)", "1100.0000000000000000", big.NewInt(1100), false},
		{"decimal truncates fractional part", "5.999999", big.NewInt(5), false},
		{"negative integer", "-7", big.NewInt(-7), false},
		{"negative decimal truncates toward zero", "-5.999999", big.NewInt(-5), false},
		{"explicit positive sign", "+9", big.NewInt(9), false},
		{"zero", "0", big.NewInt(0), false},
		{"empty string errors", "", nil, true},
		{"garbage errors", "not-a-number", nil, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := ParseDecimalToBigInt(tc.in)
			if tc.wantErr {
				assert.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}
}
