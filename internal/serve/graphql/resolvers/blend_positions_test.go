package resolvers

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math/big"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stellar/go-stellar-sdk/keypair"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/wallet-backend/internal/data"
	blenddata "github.com/stellar/wallet-backend/internal/data/blend"
	"github.com/stellar/wallet-backend/internal/indexer/types"
	"github.com/stellar/wallet-backend/internal/metrics"
	graphql1 "github.com/stellar/wallet-backend/internal/serve/graphql/generated"
)

// TestClaimableStream covers the contract's three update_user_emissions
// branches (pool/src/emissions/distributor.rs @ ba22b487), most importantly
// the "user had tokens before emissions began" branch: a holder with a
// balance but NO UserEmissionData entry is owed balance*index/scalar the
// moment an emission stream exists — not zero.
func TestClaimableStream(t *testing.T) {
	scalar := big.NewInt(100_000_000_000_000)  // 1e14: 7-decimal token, 10^7 * SCALAR_7
	index := big.NewInt(1_234_000_000_000_000) // 1.234e15

	t.Run("no user row, balance held: historical accrual owed", func(t *testing.T) {
		// balance*index/scalar = 5_000_000 * 1.234e15 / 1e14 = 61_700_000 —
		// the contract's own test vector for this branch minus the
		// pre-accrued 1_000_000 (there is no user row to carry accrued).
		got, err := claimableStream(blenddata.Emission{}, false, index, big.NewInt(5_000_000), scalar)
		require.NoError(t, err)
		assert.Equal(t, "61700000", got.String())
	})

	t.Run("no user row, no emission config: zero", func(t *testing.T) {
		got, err := claimableStream(blenddata.Emission{}, false, nil, big.NewInt(5_000_000), scalar)
		require.NoError(t, err)
		assert.Equal(t, "0", got.String())
	})

	t.Run("no user row, zero balance: zero", func(t *testing.T) {
		got, err := claimableStream(blenddata.Emission{}, false, index, big.NewInt(0), scalar)
		require.NoError(t, err)
		assert.Equal(t, "0", got.String())
	})

	t.Run("user row present: accrued plus indexed delta", func(t *testing.T) {
		// The contract's test_update_user_emissions_accrues vector:
		// 1_000_000 + floor(5_000_000 * 1.234e15 / 1e14) = 62_700_000.
		userEmission := blenddata.Emission{Accrued: "1000000", EmissionIndex: "0"}
		got, err := claimableStream(userEmission, true, index, big.NewInt(5_000_000), scalar)
		require.NoError(t, err)
		assert.Equal(t, "62700000", got.String())
	})
}

// TestProjectedEmissionIndexHelpers covers the resolver-side projection
// wrappers around rates.go's ProjectEmissionIndex: an idle-but-active stream
// must report a claimable-now index ahead of the stored one, spread over the
// right supply (reserve: the side's raw token supply; backstop: unqueued
// shares only).
func TestProjectedEmissionIndexHelpers(t *testing.T) {
	t.Run("reserve: idle active stream projects forward", func(t *testing.T) {
		d := &blendAssembly{
			now: 1_500_000_100,
			reserveEmissionByPoolToken: map[string]blenddata.ReserveEmission{
				poolTokenKey("POOL", 1): {
					Eps:           1_000_000_000_000, // 0.01 BLND/s at 1e14
					EmissionIndex: "1000000",
					Expiration:    1_600_000_000,
					LastTime:      1_500_000_000,
				},
			},
		}
		// Δt=100, supply=1e9 (100.0000000 of a 7-dec token), scalar=1e7:
		// additional = 100*1e12*1e7/1e9 = 1e12.
		got, err := d.projectedReserveEmissionIndex("POOL", 1, big.NewInt(1_000_000_000), 7)
		require.NoError(t, err)
		assert.Equal(t, "1000001000000", got.String())
	})

	t.Run("reserve: unconfigured stream is nil", func(t *testing.T) {
		d := &blendAssembly{now: 1_500_000_100, reserveEmissionByPoolToken: map[string]blenddata.ReserveEmission{}}
		got, err := d.projectedReserveEmissionIndex("POOL", 1, big.NewInt(1_000_000_000), 7)
		require.NoError(t, err)
		assert.Nil(t, got)
	})

	t.Run("backstop: projects over unqueued shares only", func(t *testing.T) {
		eps := int64(1_000_000_000_000)
		lastTime := int64(1_500_000_000)
		expiration := int64(1_600_000_000)
		emisIndex := "5000"
		bp := blenddata.BackstopPool{
			Shares:         "3000000000",
			Q4W:            "1000000000",
			EmisEps:        &eps,
			EmisIndex:      &emisIndex,
			EmisExpiration: &expiration,
			EmisLastTime:   &lastTime,
		}
		// unqueued = 3e9-1e9 = 2e9 shares; Δt=100, scalar=1e7:
		// additional = 100*1e12*1e7/2e9 = 5e11.
		got, err := projectedBackstopEmissionIndex(bp, big.NewInt(3_000_000_000), 1_500_000_100)
		require.NoError(t, err)
		assert.Equal(t, "500000005000", got.String())
	})

	t.Run("backstop: no emission config is nil", func(t *testing.T) {
		got, err := projectedBackstopEmissionIndex(blenddata.BackstopPool{Shares: "3000000000", Q4W: "0"}, big.NewInt(3_000_000_000), 1_500_000_100)
		require.NoError(t, err)
		assert.Nil(t, got)
	})
}

// TestAccountResolver_BlendPositions is a hand-computed vector exercising the
// full Account.blendPositions assembly: two reserves in one pool (a 7-decimal
// supply-only reserve r0, a 6-decimal debt-only reserve r1), one active and
// one expired reserve emission stream, a backstop deposit with two queued
// withdrawals, and lifetime CLAIM totals for both the pool and the backstop.
//
// Both reserves' last_time is set far in the future so ProjectRates'
// documented Δt<=0 branch (rates.go) returns bRate/dRate UNCHANGED
// deterministically — this exercises the real ProjectRates call path while
// keeping every downstream number (which otherwise depends on wall-clock
// "now" at resolver-execution time) exactly hand-computable, with no
// wall-clock flakiness risk.
//
// r0 curve: target=0.8 (util=8_000_000), r_base=0.01 (100_000), r_one=0.02
// (200_000), ir_mod=1.0. Pool-wide b_supply=100_000_000, d_supply=40_000_000,
// b_rate=d_rate=1e12 (rate exactly 1.0) => util=40_000_000/100_000_000=0.4.
// util<=target: utilScalar=0.4/0.8=0.5; borrowApr=irMod*(rBase+utilScalar*rOne)
// = 1.0*(0.01+0.5*0.02) = 0.02. bstopRate=0.2 (2_000_000) =>
// supplyApr=borrowApr*util*(1-bstopRate)=0.02*0.4*0.8=0.0064.
// supplyApy=(1+0.0064/52)^52-1=0.006420127418405475 (python).
// borrowApy=(1+0.02/365)^365-1=0.020200781032923 (python).
//
// r1 curve: same target/r_base/r_one/ir_mod as r0. Pool-wide
// b_supply=20_000_000, d_supply=10_000_000 => util=0.5. util<=target:
// utilScalar=0.5/0.8=0.625; borrowApr=1.0*(0.01+0.625*0.02)=0.0225.
// supplyApr=0.0225*0.5*0.8=0.009.
// supplyApy=(1+0.009/52)^52-1=0.00903983597743796 (python).
// borrowApy=(1+0.0225/365)^365-1=0.022754324920237545 (python).
//
// Prices (all 7-decimal raw fixed point): assetA=$2.50 (25_000_000),
// assetB=$1.00 (10_000_000), Comet LP self-price=$1.10 (11_000_000),
// BLND=$0.05 (500_000).
func TestAccountResolver_BlendPositions(t *testing.T) {
	account := keypair.MustRandom().Address()
	poolAddr := randomContractAddress(t)
	oracleAddr := randomContractAddress(t)
	assetA := randomContractAddress(t) // r0's asset, 7 decimals
	assetB := randomContractAddress(t) // r1's asset, 6 decimals
	cometAddr := randomContractAddress(t)
	blndAddr := randomContractAddress(t)

	futureLastTime := int64(4102444800)   // year 2100: guarantees ProjectRates' deltaT<=0 branch
	futureExpiration := int64(4102444800) // r0's bToken emission: active relative to "now"
	pastExpiration := int64(1700000000)   // r1's dToken emission: expired relative to "now"

	m := metrics.NewMetrics(prometheus.NewRegistry())
	dbMetrics := m.DB
	blendModels := blenddata.NewModels(testDBConnectionPool, dbMetrics)
	models := &data.Models{
		Contract:     &data.ContractModel{DB: testDBConnectionPool, Metrics: dbMetrics},
		StateChanges: &data.StateChangeModel{DB: testDBConnectionPool, Metrics: dbMetrics},
		Blend:        blendModels,
	}
	resolver := &accountResolver{&Resolver{models: models, blendBackstopLPContractID: cometAddr}}

	// --- contract_tokens metadata (name/symbol only; decimals authority is blend_reserves) ---
	execTestDB(t, `
		INSERT INTO contract_tokens (id, contract_id, type, name, symbol, decimals) VALUES
		($1, $2, 'sep41', 'Asset A', 'AAA', 7), ($3, $4, 'sep41', 'Asset B', 'BBB', 6)`,
		data.DeterministicContractID(assetA), assetA, data.DeterministicContractID(assetB), assetB)

	// --- pool ---
	execTestDB(t, `
		INSERT INTO blend_pools (pool_contract_id, name, oracle_contract_id, backstop_rate, status, max_positions, last_modified_ledger)
		VALUES ($1, 'Test Pool', $2, 2000000, 0, 4, 100)`,
		types.AddressBytea(poolAddr), types.AddressBytea(oracleAddr))

	// --- reserves ---
	execTestDB(t, `
		INSERT INTO blend_reserves (
			pool_contract_id, reserve_index, asset_contract_id,
			b_rate, d_rate, b_supply, d_supply, ir_mod, backstop_credit, last_time,
			decimals, c_factor, l_factor, util, max_util,
			r_base, r_one, r_two, r_three, reactivity, supply_cap, enabled, last_modified_ledger
		) VALUES
		($1, 0, $2, '1000000000000', '1000000000000', '100000000', '40000000', '10000000', '0', $3,
			7, 9500000, 9500000, 8000000, 9500000, 100000, 200000, 5000000, 15000000, 0, '0', true, 100),
		($1, 1, $4, '1000000000000', '1000000000000', '20000000', '10000000', '10000000', '0', $3,
			6, 9500000, 9500000, 8000000, 9500000, 100000, 200000, 5000000, 15000000, 0, '0', true, 100)`,
		types.AddressBytea(poolAddr), types.AddressBytea(assetA), futureLastTime, types.AddressBytea(assetB))

	// --- account positions: r0 supply+collateral (net_supplied is a decimal
	// string with a truncated fractional remainder), r1 liability only ---
	execTestDB(t, `
		INSERT INTO blend_positions (
			pool_contract_id, user_account_id, reserve_index,
			supply_b_tokens, collateral_b_tokens, liability_d_tokens, net_supplied, net_borrowed, last_modified_ledger
		) VALUES
		($1, $2, 0, '5000000', '3000000', '0', '7999000.7500000000000000', '0', 100),
		($1, $2, 1, '0', '0', '6000000', '0', '5990000', 100)`,
		types.AddressBytea(poolAddr), types.AddressBytea(account))

	// --- reserve emissions: r0's bToken (index*2+1=1) active, r1's dToken (index*2=2) expired ---
	// last_time pins ProjectEmissionIndex to its documented no-op branches,
	// mirroring the reserves' futureLastTime trick: the active stream's
	// last_time is in year 2100 (lastTime >= now), the expired stream's sits
	// exactly at its expiration (lastTime >= expiration). Emission indexes
	// stay exactly the inserted values, hand-computable with no wall-clock
	// dependence; projection math itself is covered by
	// TestProjectedEmissionIndexHelpers and rates_test.go.
	execTestDB(t, `
		INSERT INTO blend_reserve_emissions (pool_contract_id, reserve_token_id, eps, emission_index, expiration, last_time, last_modified_ledger)
		VALUES
		($1, 1, 100000000000, '2000000000000', $2, $2, 100),
		($1, 2, 999999999999, '3000000000000', $3, $3, 100)`,
		types.AddressBytea(poolAddr), futureExpiration, pastExpiration)

	// --- user emission accrual: r0 bToken stream (token_id=1), r1 dToken stream (token_id=2) ---
	execTestDB(t, `
		INSERT INTO blend_emissions (source_contract_id, user_account_id, token_id, emission_index, accrued, last_modified_ledger)
		VALUES
		($1, $2, 1, '1000000000000', '500', 100),
		($1, $2, 2, '1000000000000', '200', 100)`,
		types.AddressBytea(poolAddr), types.AddressBytea(account))

	// --- backstop position + pool-wide backstop totals/emission config ---
	q4wJSON, err := json.Marshal([]blenddata.Q4W{
		{Amount: "100000", Expiration: 1800000000},
		{Amount: "200000", Expiration: 1900000000},
	})
	require.NoError(t, err)
	execTestDB(t, `
		INSERT INTO blend_backstop_positions (pool_contract_id, user_account_id, shares, q4w, last_modified_ledger)
		VALUES ($1, $2, '1000000', $3::jsonb, 100)`,
		types.AddressBytea(poolAddr), types.AddressBytea(account), string(q4wJSON))
	// emis_last_time in year 2100 pins ProjectEmissionIndex's lastTime>=now
	// no-op branch, same as the reserve emission rows above.
	execTestDB(t, `
		INSERT INTO blend_backstop_pools (pool_contract_id, shares, tokens, q4w, emis_eps, emis_index, emis_expiration, emis_last_time, last_modified_ledger)
		VALUES ($1, '10000000', '50000000', '0', 999999999999, '5000000000000', 4102444800, 4102444800, 100)`,
		types.AddressBytea(poolAddr))
	// backstop emission stream: token_id = BackstopEmissionTokenID (-1), source = pool
	execTestDB(t, `
		INSERT INTO blend_emissions (source_contract_id, user_account_id, token_id, emission_index, accrued, last_modified_ledger)
		VALUES ($1, $2, -1, '2000000000000', '1000', 100)`,
		types.AddressBytea(poolAddr), types.AddressBytea(account))

	// --- oracle prices: reserve assets under the pool's oracle, Comet leg under its own address ---
	execTestDB(t, `
		INSERT INTO blend_oracle_prices (oracle_contract_id, asset_contract_id, price, price_decimals, price_timestamp)
		VALUES
		($1, $2, '25000000', 7, EXTRACT(EPOCH FROM NOW())::bigint),
		($1, $3, '10000000', 7, EXTRACT(EPOCH FROM NOW())::bigint),
		($4, $4, '11000000', 7, EXTRACT(EPOCH FROM NOW())::bigint),
		($4, $5, '500000', 7, EXTRACT(EPOCH FROM NOW())::bigint)`,
		types.AddressBytea(oracleAddr), types.AddressBytea(assetA), types.AddressBytea(assetB),
		types.AddressBytea(cometAddr), types.AddressBytea(blndAddr))

	// --- lifetime claimed totals: pool-source 3000 BLND, backstop-source 4000 LP ---
	insertPoolClaimed(t, account, poolAddr, "3000")
	insertBackstopClaimed(t, account, "4000")

	t.Cleanup(func() {
		execTestDB(t, `DELETE FROM blend_positions WHERE pool_contract_id = $1`, types.AddressBytea(poolAddr))
		execTestDB(t, `DELETE FROM blend_reserves WHERE pool_contract_id = $1`, types.AddressBytea(poolAddr))
		execTestDB(t, `DELETE FROM blend_reserve_emissions WHERE pool_contract_id = $1`, types.AddressBytea(poolAddr))
		execTestDB(t, `DELETE FROM blend_emissions WHERE source_contract_id = $1`, types.AddressBytea(poolAddr))
		execTestDB(t, `DELETE FROM blend_backstop_positions WHERE pool_contract_id = $1`, types.AddressBytea(poolAddr))
		execTestDB(t, `DELETE FROM blend_backstop_pools WHERE pool_contract_id = $1`, types.AddressBytea(poolAddr))
		execTestDB(t, `DELETE FROM blend_pools WHERE pool_contract_id = $1`, types.AddressBytea(poolAddr))
		execTestDB(t, `DELETE FROM blend_oracle_prices WHERE oracle_contract_id IN ($1, $2)`, types.AddressBytea(oracleAddr), types.AddressBytea(cometAddr))
		execTestDB(t, `DELETE FROM contract_tokens WHERE contract_id IN ($1, $2)`, assetA, assetB)
		execTestDB(t, `DELETE FROM blend_pool_claimed WHERE pool_contract_id = $1`, types.AddressBytea(poolAddr))
		execTestDB(t, `DELETE FROM blend_backstop_claimed WHERE user_account_id = $1`, types.AddressBytea(account))
	})

	parentAccount := &types.Account{StellarAddress: types.AddressBytea(account)}
	got, err := resolver.BlendPositions(testCtx, parentAccount)
	require.NoError(t, err)
	require.NotNil(t, got)

	assert.Equal(t, "4000", got.BackstopClaimedLp)
	require.Len(t, got.Pools, 1)
	require.Len(t, got.Backstop, 1)

	pool := got.Pools[0]
	assert.Equal(t, poolAddr, pool.PoolAddress)
	require.NotNil(t, pool.PoolName)
	assert.Equal(t, "Test Pool", *pool.PoolName)
	assert.Equal(t, "3000", pool.ClaimedBlnd)
	require.Len(t, pool.Reserves, 2)

	r0, r1 := pool.Reserves[0], pool.Reserves[1]
	assert.Equal(t, assetA, r0.AssetContractID)
	assert.Equal(t, assetB, r1.AssetContractID)

	t.Run("r0: supply+collateral reserve, active bToken emissions", func(t *testing.T) {
		require.NotNil(t, r0.TokenName)
		assert.Equal(t, "Asset A", *r0.TokenName)
		require.NotNil(t, r0.TokenSymbol)
		assert.Equal(t, "AAA", *r0.TokenSymbol)
		require.NotNil(t, r0.TokenDecimals)
		assert.EqualValues(t, 7, *r0.TokenDecimals)

		// ProjectRates' deltaT<=0 branch: rates pass through unchanged (rate=1.0), so
		// underlying == raw token amount.
		assert.Equal(t, "5000000", r0.SuppliedTokens)
		assert.Equal(t, "3000000", r0.CollateralTokens)
		assert.Equal(t, "0", r0.BorrowedTokens)

		require.NotNil(t, r0.SuppliedUsd)
		assert.InDelta(t, 2.0, *r0.SuppliedUsd, 1e-9)
		require.NotNil(t, r0.BorrowedUsd)
		assert.InDelta(t, 0.0, *r0.BorrowedUsd, 1e-9)

		require.NotNil(t, r0.SupplyApy)
		assert.InDelta(t, 0.006420127418405475, *r0.SupplyApy, 1e-12)
		require.NotNil(t, r0.BorrowApy)
		assert.InDelta(t, 0.020200781032923, *r0.BorrowApy, 1e-12)

		// Supply and borrow emission streams are independent: r0's bToken stream
		// (token_id=1) is active, its dToken stream (token_id=0) is unconfigured.
		require.NotNil(t, r0.EmissionsSupplyApr)
		assert.InDelta(t, 63.072, *r0.EmissionsSupplyApr, 1e-9)
		require.NotNil(t, r0.EmissionsBorrowApr)
		assert.InDelta(t, 0.0, *r0.EmissionsBorrowApr, 1e-12)

		require.NotNil(t, r0.PriceUsd)
		assert.InDelta(t, 2.5, *r0.PriceUsd, 1e-12)

		// interestEarned = Underlying(supply+collateral, bRate) - netSupplied
		//   = 8_000_000 - 7_999_000 (net_supplied's ".75..." truncates away) = 1_000.
		assert.Equal(t, "1000", r0.InterestEarned)
		assert.Equal(t, "0", r0.InterestPaid)

		// claimable = accrued(500) + tokenBalance(8_000_000)*(emisIndex-userIndex)(1e12)/scalar(1e14)
		//   = 500 + 80_000 = 80_500. No dToken-stream row for r0 -> contributes 0.
		assert.Equal(t, "80500", r0.EmissionsEarnedBlnd)
		require.NotNil(t, r0.EmissionsEarnedUsd)
		assert.InDelta(t, 0.0004025, *r0.EmissionsEarnedUsd, 1e-12)
	})

	t.Run("r1: debt-only reserve, expired dToken emissions", func(t *testing.T) {
		require.NotNil(t, r1.TokenDecimals)
		assert.EqualValues(t, 6, *r1.TokenDecimals)

		assert.Equal(t, "0", r1.SuppliedTokens)
		assert.Equal(t, "0", r1.CollateralTokens)
		assert.Equal(t, "6000000", r1.BorrowedTokens)

		require.NotNil(t, r1.SuppliedUsd)
		assert.InDelta(t, 0.0, *r1.SuppliedUsd, 1e-9)
		require.NotNil(t, r1.BorrowedUsd)
		assert.InDelta(t, 6.0, *r1.BorrowedUsd, 1e-9)

		require.NotNil(t, r1.SupplyApy)
		assert.InDelta(t, 0.00903983597743796, *r1.SupplyApy, 1e-12)
		require.NotNil(t, r1.BorrowApy)
		assert.InDelta(t, 0.022754324920237545, *r1.BorrowApy, 1e-12)

		// r1's dToken emission config (token_id=2) is EXPIRED (pastExpiration <
		// now) -> a concrete 0, not nil; its bToken stream (token_id=3) is
		// unconfigured -> also a concrete 0.
		require.NotNil(t, r1.EmissionsBorrowApr)
		assert.InDelta(t, 0.0, *r1.EmissionsBorrowApr, 1e-12)
		require.NotNil(t, r1.EmissionsSupplyApr)
		assert.InDelta(t, 0.0, *r1.EmissionsSupplyApr, 1e-12)

		require.NotNil(t, r1.PriceUsd)
		assert.InDelta(t, 1.0, *r1.PriceUsd, 1e-12)

		// interestPaid = Underlying(liability, dRate) - netBorrowed = 6_000_000 - 5_990_000 = 10_000.
		assert.Equal(t, "0", r1.InterestEarned)
		assert.Equal(t, "10000", r1.InterestPaid)

		// claimable = accrued(200) + tokenBalance(6_000_000)*(emisIndex-userIndex)(2e12)/scalar(1e13)
		//   = 200 + 1_200_000 = 1_200_200. Claimable is computed even though the
		// stream is expired: expiry only zeroes the APR, not the accrued index delta.
		assert.Equal(t, "1200200", r1.EmissionsEarnedBlnd)
		require.NotNil(t, r1.EmissionsEarnedUsd)
		assert.InDelta(t, 0.006001, *r1.EmissionsEarnedUsd, 1e-12)
	})

	t.Run("pool rollup", func(t *testing.T) {
		require.NotNil(t, pool.SuppliedUsd)
		assert.InDelta(t, 2.0, *pool.SuppliedUsd, 1e-9)
		require.NotNil(t, pool.BorrowedUsd)
		assert.InDelta(t, 6.0, *pool.BorrowedUsd, 1e-9)
		require.NotNil(t, pool.UsdValue)
		assert.InDelta(t, -4.0, *pool.UsdValue, 1e-9)
		// netApy = (suppliedUsd·supplyApy − borrowedUsd·borrowApy) / suppliedUsd
		//   = (2.0·0.006420127418405475 − 6.0·0.022754324920237545) / 2.0
		//   = −0.12368569468461432 / 2.0 — divided by TOTAL SUPPLIED, not the
		// net value, matching blend-sdk-js's PositionsEstimate.
		require.NotNil(t, pool.NetApy)
		assert.InDelta(t, -0.06184284734230716, *pool.NetApy, 1e-9)
	})

	t.Run("backstop position", func(t *testing.T) {
		bp := got.Backstop[0]
		assert.Equal(t, poolAddr, bp.PoolAddress)
		require.NotNil(t, bp.PoolName)
		assert.Equal(t, "Test Pool", *bp.PoolName)
		// shares is the ACTIVE (non-queued) share balance only.
		assert.Equal(t, "1000000", bp.Shares)
		// lpTokens/usdValue include queued shares — still the user's slashable,
		// interest-earning capital until withdrawn: totalShares = active
		// (1_000_000) + queued (100_000 + 200_000) = 1_300_000; lpTokens =
		// 1_300_000*poolTokens(50_000_000)/poolShares(10_000_000) = 6_500_000.
		assert.Equal(t, "6500000", bp.LpTokens)
		require.NotNil(t, bp.UsdValue)
		assert.InDelta(t, 0.715, *bp.UsdValue, 1e-9)

		// Each queued entry is valued through the same shares→LP→USD chain.
		require.Len(t, bp.Q4w, 2)
		assert.Equal(t, "100000", bp.Q4w[0].Amount)
		assert.EqualValues(t, 1800000000, bp.Q4w[0].Expiration)
		assert.Equal(t, "500000", bp.Q4w[0].LpTokens)
		require.NotNil(t, bp.Q4w[0].UsdValue)
		assert.InDelta(t, 0.055, *bp.Q4w[0].UsdValue, 1e-9)
		assert.Equal(t, "200000", bp.Q4w[1].Amount)
		assert.EqualValues(t, 1900000000, bp.Q4w[1].Expiration)
		assert.Equal(t, "1000000", bp.Q4w[1].LpTokens)
		require.NotNil(t, bp.Q4w[1].UsdValue)
		assert.InDelta(t, 0.11, *bp.Q4w[1].UsdValue, 1e-9)

		// Emissions accrue on ACTIVE shares only (queued shares are not
		// emission-eligible): claimable = accrued(1000) +
		// tokenBalance(1_000_000)*(emisIndex-userIndex)(3e12)/scalar(1e14)
		//   = 1000 + 30_000 = 31_000.
		assert.Equal(t, "31000", bp.EmissionsEarnedBlnd)
		require.NotNil(t, bp.EmissionsEarnedUsd)
		assert.InDelta(t, 0.000155, *bp.EmissionsEarnedUsd, 1e-12)
	})

	t.Run("dropping the reserve asset's price nulls only its USD fields, no error", func(t *testing.T) {
		execTestDB(t, `DELETE FROM blend_oracle_prices WHERE oracle_contract_id = $1 AND asset_contract_id = $2`,
			types.AddressBytea(oracleAddr), types.AddressBytea(assetB))
		t.Cleanup(func() {
			execTestDB(t, `
				INSERT INTO blend_oracle_prices (oracle_contract_id, asset_contract_id, price, price_decimals, price_timestamp)
				VALUES ($1, $2, '10000000', 7, EXTRACT(EPOCH FROM NOW())::bigint)`,
				types.AddressBytea(oracleAddr), types.AddressBytea(assetB))
		})

		got, err := resolver.BlendPositions(testCtx, parentAccount)
		require.NoError(t, err)
		require.Len(t, got.Pools, 1)
		require.Len(t, got.Pools[0].Reserves, 2)

		r0After, r1After := got.Pools[0].Reserves[0], got.Pools[0].Reserves[1]

		// r0 (assetA) is unaffected.
		require.NotNil(t, r0After.SuppliedUsd)
		assert.InDelta(t, 2.0, *r0After.SuppliedUsd, 1e-9)

		// r1 (assetB): supply side is trivially 0 regardless of price, but the
		// nonzero borrowed side is now unpriceable, and priceUsd itself is gone.
		require.NotNil(t, r1After.SuppliedUsd)
		assert.InDelta(t, 0.0, *r1After.SuppliedUsd, 1e-9)
		assert.Nil(t, r1After.BorrowedUsd)
		assert.Nil(t, r1After.PriceUsd)

		// Pool rollup: suppliedUsd still known (only r0 contributes nonzero supply),
		// but borrowedUsd/usdValue/netApy become nil since r1 contributes to borrowedUsd.
		poolAfter := got.Pools[0]
		require.NotNil(t, poolAfter.SuppliedUsd)
		assert.InDelta(t, 2.0, *poolAfter.SuppliedUsd, 1e-9)
		assert.Nil(t, poolAfter.BorrowedUsd)
		assert.Nil(t, poolAfter.UsdValue)
		assert.Nil(t, poolAfter.NetApy)
	})
}

// TestFindBackstopPrices pins findBackstopPrices' selection contract over rows
// already scoped to the pinned Comet oracle (see the function's doc): (nil,
// nil) unless both a self-priced LP row and a sibling BLND row are present, and
// a deterministic lowest-sibling-asset pick if an extra sibling ever appears (a
// config-error state).
func TestFindBackstopPrices(t *testing.T) {
	mk := func(oracle, asset string) blenddata.OraclePrice {
		return blenddata.OraclePrice{
			OracleContractID: types.AddressBytea(oracle),
			AssetContractID:  types.AddressBytea(asset),
		}
	}

	t.Run("no rows", func(t *testing.T) {
		lp, blnd := findBackstopPrices(testCtx, nil)
		assert.Nil(t, lp)
		assert.Nil(t, blnd)
	})

	t.Run("self-priced row without a BLND sibling is incomplete", func(t *testing.T) {
		lp, blnd := findBackstopPrices(testCtx, []blenddata.OraclePrice{mk("COMET", "COMET")})
		assert.Nil(t, lp)
		assert.Nil(t, blnd)
	})

	t.Run("sibling row without a self-priced LP row is incomplete", func(t *testing.T) {
		lp, blnd := findBackstopPrices(testCtx, []blenddata.OraclePrice{mk("COMET", "BLND")})
		assert.Nil(t, lp)
		assert.Nil(t, blnd)
	})

	t.Run("one complete group", func(t *testing.T) {
		lp, blnd := findBackstopPrices(testCtx, []blenddata.OraclePrice{mk("COMET", "COMET"), mk("COMET", "BLND")})
		require.NotNil(t, lp)
		require.NotNil(t, blnd)
		assert.EqualValues(t, "COMET", lp.AssetContractID)
		assert.EqualValues(t, "BLND", blnd.AssetContractID)
	})

	t.Run("extra sibling picks the lowest asset address, deterministically", func(t *testing.T) {
		// The query orders rows by asset, so an ambiguous extra sibling resolves
		// to the lowest asset address. Rows are passed pre-ordered here.
		rows := []blenddata.OraclePrice{
			mk("COMET", "COMET"), mk("COMET", "BLND_A"), mk("COMET", "BLND_B"),
		}
		for range 50 {
			lp, blnd := findBackstopPrices(testCtx, rows)
			require.NotNil(t, lp)
			require.NotNil(t, blnd)
			assert.EqualValues(t, "COMET", lp.AssetContractID)
			assert.EqualValues(t, "BLND_A", blnd.AssetContractID)
		}
	})
}

func TestAccountResolver_BlendPositions_EmptyAccount(t *testing.T) {
	account := keypair.MustRandom().Address()
	m := metrics.NewMetrics(prometheus.NewRegistry())
	models := &data.Models{
		Contract:     &data.ContractModel{DB: testDBConnectionPool, Metrics: m.DB},
		StateChanges: &data.StateChangeModel{DB: testDBConnectionPool, Metrics: m.DB},
		Blend:        blenddata.NewModels(testDBConnectionPool, m.DB),
	}
	resolver := &accountResolver{&Resolver{models: models}}

	got, err := resolver.BlendPositions(testCtx, &types.Account{StellarAddress: types.AddressBytea(account)})
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Empty(t, got.Pools)
	assert.Empty(t, got.Backstop)
	assert.Equal(t, "0", got.BackstopClaimedLp)
	assert.NotNil(t, got.ActiveAuctions, "empty list, not null")
	assert.Empty(t, got.ActiveAuctions)
}

// TestAccountResolver_BlendPositions_ActiveAuctions covers
// BlendAccountPositions.activeAuctions: enum mapping, poolName resolution,
// deterministic (poolAddress, auctionType) ordering inherited from the
// reader (mirroring blend_pools_test.go's "deterministic pool ordering"
// precedent: bytea order, not string order), and a defensive skip of an
// unrecognized auction_type.
func TestAccountResolver_BlendPositions_ActiveAuctions(t *testing.T) {
	account := keypair.MustRandom().Address()
	poolX := randomContractAddress(t)
	poolY := randomContractAddress(t)
	assetA := randomContractAddress(t)
	assetB := randomContractAddress(t)

	m := metrics.NewMetrics(prometheus.NewRegistry())
	dbMetrics := m.DB
	blendModels := blenddata.NewModels(testDBConnectionPool, dbMetrics)
	models := &data.Models{
		Contract:     &data.ContractModel{DB: testDBConnectionPool, Metrics: dbMetrics},
		StateChanges: &data.StateChangeModel{DB: testDBConnectionPool, Metrics: dbMetrics},
		Blend:        blendModels,
	}
	resolver := &accountResolver{&Resolver{models: models}}

	first, second := poolX, poolY
	if bytes.Compare(mustAddressBytes(t, poolX), mustAddressBytes(t, poolY)) > 0 {
		first, second = poolY, poolX
	}

	execTestDB(t, `
		INSERT INTO blend_pools (pool_contract_id, name, last_modified_ledger)
		VALUES ($1, 'Pool X', 1), ($2, 'Pool Y', 1)`,
		types.AddressBytea(poolX), types.AddressBytea(poolY))

	// A USER_LIQUIDATION auction on each pool (bid/lot each carry one asset),
	// so ordering is exercised across two real, mapped rows.
	execTestDB(t, `
		INSERT INTO blend_auctions (pool_contract_id, user_account_id, auction_type, bid, lot, start_block, last_modified_ledger)
		VALUES
		($1, $3, 0, $4::jsonb, $5::jsonb, 100, 1),
		($2, $3, 0, $6::jsonb, $7::jsonb, 200, 1)`,
		types.AddressBytea(poolX), types.AddressBytea(poolY), types.AddressBytea(account),
		fmt.Sprintf(`{"%s": "500"}`, assetA), fmt.Sprintf(`{"%s": "600"}`, assetB),
		fmt.Sprintf(`{"%s": "700"}`, assetA), fmt.Sprintf(`{"%s": "800"}`, assetB))

	t.Cleanup(func() {
		execTestDB(t, `DELETE FROM blend_auctions WHERE pool_contract_id IN ($1, $2)`, types.AddressBytea(poolX), types.AddressBytea(poolY))
		execTestDB(t, `DELETE FROM blend_pools WHERE pool_contract_id IN ($1, $2)`, types.AddressBytea(poolX), types.AddressBytea(poolY))
	})

	got, err := resolver.BlendPositions(testCtx, &types.Account{StellarAddress: types.AddressBytea(account)})
	require.NoError(t, err)
	require.NotNil(t, got)

	require.Len(t, got.ActiveAuctions, 2)
	a0, a1 := got.ActiveAuctions[0], got.ActiveAuctions[1]
	assert.Equal(t, first, a0.PoolAddress, "bytea-order-first pool surfaces first")
	assert.Equal(t, second, a1.PoolAddress)
	for _, a := range got.ActiveAuctions {
		assert.Equal(t, graphql1.BlendAuctionTypeUserLiquidation, a.AuctionType)
		require.NotNil(t, a.PoolName)
		require.Len(t, a.Bid, 1)
		require.Len(t, a.Lot, 1)
	}

	t.Run("an unrecognized auction_type is skipped, not surfaced as a malformed enum", func(t *testing.T) {
		otherAccount := keypair.MustRandom().Address()
		execTestDB(t, `
			INSERT INTO blend_auctions (pool_contract_id, user_account_id, auction_type, bid, lot, start_block, last_modified_ledger)
			VALUES ($1, $2, 99, '{}'::jsonb, '{}'::jsonb, 300, 1)`,
			types.AddressBytea(poolX), types.AddressBytea(otherAccount))
		t.Cleanup(func() {
			execTestDB(t, `DELETE FROM blend_auctions WHERE user_account_id = $1`, types.AddressBytea(otherAccount))
		})

		got, err := resolver.BlendPositions(testCtx, &types.Account{StellarAddress: types.AddressBytea(otherAccount)})
		require.NoError(t, err)
		require.NotNil(t, got)
		assert.Empty(t, got.ActiveAuctions)
	})
}

// insertPoolClaimed seeds an account's lifetime pool-source claimed BLND total
// for one pool.
func insertPoolClaimed(t *testing.T, account, poolAddr, claimedBlnd string) {
	t.Helper()
	execTestDB(t, `
		INSERT INTO blend_pool_claimed (pool_contract_id, user_account_id, claimed_blnd, last_modified_ledger)
		VALUES ($1, $2, $3, 1)`,
		types.AddressBytea(poolAddr), types.AddressBytea(account), claimedBlnd)
}

// insertBackstopClaimed seeds an account's account-wide lifetime backstop-source
// claimed Comet LP total.
func insertBackstopClaimed(t *testing.T, account, claimedLp string) {
	t.Helper()
	execTestDB(t, `
		INSERT INTO blend_backstop_claimed (user_account_id, claimed_lp, last_modified_ledger)
		VALUES ($1, $2, 1)`,
		types.AddressBytea(account), claimedLp)
}
