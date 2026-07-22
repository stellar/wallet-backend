package resolvers

import (
	"bytes"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vektah/gqlparser/v2/gqlerror"

	"github.com/stellar/wallet-backend/internal/data"
	blenddata "github.com/stellar/wallet-backend/internal/data/blend"
	"github.com/stellar/wallet-backend/internal/indexer/types"
	"github.com/stellar/wallet-backend/internal/metrics"
	graphql1 "github.com/stellar/wallet-backend/internal/serve/graphql/generated"
)

// TestQueryResolver_BlendPools is a hand-computed vector exercising the
// full Query.blendPools/blendPool assembly across two independent pools:
// Pool Alpha (two reserves, mirroring blend_positions_test's r0/r1 curve
// numbers verbatim since the pool-wide emissions/valuation math applies the
// exact same reserve-level BSupply/BRate/DSupply/DRate this task's shared
// computeReserveRates already exercises there) and Pool Beta (one reserve,
// a different curve, to prove the weighted-mean pool aggregates collapse to
// a single reserve's own numbers when there's only one).
//
// Both pools' reserves use a far-future last_time so ProjectRates' Δt<=0
// branch (rates.go) returns bRate/dRate UNCHANGED deterministically — see
// blend_positions_test.go's identical comment for why this keeps every
// downstream number hand-computable with no wall-clock flakiness.
//
// Pool Alpha r0: target=0.8, r_base=0.01, r_one=0.02, ir_mod=1.0,
// b_supply=100_000_000, d_supply=40_000_000, b_rate=d_rate=1e12 (rate 1.0)
// => util=0.4. supplyApr=0.0064, borrowApr=0.02.
// supplyApy=(1+0.0064/52)^52-1=0.006420127418405475 (python).
// borrowApy=(1+0.02/365)^365-1=0.020200781032923 (python).
// price=$2.50 (7 decimals) => suppliedUsd=borrowedUsd base: suppliedTokens
// (pool-wide)=100_000_000 -> USD=25.0; borrowedTokens=40_000_000 -> USD=10.0.
// bToken (id=1) emission active: eps=100000000000, blndUSD=$0.05 =>
// emissionsSupplyApr = (eps/1e14*31536000*0.05)/25.0 = 63.072. dToken (id=0)
// has no emission config => emissionsBorrowApr=0.
//
// Pool Alpha r1: target=0.8, r_base=0.01, r_one=0.02, ir_mod=1.0,
// b_supply=20_000_000, d_supply=10_000_000, b_rate=d_rate=1e12 => util=0.5.
// supplyApr=0.009, borrowApr=0.0225.
// supplyApy=(1+0.009/52)^52-1=0.00903983597743796 (python).
// borrowApy=(1+0.0225/365)^365-1=0.022754324920237545 (python).
// price=$1.00 (6 decimals) => suppliedTokens=20_000_000 -> USD=20.0;
// borrowedTokens=10_000_000 -> USD=10.0. bToken (id=3) has no config =>
// emissionsSupplyApr=0. dToken (id=2) emission is EXPIRED =>
// emissionsBorrowApr=0 (a concrete 0, not nil).
//
// Pool Alpha aggregates: suppliedUsd=25+20=45.0, borrowedUsd=10+10=20.0.
// interestApy = (25*0.006420127418405475 + 20*0.00903983597743796)/45
//
//	= 0.007584442333531023 (python).
//
// netApy = (25*(0.006420127418405475+63.072) + 20*(0.00903983597743796+0))/45
//
//	= 35.04758444233353 (python).
//
// backstopUsd = tokens(50_000_000, 7dec) * LP price($1.10) = 5.5.
//
// Pool Beta r0 (single reserve): target=0.5 (5_000_000), r_base=0.02
// (200_000), r_one=0.03 (300_000), ir_mod=1.0, b_supply=50_000_000,
// d_supply=20_000_000 => util=0.4<=target. utilScalar=0.4/0.5=0.8;
// borrowApr=1.0*(0.02+0.8*0.03)=0.044; supplyApr=borrowApr*util*
// (1-bstopRate=0.1)=0.044*0.4*0.9=0.01584.
// supplyApy=(1+0.01584/52)^52-1=0.01596366724981446 (python).
// borrowApy=(1+0.044/365)^365-1=0.04497958376438138 (python).
// price=$3.00 (5 decimals) => suppliedTokens=50_000_000 -> USD=1500.0;
// borrowedTokens=20_000_000 -> USD=600.0. No emission config on either
// token id => both emissions APRs are 0.
// Single reserve => pool interestApy/netApy both equal supplyApy exactly
// (emissionsSupplyApr contributes 0).
// backstopUsd = tokens(4_000_000, 7dec) * LP price($1.10) = 0.44.
func TestQueryResolver_BlendPools(t *testing.T) {
	poolA := randomContractAddress(t)
	poolB := randomContractAddress(t)
	oracleA := randomContractAddress(t)
	oracleB := randomContractAddress(t)
	assetA1 := randomContractAddress(t) // Pool Alpha r0, 7 decimals
	assetA2 := randomContractAddress(t) // Pool Alpha r1, 6 decimals
	assetB1 := randomContractAddress(t) // Pool Beta r0, 5 decimals
	cometAddr := randomContractAddress(t)
	blndAddr := randomContractAddress(t)

	// blendPools' deterministic order follows Pools.GetAll's ORDER BY
	// pool_contract_id (bytea order), not string/address order — compute
	// which of poolA/poolB sorts first so the ordering assertions below
	// don't depend on the random addresses' incidental string order.
	first, second := poolA, poolB
	if bytes.Compare(mustAddressBytes(t, poolA), mustAddressBytes(t, poolB)) > 0 {
		first, second = poolB, poolA
	}

	futureLastTime := int64(4102444800)   // year 2100: guarantees ProjectRates' deltaT<=0 branch
	futureExpiration := int64(4102444800) // Pool Alpha's bToken emission: active relative to "now"
	pastExpiration := int64(1700000000)   // Pool Alpha's dToken emission: expired relative to "now"

	m := metrics.NewMetrics(prometheus.NewRegistry())
	dbMetrics := m.DB
	blendModels := blenddata.NewModels(testDBConnectionPool, dbMetrics)
	models := &data.Models{
		Contract: &data.ContractModel{DB: testDBConnectionPool, Metrics: dbMetrics},
		Blend:    blendModels,
	}
	resolver := &queryResolver{&Resolver{models: models}}

	// --- contract_tokens metadata (name/symbol only; decimals authority is blend_reserves) ---
	execTestDB(t, `
		INSERT INTO contract_tokens (id, contract_id, type, name, symbol, decimals) VALUES
		($1, $2, 'sep41', 'Asset Alpha 1', 'AA1', 7),
		($3, $4, 'sep41', 'Asset Alpha 2', 'AA2', 6),
		($5, $6, 'sep41', 'Asset Beta 1', 'AB1', 5)`,
		data.DeterministicContractID(assetA1), assetA1,
		data.DeterministicContractID(assetA2), assetA2,
		data.DeterministicContractID(assetB1), assetB1)

	// --- pools --- (Pool Alpha is admin-owned and in the reward zone; Pool Beta has neither)
	adminA := randomContractAddress(t)
	execTestDB(t, `
		INSERT INTO blend_pools (pool_contract_id, name, oracle_contract_id, backstop_rate, status, max_positions, admin, in_reward_zone, last_modified_ledger)
		VALUES
		($1, 'Pool Alpha', $2, 2000000, 0, 4, $5, true, 100),
		($3, 'Pool Beta', $4, 1000000, 1, 6, NULL, false, 100)`,
		types.AddressBytea(poolA), types.AddressBytea(oracleA),
		types.AddressBytea(poolB), types.AddressBytea(oracleB),
		types.AddressBytea(adminA))

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
			6, 9500000, 9500000, 8000000, 9500000, 100000, 200000, 5000000, 15000000, 0, '0', true, 100),
		($5, 0, $6, '1000000000000', '1000000000000', '50000000', '20000000', '10000000', '0', $3,
			5, 9000000, 9000000, 5000000, 9500000, 200000, 300000, 3000000, 10000000, 0, '0', true, 100)`,
		types.AddressBytea(poolA), types.AddressBytea(assetA1), futureLastTime, types.AddressBytea(assetA2),
		types.AddressBytea(poolB), types.AddressBytea(assetB1))

	// --- reserve emissions: Pool Alpha's bToken (index*2+1=1) active, dToken (index*2=2) expired; Pool Beta has none ---
	execTestDB(t, `
		INSERT INTO blend_reserve_emissions (pool_contract_id, reserve_token_id, eps, emission_index, expiration, last_time, last_modified_ledger)
		VALUES
		($1, 1, 100000000000, '2000000000000', $2, 100, 100),
		($1, 2, 999999999999, '3000000000000', $3, 100, 100)`,
		types.AddressBytea(poolA), futureExpiration, pastExpiration)

	// --- backstop pool totals: pool-wide backstop-LP token balances ---
	execTestDB(t, `
		INSERT INTO blend_backstop_pools (pool_contract_id, shares, tokens, q4w, last_modified_ledger)
		VALUES
		($1, '10000000', '50000000', '0', 100),
		($2, '1000000', '4000000', '0', 100)`,
		types.AddressBytea(poolA), types.AddressBytea(poolB))

	// --- oracle prices: reserve assets under each pool's own oracle, Comet leg under its own address.
	// price_timestamp must be fresh: rows older than blendrates.MaxPriceAge are treated as missing. ---
	execTestDB(t, `
		INSERT INTO blend_oracle_prices (oracle_contract_id, asset_contract_id, price, price_decimals, price_timestamp)
		VALUES
		($1, $2, '25000000', 7, EXTRACT(EPOCH FROM NOW())::bigint),
		($1, $3, '10000000', 7, EXTRACT(EPOCH FROM NOW())::bigint),
		($4, $5, '30000000', 7, EXTRACT(EPOCH FROM NOW())::bigint),
		($6, $6, '11000000', 7, EXTRACT(EPOCH FROM NOW())::bigint),
		($6, $7, '500000', 7, EXTRACT(EPOCH FROM NOW())::bigint)`,
		types.AddressBytea(oracleA), types.AddressBytea(assetA1), types.AddressBytea(assetA2),
		types.AddressBytea(oracleB), types.AddressBytea(assetB1),
		types.AddressBytea(cometAddr), types.AddressBytea(blndAddr))

	t.Cleanup(func() {
		execTestDB(t, `DELETE FROM blend_reserves WHERE pool_contract_id IN ($1, $2)`, types.AddressBytea(poolA), types.AddressBytea(poolB))
		execTestDB(t, `DELETE FROM blend_reserve_emissions WHERE pool_contract_id = $1`, types.AddressBytea(poolA))
		execTestDB(t, `DELETE FROM blend_backstop_pools WHERE pool_contract_id IN ($1, $2)`, types.AddressBytea(poolA), types.AddressBytea(poolB))
		execTestDB(t, `DELETE FROM blend_pools WHERE pool_contract_id IN ($1, $2)`, types.AddressBytea(poolA), types.AddressBytea(poolB))
		execTestDB(t, `DELETE FROM blend_oracle_prices WHERE oracle_contract_id IN ($1, $2, $3)`,
			types.AddressBytea(oracleA), types.AddressBytea(oracleB), types.AddressBytea(cometAddr))
		execTestDB(t, `DELETE FROM contract_tokens WHERE contract_id IN ($1, $2, $3)`, assetA1, assetA2, assetB1)
	})

	got, err := resolver.BlendPools(testCtx)
	require.NoError(t, err)
	require.Len(t, got, 2)

	t.Run("deterministic pool ordering", func(t *testing.T) {
		assert.Equal(t, first, got[0].Address)
		assert.Equal(t, second, got[1].Address)
	})

	poolByAddr := map[string]*graphql1.BlendPool{}
	for _, p := range got {
		poolByAddr[p.Address] = p
	}
	alpha := poolByAddr[poolA]
	beta := poolByAddr[poolB]
	require.NotNil(t, alpha)
	require.NotNil(t, beta)

	t.Run("Pool Alpha header", func(t *testing.T) {
		require.NotNil(t, alpha.Name)
		assert.Equal(t, "Pool Alpha", *alpha.Name)
		require.NotNil(t, alpha.Status)
		assert.Equal(t, graphql1.BlendPoolStatusAdminActive, *alpha.Status)
		require.NotNil(t, alpha.OracleContractID)
		assert.Equal(t, oracleA, *alpha.OracleContractID)
		require.NotNil(t, alpha.BackstopRate)
		assert.EqualValues(t, 2000000, *alpha.BackstopRate)
		require.NotNil(t, alpha.MaxPositions)
		assert.EqualValues(t, 4, *alpha.MaxPositions)
		require.NotNil(t, alpha.Admin)
		assert.Equal(t, adminA, *alpha.Admin)
		assert.True(t, alpha.InRewardZone)
		require.Len(t, alpha.Reserves, 2)
	})

	t.Run("Pool Beta: no admin, not in reward zone", func(t *testing.T) {
		assert.Nil(t, beta.Admin)
		assert.False(t, beta.InRewardZone)
	})

	r0, r1 := alpha.Reserves[0], alpha.Reserves[1]

	t.Run("Pool Alpha r0: active bToken emissions", func(t *testing.T) {
		assert.Equal(t, assetA1, r0.AssetContractID)
		require.NotNil(t, r0.TokenName)
		assert.Equal(t, "Asset Alpha 1", *r0.TokenName)
		require.NotNil(t, r0.TokenSymbol)
		assert.Equal(t, "AA1", *r0.TokenSymbol)
		require.NotNil(t, r0.TokenDecimals)
		assert.EqualValues(t, 7, *r0.TokenDecimals)
		assert.True(t, r0.Enabled)

		require.NotNil(t, r0.Utilization)
		assert.InDelta(t, 0.4, *r0.Utilization, 1e-12)
		require.NotNil(t, r0.SupplyApy)
		assert.InDelta(t, 0.006420127418405475, *r0.SupplyApy, 1e-12)
		require.NotNil(t, r0.BorrowApy)
		assert.InDelta(t, 0.020200781032923, *r0.BorrowApy, 1e-12)

		require.NotNil(t, r0.EmissionsSupplyApr)
		assert.InDelta(t, 63.072, *r0.EmissionsSupplyApr, 1e-9)
		require.NotNil(t, r0.EmissionsBorrowApr)
		assert.InDelta(t, 0.0, *r0.EmissionsBorrowApr, 1e-12)

		assert.Equal(t, "100000000", r0.SuppliedTokens)
		assert.Equal(t, "40000000", r0.BorrowedTokens)
		require.NotNil(t, r0.SuppliedUsd)
		assert.InDelta(t, 25.0, *r0.SuppliedUsd, 1e-9)
		require.NotNil(t, r0.BorrowedUsd)
		assert.InDelta(t, 10.0, *r0.BorrowedUsd, 1e-9)

		assert.EqualValues(t, 9500000, *r0.CFactor)
		assert.EqualValues(t, 9500000, *r0.LFactor)
		require.NotNil(t, r0.PriceUsd)
		assert.InDelta(t, 2.5, *r0.PriceUsd, 1e-12)
	})

	t.Run("Pool Alpha r1: expired dToken emissions", func(t *testing.T) {
		assert.Equal(t, assetA2, r1.AssetContractID)
		require.NotNil(t, r1.TokenDecimals)
		assert.EqualValues(t, 6, *r1.TokenDecimals)

		require.NotNil(t, r1.Utilization)
		assert.InDelta(t, 0.5, *r1.Utilization, 1e-12)
		require.NotNil(t, r1.SupplyApy)
		assert.InDelta(t, 0.00903983597743796, *r1.SupplyApy, 1e-12)
		require.NotNil(t, r1.BorrowApy)
		assert.InDelta(t, 0.022754324920237545, *r1.BorrowApy, 1e-12)

		require.NotNil(t, r1.EmissionsSupplyApr)
		assert.InDelta(t, 0.0, *r1.EmissionsSupplyApr, 1e-12)
		// dToken emission config is EXPIRED (pastExpiration < now) -> a concrete 0, not nil.
		require.NotNil(t, r1.EmissionsBorrowApr)
		assert.InDelta(t, 0.0, *r1.EmissionsBorrowApr, 1e-12)

		assert.Equal(t, "20000000", r1.SuppliedTokens)
		assert.Equal(t, "10000000", r1.BorrowedTokens)
		require.NotNil(t, r1.SuppliedUsd)
		assert.InDelta(t, 20.0, *r1.SuppliedUsd, 1e-9)
		require.NotNil(t, r1.BorrowedUsd)
		assert.InDelta(t, 10.0, *r1.BorrowedUsd, 1e-9)

		require.NotNil(t, r1.PriceUsd)
		assert.InDelta(t, 1.0, *r1.PriceUsd, 1e-12)
	})

	t.Run("Pool Alpha aggregates", func(t *testing.T) {
		require.NotNil(t, alpha.SuppliedUsd)
		assert.InDelta(t, 45.0, *alpha.SuppliedUsd, 1e-9)
		require.NotNil(t, alpha.BorrowedUsd)
		assert.InDelta(t, 20.0, *alpha.BorrowedUsd, 1e-9)
		require.NotNil(t, alpha.BackstopUsd)
		assert.InDelta(t, 5.5, *alpha.BackstopUsd, 1e-9)
		require.NotNil(t, alpha.InterestApy)
		assert.InDelta(t, 0.007584442333531023, *alpha.InterestApy, 1e-9)
		require.NotNil(t, alpha.NetApy)
		assert.InDelta(t, 35.04758444233353, *alpha.NetApy, 1e-9)
	})

	t.Run("Pool Beta: single reserve, no emissions", func(t *testing.T) {
		require.NotNil(t, beta.Name)
		assert.Equal(t, "Pool Beta", *beta.Name)
		require.NotNil(t, beta.Status)
		assert.Equal(t, graphql1.BlendPoolStatusActive, *beta.Status)
		require.Len(t, beta.Reserves, 1)

		rb := beta.Reserves[0]
		assert.Equal(t, assetB1, rb.AssetContractID)
		require.NotNil(t, rb.TokenDecimals)
		assert.EqualValues(t, 5, *rb.TokenDecimals)

		require.NotNil(t, rb.Utilization)
		assert.InDelta(t, 0.4, *rb.Utilization, 1e-12)
		require.NotNil(t, rb.SupplyApy)
		assert.InDelta(t, 0.01596366724981446, *rb.SupplyApy, 1e-12)
		require.NotNil(t, rb.BorrowApy)
		assert.InDelta(t, 0.04497958376438138, *rb.BorrowApy, 1e-12)
		require.NotNil(t, rb.EmissionsSupplyApr)
		assert.InDelta(t, 0.0, *rb.EmissionsSupplyApr, 1e-12)
		require.NotNil(t, rb.EmissionsBorrowApr)
		assert.InDelta(t, 0.0, *rb.EmissionsBorrowApr, 1e-12)

		require.NotNil(t, rb.SuppliedUsd)
		assert.InDelta(t, 1500.0, *rb.SuppliedUsd, 1e-9)
		require.NotNil(t, rb.BorrowedUsd)
		assert.InDelta(t, 600.0, *rb.BorrowedUsd, 1e-9)
		require.NotNil(t, rb.PriceUsd)
		assert.InDelta(t, 3.0, *rb.PriceUsd, 1e-12)

		require.NotNil(t, beta.SuppliedUsd)
		assert.InDelta(t, 1500.0, *beta.SuppliedUsd, 1e-9)
		require.NotNil(t, beta.BorrowedUsd)
		assert.InDelta(t, 600.0, *beta.BorrowedUsd, 1e-9)
		require.NotNil(t, beta.BackstopUsd)
		assert.InDelta(t, 0.44, *beta.BackstopUsd, 1e-9)
		require.NotNil(t, beta.InterestApy)
		assert.InDelta(t, 0.01596366724981446, *beta.InterestApy, 1e-9)
		require.NotNil(t, beta.NetApy)
		assert.InDelta(t, 0.01596366724981446, *beta.NetApy, 1e-9)
	})

	t.Run("dropping a reserve asset's price nulls that reserve and both pool aggregates, no error", func(t *testing.T) {
		execTestDB(t, `DELETE FROM blend_oracle_prices WHERE oracle_contract_id = $1 AND asset_contract_id = $2`,
			types.AddressBytea(oracleA), types.AddressBytea(assetA2))
		t.Cleanup(func() {
			execTestDB(t, `
				INSERT INTO blend_oracle_prices (oracle_contract_id, asset_contract_id, price, price_decimals, price_timestamp)
				VALUES ($1, $2, '10000000', 7, EXTRACT(EPOCH FROM NOW())::bigint)`,
				types.AddressBytea(oracleA), types.AddressBytea(assetA2))
		})

		got, err := resolver.BlendPools(testCtx)
		require.NoError(t, err)
		require.Len(t, got, 2)

		var alphaAfter, betaAfter *graphql1.BlendPool
		for _, p := range got {
			switch p.Address {
			case poolA:
				alphaAfter = p
			case poolB:
				betaAfter = p
			}
		}
		require.NotNil(t, alphaAfter)
		require.NotNil(t, betaAfter)

		// r0 (assetA1) is unaffected.
		require.NotNil(t, alphaAfter.Reserves[0].SuppliedUsd)
		assert.InDelta(t, 25.0, *alphaAfter.Reserves[0].SuppliedUsd, 1e-9)

		// r1 (assetA2): both sides are nonzero, so both become unpriceable.
		assert.Nil(t, alphaAfter.Reserves[1].SuppliedUsd)
		assert.Nil(t, alphaAfter.Reserves[1].BorrowedUsd)
		// r1's bToken (id=3) has no emission config at all, so emissionsSupplyApr
		// stays a concrete 0 regardless of price — only an ACTIVE, non-expired
		// config with a missing price would turn nil (see emissionsAPRFor).
		require.NotNil(t, alphaAfter.Reserves[1].EmissionsSupplyApr)
		assert.InDelta(t, 0.0, *alphaAfter.Reserves[1].EmissionsSupplyApr, 1e-12)

		// Pool Alpha aggregates: propagate nil since r1 contributes to both sides.
		assert.Nil(t, alphaAfter.SuppliedUsd)
		assert.Nil(t, alphaAfter.BorrowedUsd)
		assert.Nil(t, alphaAfter.InterestApy)
		assert.Nil(t, alphaAfter.NetApy)

		// Pool Beta is entirely unaffected (different oracle/assets).
		require.NotNil(t, betaAfter.SuppliedUsd)
		assert.InDelta(t, 1500.0, *betaAfter.SuppliedUsd, 1e-9)
		require.NotNil(t, betaAfter.NetApy)
		assert.InDelta(t, 0.01596366724981446, *betaAfter.NetApy, 1e-9)
	})

	t.Run("a stale price is treated exactly like a missing one", func(t *testing.T) {
		// Age r1's (assetA2) price past MaxPriceAge: the pool contract would
		// reject it, so the API must null the same fields a deleted row does.
		execTestDB(t, `UPDATE blend_oracle_prices SET price_timestamp = EXTRACT(EPOCH FROM NOW())::bigint - 90000
			WHERE oracle_contract_id = $1 AND asset_contract_id = $2`,
			types.AddressBytea(oracleA), types.AddressBytea(assetA2))
		t.Cleanup(func() {
			execTestDB(t, `UPDATE blend_oracle_prices SET price_timestamp = EXTRACT(EPOCH FROM NOW())::bigint
				WHERE oracle_contract_id = $1 AND asset_contract_id = $2`,
				types.AddressBytea(oracleA), types.AddressBytea(assetA2))
		})

		got, err := resolver.BlendPools(testCtx)
		require.NoError(t, err)
		require.Len(t, got, 2)

		var alphaAfter *graphql1.BlendPool
		for _, p := range got {
			if p.Address == poolA {
				alphaAfter = p
			}
		}
		require.NotNil(t, alphaAfter)

		// r0 (assetA1, fresh price) is unaffected; r1 and the pool aggregates
		// null out exactly as in the deleted-price case above.
		require.NotNil(t, alphaAfter.Reserves[0].SuppliedUsd)
		assert.InDelta(t, 25.0, *alphaAfter.Reserves[0].SuppliedUsd, 1e-9)
		assert.Nil(t, alphaAfter.Reserves[1].SuppliedUsd)
		assert.Nil(t, alphaAfter.Reserves[1].BorrowedUsd)
		assert.Nil(t, alphaAfter.SuppliedUsd)
		assert.Nil(t, alphaAfter.BorrowedUsd)
		assert.Nil(t, alphaAfter.InterestApy)
		assert.Nil(t, alphaAfter.NetApy)
	})

	t.Run("stale Comet LP rows null backstopUsd", func(t *testing.T) {
		execTestDB(t, `UPDATE blend_oracle_prices SET price_timestamp = EXTRACT(EPOCH FROM NOW())::bigint - 90000
			WHERE oracle_contract_id = $1`, types.AddressBytea(cometAddr))
		t.Cleanup(func() {
			execTestDB(t, `UPDATE blend_oracle_prices SET price_timestamp = EXTRACT(EPOCH FROM NOW())::bigint
				WHERE oracle_contract_id = $1`, types.AddressBytea(cometAddr))
		})

		got, err := resolver.BlendPools(testCtx)
		require.NoError(t, err)
		require.Len(t, got, 2)
		for _, p := range got {
			assert.Nil(t, p.BackstopUsd, "pool %s backstopUsd must be nil once the LP price is stale", p.Address)
		}
	})

	t.Run("blendPool single-pool lookup returns the matching pool", func(t *testing.T) {
		one, err := resolver.BlendPool(testCtx, poolA)
		require.NoError(t, err)
		require.NotNil(t, one)
		assert.Equal(t, poolA, one.Address)
		require.NotNil(t, one.Name)
		assert.Equal(t, "Pool Alpha", *one.Name)
		require.Len(t, one.Reserves, 2)
		assert.Equal(t, assetA1, one.Reserves[0].AssetContractID)
		assert.Equal(t, assetA2, one.Reserves[1].AssetContractID)
	})

	t.Run("blendPool with an unknown but valid contract address returns nil, no error", func(t *testing.T) {
		unknown := randomContractAddress(t)
		one, err := resolver.BlendPool(testCtx, unknown)
		require.NoError(t, err)
		assert.Nil(t, one)
	})

	t.Run("blendPool with a malformed address returns a BAD_USER_INPUT error", func(t *testing.T) {
		one, err := resolver.BlendPool(testCtx, "not-an-address")
		require.Error(t, err)
		assert.Nil(t, one)

		var gqlErr *gqlerror.Error
		require.ErrorAs(t, err, &gqlErr)
		assert.Equal(t, "BAD_USER_INPUT", gqlErr.Extensions["code"])
	})
}

func TestQueryResolver_BlendPools_Empty(t *testing.T) {
	// This test asserts on the catalog behavior when there happen to be no
	// Blend v2 pools at all. It does not seed or clean up any rows, so it
	// only makes a meaningful assertion when run in isolation; run as part
	// of the full suite it merely exercises the "no error" path alongside
	// whatever pools other tests have (transiently) inserted.
	m := metrics.NewMetrics(prometheus.NewRegistry())
	models := &data.Models{
		Contract: &data.ContractModel{DB: testDBConnectionPool, Metrics: m.DB},
		Blend:    blenddata.NewModels(testDBConnectionPool, m.DB),
	}
	resolver := &queryResolver{&Resolver{models: models}}

	got, err := resolver.BlendPools(testCtx)
	require.NoError(t, err)
	assert.NotNil(t, got)
}
