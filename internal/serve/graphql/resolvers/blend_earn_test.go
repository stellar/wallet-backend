package resolvers

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/wallet-backend/internal/data"
	blenddata "github.com/stellar/wallet-backend/internal/data/blend"
	"github.com/stellar/wallet-backend/internal/indexer/types"
	"github.com/stellar/wallet-backend/internal/metrics"
	graphql1 "github.com/stellar/wallet-backend/internal/serve/graphql/generated"
)

// TestQueryResolver_BlendEarnOptions is a hand-computed vector exercising
// Query.blendEarnOptions' asset-first grouping. Asset X's two priced
// reserves reuse blend_pools_test's Pool Alpha r0/r1 curve/util/price
// numbers verbatim (same b/dRate/b/dSupply/curve/price), so their
// supplyApy/suppliedUsd are the exact same already-hand-verified values;
// Asset Y's single reserve reuses Pool Beta r0 likewise. This asserts:
//
//   - Asset X: 2 pools (Pool One suppliedUsd=25.0, Pool Two suppliedUsd=20.0)
//     plus 2 more with no oracle price at all (Pool Three/Four); Asset Y: 1
//     pool (Pool Five, suppliedUsd=1500.0); Asset Z: excluded — its only
//     reserve has enabled=false, regardless of it also carrying a valid price.
//   - Assets ordered ascending by contract address (plain Go string compare).
//   - Each asset's pools ordered by suppliedUsd descending; a pool with no
//     price sorts last, and ties (including nil vs. nil) break by ascending
//     pool address.
//
// Pool One/Two/Three/Four's Asset X reserve: target=0.8, r_base=0.01,
// r_one=0.02, ir_mod=1.0. Pool One: b_supply=100_000_000, d_supply=40_000_000
// => util=0.4, supplyApr=0.0064, supplyApy=(1+0.0064/52)^52-1=
// 0.006420127418405475 (python); price=$2.50 (7 decimals) => suppliedTokens
// (pool-wide)=100_000_000 -> USD=25.0. Pool Two: b_supply=20_000_000,
// d_supply=10_000_000 => util=0.5, supplyApr=0.009, supplyApy=
// (1+0.009/52)^52-1=0.00903983597743796 (python); price=$1.00 (6 decimals)
// => suppliedTokens=20_000_000 -> USD=20.0. Pool Three/Four: no oracle price
// row for Asset X under either pool's oracle at all => suppliedUsd nil for
// both (b_supply=10_000_000, decimals=7, so suppliedTokens is genuinely
// nonzero, not the "amount==0 => 0.0" case).
//
// Pool Five's Asset Y reserve: target=0.5, r_base=0.02, r_one=0.03,
// ir_mod=1.0, b_supply=50_000_000, d_supply=20_000_000 => util=0.4<=target;
// utilScalar=0.8; borrowApr=1.0*(0.02+0.8*0.03)=0.044; supplyApr=borrowApr*
// util*(1-bstopRate=0.1)=0.044*0.4*0.9=0.01584; supplyApy=(1+0.01584/52)^52-1
// =0.01596366724981446 (python); price=$3.00 (5 decimals) =>
// suppliedTokens=50_000_000 -> USD=1500.0.
func TestQueryResolver_BlendEarnOptions(t *testing.T) {
	assetX := randomContractAddress(t)
	assetY := randomContractAddress(t)
	assetZ := randomContractAddress(t)

	poolOne := randomContractAddress(t)
	poolTwo := randomContractAddress(t)
	poolFive := randomContractAddress(t)
	poolThree := randomContractAddress(t)
	poolFour := randomContractAddress(t)

	// Supply-rejecting pools: the contract refuses supply for status >= 4
	// (4 Admin Frozen, 5 Frozen, 6 Setup), and an unknown (NULL) status can't
	// be confirmed eligible. Their enabled Asset X reserves must not appear.
	poolFrozen := randomContractAddress(t)
	poolSetup := randomContractAddress(t)
	poolUnknown := randomContractAddress(t)

	oracleOne := randomContractAddress(t)
	oracleTwo := randomContractAddress(t)
	oracleFive := randomContractAddress(t)
	oracleThree := randomContractAddress(t)
	oracleFour := randomContractAddress(t)

	// Asset X's ordering under Pool Three/Four (both nil suppliedUsd) falls
	// back to ascending pool address; compute that order up front from the
	// random addresses so the assertion below doesn't guess.
	loP34, hiP34 := poolThree, poolFour
	if loP34 > hiP34 {
		loP34, hiP34 = hiP34, loP34
	}

	// Asset ordering (X vs. Y) is likewise a plain string compare of the
	// two random C-addresses; Z is excluded from output entirely so its
	// address doesn't need to be placed in this ordering.
	assetLo, assetHi := assetX, assetY
	if assetLo > assetHi {
		assetLo, assetHi = assetHi, assetLo
	}

	futureLastTime := int64(4102444800) // year 2100: ProjectRates' deltaT<=0 branch stays deterministic

	m := metrics.NewMetrics(prometheus.NewRegistry())
	dbMetrics := m.DB
	blendModels := blenddata.NewModels(testDBConnectionPool, dbMetrics)
	models := &data.Models{
		Contract: &data.ContractModel{DB: testDBConnectionPool, Metrics: dbMetrics},
		Blend:    blendModels,
	}
	resolver := &queryResolver{&Resolver{models: models}}

	// --- contract_tokens metadata (Asset Z intentionally omitted: it's excluded anyway) ---
	execTestDB(t, `
		INSERT INTO contract_tokens (id, contract_id, type, name, symbol, decimals) VALUES
		($1, $2, 'sep41', 'Asset X', 'AX', 7),
		($3, $4, 'sep41', 'Asset Y', 'AY', 5)`,
		data.DeterministicContractID(assetX), assetX,
		data.DeterministicContractID(assetY), assetY)

	// --- pools ---
	execTestDB(t, `
		INSERT INTO blend_pools (pool_contract_id, name, oracle_contract_id, backstop_rate, status, max_positions, last_modified_ledger)
		VALUES
		($1, 'Pool One', $2, 2000000, 0, 4, 100),
		($3, 'Pool Two', $4, 2000000, 0, 4, 100),
		($5, 'Pool Five', $6, 1000000, 0, 4, 100),
		($7, 'Pool Three', $8, 2000000, 0, 4, 100),
		($9, 'Pool Four', $10, 2000000, 0, 4, 100),
		($11, 'Pool Frozen', $2, 2000000, 4, 4, 100),
		($12, 'Pool Setup', $2, 2000000, 6, 4, 100),
		($13, 'Pool Unknown', $2, 2000000, NULL, 4, 100)`,
		types.AddressBytea(poolOne), types.AddressBytea(oracleOne),
		types.AddressBytea(poolTwo), types.AddressBytea(oracleTwo),
		types.AddressBytea(poolFive), types.AddressBytea(oracleFive),
		types.AddressBytea(poolThree), types.AddressBytea(oracleThree),
		types.AddressBytea(poolFour), types.AddressBytea(oracleFour),
		types.AddressBytea(poolFrozen), types.AddressBytea(poolSetup), types.AddressBytea(poolUnknown))

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
		($1, 1, $4, '1000000000000', '1000000000000', '100000', '1000', '10000000', '0', $3,
			7, 9500000, 9500000, 8000000, 9500000, 100000, 200000, 5000000, 15000000, 0, '0', false, 100),
		($5, 0, $2, '1000000000000', '1000000000000', '20000000', '10000000', '10000000', '0', $3,
			6, 9500000, 9500000, 8000000, 9500000, 100000, 200000, 5000000, 15000000, 0, '0', true, 100),
		($6, 0, $7, '1000000000000', '1000000000000', '50000000', '20000000', '10000000', '0', $3,
			5, 9000000, 9000000, 5000000, 9500000, 200000, 300000, 3000000, 10000000, 0, '0', true, 100),
		($8, 0, $2, '1000000000000', '1000000000000', '10000000', '4000000', '10000000', '0', $3,
			7, 9500000, 9500000, 8000000, 9500000, 100000, 200000, 5000000, 15000000, 0, '0', true, 100),
		($9, 0, $2, '1000000000000', '1000000000000', '10000000', '4000000', '10000000', '0', $3,
			7, 9500000, 9500000, 8000000, 9500000, 100000, 200000, 5000000, 15000000, 0, '0', true, 100),
		($10, 0, $2, '1000000000000', '1000000000000', '10000000', '4000000', '10000000', '0', $3,
			7, 9500000, 9500000, 8000000, 9500000, 100000, 200000, 5000000, 15000000, 0, '0', true, 100),
		($11, 0, $2, '1000000000000', '1000000000000', '10000000', '4000000', '10000000', '0', $3,
			7, 9500000, 9500000, 8000000, 9500000, 100000, 200000, 5000000, 15000000, 0, '0', true, 100),
		($12, 0, $2, '1000000000000', '1000000000000', '10000000', '4000000', '10000000', '0', $3,
			7, 9500000, 9500000, 8000000, 9500000, 100000, 200000, 5000000, 15000000, 0, '0', true, 100)`,
		types.AddressBytea(poolOne), types.AddressBytea(assetX), futureLastTime, types.AddressBytea(assetZ),
		types.AddressBytea(poolTwo), types.AddressBytea(poolFive), types.AddressBytea(assetY),
		types.AddressBytea(poolThree), types.AddressBytea(poolFour),
		types.AddressBytea(poolFrozen), types.AddressBytea(poolSetup), types.AddressBytea(poolUnknown))

	// --- oracle prices: Pool Three/Four intentionally have none for Asset X;
	// the Comet self-priced LP row + BLND sibling ($0.05) feed emissions APR ---
	cometAddr := randomContractAddress(t)
	blndAddr := randomContractAddress(t)
	execTestDB(t, `
		INSERT INTO blend_oracle_prices (oracle_contract_id, asset_contract_id, price, price_decimals, price_timestamp)
		VALUES
		($1, $2, '25000000', 7, EXTRACT(EPOCH FROM NOW())::bigint),
		($3, $2, '10000000', 7, EXTRACT(EPOCH FROM NOW())::bigint),
		($4, $5, '30000000', 7, EXTRACT(EPOCH FROM NOW())::bigint),
		($6, $6, '11000000', 7, EXTRACT(EPOCH FROM NOW())::bigint),
		($6, $7, '500000', 7, EXTRACT(EPOCH FROM NOW())::bigint)`,
		types.AddressBytea(oracleOne), types.AddressBytea(assetX),
		types.AddressBytea(oracleTwo),
		types.AddressBytea(oracleFive), types.AddressBytea(assetY),
		types.AddressBytea(cometAddr), types.AddressBytea(blndAddr))

	// --- reserve emissions: Pool One's Asset X bToken (index*2+1 = 1) active,
	// mirroring blend_pools_test's Pool Alpha r0 stream (emissionsSupplyApr =
	// (eps/1e14 · 31536000 · blndUSD 0.05) / suppliedUsd 25.0 = 63.072) ---
	execTestDB(t, `
		INSERT INTO blend_reserve_emissions (pool_contract_id, reserve_token_id, eps, emission_index, expiration, last_time, last_modified_ledger)
		VALUES ($1, 1, 100000000000, '2000000000000', 4102444800, 100, 100)`,
		types.AddressBytea(poolOne))

	t.Cleanup(func() {
		execTestDB(t, `DELETE FROM blend_reserves WHERE pool_contract_id IN ($1, $2, $3, $4, $5, $6, $7, $8)`,
			types.AddressBytea(poolOne), types.AddressBytea(poolTwo), types.AddressBytea(poolFive),
			types.AddressBytea(poolThree), types.AddressBytea(poolFour),
			types.AddressBytea(poolFrozen), types.AddressBytea(poolSetup), types.AddressBytea(poolUnknown))
		execTestDB(t, `DELETE FROM blend_pools WHERE pool_contract_id IN ($1, $2, $3, $4, $5, $6, $7, $8)`,
			types.AddressBytea(poolOne), types.AddressBytea(poolTwo), types.AddressBytea(poolFive),
			types.AddressBytea(poolThree), types.AddressBytea(poolFour),
			types.AddressBytea(poolFrozen), types.AddressBytea(poolSetup), types.AddressBytea(poolUnknown))
		execTestDB(t, `DELETE FROM blend_oracle_prices WHERE oracle_contract_id IN ($1, $2, $3, $4)`,
			types.AddressBytea(oracleOne), types.AddressBytea(oracleTwo), types.AddressBytea(oracleFive),
			types.AddressBytea(cometAddr))
		execTestDB(t, `DELETE FROM blend_reserve_emissions WHERE pool_contract_id = $1`, types.AddressBytea(poolOne))
		execTestDB(t, `DELETE FROM contract_tokens WHERE contract_id IN ($1, $2)`, assetX, assetY)
	})

	got, err := resolver.BlendEarnOptions(testCtx)
	require.NoError(t, err)
	require.Len(t, got, 2, "Asset Z must be excluded: its only reserve is disabled")

	t.Run("deterministic asset ordering", func(t *testing.T) {
		assert.Equal(t, assetLo, got[0].AssetContractID)
		assert.Equal(t, assetHi, got[1].AssetContractID)
	})

	optByAsset := map[string]*graphql1.BlendEarnOption{}
	for _, o := range got {
		optByAsset[o.AssetContractID] = o
	}
	x := optByAsset[assetX]
	y := optByAsset[assetY]
	require.NotNil(t, x)
	require.NotNil(t, y)

	t.Run("Asset X metadata and pool count", func(t *testing.T) {
		require.NotNil(t, x.TokenName)
		assert.Equal(t, "Asset X", *x.TokenName)
		require.NotNil(t, x.TokenSymbol)
		assert.Equal(t, "AX", *x.TokenSymbol)
		require.NotNil(t, x.TokenDecimals)
		assert.EqualValues(t, 7, *x.TokenDecimals)
		require.Len(t, x.Pools, 4)
	})

	t.Run("Asset X pools ordered by suppliedUsd descending, nil last, tie-break by pool address", func(t *testing.T) {
		p0, p1, p2, p3 := x.Pools[0], x.Pools[1], x.Pools[2], x.Pools[3]

		assert.Equal(t, poolOne, p0.PoolAddress)
		require.NotNil(t, p0.PoolName)
		assert.Equal(t, "Pool One", *p0.PoolName)
		require.NotNil(t, p0.SupplyApy)
		assert.InDelta(t, 0.006420127418405475, *p0.SupplyApy, 1e-12)
		require.NotNil(t, p0.SuppliedUsd)
		assert.InDelta(t, 25.0, *p0.SuppliedUsd, 1e-9)
		// Active bToken emission stream: same hand-computed vector as
		// blend_pools_test's Pool Alpha r0.
		require.NotNil(t, p0.EmissionsSupplyApr)
		assert.InDelta(t, 63.072, *p0.EmissionsSupplyApr, 1e-9)

		assert.Equal(t, poolTwo, p1.PoolAddress)
		require.NotNil(t, p1.SupplyApy)
		assert.InDelta(t, 0.00903983597743796, *p1.SupplyApy, 1e-12)
		require.NotNil(t, p1.SuppliedUsd)
		assert.InDelta(t, 20.0, *p1.SuppliedUsd, 1e-9)
		// No emission stream configured -> a concrete 0, not nil.
		require.NotNil(t, p1.EmissionsSupplyApr)
		assert.InDelta(t, 0.0, *p1.EmissionsSupplyApr, 1e-12)

		// Pool Three/Four: no oracle price at all -> suppliedUsd nil, sorted
		// last, ties broken by ascending pool address.
		assert.Nil(t, p2.SuppliedUsd)
		assert.Nil(t, p3.SuppliedUsd)
		assert.Equal(t, loP34, p2.PoolAddress)
		assert.Equal(t, hiP34, p3.PoolAddress)
	})

	t.Run("supply-rejecting pools (status >= 4 or unknown) are not earn destinations", func(t *testing.T) {
		for _, p := range x.Pools {
			assert.NotEqual(t, poolFrozen, p.PoolAddress, "Admin Frozen (4) rejects supply on-chain")
			assert.NotEqual(t, poolSetup, p.PoolAddress, "Setup (6) rejects supply on-chain")
			assert.NotEqual(t, poolUnknown, p.PoolAddress, "a NULL status can't be confirmed supply-eligible")
		}
	})

	t.Run("Asset Y metadata and single pool", func(t *testing.T) {
		require.NotNil(t, y.TokenName)
		assert.Equal(t, "Asset Y", *y.TokenName)
		require.NotNil(t, y.TokenDecimals)
		assert.EqualValues(t, 5, *y.TokenDecimals)
		require.Len(t, y.Pools, 1)

		p := y.Pools[0]
		assert.Equal(t, poolFive, p.PoolAddress)
		require.NotNil(t, p.PoolName)
		assert.Equal(t, "Pool Five", *p.PoolName)
		require.NotNil(t, p.SupplyApy)
		assert.InDelta(t, 0.01596366724981446, *p.SupplyApy, 1e-12)
		require.NotNil(t, p.SuppliedUsd)
		assert.InDelta(t, 1500.0, *p.SuppliedUsd, 1e-9)
	})
}

func TestQueryResolver_BlendEarnOptions_Empty(t *testing.T) {
	// Mirrors TestQueryResolver_BlendPools_Empty: only meaningful run in
	// isolation, but exercises the no-pools/no-error path either way.
	m := metrics.NewMetrics(prometheus.NewRegistry())
	models := &data.Models{
		Contract: &data.ContractModel{DB: testDBConnectionPool, Metrics: m.DB},
		Blend:    blenddata.NewModels(testDBConnectionPool, m.DB),
	}
	resolver := &queryResolver{&Resolver{models: models}}

	got, err := resolver.BlendEarnOptions(testCtx)
	require.NoError(t, err)
	assert.NotNil(t, got)
}

func TestQueryResolver_BlendEarnOptions_UnenrichedMetadataFallsBackToReserveDecimals(t *testing.T) {
	// SEP-41 classification inserts a contract_tokens row (nil name/symbol,
	// decimals 0) BEFORE RPC enrichment fills it. Until enrichment lands, the
	// reserve config's decimals — set on-chain from the token itself — are
	// authoritative for the earn view.
	asset := randomContractAddress(t)
	poolAddr := randomContractAddress(t)
	oracleAddr := randomContractAddress(t)

	m := metrics.NewMetrics(prometheus.NewRegistry())
	models := &data.Models{
		Contract: &data.ContractModel{DB: testDBConnectionPool, Metrics: m.DB},
		Blend:    blenddata.NewModels(testDBConnectionPool, m.DB),
	}
	resolver := &queryResolver{&Resolver{models: models}}

	execTestDB(t, `
		INSERT INTO contract_tokens (id, contract_id, type, name, symbol, decimals) VALUES
		($1, $2, 'sep41', NULL, NULL, 0)`,
		data.DeterministicContractID(asset), asset)
	execTestDB(t, `
		INSERT INTO blend_pools (pool_contract_id, name, oracle_contract_id, backstop_rate, status, max_positions, last_modified_ledger)
		VALUES ($1, 'Pool U', $2, 2000000, 0, 4, 100)`,
		types.AddressBytea(poolAddr), types.AddressBytea(oracleAddr))
	execTestDB(t, `
		INSERT INTO blend_reserves (
			pool_contract_id, reserve_index, asset_contract_id,
			b_rate, d_rate, b_supply, d_supply, ir_mod, backstop_credit, last_time,
			decimals, c_factor, l_factor, util, max_util,
			r_base, r_one, r_two, r_three, reactivity, supply_cap, enabled, last_modified_ledger
		) VALUES ($1, 0, $2, '1000000000000', '1000000000000', '100000000', '40000000', '10000000', '0', 4102444800,
			6, 9500000, 9500000, 8000000, 9500000, 100000, 200000, 5000000, 15000000, 0, '0', true, 100)`,
		types.AddressBytea(poolAddr), types.AddressBytea(asset))
	t.Cleanup(func() {
		execTestDB(t, `DELETE FROM blend_reserves WHERE pool_contract_id = $1`, types.AddressBytea(poolAddr))
		execTestDB(t, `DELETE FROM blend_pools WHERE pool_contract_id = $1`, types.AddressBytea(poolAddr))
		execTestDB(t, `DELETE FROM contract_tokens WHERE contract_id = $1`, asset)
	})

	got, err := resolver.BlendEarnOptions(testCtx)
	require.NoError(t, err)

	var opt *graphql1.BlendEarnOption
	for _, o := range got {
		if o.AssetContractID == asset {
			opt = o
		}
	}
	require.NotNil(t, opt)
	assert.Nil(t, opt.TokenName, "unenriched row has no name")
	require.NotNil(t, opt.TokenDecimals)
	assert.EqualValues(t, 6, *opt.TokenDecimals, "reserve decimals win over the unenriched row's 0")
}
