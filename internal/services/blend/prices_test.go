// Unit tests for PriceSnapshotService. These exercise the real
// blenddata.OraclePriceModel against a PostgreSQL test database (dbtest,
// mirroring internal/data/blend/oracle_prices_test.go's fixture usage) and
// mocked services.ContractMetadataService/RPCService — no real RPC.
package blend

import (
	"context"
	"math/big"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stellar/go-stellar-sdk/xdr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	blenddata "github.com/stellar/wallet-backend/internal/data/blend"
	"github.com/stellar/wallet-backend/internal/db"
	"github.com/stellar/wallet-backend/internal/db/dbtest"
	"github.com/stellar/wallet-backend/internal/entities"
	"github.com/stellar/wallet-backend/internal/indexer/types"
	"github.com/stellar/wallet-backend/internal/metrics"
	"github.com/stellar/wallet-backend/internal/services"
)

// newPriceSnapshotFixture opens an isolated test database (migrations
// applied) and wraps it in a real OraclePriceModel, mirroring
// internal/data/blend/oracle_prices_test.go's newOraclePricesFixture.
func newPriceSnapshotFixture(t *testing.T) (context.Context, *pgxpool.Pool, *blenddata.OraclePriceModel, func()) {
	t.Helper()
	ctx := context.Background()

	dbt := dbtest.Open(t)
	pool, err := db.OpenDBConnectionPool(ctx, dbt.DSN)
	require.NoError(t, err)

	m := &blenddata.OraclePriceModel{
		DB:      pool,
		Metrics: metrics.NewMetrics(prometheus.NewRegistry()).DB,
	}

	cleanup := func() {
		pool.Close()
		dbt.Close()
	}
	return ctx, pool, m, cleanup
}

// insertBlendPool seeds a blend_pools row wiring poolAddr to oracleAddr, the
// minimum blend_pools shape GetPriceTargets' join needs.
func insertBlendPool(t *testing.T, ctx context.Context, pool *pgxpool.Pool, poolAddr, oracleAddr string) {
	t.Helper()
	_, err := pool.Exec(ctx, `
		INSERT INTO blend_pools (pool_contract_id, oracle_contract_id, last_modified_ledger)
		VALUES ($1, $2, 1)
	`, types.AddressBytea(poolAddr), types.AddressBytea(oracleAddr))
	require.NoError(t, err)
}

// insertBlendReserve seeds a blend_reserves row for (poolAddr, assetAddr).
// Only pool_contract_id/reserve_index/asset_contract_id matter to
// GetPriceTargets; other NOT NULL columns get harmless defaults.
func insertBlendReserve(t *testing.T, ctx context.Context, pool *pgxpool.Pool, poolAddr, assetAddr string, reserveIndex int32) {
	t.Helper()
	_, err := pool.Exec(ctx, `
		INSERT INTO blend_reserves (
			pool_contract_id, reserve_index, asset_contract_id,
			b_rate, d_rate, b_supply, d_supply, ir_mod, backstop_credit,
			last_time, decimals, c_factor, l_factor, util, max_util,
			r_base, r_one, r_two, r_three, reactivity, supply_cap, enabled,
			last_modified_ledger
		) VALUES (
			$1, $2, $3,
			'1', '1', '0', '0', '0', '0',
			0, 7, 0, 0, 0, 0,
			0, 0, 0, 0, 0, '0', true,
			0
		)
	`, types.AddressBytea(poolAddr), reserveIndex, types.AddressBytea(assetAddr))
	require.NoError(t, err)
}

// getOraclePriceRow reads back one blend_oracle_prices row, if present.
func getOraclePriceRow(t *testing.T, ctx context.Context, pool *pgxpool.Pool, oracleAddr, assetAddr string) (blenddata.OraclePrice, bool) {
	t.Helper()
	var row blenddata.OraclePrice
	err := pool.QueryRow(ctx, `
		SELECT price, price_decimals, price_timestamp
		FROM blend_oracle_prices WHERE oracle_contract_id = $1 AND asset_contract_id = $2
	`, types.AddressBytea(oracleAddr), types.AddressBytea(assetAddr)).Scan(
		&row.Price, &row.PriceDecimals, &row.PriceTimestamp,
	)
	if err != nil {
		return blenddata.OraclePrice{}, false
	}
	return row, true
}

func countOraclePriceRows(t *testing.T, ctx context.Context, pool *pgxpool.Pool) int {
	t.Helper()
	var n int
	require.NoError(t, pool.QueryRow(ctx, `SELECT COUNT(*) FROM blend_oracle_prices`).Scan(&n))
	return n
}

// priceDataScVal builds the Some(PriceData) ScVal a SEP-40 lastprice(asset)
// call returns: an ScMap with field-name Symbol keys "price"/"timestamp",
// mirroring TestDecodePriceData's fixtures in scval_test.go.
func priceDataScVal(price int64, timestamp uint64) xdr.ScVal {
	v := xdr.ScVal{Type: xdr.ScValTypeScvMap}
	m := mapScVal(
		xdr.ScMapEntry{Key: symScVal("price"), Val: i128ScVal(price)},
		xdr.ScMapEntry{Key: symScVal("timestamp"), Val: u64ScVal(timestamp)},
	)
	v.Map = &m
	return v
}

func newTestSnapshotService(t *testing.T, oraclePrices *blenddata.OraclePriceModel, meta services.ContractMetadataService, cometID string, rpc services.RPCService, bpm *metrics.BlendPriceMetrics) *PriceSnapshotService {
	t.Helper()
	svc, err := NewPriceSnapshotService(PriceSnapshotConfig{
		OraclePrices:         oraclePrices,
		Metadata:             meta,
		Interval:             time.Minute,
		BackstopLPContractID: cometID,
		RPC:                  rpc,
		Metrics:              bpm,
	})
	require.NoError(t, err)
	return svc
}

func TestNewPriceSnapshotService_Validation(t *testing.T) {
	_, _, oraclePrices, cleanup := newPriceSnapshotFixture(t)
	defer cleanup()
	meta := services.NewContractMetadataServiceMock(t)
	bpm := metrics.NewMetrics(prometheus.NewRegistry()).BlendPrices

	t.Run("requires OraclePrices", func(t *testing.T) {
		_, err := NewPriceSnapshotService(PriceSnapshotConfig{Metadata: meta, Interval: time.Minute, Metrics: bpm})
		assert.Error(t, err)
	})
	t.Run("requires Metadata", func(t *testing.T) {
		_, err := NewPriceSnapshotService(PriceSnapshotConfig{OraclePrices: oraclePrices, Interval: time.Minute, Metrics: bpm})
		assert.Error(t, err)
	})
	t.Run("requires a positive Interval", func(t *testing.T) {
		_, err := NewPriceSnapshotService(PriceSnapshotConfig{OraclePrices: oraclePrices, Metadata: meta, Metrics: bpm})
		assert.Error(t, err)
	})
	t.Run("requires Metrics", func(t *testing.T) {
		_, err := NewPriceSnapshotService(PriceSnapshotConfig{OraclePrices: oraclePrices, Metadata: meta, Interval: time.Minute})
		assert.Error(t, err)
	})
	t.Run("BackstopLPContractID is optional", func(t *testing.T) {
		svc, err := NewPriceSnapshotService(PriceSnapshotConfig{OraclePrices: oraclePrices, Metadata: meta, Interval: time.Minute, Metrics: bpm})
		require.NoError(t, err)
		require.NotNil(t, svc)
	})
	t.Run("requires RPC when BackstopLPContractID is set", func(t *testing.T) {
		_, err := NewPriceSnapshotService(PriceSnapshotConfig{OraclePrices: oraclePrices, Metadata: meta, Interval: time.Minute, BackstopLPContractID: randomContractAddr(t), Metrics: bpm})
		assert.ErrorContains(t, err, "RPC is required")
	})
}

// TestSnapshotOnce_ReserveAssets covers the common path: two oracles, one
// backing two pool reserves' assets and the other backing one, each fetched
// and persisted correctly.
func TestSnapshotOnce_ReserveAssets(t *testing.T) {
	ctx, pool, oraclePrices, cleanup := newPriceSnapshotFixture(t)
	defer cleanup()

	oracle1 := randomContractAddr(t)
	oracle2 := randomContractAddr(t)
	poolA := randomContractAddr(t)
	poolB := randomContractAddr(t)
	assetX := randomContractAddr(t)
	assetY := randomContractAddr(t)
	assetZ := randomContractAddr(t)

	insertBlendPool(t, ctx, pool, poolA, oracle1)
	insertBlendReserve(t, ctx, pool, poolA, assetX, 0)
	insertBlendReserve(t, ctx, pool, poolA, assetY, 1)
	insertBlendPool(t, ctx, pool, poolB, oracle2)
	insertBlendReserve(t, ctx, pool, poolB, assetZ, 0)

	assetXArg, err := buildSep40StellarAsset(assetX)
	require.NoError(t, err)
	assetYArg, err := buildSep40StellarAsset(assetY)
	require.NoError(t, err)
	assetZArg, err := buildSep40StellarAsset(assetZ)
	require.NoError(t, err)

	// Oracle-reported timestamps must be recent: SnapshotOnce skips prices
	// older than MaxPriceAge against the wall clock.
	tsX := uint64(time.Now().Unix() - 60)
	tsY := tsX + 1
	tsZ := tsX + 2

	meta := services.NewContractMetadataServiceMock(t)
	meta.On("FetchSingleField", mock.Anything, oracle1, "decimals", []xdr.ScVal(nil)).
		Return(u32ScVal(7), nil).Once()
	meta.On("FetchSingleField", mock.Anything, oracle1, "lastprice", []xdr.ScVal{assetXArg}).
		Return(priceDataScVal(100_000_000, tsX), nil).Once()
	meta.On("FetchSingleField", mock.Anything, oracle1, "lastprice", []xdr.ScVal{assetYArg}).
		Return(priceDataScVal(200_000_000, tsY), nil).Once()
	meta.On("FetchSingleField", mock.Anything, oracle2, "decimals", []xdr.ScVal(nil)).
		Return(u32ScVal(14), nil).Once()
	meta.On("FetchSingleField", mock.Anything, oracle2, "lastprice", []xdr.ScVal{assetZArg}).
		Return(priceDataScVal(300_000_000, tsZ), nil).Once()

	bpm := metrics.NewMetrics(prometheus.NewRegistry()).BlendPrices
	svc := newTestSnapshotService(t, oraclePrices, meta, "", nil, bpm)

	require.NoError(t, svc.SnapshotOnce(ctx))

	rowX, ok := getOraclePriceRow(t, ctx, pool, oracle1, assetX)
	require.True(t, ok)
	assert.Equal(t, "100000000", rowX.Price)
	assert.Equal(t, int32(7), rowX.PriceDecimals)
	assert.Equal(t, int64(tsX), rowX.PriceTimestamp)

	rowY, ok := getOraclePriceRow(t, ctx, pool, oracle1, assetY)
	require.True(t, ok)
	assert.Equal(t, "200000000", rowY.Price)
	assert.Equal(t, int32(7), rowY.PriceDecimals)

	rowZ, ok := getOraclePriceRow(t, ctx, pool, oracle2, assetZ)
	require.True(t, ok)
	assert.Equal(t, "300000000", rowZ.Price)
	assert.Equal(t, int32(14), rowZ.PriceDecimals)

	assert.Equal(t, 3, countOraclePriceRows(t, ctx, pool))
	assert.Equal(t, 3.0, testutil.ToFloat64(bpm.FetchesTotal.WithLabelValues("success")))
	assert.Equal(t, 3.0, testutil.ToFloat64(bpm.PricesTracked))
	assert.InDelta(t, 60, testutil.ToFloat64(bpm.OldestPriceAge), 5,
		"oldest-price-age gauge should reflect the oldest oracle-reported timestamp")
}

// TestSnapshotOnce_UpsertFailureZeroesPricesTracked covers a BatchUpsert
// failure: the pass reports the error, and the prices-tracked gauge reads 0 —
// it counts rows actually written, not rows collected.
func TestSnapshotOnce_UpsertFailureZeroesPricesTracked(t *testing.T) {
	ctx, pool, oraclePrices, cleanup := newPriceSnapshotFixture(t)
	defer cleanup()

	oracle := randomContractAddr(t)
	poolA := randomContractAddr(t)
	asset := randomContractAddr(t)
	insertBlendPool(t, ctx, pool, poolA, oracle)
	insertBlendReserve(t, ctx, pool, poolA, asset, 0)

	assetArg, err := buildSep40StellarAsset(asset)
	require.NoError(t, err)

	meta := services.NewContractMetadataServiceMock(t)
	meta.On("FetchSingleField", mock.Anything, oracle, "decimals", []xdr.ScVal(nil)).
		Return(u32ScVal(7), nil).Once()
	meta.On("FetchSingleField", mock.Anything, oracle, "lastprice", []xdr.ScVal{assetArg}).
		Return(priceDataScVal(100_000_000, uint64(time.Now().Unix())), nil).Once()

	// Sabotage the upsert target after target discovery's tables are seeded:
	// GetPriceTargets reads blend_pools/blend_reserves and still succeeds.
	_, err = pool.Exec(ctx, `DROP TABLE blend_oracle_prices`)
	require.NoError(t, err)

	bpm := metrics.NewMetrics(prometheus.NewRegistry()).BlendPrices
	svc := newTestSnapshotService(t, oraclePrices, meta, "", nil, bpm)

	err = svc.SnapshotOnce(ctx)
	require.ErrorContains(t, err, "persisting 1 rows")

	assert.Equal(t, 1.0, testutil.ToFloat64(bpm.FetchesTotal.WithLabelValues("success")),
		"the fetch itself succeeded — only persistence failed")
	assert.Equal(t, 0.0, testutil.ToFloat64(bpm.PricesTracked),
		"nothing was written, so the gauge must not report collected rows")
}

// TestSnapshotOnce_NonePriceSkipped covers a SEP-40 Option::None response
// (ScvVoid): no row is written, no error is raised, and the "none" fetch
// outcome is counted.
func TestSnapshotOnce_NonePriceSkipped(t *testing.T) {
	ctx, pool, oraclePrices, cleanup := newPriceSnapshotFixture(t)
	defer cleanup()

	oracle := randomContractAddr(t)
	poolA := randomContractAddr(t)
	asset := randomContractAddr(t)
	insertBlendPool(t, ctx, pool, poolA, oracle)
	insertBlendReserve(t, ctx, pool, poolA, asset, 0)

	assetArg, err := buildSep40StellarAsset(asset)
	require.NoError(t, err)

	meta := services.NewContractMetadataServiceMock(t)
	meta.On("FetchSingleField", mock.Anything, oracle, "decimals", []xdr.ScVal(nil)).
		Return(u32ScVal(7), nil).Once()
	meta.On("FetchSingleField", mock.Anything, oracle, "lastprice", []xdr.ScVal{assetArg}).
		Return(voidScVal(), nil).Once()

	bpm := metrics.NewMetrics(prometheus.NewRegistry()).BlendPrices
	svc := newTestSnapshotService(t, oraclePrices, meta, "", nil, bpm)

	require.NoError(t, svc.SnapshotOnce(ctx))

	_, ok := getOraclePriceRow(t, ctx, pool, oracle, asset)
	assert.False(t, ok, "no row should be written for a None price")
	assert.Equal(t, 0, countOraclePriceRows(t, ctx, pool))
	assert.Equal(t, 1.0, testutil.ToFloat64(bpm.FetchesTotal.WithLabelValues("none")))
	assert.Equal(t, 0.0, testutil.ToFloat64(bpm.PricesTracked))
}

// TestSnapshotOnce_StalePriceSkipped covers the staleness guard: a price
// whose oracle-reported timestamp is older than MaxPriceAge is not persisted
// (the pool contract would reject it anyway), counted as a "stale" fetch
// outcome, and still drives the oldest-price-age gauge so a dead oracle is
// observable.
func TestSnapshotOnce_StalePriceSkipped(t *testing.T) {
	ctx, pool, oraclePrices, cleanup := newPriceSnapshotFixture(t)
	defer cleanup()

	oracle := randomContractAddr(t)
	poolA := randomContractAddr(t)
	assetFresh := randomContractAddr(t)
	assetStale := randomContractAddr(t)
	insertBlendPool(t, ctx, pool, poolA, oracle)
	insertBlendReserve(t, ctx, pool, poolA, assetFresh, 0)
	insertBlendReserve(t, ctx, pool, poolA, assetStale, 1)

	assetFreshArg, err := buildSep40StellarAsset(assetFresh)
	require.NoError(t, err)
	assetStaleArg, err := buildSep40StellarAsset(assetStale)
	require.NoError(t, err)

	staleAge := int64(25 * 60 * 60) // 25h, just past the 24h MaxPriceAge
	meta := services.NewContractMetadataServiceMock(t)
	meta.On("FetchSingleField", mock.Anything, oracle, "decimals", []xdr.ScVal(nil)).
		Return(u32ScVal(7), nil).Once()
	meta.On("FetchSingleField", mock.Anything, oracle, "lastprice", []xdr.ScVal{assetFreshArg}).
		Return(priceDataScVal(100_000_000, uint64(time.Now().Unix())), nil).Once()
	meta.On("FetchSingleField", mock.Anything, oracle, "lastprice", []xdr.ScVal{assetStaleArg}).
		Return(priceDataScVal(200_000_000, uint64(time.Now().Unix()-staleAge)), nil).Once()

	bpm := metrics.NewMetrics(prometheus.NewRegistry()).BlendPrices
	svc := newTestSnapshotService(t, oraclePrices, meta, "", nil, bpm)

	require.NoError(t, svc.SnapshotOnce(ctx))

	_, ok := getOraclePriceRow(t, ctx, pool, oracle, assetFresh)
	assert.True(t, ok, "the fresh price must persist")
	_, ok = getOraclePriceRow(t, ctx, pool, oracle, assetStale)
	assert.False(t, ok, "the stale price must not persist")
	assert.Equal(t, 1, countOraclePriceRows(t, ctx, pool))

	assert.Equal(t, 1.0, testutil.ToFloat64(bpm.FetchesTotal.WithLabelValues("success")))
	assert.Equal(t, 1.0, testutil.ToFloat64(bpm.FetchesTotal.WithLabelValues("stale")))
	assert.InDelta(t, staleAge, testutil.ToFloat64(bpm.OldestPriceAge), 5,
		"the gauge must include skipped-as-stale prices — that's the dead-oracle signal")
}

// TestSnapshotOnce_NonPositivePriceSkipped covers the validity guard: a zero
// or negative price is never persisted (the pool contract rejects it) and is
// counted as an "invalid" fetch outcome, without erroring the pass.
func TestSnapshotOnce_NonPositivePriceSkipped(t *testing.T) {
	ctx, pool, oraclePrices, cleanup := newPriceSnapshotFixture(t)
	defer cleanup()

	oracle := randomContractAddr(t)
	poolA := randomContractAddr(t)
	assetZero := randomContractAddr(t)
	assetNegative := randomContractAddr(t)
	insertBlendPool(t, ctx, pool, poolA, oracle)
	insertBlendReserve(t, ctx, pool, poolA, assetZero, 0)
	insertBlendReserve(t, ctx, pool, poolA, assetNegative, 1)

	assetZeroArg, err := buildSep40StellarAsset(assetZero)
	require.NoError(t, err)
	assetNegativeArg, err := buildSep40StellarAsset(assetNegative)
	require.NoError(t, err)

	meta := services.NewContractMetadataServiceMock(t)
	meta.On("FetchSingleField", mock.Anything, oracle, "decimals", []xdr.ScVal(nil)).
		Return(u32ScVal(7), nil).Once()
	meta.On("FetchSingleField", mock.Anything, oracle, "lastprice", []xdr.ScVal{assetZeroArg}).
		Return(priceDataScVal(0, uint64(time.Now().Unix())), nil).Once()
	meta.On("FetchSingleField", mock.Anything, oracle, "lastprice", []xdr.ScVal{assetNegativeArg}).
		Return(priceDataScVal(-5, uint64(time.Now().Unix())), nil).Once()

	bpm := metrics.NewMetrics(prometheus.NewRegistry()).BlendPrices
	svc := newTestSnapshotService(t, oraclePrices, meta, "", nil, bpm)

	require.NoError(t, svc.SnapshotOnce(ctx))

	assert.Equal(t, 0, countOraclePriceRows(t, ctx, pool))
	assert.Equal(t, 2.0, testutil.ToFloat64(bpm.FetchesTotal.WithLabelValues("invalid")))
	assert.Equal(t, 0.0, testutil.ToFloat64(bpm.PricesTracked))
}

// TestSnapshotOnce_OracleErrorIsolated covers one oracle's failure not
// preventing another oracle's rows from being persisted.
func TestSnapshotOnce_OracleErrorIsolated(t *testing.T) {
	ctx, pool, oraclePrices, cleanup := newPriceSnapshotFixture(t)
	defer cleanup()

	oracleBad := randomContractAddr(t)
	oracleGood := randomContractAddr(t)
	poolBad := randomContractAddr(t)
	poolGood := randomContractAddr(t)
	assetBad := randomContractAddr(t)
	assetGood := randomContractAddr(t)

	insertBlendPool(t, ctx, pool, poolBad, oracleBad)
	insertBlendReserve(t, ctx, pool, poolBad, assetBad, 0)
	insertBlendPool(t, ctx, pool, poolGood, oracleGood)
	insertBlendReserve(t, ctx, pool, poolGood, assetGood, 0)

	assetGoodArg, err := buildSep40StellarAsset(assetGood)
	require.NoError(t, err)

	meta := services.NewContractMetadataServiceMock(t)
	meta.On("FetchSingleField", mock.Anything, oracleBad, "decimals", []xdr.ScVal(nil)).
		Return(xdr.ScVal{}, assert.AnError).Once()
	meta.On("FetchSingleField", mock.Anything, oracleGood, "decimals", []xdr.ScVal(nil)).
		Return(u32ScVal(7), nil).Once()
	meta.On("FetchSingleField", mock.Anything, oracleGood, "lastprice", []xdr.ScVal{assetGoodArg}).
		Return(priceDataScVal(500_000_000, uint64(time.Now().Unix())), nil).Once()

	bpm := metrics.NewMetrics(prometheus.NewRegistry()).BlendPrices
	svc := newTestSnapshotService(t, oraclePrices, meta, "", nil, bpm)

	err = svc.SnapshotOnce(ctx)
	require.Error(t, err)
	assert.ErrorContains(t, err, oracleBad)

	_, ok := getOraclePriceRow(t, ctx, pool, oracleBad, assetBad)
	assert.False(t, ok, "the failing oracle's target must not have a row")

	rowGood, ok := getOraclePriceRow(t, ctx, pool, oracleGood, assetGood)
	require.True(t, ok, "the healthy oracle's row must still persist")
	assert.Equal(t, "500000000", rowGood.Price)

	assert.Equal(t, 1, countOraclePriceRows(t, ctx, pool))
}

// TestSnapshotOnce_CometLeg covers the optional BLND/LP-share derived-pricing
// leg: when BackstopLPContractID is configured, fetchCometState reads the
// pool's ledger entries and two rows are written under oracle_contract_id = the Comet pool
// address — the BLND row keyed by the real BLND token address, the LP-share
// row self-priced (asset_contract_id == oracle_contract_id).
func TestSnapshotOnce_CometLeg(t *testing.T) {
	ctx, pool, oraclePrices, cleanup := newPriceSnapshotFixture(t)
	defer cleanup()

	cometID := randomContractAddr(t)
	blndAddr := randomContractAddr(t)
	usdcAddr := randomContractAddr(t)

	// One atomic getLedgerEntries read serves the whole Comet leg: the
	// instance (WASM hash check), the AllRecordData record map, and
	// TotalShares — mirroring the live mainnet entry dump in comet.go.
	meta := services.NewContractMetadataServiceMock(t)
	entries := []entities.LedgerEntryResult{
		cometEntryResult(t, cometID, xdr.ScVal{Type: xdr.ScValTypeScvLedgerKeyContractInstance}, cometInstanceVal(t, cometWasmHash)),
		cometEntryResult(t, cometID, cometUnitKeyScVal("AllRecordData"), cometRecordMapVal(t,
			[]string{blndAddr, usdcAddr},
			[]xdr.ScVal{
				cometRecordVal(t, big.NewInt(40_000_000_000_000), big.NewInt(8_000_000), 100_000_000_000, 0), // 4,000,000.0 BLND, weight 0.8
				cometRecordVal(t, big.NewInt(2_000_000_000_000), big.NewInt(2_000_000), 100_000_000_000, 1),  // 200,000.0 USDC, weight 0.2
			})),
		cometEntryResult(t, cometID, cometUnitKeyScVal("TotalShares"), i128ScVal(10_000_000_000_000)), // 1,000,000.0 shares
	}
	rpc := services.NewRPCServiceMock(t)
	rpc.On("GetLedgerEntries", mock.MatchedBy(func(keys []string) bool { return len(keys) == 3 })).
		Return(entities.RPCGetLedgerEntriesResult{LatestLedger: 1, Entries: entries}, nil).Once()

	bpm := metrics.NewMetrics(prometheus.NewRegistry()).BlendPrices
	svc := newTestSnapshotService(t, oraclePrices, meta, cometID, rpc, bpm)

	require.NoError(t, svc.SnapshotOnce(ctx))

	blndRow, ok := getOraclePriceRow(t, ctx, pool, cometID, blndAddr)
	require.True(t, ok, "expected a BLND leg row keyed by the BLND token address")
	assert.Equal(t, "2000000", blndRow.Price)
	assert.Equal(t, int32(7), blndRow.PriceDecimals)

	lpRow, ok := getOraclePriceRow(t, ctx, pool, cometID, cometID)
	require.True(t, ok, "expected a self-priced LP-share row")
	assert.Equal(t, "10000000", lpRow.Price)
	assert.Equal(t, int32(7), lpRow.PriceDecimals)

	assert.Equal(t, 2, countOraclePriceRows(t, ctx, pool))
	assert.Equal(t, 2.0, testutil.ToFloat64(bpm.FetchesTotal.WithLabelValues("success")))
	assert.Equal(t, 2.0, testutil.ToFloat64(bpm.PricesTracked))

	now := time.Now().Unix()
	assert.InDelta(t, now, blndRow.PriceTimestamp, 5, "BLND row timestamp should be ~now, not an oracle-reported one")
	assert.InDelta(t, now, lpRow.PriceTimestamp, 5, "LP row timestamp should be ~now, not an oracle-reported one")
}
