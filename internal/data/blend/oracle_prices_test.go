// Unit tests for the Blend v2 OraclePriceModel.
// These tests exercise real SQL and require a PostgreSQL test database.
// Uses an external test package to avoid an import cycle with internal/data.
package blend_test

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stellar/go-stellar-sdk/keypair"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/wallet-backend/internal/data/blend"
	"github.com/stellar/wallet-backend/internal/db"
	"github.com/stellar/wallet-backend/internal/db/dbtest"
	"github.com/stellar/wallet-backend/internal/indexer/types"
	"github.com/stellar/wallet-backend/internal/metrics"
)

func newOraclePricesFixture(t *testing.T) (context.Context, *pgxpool.Pool, *blend.OraclePriceModel, func()) {
	t.Helper()
	ctx := context.Background()

	dbt := dbtest.Open(t)
	pool, err := db.OpenDBConnectionPool(ctx, dbt.DSN)
	require.NoError(t, err)

	m := &blend.OraclePriceModel{
		DB:      pool,
		Metrics: metrics.NewMetrics(prometheus.NewRegistry()).DB,
	}

	cleanup := func() {
		pool.Close()
		dbt.Close()
	}
	return ctx, pool, m, cleanup
}

// insertPool seeds a blend_pools row directly via raw SQL. An empty oracleAddr
// stores oracle_contract_id as SQL NULL.
func insertPool(t *testing.T, ctx context.Context, pool *pgxpool.Pool, poolAddr, oracleAddr string) {
	t.Helper()
	var oracle any
	if oracleAddr != "" {
		oracle = types.AddressBytea(oracleAddr)
	}
	_, err := pool.Exec(ctx, `
		INSERT INTO blend_pools (pool_contract_id, oracle_contract_id, last_modified_ledger)
		VALUES ($1, $2, 1)
	`, types.AddressBytea(poolAddr), oracle)
	require.NoError(t, err)
}

type oraclePriceRow struct {
	Price          string
	PriceDecimals  int32
	PriceTimestamp int64
	UpdatedAt      time.Time
}

func getOraclePrice(t *testing.T, ctx context.Context, pool *pgxpool.Pool, oracleAddr, assetAddr string) (oraclePriceRow, bool) {
	t.Helper()
	var row oraclePriceRow
	err := pool.QueryRow(ctx, `
		SELECT price, price_decimals, price_timestamp, updated_at
		FROM blend_oracle_prices WHERE oracle_contract_id = $1 AND asset_contract_id = $2
	`, types.AddressBytea(oracleAddr), types.AddressBytea(assetAddr)).Scan(
		&row.Price, &row.PriceDecimals, &row.PriceTimestamp, &row.UpdatedAt,
	)
	if err != nil {
		return oraclePriceRow{}, false
	}
	return row, true
}

func TestGetPriceTargets(t *testing.T) {
	ctx, pool, m, cleanup := newOraclePricesFixture(t)
	defer cleanup()

	oracle1 := keypair.MustRandom().Address()
	oracle2 := keypair.MustRandom().Address()
	poolA := keypair.MustRandom().Address()
	poolB := keypair.MustRandom().Address()
	poolC := keypair.MustRandom().Address()
	poolNoOracle := keypair.MustRandom().Address()
	assetX := keypair.MustRandom().Address()
	assetY := keypair.MustRandom().Address()
	assetZ := keypair.MustRandom().Address()
	assetExcluded := keypair.MustRandom().Address()

	// poolA and poolB share oracle1.
	insertPool(t, ctx, pool, poolA, oracle1)
	insertPool(t, ctx, pool, poolB, oracle1)
	// poolC has its own oracle.
	insertPool(t, ctx, pool, poolC, oracle2)
	// poolNoOracle has no oracle wired yet; its reserves must be excluded.
	insertPool(t, ctx, pool, poolNoOracle, "")

	insertReserve(t, ctx, pool, poolA, assetX, 0, "1", "1")
	insertReserve(t, ctx, pool, poolB, assetX, 0, "1", "1") // same asset via a different pool -> dedup
	insertReserve(t, ctx, pool, poolB, assetY, 1, "1", "1")
	insertReserve(t, ctx, pool, poolC, assetZ, 0, "1", "1")
	insertReserve(t, ctx, pool, poolNoOracle, assetExcluded, 0, "1", "1")

	targets, err := m.GetPriceTargets(ctx)
	require.NoError(t, err)

	want := []blend.PriceTarget{
		{OracleContractID: types.AddressBytea(oracle1), AssetContractID: types.AddressBytea(assetX)},
		{OracleContractID: types.AddressBytea(oracle1), AssetContractID: types.AddressBytea(assetY)},
		{OracleContractID: types.AddressBytea(oracle2), AssetContractID: types.AddressBytea(assetZ)},
	}
	assert.ElementsMatch(t, want, targets)
}

func TestOraclePriceBatchUpsert(t *testing.T) {
	ctx, pool, m, cleanup := newOraclePricesFixture(t)
	defer cleanup()

	oracleAddr := keypair.MustRandom().Address()
	assetA := keypair.MustRandom().Address()
	assetB := keypair.MustRandom().Address()

	require.NoError(t, m.BatchUpsert(ctx, []blend.OraclePrice{
		{
			OracleContractID: types.AddressBytea(oracleAddr), AssetContractID: types.AddressBytea(assetA),
			Price: "100000000", PriceDecimals: 7, PriceTimestamp: 1000,
		},
		{
			OracleContractID: types.AddressBytea(oracleAddr), AssetContractID: types.AddressBytea(assetB),
			Price: "200000000", PriceDecimals: 7, PriceTimestamp: 1000,
		},
	}))

	rowA, ok := getOraclePrice(t, ctx, pool, oracleAddr, assetA)
	require.True(t, ok)
	assert.Equal(t, "100000000", rowA.Price)
	assert.Equal(t, int64(1000), rowA.PriceTimestamp)

	rowB, ok := getOraclePrice(t, ctx, pool, oracleAddr, assetB)
	require.True(t, ok)
	assert.Equal(t, "200000000", rowB.Price)

	// Ensure NOW() is measurably different on the re-upsert below.
	time.Sleep(10 * time.Millisecond)

	require.NoError(t, m.BatchUpsert(ctx, []blend.OraclePrice{
		{
			OracleContractID: types.AddressBytea(oracleAddr), AssetContractID: types.AddressBytea(assetA),
			Price: "150000000", PriceDecimals: 7, PriceTimestamp: 2000,
		},
	}))

	rowA2, ok := getOraclePrice(t, ctx, pool, oracleAddr, assetA)
	require.True(t, ok)
	assert.Equal(t, "150000000", rowA2.Price, "re-upserted row's price must change")
	assert.Equal(t, int64(2000), rowA2.PriceTimestamp, "re-upserted row's timestamp must change")
	assert.True(t, rowA2.UpdatedAt.After(rowA.UpdatedAt), "updated_at must advance on re-upsert")

	rowB2, ok := getOraclePrice(t, ctx, pool, oracleAddr, assetB)
	require.True(t, ok)
	assert.Equal(t, "200000000", rowB2.Price, "untouched row's price must not change")
	assert.Equal(t, rowB.UpdatedAt, rowB2.UpdatedAt, "untouched row's updated_at must not change")

	t.Run("is a no-op when no rows are staged", func(t *testing.T) {
		require.NoError(t, m.BatchUpsert(ctx, nil))
	})
}

func TestOraclePriceModel_GetByOracles(t *testing.T) {
	ctx, _, m, cleanup := newOraclePricesFixture(t)
	defer cleanup()

	oracleA := keypair.MustRandom().Address()
	oracleB := keypair.MustRandom().Address()
	oracleC := keypair.MustRandom().Address()
	assetX := keypair.MustRandom().Address()
	assetY := keypair.MustRandom().Address()

	require.NoError(t, m.BatchUpsert(ctx, []blend.OraclePrice{
		{OracleContractID: types.AddressBytea(oracleA), AssetContractID: types.AddressBytea(assetX), Price: "1", PriceDecimals: 7, PriceTimestamp: 1},
		{OracleContractID: types.AddressBytea(oracleA), AssetContractID: types.AddressBytea(assetY), Price: "2", PriceDecimals: 7, PriceTimestamp: 1},
		{OracleContractID: types.AddressBytea(oracleB), AssetContractID: types.AddressBytea(assetX), Price: "3", PriceDecimals: 7, PriceTimestamp: 1},
		{OracleContractID: types.AddressBytea(oracleC), AssetContractID: types.AddressBytea(assetX), Price: "4", PriceDecimals: 7, PriceTimestamp: 1},
	}))

	got, err := m.GetByOracles(ctx, []string{oracleA, oracleB, keypair.MustRandom().Address()})
	require.NoError(t, err)
	require.Len(t, got, 3, "oracleA's 2 rows + oracleB's 1 row; oracleC and the unknown oracle excluded")
	for _, p := range got {
		assert.NotEqual(t, types.AddressBytea(oracleC), p.OracleContractID)
	}

	t.Run("empty oracleIDs returns an empty slice without querying", func(t *testing.T) {
		m := &blend.OraclePriceModel{}
		got, err := m.GetByOracles(context.Background(), nil)
		require.NoError(t, err)
		require.NotNil(t, got)
		require.Empty(t, got)
	})
}

func TestOraclePriceModel_GetBackstopLPPrices(t *testing.T) {
	ctx, _, m, cleanup := newOraclePricesFixture(t)
	defer cleanup()

	cometOracle := keypair.MustRandom().Address() // pinned Comet pool contract, self-priced as its own oracle
	blndAsset := keypair.MustRandom().Address()
	normalOracle := keypair.MustRandom().Address()
	normalAsset := keypair.MustRandom().Address()
	// A SECOND, unpinned self-priced group: a permissionless pool whose oracle
	// is coincidentally one of its own reserve assets. It satisfies the old
	// self-priced inference but is NOT the configured Comet contract, so it must
	// never be returned.
	otherComet := keypair.MustRandom().Address()
	otherSibling := keypair.MustRandom().Address()

	require.NoError(t, m.BatchUpsert(ctx, []blend.OraclePrice{
		// Comet LP self-priced row: asset == oracle.
		{OracleContractID: types.AddressBytea(cometOracle), AssetContractID: types.AddressBytea(cometOracle), Price: "100", PriceDecimals: 7, PriceTimestamp: 1},
		// Sibling BLND price quoted under the same Comet oracle.
		{OracleContractID: types.AddressBytea(cometOracle), AssetContractID: types.AddressBytea(blndAsset), Price: "50", PriceDecimals: 7, PriceTimestamp: 1},
		// An unrelated, non-self-priced oracle row must be excluded.
		{OracleContractID: types.AddressBytea(normalOracle), AssetContractID: types.AddressBytea(normalAsset), Price: "10", PriceDecimals: 7, PriceTimestamp: 1},
		// A second self-priced group under a DIFFERENT oracle must be excluded.
		{OracleContractID: types.AddressBytea(otherComet), AssetContractID: types.AddressBytea(otherComet), Price: "999", PriceDecimals: 7, PriceTimestamp: 1},
		{OracleContractID: types.AddressBytea(otherComet), AssetContractID: types.AddressBytea(otherSibling), Price: "888", PriceDecimals: 7, PriceTimestamp: 1},
	}))

	got, err := m.GetBackstopLPPrices(ctx, cometOracle)
	require.NoError(t, err)
	require.Len(t, got, 2, "exactly the pinned Comet self-price row plus its sibling BLND row")

	gotPrices := map[types.AddressBytea]string{}
	for _, p := range got {
		assert.Equal(t, types.AddressBytea(cometOracle), p.OracleContractID, "only the pinned Comet oracle's rows, never the second self-priced group")
		gotPrices[p.AssetContractID] = p.Price
	}
	assert.Equal(t, "100", gotPrices[types.AddressBytea(cometOracle)])
	assert.Equal(t, "50", gotPrices[types.AddressBytea(blndAsset)])

	t.Run("empty cometID returns an empty slice without querying", func(t *testing.T) {
		m := &blend.OraclePriceModel{} // no DB: proves it never queries
		got, err := m.GetBackstopLPPrices(context.Background(), "")
		require.NoError(t, err)
		require.NotNil(t, got)
		require.Empty(t, got)
	})
}
