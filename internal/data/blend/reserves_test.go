// Unit tests for the Blend v2 ReserveModel.
// These tests exercise real SQL and require a PostgreSQL test database.
// Uses an external test package to avoid an import cycle with internal/data.
package blend_test

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5"
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

func newReservesFixture(t *testing.T) (context.Context, *pgxpool.Pool, *blend.ReserveModel, func()) {
	t.Helper()
	ctx := context.Background()

	dbt := dbtest.Open(t)
	pool, err := db.OpenDBConnectionPool(ctx, dbt.DSN)
	require.NoError(t, err)

	m := &blend.ReserveModel{
		DB:      pool,
		Metrics: metrics.NewMetrics(prometheus.NewRegistry()).DB,
	}

	cleanup := func() {
		pool.Close()
		dbt.Close()
	}
	return ctx, pool, m, cleanup
}

type reserveRow struct {
	BRate, DRate, BSupply, DSupply, IRMod, BackstopCredit string
	LastTime                                              int64
	Decimals, CFactor, LFactor, Util, MaxUtil             int32
	RBase, ROne, RTwo, RThree, Reactivity                 int32
	SupplyCap                                             string
	Enabled                                               bool
	LastModifiedLedger                                    int32
}

func getReserve(t *testing.T, ctx context.Context, pool *pgxpool.Pool, poolAddr string, reserveIndex int32) (reserveRow, bool) {
	t.Helper()
	var row reserveRow
	err := pool.QueryRow(ctx, `
		SELECT b_rate, d_rate, b_supply, d_supply, ir_mod, backstop_credit, last_time,
			decimals, c_factor, l_factor, util, max_util,
			r_base, r_one, r_two, r_three, reactivity, supply_cap, enabled, last_modified_ledger
		FROM blend_reserves WHERE pool_contract_id = $1 AND reserve_index = $2
	`, types.AddressBytea(poolAddr), reserveIndex).Scan(
		&row.BRate, &row.DRate, &row.BSupply, &row.DSupply, &row.IRMod, &row.BackstopCredit, &row.LastTime,
		&row.Decimals, &row.CFactor, &row.LFactor, &row.Util, &row.MaxUtil,
		&row.RBase, &row.ROne, &row.RTwo, &row.RThree, &row.Reactivity, &row.SupplyCap, &row.Enabled, &row.LastModifiedLedger,
	)
	if err != nil {
		return reserveRow{}, false
	}
	return row, true
}

// fullReserve builds a fully-populated blend.Reserve row for use as a BatchUpsert fixture.
func fullReserve(poolAddr, assetAddr string, index int32, ledger uint32) blend.Reserve {
	return blend.Reserve{
		PoolContractID:     types.AddressBytea(poolAddr),
		ReserveIndex:       index,
		AssetContractID:    types.AddressBytea(assetAddr),
		BRate:              "1000000000000",
		DRate:              "1000000000000",
		BSupply:            "100",
		DSupply:            "50",
		IRMod:              "1000000000000",
		BackstopCredit:     "0",
		LastTime:           1000,
		Decimals:           7,
		CFactor:            900000,
		LFactor:            900000,
		Util:               500000,
		MaxUtil:            950000,
		RBase:              10000,
		ROne:               20000,
		RTwo:               30000,
		RThree:             40000,
		Reactivity:         1000,
		SupplyCap:          "1000000",
		Enabled:            true,
		LastModifiedLedger: ledger,
	}
}

func TestReserveModel_BatchUpsert(t *testing.T) {
	ctx, pool, m, cleanup := newReservesFixture(t)
	defer cleanup()

	poolAddr := keypair.MustRandom().Address()
	assetAddr := keypair.MustRandom().Address()

	t.Run("inserts a fresh row with every column", func(t *testing.T) {
		runInTx(t, ctx, pool, func(tx pgx.Tx) {
			require.NoError(t, m.BatchUpsert(ctx, tx, []blend.Reserve{fullReserve(poolAddr, assetAddr, 0, 10)}))
		})

		row, ok := getReserve(t, ctx, pool, poolAddr, 0)
		require.True(t, ok)
		assert.Equal(t, "1000000000000", row.BRate)
		assert.Equal(t, "100", row.BSupply)
		assert.Equal(t, int32(7), row.Decimals)
		assert.Equal(t, int32(900000), row.CFactor)
		assert.True(t, row.Enabled)
		assert.Equal(t, int32(10), row.LastModifiedLedger)
	})

	t.Run("re-upsert replaces every column, including config", func(t *testing.T) {
		r := fullReserve(poolAddr, assetAddr, 0, 11)
		r.BRate = "2000000000000"
		r.Decimals = 6
		r.Enabled = false

		runInTx(t, ctx, pool, func(tx pgx.Tx) {
			require.NoError(t, m.BatchUpsert(ctx, tx, []blend.Reserve{r}))
		})

		row, ok := getReserve(t, ctx, pool, poolAddr, 0)
		require.True(t, ok)
		assert.Equal(t, "2000000000000", row.BRate)
		assert.Equal(t, int32(6), row.Decimals)
		assert.False(t, row.Enabled)
		assert.Equal(t, int32(11), row.LastModifiedLedger)
	})

	t.Run("is a no-op when no rows are staged", func(t *testing.T) {
		require.NoError(t, m.BatchUpsert(ctx, nil, nil))
	})
}

func TestReserveModel_BatchUpdateData(t *testing.T) {
	ctx, pool, m, cleanup := newReservesFixture(t)
	defer cleanup()

	poolAddr := keypair.MustRandom().Address()
	assetAddr := keypair.MustRandom().Address()
	runInTx(t, ctx, pool, func(tx pgx.Tx) {
		require.NoError(t, m.BatchUpsert(ctx, tx, []blend.Reserve{fullReserve(poolAddr, assetAddr, 2, 5)}))
	})

	t.Run("updates only the ResData columns, preserving config columns", func(t *testing.T) {
		runInTx(t, ctx, pool, func(tx pgx.Tx) {
			require.NoError(t, m.BatchUpdateData(ctx, tx, []blend.ReserveDataUpdate{{
				Pool: poolAddr, Asset: assetAddr,
				BRate: "3000000000000", DRate: "3100000000000", IRMod: "999",
				BSupply: "555", DSupply: "444", BackstopCredit: "12",
				LastTime: 2000, LedgerNumber: 20,
			}}))
		})

		row, ok := getReserve(t, ctx, pool, poolAddr, 2)
		require.True(t, ok)
		assert.Equal(t, "3000000000000", row.BRate)
		assert.Equal(t, "3100000000000", row.DRate)
		assert.Equal(t, "999", row.IRMod)
		assert.Equal(t, "555", row.BSupply)
		assert.Equal(t, "444", row.DSupply)
		assert.Equal(t, "12", row.BackstopCredit)
		assert.Equal(t, int64(2000), row.LastTime)
		assert.Equal(t, int32(20), row.LastModifiedLedger)
		// Config columns must be untouched by a data-only update.
		assert.Equal(t, int32(7), row.Decimals)
		assert.Equal(t, int32(900000), row.CFactor)
		assert.Equal(t, "1000000", row.SupplyCap)
		assert.True(t, row.Enabled)
	})

	t.Run("last_modified_ledger never regresses (GREATEST)", func(t *testing.T) {
		runInTx(t, ctx, pool, func(tx pgx.Tx) {
			require.NoError(t, m.BatchUpdateData(ctx, tx, []blend.ReserveDataUpdate{{
				Pool: poolAddr, Asset: assetAddr,
				BRate: "1", DRate: "1", IRMod: "1", BSupply: "1", DSupply: "1", BackstopCredit: "1",
				LastTime: 1, LedgerNumber: 1, // older than the ledger 20 set above
			}}))
		})

		row, ok := getReserve(t, ctx, pool, poolAddr, 2)
		require.True(t, ok)
		assert.Equal(t, int32(20), row.LastModifiedLedger, "ledger must not regress")
		// Data columns are still overwritten unconditionally regardless of ledger ordering.
		assert.Equal(t, "1", row.BRate)
	})

	t.Run("unknown (pool, asset) pair updates nothing and returns no error", func(t *testing.T) {
		unknownAsset := keypair.MustRandom().Address()
		runInTx(t, ctx, pool, func(tx pgx.Tx) {
			require.NoError(t, m.BatchUpdateData(ctx, tx, []blend.ReserveDataUpdate{{
				Pool: poolAddr, Asset: unknownAsset,
				BRate: "999999", DRate: "1", IRMod: "1", BSupply: "1", DSupply: "1", BackstopCredit: "1",
				LastTime: 1, LedgerNumber: 999,
			}}))
		})

		// The existing reserve (index 2, keyed by assetAddr) must be untouched.
		row, ok := getReserve(t, ctx, pool, poolAddr, 2)
		require.True(t, ok)
		assert.NotEqual(t, int32(999), row.LastModifiedLedger)
		assert.NotEqual(t, "999999", row.BRate)
	})

	t.Run("rejects duplicate (pool, asset) keys", func(t *testing.T) {
		// UPDATE ... FROM would apply only one of the two source rows, and the
		// ResData columns are absolute state rather than deltas, so there is no
		// order-free merge — the model demands pre-aggregated input instead.
		before, ok := getReserve(t, ctx, pool, poolAddr, 2)
		require.True(t, ok)

		tx, err := pool.Begin(ctx)
		require.NoError(t, err)
		defer func() { _ = tx.Rollback(ctx) }()
		err = m.BatchUpdateData(ctx, tx, []blend.ReserveDataUpdate{{
			Pool: poolAddr, Asset: assetAddr,
			BRate: "10", DRate: "10", IRMod: "10", BSupply: "10", DSupply: "10", BackstopCredit: "10",
			LastTime: 3000, LedgerNumber: 30,
		}, {
			Pool: poolAddr, Asset: assetAddr,
			BRate: "20", DRate: "20", IRMod: "20", BSupply: "20", DSupply: "20", BackstopCredit: "20",
			LastTime: 3001, LedgerNumber: 31,
		}})
		require.ErrorContains(t, err, "duplicate reserve data update")
		require.ErrorContains(t, err, "pre-aggregated")

		// The guard fires before any SQL runs, so the table is untouched.
		after, ok := getReserve(t, ctx, pool, poolAddr, 2)
		require.True(t, ok)
		assert.Equal(t, before, after)
	})

	t.Run("is a no-op when no rows are staged", func(t *testing.T) {
		require.NoError(t, m.BatchUpdateData(ctx, nil, nil))
	})
}

func TestReserveModel_BatchUpdateConfig(t *testing.T) {
	ctx, pool, m, cleanup := newReservesFixture(t)
	defer cleanup()

	poolAddr := keypair.MustRandom().Address()
	assetAddr := keypair.MustRandom().Address()
	runInTx(t, ctx, pool, func(tx pgx.Tx) {
		require.NoError(t, m.BatchUpsert(ctx, tx, []blend.Reserve{fullReserve(poolAddr, assetAddr, 2, 5)}))
	})

	t.Run("updates only the ResConfig columns, preserving live data columns", func(t *testing.T) {
		runInTx(t, ctx, pool, func(tx pgx.Tx) {
			require.NoError(t, m.BatchUpdateConfig(ctx, tx, []blend.ReserveConfigUpdate{{
				Pool: poolAddr, Asset: assetAddr,
				Decimals: 6, CFactor: 800000, LFactor: 850000,
				Util: 600000, MaxUtil: 900000,
				RBase: 11000, ROne: 21000, RTwo: 31000, RThree: 41000,
				Reactivity: 2000, SupplyCap: "2000000", Enabled: false,
				LedgerNumber: 20,
			}}))
		})

		row, ok := getReserve(t, ctx, pool, poolAddr, 2)
		require.True(t, ok)
		assert.Equal(t, int32(6), row.Decimals)
		assert.Equal(t, int32(800000), row.CFactor)
		assert.Equal(t, int32(850000), row.LFactor)
		assert.Equal(t, int32(600000), row.Util)
		assert.Equal(t, int32(900000), row.MaxUtil)
		assert.Equal(t, int32(11000), row.RBase)
		assert.Equal(t, int32(2000), row.Reactivity)
		assert.Equal(t, "2000000", row.SupplyCap)
		assert.False(t, row.Enabled)
		assert.Equal(t, int32(20), row.LastModifiedLedger)
		// Live data columns must be untouched by a config-only update.
		assert.Equal(t, "1000000000000", row.BRate)
		assert.Equal(t, "1000000000000", row.DRate)
		assert.Equal(t, "100", row.BSupply)
		assert.Equal(t, "50", row.DSupply)
		assert.Equal(t, int64(1000), row.LastTime)
	})

	t.Run("last_modified_ledger never regresses (GREATEST)", func(t *testing.T) {
		runInTx(t, ctx, pool, func(tx pgx.Tx) {
			require.NoError(t, m.BatchUpdateConfig(ctx, tx, []blend.ReserveConfigUpdate{{
				Pool: poolAddr, Asset: assetAddr,
				Decimals: 5, SupplyCap: "1",
				LedgerNumber: 1, // older than the ledger 20 set above
			}}))
		})

		row, ok := getReserve(t, ctx, pool, poolAddr, 2)
		require.True(t, ok)
		assert.Equal(t, int32(20), row.LastModifiedLedger, "ledger must not regress")
		assert.Equal(t, int32(5), row.Decimals)
	})

	t.Run("unknown (pool, asset) pair updates nothing and returns no error", func(t *testing.T) {
		unknownAsset := keypair.MustRandom().Address()
		runInTx(t, ctx, pool, func(tx pgx.Tx) {
			require.NoError(t, m.BatchUpdateConfig(ctx, tx, []blend.ReserveConfigUpdate{{
				Pool: poolAddr, Asset: unknownAsset,
				Decimals: 9, SupplyCap: "9", LedgerNumber: 999,
			}}))
		})

		row, ok := getReserve(t, ctx, pool, poolAddr, 2)
		require.True(t, ok)
		assert.NotEqual(t, int32(9), row.Decimals)
		assert.NotEqual(t, int32(999), row.LastModifiedLedger)
	})

	t.Run("rejects duplicate (pool, asset) keys", func(t *testing.T) {
		before, ok := getReserve(t, ctx, pool, poolAddr, 2)
		require.True(t, ok)

		tx, err := pool.Begin(ctx)
		require.NoError(t, err)
		defer func() { _ = tx.Rollback(ctx) }()
		err = m.BatchUpdateConfig(ctx, tx, []blend.ReserveConfigUpdate{{
			Pool: poolAddr, Asset: assetAddr, Decimals: 1, SupplyCap: "1", LedgerNumber: 30,
		}, {
			Pool: poolAddr, Asset: assetAddr, Decimals: 2, SupplyCap: "2", LedgerNumber: 31,
		}})
		require.ErrorContains(t, err, "duplicate reserve config update")
		require.ErrorContains(t, err, "pre-aggregated")

		after, ok := getReserve(t, ctx, pool, poolAddr, 2)
		require.True(t, ok)
		assert.Equal(t, before, after)
	})

	t.Run("is a no-op when no rows are staged", func(t *testing.T) {
		require.NoError(t, m.BatchUpdateConfig(ctx, nil, nil))
	})
}
