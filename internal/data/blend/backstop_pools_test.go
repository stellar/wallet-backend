// Unit tests for the Blend v2 BackstopPoolModel.
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

func newBackstopPoolsFixture(t *testing.T) (context.Context, *pgxpool.Pool, *blend.BackstopPoolModel, func()) {
	t.Helper()
	ctx := context.Background()

	dbt := dbtest.Open(t)
	pool, err := db.OpenDBConnectionPool(ctx, dbt.DSN)
	require.NoError(t, err)

	m := &blend.BackstopPoolModel{
		DB:      pool,
		Metrics: metrics.NewMetrics(prometheus.NewRegistry()).DB,
	}

	cleanup := func() {
		pool.Close()
		dbt.Close()
	}
	return ctx, pool, m, cleanup
}

type backstopPoolRow struct {
	Shares, Tokens, Q4W string
	EmisEps             *int64
	EmisIndex           *string
	EmisExpiration      *int64
	EmisLastTime        *int64
	LastModifiedLedger  int32
}

func getBackstopPool(t *testing.T, ctx context.Context, pool *pgxpool.Pool, poolAddr string) (backstopPoolRow, bool) {
	t.Helper()
	var row backstopPoolRow
	err := pool.QueryRow(ctx, `
		SELECT shares, tokens, q4w, emis_eps, emis_index, emis_expiration, emis_last_time, last_modified_ledger
		FROM blend_backstop_pools WHERE pool_contract_id = $1
	`, types.AddressBytea(poolAddr)).Scan(
		&row.Shares, &row.Tokens, &row.Q4W, &row.EmisEps, &row.EmisIndex, &row.EmisExpiration, &row.EmisLastTime, &row.LastModifiedLedger,
	)
	if err != nil {
		return backstopPoolRow{}, false
	}
	return row, true
}

func i64Ptr(i int64) *int64 { return &i }

func TestBackstopPoolModel_BatchUpsertBalances(t *testing.T) {
	ctx, pool, m, cleanup := newBackstopPoolsFixture(t)
	defer cleanup()

	poolAddr := keypair.MustRandom().Address()

	t.Run("inserts a fresh row with balances only; emis_* columns stay NULL", func(t *testing.T) {
		runInTx(t, ctx, pool, func(tx pgx.Tx) {
			require.NoError(t, m.BatchUpsertBalances(ctx, tx, []blend.BackstopPool{{
				PoolContractID:     types.AddressBytea(poolAddr),
				Shares:             "1000",
				Tokens:             "2000",
				Q4W:                "50",
				LastModifiedLedger: 1,
			}}))
		})

		row, ok := getBackstopPool(t, ctx, pool, poolAddr)
		require.True(t, ok)
		assert.Equal(t, "1000", row.Shares)
		assert.Equal(t, "2000", row.Tokens)
		assert.Equal(t, "50", row.Q4W)
		assert.Nil(t, row.EmisEps)
	})

	t.Run("updating balances after emissions were set leaves emis_* columns untouched", func(t *testing.T) {
		poolAddr := keypair.MustRandom().Address()
		runInTx(t, ctx, pool, func(tx pgx.Tx) {
			require.NoError(t, m.BatchUpsertEmissions(ctx, tx, []blend.BackstopPoolEmission{{
				Pool: poolAddr, EmisEps: i64Ptr(42), EmisIndex: strPtr("7"), EmisExpiration: i64Ptr(999), EmisLastTime: i64Ptr(100),
				LedgerNumber: 1,
			}}))
		})
		runInTx(t, ctx, pool, func(tx pgx.Tx) {
			require.NoError(t, m.BatchUpsertBalances(ctx, tx, []blend.BackstopPool{{
				PoolContractID:     types.AddressBytea(poolAddr),
				Shares:             "10",
				Tokens:             "20",
				Q4W:                "0",
				LastModifiedLedger: 2,
			}}))
		})

		row, ok := getBackstopPool(t, ctx, pool, poolAddr)
		require.True(t, ok)
		assert.Equal(t, "10", row.Shares)
		require.NotNil(t, row.EmisEps, "emis columns set earlier must survive a balances-only upsert")
		assert.Equal(t, int64(42), *row.EmisEps)
	})

	t.Run("is a no-op when no rows are staged", func(t *testing.T) {
		require.NoError(t, m.BatchUpsertBalances(ctx, nil, nil))
	})
}

func TestBackstopPoolModel_BatchUpsertEmissions(t *testing.T) {
	ctx, pool, m, cleanup := newBackstopPoolsFixture(t)
	defer cleanup()

	t.Run("fresh insert via emissions: balance columns take their DEFAULT '0'", func(t *testing.T) {
		poolAddr := keypair.MustRandom().Address()
		runInTx(t, ctx, pool, func(tx pgx.Tx) {
			require.NoError(t, m.BatchUpsertEmissions(ctx, tx, []blend.BackstopPoolEmission{{
				Pool: poolAddr, EmisEps: i64Ptr(5), EmisIndex: strPtr("1"), EmisExpiration: i64Ptr(200), EmisLastTime: i64Ptr(50),
				LedgerNumber: 1,
			}}))
		})

		row, ok := getBackstopPool(t, ctx, pool, poolAddr)
		require.True(t, ok)
		assert.Equal(t, "0", row.Shares)
		assert.Equal(t, "0", row.Tokens)
		assert.Equal(t, "0", row.Q4W)
		require.NotNil(t, row.EmisEps)
		assert.Equal(t, int64(5), *row.EmisEps)
		require.NotNil(t, row.EmisIndex)
		assert.Equal(t, "1", *row.EmisIndex)
	})

	t.Run("updating emissions after balances were set leaves balance columns untouched", func(t *testing.T) {
		poolAddr := keypair.MustRandom().Address()
		runInTx(t, ctx, pool, func(tx pgx.Tx) {
			require.NoError(t, m.BatchUpsertBalances(ctx, tx, []blend.BackstopPool{{
				PoolContractID:     types.AddressBytea(poolAddr),
				Shares:             "777",
				Tokens:             "888",
				Q4W:                "1",
				LastModifiedLedger: 1,
			}}))
		})
		runInTx(t, ctx, pool, func(tx pgx.Tx) {
			require.NoError(t, m.BatchUpsertEmissions(ctx, tx, []blend.BackstopPoolEmission{{
				Pool: poolAddr, EmisEps: i64Ptr(9), EmisIndex: strPtr("3"), EmisExpiration: i64Ptr(300), EmisLastTime: i64Ptr(150),
				LedgerNumber: 2,
			}}))
		})

		row, ok := getBackstopPool(t, ctx, pool, poolAddr)
		require.True(t, ok)
		assert.Equal(t, "777", row.Shares, "balances set earlier must survive an emissions-only upsert")
		assert.Equal(t, "888", row.Tokens)
		require.NotNil(t, row.EmisEps)
		assert.Equal(t, int64(9), *row.EmisEps)
	})

	t.Run("re-upsert replaces emis_* columns", func(t *testing.T) {
		poolAddr := keypair.MustRandom().Address()
		runInTx(t, ctx, pool, func(tx pgx.Tx) {
			require.NoError(t, m.BatchUpsertEmissions(ctx, tx, []blend.BackstopPoolEmission{{
				Pool: poolAddr, EmisEps: i64Ptr(1), EmisIndex: strPtr("10"), EmisExpiration: i64Ptr(1), EmisLastTime: i64Ptr(1),
				LedgerNumber: 1,
			}}))
		})
		runInTx(t, ctx, pool, func(tx pgx.Tx) {
			require.NoError(t, m.BatchUpsertEmissions(ctx, tx, []blend.BackstopPoolEmission{{
				Pool: poolAddr, EmisEps: i64Ptr(2), EmisIndex: strPtr("20"), EmisExpiration: i64Ptr(2), EmisLastTime: i64Ptr(2),
				LedgerNumber: 2,
			}}))
		})

		row, ok := getBackstopPool(t, ctx, pool, poolAddr)
		require.True(t, ok)
		require.NotNil(t, row.EmisEps)
		assert.Equal(t, int64(2), *row.EmisEps)
		require.NotNil(t, row.EmisIndex)
		assert.Equal(t, "20", *row.EmisIndex)
	})

	t.Run("is a no-op when no rows are staged", func(t *testing.T) {
		require.NoError(t, m.BatchUpsertEmissions(ctx, nil, nil))
	})
}
