// Unit tests for the Blend v2 PoolModel.
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

func newPoolsFixture(t *testing.T) (context.Context, *pgxpool.Pool, *blend.PoolModel, func()) {
	t.Helper()
	ctx := context.Background()

	dbt := dbtest.Open(t)
	pool, err := db.OpenDBConnectionPool(ctx, dbt.DSN)
	require.NoError(t, err)

	m := &blend.PoolModel{
		DB:      pool,
		Metrics: metrics.NewMetrics(prometheus.NewRegistry()).DB,
	}

	cleanup := func() {
		pool.Close()
		dbt.Close()
	}
	return ctx, pool, m, cleanup
}

type poolRow struct {
	Name               *string
	OracleContractID   *string
	BackstopRate       *int32
	Status             *int32
	MaxPositions       *int32
	MinCollateral      *string
	LastModifiedLedger int32
}

func getPool(t *testing.T, ctx context.Context, pool *pgxpool.Pool, poolAddr string) (poolRow, bool) {
	t.Helper()
	var row poolRow
	var oracle *types.AddressBytea
	err := pool.QueryRow(ctx, `
		SELECT name, oracle_contract_id, backstop_rate, status, max_positions, min_collateral, last_modified_ledger
		FROM blend_pools WHERE pool_contract_id = $1
	`, types.AddressBytea(poolAddr)).Scan(
		&row.Name, &oracle, &row.BackstopRate, &row.Status, &row.MaxPositions, &row.MinCollateral, &row.LastModifiedLedger,
	)
	if err != nil {
		return poolRow{}, false
	}
	if oracle != nil {
		s := string(*oracle)
		row.OracleContractID = &s
	}
	return row, true
}

func strPtr(s string) *string { return &s }
func i32Ptr(i int32) *int32   { return &i }

func TestPoolModel_BatchUpsert(t *testing.T) {
	ctx, pool, m, cleanup := newPoolsFixture(t)
	defer cleanup()

	poolAddr := keypair.MustRandom().Address()
	oracleAddr := keypair.MustRandom().Address()

	t.Run("inserts a fresh row with all fields", func(t *testing.T) {
		runInTx(t, ctx, pool, func(tx pgx.Tx) {
			require.NoError(t, m.BatchUpsert(ctx, tx, []blend.Pool{{
				PoolContractID:     types.AddressBytea(poolAddr),
				Name:               strPtr("Fixed Pool v2"),
				OracleContractID:   types.AddressBytea(oracleAddr),
				BackstopRate:       i32Ptr(2000),
				Status:             i32Ptr(0),
				MaxPositions:       i32Ptr(4),
				MinCollateral:      strPtr("100"),
				LastModifiedLedger: 10,
			}}))
		})

		row, ok := getPool(t, ctx, pool, poolAddr)
		require.True(t, ok)
		require.NotNil(t, row.Name)
		assert.Equal(t, "Fixed Pool v2", *row.Name)
		require.NotNil(t, row.OracleContractID)
		assert.Equal(t, oracleAddr, *row.OracleContractID)
		require.NotNil(t, row.BackstopRate)
		assert.Equal(t, int32(2000), *row.BackstopRate)
		require.NotNil(t, row.Status)
		assert.Equal(t, int32(0), *row.Status)
		assert.Equal(t, int32(10), row.LastModifiedLedger)
	})

	t.Run("re-upsert with a nil field preserves the existing value (COALESCE)", func(t *testing.T) {
		runInTx(t, ctx, pool, func(tx pgx.Tx) {
			require.NoError(t, m.BatchUpsert(ctx, tx, []blend.Pool{{
				PoolContractID:     types.AddressBytea(poolAddr),
				Name:               nil, // not known by this event
				Status:             i32Ptr(3),
				LastModifiedLedger: 11,
			}}))
		})

		row, ok := getPool(t, ctx, pool, poolAddr)
		require.True(t, ok)
		require.NotNil(t, row.Name, "name must be preserved when the update carries nil")
		assert.Equal(t, "Fixed Pool v2", *row.Name)
		require.NotNil(t, row.Status)
		assert.Equal(t, int32(3), *row.Status, "status must be updated")
		assert.Equal(t, int32(11), row.LastModifiedLedger, "last_modified_ledger is always taken")
	})

	t.Run("empty OracleContractID is stored as SQL NULL", func(t *testing.T) {
		poolAddr := keypair.MustRandom().Address()
		runInTx(t, ctx, pool, func(tx pgx.Tx) {
			require.NoError(t, m.BatchUpsert(ctx, tx, []blend.Pool{{
				PoolContractID:     types.AddressBytea(poolAddr),
				OracleContractID:   "",
				LastModifiedLedger: 1,
			}}))
		})

		var isNull bool
		err := pool.QueryRow(ctx, `SELECT oracle_contract_id IS NULL FROM blend_pools WHERE pool_contract_id = $1`,
			types.AddressBytea(poolAddr)).Scan(&isNull)
		require.NoError(t, err)
		assert.True(t, isNull, "empty OracleContractID must be stored as NULL")
	})

	t.Run("is a no-op when no rows are staged", func(t *testing.T) {
		require.NoError(t, m.BatchUpsert(ctx, nil, nil))
	})
}
