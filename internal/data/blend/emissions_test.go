// Unit tests for the Blend v2 ReserveEmissionModel and EmissionModel.
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

func newReserveEmissionsFixture(t *testing.T) (context.Context, *pgxpool.Pool, *blend.ReserveEmissionModel, func()) {
	t.Helper()
	ctx := context.Background()

	dbt := dbtest.Open(t)
	pool, err := db.OpenDBConnectionPool(ctx, dbt.DSN)
	require.NoError(t, err)

	m := &blend.ReserveEmissionModel{
		DB:      pool,
		Metrics: metrics.NewMetrics(prometheus.NewRegistry()).DB,
	}

	cleanup := func() {
		pool.Close()
		dbt.Close()
	}
	return ctx, pool, m, cleanup
}

func newEmissionsFixture(t *testing.T) (context.Context, *pgxpool.Pool, *blend.EmissionModel, func()) {
	t.Helper()
	ctx := context.Background()

	dbt := dbtest.Open(t)
	pool, err := db.OpenDBConnectionPool(ctx, dbt.DSN)
	require.NoError(t, err)

	m := &blend.EmissionModel{
		DB:      pool,
		Metrics: metrics.NewMetrics(prometheus.NewRegistry()).DB,
	}

	cleanup := func() {
		pool.Close()
		dbt.Close()
	}
	return ctx, pool, m, cleanup
}

type reserveEmissionRow struct {
	Eps                int64
	EmissionIndex      string
	Expiration         int64
	LastTime           int64
	LastModifiedLedger int32
}

func getReserveEmission(t *testing.T, ctx context.Context, pool *pgxpool.Pool, poolAddr string, tokenID int32) (reserveEmissionRow, bool) {
	t.Helper()
	var row reserveEmissionRow
	err := pool.QueryRow(ctx, `
		SELECT eps, emission_index, expiration, last_time, last_modified_ledger
		FROM blend_reserve_emissions WHERE pool_contract_id = $1 AND reserve_token_id = $2
	`, types.AddressBytea(poolAddr), tokenID).Scan(&row.Eps, &row.EmissionIndex, &row.Expiration, &row.LastTime, &row.LastModifiedLedger)
	if err != nil {
		return reserveEmissionRow{}, false
	}
	return row, true
}

func TestReserveEmissionModel_BatchUpsert(t *testing.T) {
	ctx, pool, m, cleanup := newReserveEmissionsFixture(t)
	defer cleanup()

	poolAddr := keypair.MustRandom().Address()

	t.Run("inserts a fresh row", func(t *testing.T) {
		runInTx(t, ctx, pool, func(tx pgx.Tx) {
			require.NoError(t, m.BatchUpsert(ctx, tx, []blend.ReserveEmission{{
				PoolContractID: types.AddressBytea(poolAddr), ReserveTokenID: 0,
				Eps: 100, EmissionIndex: "1", Expiration: 5000, LastTime: 100,
				LastModifiedLedger: 1,
			}}))
		})

		row, ok := getReserveEmission(t, ctx, pool, poolAddr, 0)
		require.True(t, ok)
		assert.Equal(t, int64(100), row.Eps)
		assert.Equal(t, "1", row.EmissionIndex)
		assert.Equal(t, int64(5000), row.Expiration)
	})

	t.Run("re-upsert replaces every column", func(t *testing.T) {
		runInTx(t, ctx, pool, func(tx pgx.Tx) {
			require.NoError(t, m.BatchUpsert(ctx, tx, []blend.ReserveEmission{{
				PoolContractID: types.AddressBytea(poolAddr), ReserveTokenID: 0,
				Eps: 200, EmissionIndex: "2", Expiration: 6000, LastTime: 200,
				LastModifiedLedger: 2,
			}}))
		})

		row, ok := getReserveEmission(t, ctx, pool, poolAddr, 0)
		require.True(t, ok)
		assert.Equal(t, int64(200), row.Eps)
		assert.Equal(t, "2", row.EmissionIndex)
		assert.Equal(t, int64(6000), row.Expiration)
		assert.Equal(t, int32(2), row.LastModifiedLedger)
	})

	t.Run("is a no-op when no rows are staged", func(t *testing.T) {
		require.NoError(t, m.BatchUpsert(ctx, nil, nil))
	})
}

type emissionRow struct {
	EmissionIndex      string
	Accrued            string
	LastModifiedLedger int32
}

func getEmission(t *testing.T, ctx context.Context, pool *pgxpool.Pool, sourceAddr, userAddr string, tokenID int32) (emissionRow, bool) {
	t.Helper()
	var row emissionRow
	err := pool.QueryRow(ctx, `
		SELECT emission_index, accrued, last_modified_ledger
		FROM blend_emissions WHERE source_contract_id = $1 AND user_account_id = $2 AND token_id = $3
	`, types.AddressBytea(sourceAddr), types.AddressBytea(userAddr), tokenID).Scan(&row.EmissionIndex, &row.Accrued, &row.LastModifiedLedger)
	if err != nil {
		return emissionRow{}, false
	}
	return row, true
}

func TestEmissionModel_BatchUpsert(t *testing.T) {
	ctx, pool, m, cleanup := newEmissionsFixture(t)
	defer cleanup()

	sourceAddr := keypair.MustRandom().Address()
	userAddr := keypair.MustRandom().Address()

	t.Run("inserts a fresh reserve-emission row", func(t *testing.T) {
		runInTx(t, ctx, pool, func(tx pgx.Tx) {
			require.NoError(t, m.BatchUpsert(ctx, tx, []blend.Emission{{
				SourceContractID: types.AddressBytea(sourceAddr), UserAccountID: types.AddressBytea(userAddr),
				TokenID: 0, EmissionIndex: "1", Accrued: "100",
				LastModifiedLedger: 1,
			}}))
		})

		row, ok := getEmission(t, ctx, pool, sourceAddr, userAddr, 0)
		require.True(t, ok)
		assert.Equal(t, "1", row.EmissionIndex)
		assert.Equal(t, "100", row.Accrued)
	})

	t.Run("backstop emission stream uses the sentinel token id and stays distinct", func(t *testing.T) {
		runInTx(t, ctx, pool, func(tx pgx.Tx) {
			require.NoError(t, m.BatchUpsert(ctx, tx, []blend.Emission{{
				SourceContractID: types.AddressBytea(sourceAddr), UserAccountID: types.AddressBytea(userAddr),
				TokenID: blend.BackstopEmissionTokenID, EmissionIndex: "9", Accrued: "999",
				LastModifiedLedger: 1,
			}}))
		})

		backstopRow, ok := getEmission(t, ctx, pool, sourceAddr, userAddr, blend.BackstopEmissionTokenID)
		require.True(t, ok)
		assert.Equal(t, "999", backstopRow.Accrued)

		// The reserve-emission row (token_id 0) from the previous subtest must be untouched.
		reserveRow, ok := getEmission(t, ctx, pool, sourceAddr, userAddr, 0)
		require.True(t, ok)
		assert.Equal(t, "100", reserveRow.Accrued)
	})

	t.Run("re-upsert replaces emission_index and accrued", func(t *testing.T) {
		runInTx(t, ctx, pool, func(tx pgx.Tx) {
			require.NoError(t, m.BatchUpsert(ctx, tx, []blend.Emission{{
				SourceContractID: types.AddressBytea(sourceAddr), UserAccountID: types.AddressBytea(userAddr),
				TokenID: 0, EmissionIndex: "5", Accrued: "555",
				LastModifiedLedger: 3,
			}}))
		})

		row, ok := getEmission(t, ctx, pool, sourceAddr, userAddr, 0)
		require.True(t, ok)
		assert.Equal(t, "5", row.EmissionIndex)
		assert.Equal(t, "555", row.Accrued)
		assert.Equal(t, int32(3), row.LastModifiedLedger)
	})

	t.Run("is a no-op when no rows are staged", func(t *testing.T) {
		require.NoError(t, m.BatchUpsert(ctx, nil, nil))
	})
}
