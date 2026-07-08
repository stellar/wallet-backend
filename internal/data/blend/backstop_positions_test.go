// Unit tests for the Blend v2 BackstopPositionModel.
// These tests exercise real SQL and require a PostgreSQL test database.
// Uses an external test package to avoid an import cycle with internal/data.
package blend_test

import (
	"context"
	"encoding/json"
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

func newBackstopPositionsFixture(t *testing.T) (context.Context, *pgxpool.Pool, *blend.BackstopPositionModel, func()) {
	t.Helper()
	ctx := context.Background()

	dbt := dbtest.Open(t)
	pool, err := db.OpenDBConnectionPool(ctx, dbt.DSN)
	require.NoError(t, err)

	m := &blend.BackstopPositionModel{
		DB:      pool,
		Metrics: metrics.NewMetrics(prometheus.NewRegistry()).DB,
	}

	cleanup := func() {
		pool.Close()
		dbt.Close()
	}
	return ctx, pool, m, cleanup
}

type backstopPositionRow struct {
	Shares             string
	Q4W                []blend.Q4W
	LastModifiedLedger int32
}

func getBackstopPosition(t *testing.T, ctx context.Context, pool *pgxpool.Pool, poolAddr, userAddr string) (backstopPositionRow, bool) {
	t.Helper()
	var shares string
	var q4wRaw []byte
	var ledger int32
	err := pool.QueryRow(ctx, `
		SELECT shares, q4w, last_modified_ledger
		FROM blend_backstop_positions WHERE pool_contract_id = $1 AND user_account_id = $2
	`, types.AddressBytea(poolAddr), types.AddressBytea(userAddr)).Scan(&shares, &q4wRaw, &ledger)
	if err != nil {
		return backstopPositionRow{}, false
	}
	var q4w []blend.Q4W
	require.NoError(t, json.Unmarshal(q4wRaw, &q4w))
	return backstopPositionRow{Shares: shares, Q4W: q4w, LastModifiedLedger: ledger}, true
}

func TestBackstopPositionModel_BatchUpsert(t *testing.T) {
	ctx, pool, m, cleanup := newBackstopPositionsFixture(t)
	defer cleanup()

	poolAddr := keypair.MustRandom().Address()

	t.Run("round-trips a Q4W list through JSONB", func(t *testing.T) {
		userAddr := keypair.MustRandom().Address()
		runInTx(t, ctx, pool, func(tx pgx.Tx) {
			require.NoError(t, m.BatchUpsert(ctx, tx, []blend.BackstopPosition{{
				PoolContractID: types.AddressBytea(poolAddr),
				UserAccountID:  types.AddressBytea(userAddr),
				Shares:         "1000",
				Q4W: []blend.Q4W{
					{Amount: "100", Expiration: 111},
					{Amount: "200", Expiration: 222},
				},
				LastModifiedLedger: 5,
			}}))
		})

		row, ok := getBackstopPosition(t, ctx, pool, poolAddr, userAddr)
		require.True(t, ok)
		assert.Equal(t, "1000", row.Shares)
		require.Len(t, row.Q4W, 2)
		assert.Equal(t, "100", row.Q4W[0].Amount)
		assert.Equal(t, int64(111), row.Q4W[0].Expiration)
		assert.Equal(t, "200", row.Q4W[1].Amount)
		assert.Equal(t, int64(222), row.Q4W[1].Expiration)
		assert.Equal(t, int32(5), row.LastModifiedLedger)
	})

	t.Run("an empty Q4W slice round-trips as an empty list", func(t *testing.T) {
		userAddr := keypair.MustRandom().Address()
		runInTx(t, ctx, pool, func(tx pgx.Tx) {
			require.NoError(t, m.BatchUpsert(ctx, tx, []blend.BackstopPosition{{
				PoolContractID:     types.AddressBytea(poolAddr),
				UserAccountID:      types.AddressBytea(userAddr),
				Shares:             "500",
				Q4W:                nil,
				LastModifiedLedger: 6,
			}}))
		})

		row, ok := getBackstopPosition(t, ctx, pool, poolAddr, userAddr)
		require.True(t, ok)
		assert.Empty(t, row.Q4W)
	})

	t.Run("re-upsert fully replaces shares and q4w", func(t *testing.T) {
		userAddr := keypair.MustRandom().Address()
		runInTx(t, ctx, pool, func(tx pgx.Tx) {
			require.NoError(t, m.BatchUpsert(ctx, tx, []blend.BackstopPosition{{
				PoolContractID:     types.AddressBytea(poolAddr),
				UserAccountID:      types.AddressBytea(userAddr),
				Shares:             "10",
				Q4W:                []blend.Q4W{{Amount: "1", Expiration: 1}},
				LastModifiedLedger: 1,
			}}))
		})
		runInTx(t, ctx, pool, func(tx pgx.Tx) {
			require.NoError(t, m.BatchUpsert(ctx, tx, []blend.BackstopPosition{{
				PoolContractID:     types.AddressBytea(poolAddr),
				UserAccountID:      types.AddressBytea(userAddr),
				Shares:             "999",
				Q4W:                nil,
				LastModifiedLedger: 2,
			}}))
		})

		row, ok := getBackstopPosition(t, ctx, pool, poolAddr, userAddr)
		require.True(t, ok)
		assert.Equal(t, "999", row.Shares)
		assert.Empty(t, row.Q4W)
		assert.Equal(t, int32(2), row.LastModifiedLedger)
	})

	t.Run("is a no-op when no rows are staged", func(t *testing.T) {
		require.NoError(t, m.BatchUpsert(ctx, nil, nil))
	})
}

func TestBackstopPositionModel_DeleteByPoolUser(t *testing.T) {
	ctx, pool, m, cleanup := newBackstopPositionsFixture(t)
	defer cleanup()

	poolA := keypair.MustRandom().Address()
	poolB := keypair.MustRandom().Address()
	userA := keypair.MustRandom().Address()
	userB := keypair.MustRandom().Address()

	runInTx(t, ctx, pool, func(tx pgx.Tx) {
		require.NoError(t, m.BatchUpsert(ctx, tx, []blend.BackstopPosition{
			{PoolContractID: types.AddressBytea(poolA), UserAccountID: types.AddressBytea(userA), Shares: "1", LastModifiedLedger: 1},
			{PoolContractID: types.AddressBytea(poolA), UserAccountID: types.AddressBytea(userB), Shares: "2", LastModifiedLedger: 1},
			{PoolContractID: types.AddressBytea(poolB), UserAccountID: types.AddressBytea(userA), Shares: "3", LastModifiedLedger: 1},
		}))
	})

	runInTx(t, ctx, pool, func(tx pgx.Tx) {
		require.NoError(t, m.DeleteByPoolUser(ctx, tx, []blend.PoolUserKey{{Pool: poolA, User: userA}}))
	})

	_, ok := getBackstopPosition(t, ctx, pool, poolA, userA)
	assert.False(t, ok, "(poolA, userA) should be deleted")
	_, ok = getBackstopPosition(t, ctx, pool, poolA, userB)
	assert.True(t, ok, "a different user in the same pool must be untouched")
	_, ok = getBackstopPosition(t, ctx, pool, poolB, userA)
	assert.True(t, ok, "the same user in a different pool must be untouched")

	t.Run("is a no-op when no keys are staged", func(t *testing.T) {
		require.NoError(t, m.DeleteByPoolUser(ctx, nil, nil))
	})
}
