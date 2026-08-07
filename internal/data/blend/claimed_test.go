// Unit tests for the Blend v2 PoolClaimedModel and BackstopClaimedModel.
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

func newClaimedFixture(t *testing.T) (context.Context, *pgxpool.Pool, *blend.PoolClaimedModel, *blend.BackstopClaimedModel, func()) {
	t.Helper()
	ctx := context.Background()

	dbt := dbtest.Open(t)
	pool, err := db.OpenDBConnectionPool(ctx, dbt.DSN)
	require.NoError(t, err)

	dbMetrics := metrics.NewMetrics(prometheus.NewRegistry()).DB
	pm := &blend.PoolClaimedModel{DB: pool, Metrics: dbMetrics}
	bm := &blend.BackstopClaimedModel{DB: pool, Metrics: dbMetrics}

	cleanup := func() {
		pool.Close()
		dbt.Close()
	}
	return ctx, pool, pm, bm, cleanup
}

func getPoolClaimed(t *testing.T, ctx context.Context, pool *pgxpool.Pool, poolAddr, userAddr string) (claimed string, ledger int32, ok bool) {
	t.Helper()
	err := pool.QueryRow(ctx, `
		SELECT claimed_blnd, last_modified_ledger FROM blend_pool_claimed
		WHERE pool_contract_id = $1 AND user_account_id = $2
	`, types.AddressBytea(poolAddr), types.AddressBytea(userAddr)).Scan(&claimed, &ledger)
	if err != nil {
		return "", 0, false
	}
	return claimed, ledger, true
}

func getBackstopClaimed(t *testing.T, ctx context.Context, pool *pgxpool.Pool, userAddr string) (claimed string, ledger int32, ok bool) {
	t.Helper()
	err := pool.QueryRow(ctx, `
		SELECT claimed_lp, last_modified_ledger FROM blend_backstop_claimed
		WHERE user_account_id = $1
	`, types.AddressBytea(userAddr)).Scan(&claimed, &ledger)
	if err != nil {
		return "", 0, false
	}
	return claimed, ledger, true
}

func TestPoolClaimedModel_BatchApplyDeltas(t *testing.T) {
	ctx, pool, pm, _, cleanup := newClaimedFixture(t)
	defer cleanup()

	poolAddr := keypair.MustRandom().Address() // C-address stand-in; only its bytes matter here
	userA := keypair.MustRandom().Address()
	userB := keypair.MustRandom().Address()

	t.Run("empty input is a no-op", func(t *testing.T) {
		runInTx(t, ctx, pool, func(tx pgx.Tx) {
			require.NoError(t, pm.BatchApplyDeltas(ctx, tx, nil))
		})
	})

	t.Run("first delta inserts, second accumulates", func(t *testing.T) {
		runInTx(t, ctx, pool, func(tx pgx.Tx) {
			require.NoError(t, pm.BatchApplyDeltas(ctx, tx, []blend.PoolClaimedDelta{
				{Pool: poolAddr, User: userA, ClaimedBlnd: "100", LedgerNumber: 5},
			}))
		})
		claimed, ledger, ok := getPoolClaimed(t, ctx, pool, poolAddr, userA)
		require.True(t, ok)
		assert.Equal(t, "100", claimed)
		assert.Equal(t, int32(5), ledger)

		runInTx(t, ctx, pool, func(tx pgx.Tx) {
			require.NoError(t, pm.BatchApplyDeltas(ctx, tx, []blend.PoolClaimedDelta{
				{Pool: poolAddr, User: userA, ClaimedBlnd: "40", LedgerNumber: 9},
			}))
		})
		claimed, ledger, ok = getPoolClaimed(t, ctx, pool, poolAddr, userA)
		require.True(t, ok)
		assert.Equal(t, "140", claimed, "second claim accumulates onto the first")
		assert.Equal(t, int32(9), ledger, "ledger advances to the newer claim")
	})

	t.Run("distinct users are independent", func(t *testing.T) {
		runInTx(t, ctx, pool, func(tx pgx.Tx) {
			require.NoError(t, pm.BatchApplyDeltas(ctx, tx, []blend.PoolClaimedDelta{
				{Pool: poolAddr, User: userB, ClaimedBlnd: "7", LedgerNumber: 3},
			}))
		})
		claimed, _, ok := getPoolClaimed(t, ctx, pool, poolAddr, userB)
		require.True(t, ok)
		assert.Equal(t, "7", claimed)
		// userA untouched.
		claimedA, _, _ := getPoolClaimed(t, ctx, pool, poolAddr, userA)
		assert.Equal(t, "140", claimedA)
	})

	t.Run("duplicate key in one batch is rejected", func(t *testing.T) {
		runInTx(t, ctx, pool, func(tx pgx.Tx) {
			err := pm.BatchApplyDeltas(ctx, tx, []blend.PoolClaimedDelta{
				{Pool: poolAddr, User: userA, ClaimedBlnd: "1", LedgerNumber: 10},
				{Pool: poolAddr, User: userA, ClaimedBlnd: "2", LedgerNumber: 10},
			})
			require.Error(t, err)
			assert.Contains(t, err.Error(), "pre-aggregated")
		})
	})
}

func TestBackstopClaimedModel_BatchApplyDeltas(t *testing.T) {
	ctx, pool, _, bm, cleanup := newClaimedFixture(t)
	defer cleanup()

	userA := keypair.MustRandom().Address()
	userB := keypair.MustRandom().Address()

	t.Run("empty input is a no-op", func(t *testing.T) {
		runInTx(t, ctx, pool, func(tx pgx.Tx) {
			require.NoError(t, bm.BatchApplyDeltas(ctx, tx, nil))
		})
	})

	t.Run("accumulates per user account-wide", func(t *testing.T) {
		runInTx(t, ctx, pool, func(tx pgx.Tx) {
			require.NoError(t, bm.BatchApplyDeltas(ctx, tx, []blend.BackstopClaimedDelta{
				{User: userA, ClaimedLp: "1000", LedgerNumber: 4},
				{User: userB, ClaimedLp: "50", LedgerNumber: 4},
			}))
		})
		runInTx(t, ctx, pool, func(tx pgx.Tx) {
			require.NoError(t, bm.BatchApplyDeltas(ctx, tx, []blend.BackstopClaimedDelta{
				{User: userA, ClaimedLp: "250", LedgerNumber: 8},
			}))
		})
		claimedA, ledgerA, ok := getBackstopClaimed(t, ctx, pool, userA)
		require.True(t, ok)
		assert.Equal(t, "1250", claimedA)
		assert.Equal(t, int32(8), ledgerA)

		claimedB, _, ok := getBackstopClaimed(t, ctx, pool, userB)
		require.True(t, ok)
		assert.Equal(t, "50", claimedB)
	})

	t.Run("duplicate key in one batch is rejected", func(t *testing.T) {
		runInTx(t, ctx, pool, func(tx pgx.Tx) {
			err := bm.BatchApplyDeltas(ctx, tx, []blend.BackstopClaimedDelta{
				{User: userA, ClaimedLp: "1", LedgerNumber: 9},
				{User: userA, ClaimedLp: "2", LedgerNumber: 9},
			})
			require.Error(t, err)
			assert.Contains(t, err.Error(), "pre-aggregated")
		})
	})
}
