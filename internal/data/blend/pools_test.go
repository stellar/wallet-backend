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
	Admin              *string
	BackstopContractID *string
	InRewardZone       bool
	LastModifiedLedger int32
}

func getPool(t *testing.T, ctx context.Context, pool *pgxpool.Pool, poolAddr string) (poolRow, bool) {
	t.Helper()
	var row poolRow
	var oracle, admin, backstop *types.AddressBytea
	err := pool.QueryRow(ctx, `
		SELECT name, oracle_contract_id, backstop_rate, status, max_positions, min_collateral,
			admin, backstop_contract_id, in_reward_zone, last_modified_ledger
		FROM blend_pools WHERE pool_contract_id = $1
	`, types.AddressBytea(poolAddr)).Scan(
		&row.Name, &oracle, &row.BackstopRate, &row.Status, &row.MaxPositions, &row.MinCollateral,
		&admin, &backstop, &row.InRewardZone, &row.LastModifiedLedger,
	)
	if err != nil {
		return poolRow{}, false
	}
	if oracle != nil {
		s := string(*oracle)
		row.OracleContractID = &s
	}
	if admin != nil {
		s := string(*admin)
		row.Admin = &s
	}
	if backstop != nil {
		s := string(*backstop)
		row.BackstopContractID = &s
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
	adminAddr := keypair.MustRandom().Address()
	backstopAddr := keypair.MustRandom().Address()

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
				Admin:              types.AddressBytea(adminAddr),
				BackstopContractID: types.AddressBytea(backstopAddr),
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
		require.NotNil(t, row.Admin)
		assert.Equal(t, adminAddr, *row.Admin)
		require.NotNil(t, row.BackstopContractID)
		assert.Equal(t, backstopAddr, *row.BackstopContractID)
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
		require.NotNil(t, row.Admin, "admin must be preserved when the update carries empty")
		assert.Equal(t, adminAddr, *row.Admin)
		require.NotNil(t, row.BackstopContractID, "backstop must be preserved when the update carries empty")
		assert.Equal(t, backstopAddr, *row.BackstopContractID)
		assert.Equal(t, int32(11), row.LastModifiedLedger, "last_modified_ledger advances")
	})

	t.Run("re-upsert with a lower ledger never regresses last_modified_ledger", func(t *testing.T) {
		runInTx(t, ctx, pool, func(tx pgx.Tx) {
			// Validator enrichment carries no ledger context and writes 0.
			require.NoError(t, m.BatchUpsert(ctx, tx, []blend.Pool{{
				PoolContractID:     types.AddressBytea(poolAddr),
				Status:             i32Ptr(4),
				LastModifiedLedger: 0,
			}}))
		})

		row, ok := getPool(t, ctx, pool, poolAddr)
		require.True(t, ok)
		require.NotNil(t, row.Status)
		assert.Equal(t, int32(4), *row.Status, "config values still update")
		assert.Equal(t, int32(11), row.LastModifiedLedger, "ledger keeps the higher value (GREATEST)")
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

func TestPoolModel_SetRewardZone(t *testing.T) {
	ctx, pool, m, cleanup := newPoolsFixture(t)
	defer cleanup()

	poolA := keypair.MustRandom().Address()
	poolB := keypair.MustRandom().Address()
	poolC := keypair.MustRandom().Address()
	poolOther := keypair.MustRandom().Address()   // belongs to backstopOther
	poolUnknown := keypair.MustRandom().Address() // no known backstop
	backstop := types.AddressBytea(keypair.MustRandom().Address())
	backstopOther := types.AddressBytea(keypair.MustRandom().Address())

	runInTx(t, ctx, pool, func(tx pgx.Tx) {
		require.NoError(t, m.BatchUpsert(ctx, tx, []blend.Pool{
			{PoolContractID: types.AddressBytea(poolA), BackstopContractID: backstop, LastModifiedLedger: 1},
			{PoolContractID: types.AddressBytea(poolB), BackstopContractID: backstop, LastModifiedLedger: 1},
			{PoolContractID: types.AddressBytea(poolC), BackstopContractID: backstop, LastModifiedLedger: 1},
			{PoolContractID: types.AddressBytea(poolOther), BackstopContractID: backstopOther, LastModifiedLedger: 1},
			{PoolContractID: types.AddressBytea(poolUnknown), LastModifiedLedger: 1},
		}))
		require.NoError(t, m.SetRewardZone(ctx, tx, backstopOther, []types.AddressBytea{types.AddressBytea(poolOther)}, 50))
	})

	assertOtherUntouched := func(t *testing.T) {
		t.Helper()
		row, ok := getPool(t, ctx, pool, poolOther)
		require.True(t, ok)
		assert.True(t, row.InRewardZone, "another backstop's pool keeps its membership")
		assert.Equal(t, int32(50), row.LastModifiedLedger)
	}

	t.Run("marks the given pools as reward-zone members, others as non-members", func(t *testing.T) {
		runInTx(t, ctx, pool, func(tx pgx.Tx) {
			require.NoError(t, m.SetRewardZone(ctx, tx, backstop, []types.AddressBytea{
				types.AddressBytea(poolA), types.AddressBytea(poolB), types.AddressBytea(poolUnknown),
			}, 100))
		})

		rowA, ok := getPool(t, ctx, pool, poolA)
		require.True(t, ok)
		assert.True(t, rowA.InRewardZone)
		assert.Equal(t, int32(100), rowA.LastModifiedLedger, "membership changed, ledger bumped")

		rowB, ok := getPool(t, ctx, pool, poolB)
		require.True(t, ok)
		assert.True(t, rowB.InRewardZone)
		assert.Equal(t, int32(100), rowB.LastModifiedLedger)

		rowC, ok := getPool(t, ctx, pool, poolC)
		require.True(t, ok)
		assert.False(t, rowC.InRewardZone)
		assert.Equal(t, int32(1), rowC.LastModifiedLedger, "not a member before or after, unchanged")

		rowUnknown, ok := getPool(t, ctx, pool, poolUnknown)
		require.True(t, ok)
		assert.False(t, rowUnknown.InRewardZone, "a pool with no known backstop is never touched")
		assertOtherUntouched(t)
	})

	t.Run("dropping a pool from the set flips it false; unchanged members keep their ledger", func(t *testing.T) {
		runInTx(t, ctx, pool, func(tx pgx.Tx) {
			require.NoError(t, m.SetRewardZone(ctx, tx, backstop, []types.AddressBytea{
				types.AddressBytea(poolB),
			}, 200))
		})

		rowA, ok := getPool(t, ctx, pool, poolA)
		require.True(t, ok)
		assert.False(t, rowA.InRewardZone, "A dropped from the set")
		assert.Equal(t, int32(200), rowA.LastModifiedLedger, "membership changed, ledger bumped")

		rowB, ok := getPool(t, ctx, pool, poolB)
		require.True(t, ok)
		assert.True(t, rowB.InRewardZone, "B stays a member")
		assert.Equal(t, int32(100), rowB.LastModifiedLedger, "membership unchanged, ledger not bumped")
	})

	t.Run("an empty set clears membership across the backstop's pools (not a no-op)", func(t *testing.T) {
		runInTx(t, ctx, pool, func(tx pgx.Tx) {
			require.NoError(t, m.SetRewardZone(ctx, tx, backstop, []types.AddressBytea{}, 300))
		})

		rowA, ok := getPool(t, ctx, pool, poolA)
		require.True(t, ok)
		assert.False(t, rowA.InRewardZone)

		rowB, ok := getPool(t, ctx, pool, poolB)
		require.True(t, ok)
		assert.False(t, rowB.InRewardZone)
		assert.Equal(t, int32(300), rowB.LastModifiedLedger, "B's membership changed (true->false), ledger bumped")

		rowC, ok := getPool(t, ctx, pool, poolC)
		require.True(t, ok)
		assert.False(t, rowC.InRewardZone)
		assert.Equal(t, int32(1), rowC.LastModifiedLedger, "C was never a member, unchanged")
		assertOtherUntouched(t)
	})
}
