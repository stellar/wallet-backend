// Unit tests for the Blend v2 PositionModel.
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

func newPositionsFixture(t *testing.T) (context.Context, *pgxpool.Pool, *blend.PositionModel, func()) {
	t.Helper()
	ctx := context.Background()

	dbt := dbtest.Open(t)
	pool, err := db.OpenDBConnectionPool(ctx, dbt.DSN)
	require.NoError(t, err)

	m := &blend.PositionModel{
		DB:      pool,
		Metrics: metrics.NewMetrics(prometheus.NewRegistry()).DB,
	}

	cleanup := func() {
		pool.Close()
		dbt.Close()
	}
	return ctx, pool, m, cleanup
}

func runInTx(t *testing.T, ctx context.Context, pool *pgxpool.Pool, fn func(pgx.Tx)) {
	t.Helper()
	tx, err := pool.Begin(ctx)
	require.NoError(t, err)
	defer func() { _ = tx.Rollback(ctx) }()
	fn(tx)
	require.NoError(t, tx.Commit(ctx))
}

// insertPositionLedger is the fixed last_modified_ledger seeded by insertPosition.
// Tests that need to assert a ledger bump use it as the "before" value.
const insertPositionLedger = 1

// insertPosition seeds a blend_positions row directly via raw SQL so tests can set up
// fixtures (including net_supplied/net_borrowed) without going through the model.
func insertPosition(
	t *testing.T, ctx context.Context, pool *pgxpool.Pool,
	poolAddr, userAddr string, reserveIndex int32,
	supplyB, collateralB, liabilityD, netSupplied, netBorrowed string,
) {
	t.Helper()
	_, err := pool.Exec(ctx, `
		INSERT INTO blend_positions (
			pool_contract_id, user_account_id, reserve_index,
			supply_b_tokens, collateral_b_tokens, liability_d_tokens,
			net_supplied, net_borrowed, last_modified_ledger
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
	`, types.AddressBytea(poolAddr), types.AddressBytea(userAddr), reserveIndex,
		supplyB, collateralB, liabilityD, netSupplied, netBorrowed, insertPositionLedger)
	require.NoError(t, err)
}

// insertReserve seeds a blend_reserves row with the given rates. Other NOT NULL
// columns are filled with harmless zero/false defaults since the join-based
// position methods only read pool_contract_id, asset_contract_id, reserve_index,
// b_rate, and d_rate.
func insertReserve(
	t *testing.T, ctx context.Context, pool *pgxpool.Pool,
	poolAddr, assetAddr string, reserveIndex int32, bRate, dRate string,
) {
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
			$4, $5, '0', '0', '0', '0',
			0, 7, 0, 0, 0, 0,
			0, 0, 0, 0, 0, '0', true,
			0
		)
	`, types.AddressBytea(poolAddr), reserveIndex, types.AddressBytea(assetAddr), bRate, dRate)
	require.NoError(t, err)
}

type positionRow struct {
	SupplyB, CollateralB, LiabilityD string
	NetSupplied, NetBorrowed         string
	LastModifiedLedger               int32
}

func getPosition(t *testing.T, ctx context.Context, pool *pgxpool.Pool, poolAddr, userAddr string, reserveIndex int32) (positionRow, bool) {
	t.Helper()
	var row positionRow
	err := pool.QueryRow(ctx, `
		SELECT supply_b_tokens, collateral_b_tokens, liability_d_tokens, net_supplied, net_borrowed, last_modified_ledger
		FROM blend_positions
		WHERE pool_contract_id = $1 AND user_account_id = $2 AND reserve_index = $3
	`, types.AddressBytea(poolAddr), types.AddressBytea(userAddr), reserveIndex).Scan(
		&row.SupplyB, &row.CollateralB, &row.LiabilityD, &row.NetSupplied, &row.NetBorrowed, &row.LastModifiedLedger,
	)
	if err != nil {
		return positionRow{}, false
	}
	return row, true
}

func TestPositionModel_BatchUpsertSnapshots(t *testing.T) {
	ctx, pool, m, cleanup := newPositionsFixture(t)
	defer cleanup()

	poolAddr := keypair.MustRandom().Address()
	userAddr := keypair.MustRandom().Address()

	t.Run("inserts a fresh row", func(t *testing.T) {
		runInTx(t, ctx, pool, func(tx pgx.Tx) {
			require.NoError(t, m.BatchUpsertSnapshots(ctx, tx, []blend.PositionSnapshot{{
				Pool: poolAddr, User: userAddr, ReserveIndex: 0,
				SupplyBTokens: "100", CollateralBTokens: "200", LiabilityDTokens: "50",
				LedgerNumber: 10,
			}}))
		})

		row, ok := getPosition(t, ctx, pool, poolAddr, userAddr, 0)
		require.True(t, ok)
		assert.Equal(t, "100", row.SupplyB)
		assert.Equal(t, "200", row.CollateralB)
		assert.Equal(t, "50", row.LiabilityD)
		assert.Equal(t, "0", row.NetSupplied)
		assert.Equal(t, "0", row.NetBorrowed)
		assert.Equal(t, int32(10), row.LastModifiedLedger)
	})

	t.Run("re-upsert replaces token columns without touching net_supplied/net_borrowed", func(t *testing.T) {
		userAddr := keypair.MustRandom().Address()
		// Seed net_supplied/net_borrowed via a net-delta application first.
		runInTx(t, ctx, pool, func(tx pgx.Tx) {
			require.NoError(t, m.BatchUpsertSnapshots(ctx, tx, []blend.PositionSnapshot{{
				Pool: poolAddr, User: userAddr, ReserveIndex: 1,
				SupplyBTokens: "1", CollateralBTokens: "2", LiabilityDTokens: "3",
				LedgerNumber: 5,
			}}))
		})
		assetForUpsertNetTest := keypair.MustRandom().Address()
		insertReserve(t, ctx, pool, poolAddr, assetForUpsertNetTest, 1, "1000000000000", "1000000000000")
		runInTx(t, ctx, pool, func(tx pgx.Tx) {
			require.NoError(t, m.BatchApplyNetDeltas(ctx, tx, []blend.PositionNetDelta{{
				Pool: poolAddr, User: userAddr, Asset: assetForUpsertNetTest,
				NetSuppliedDelta: "777", NetBorrowedDelta: "888", LedgerNumber: 6,
			}}))
		})
		row, ok := getPosition(t, ctx, pool, poolAddr, userAddr, 1)
		require.True(t, ok)
		require.Equal(t, "777", row.NetSupplied)
		require.Equal(t, "888", row.NetBorrowed)

		// Now re-upsert the token snapshot with new values.
		runInTx(t, ctx, pool, func(tx pgx.Tx) {
			require.NoError(t, m.BatchUpsertSnapshots(ctx, tx, []blend.PositionSnapshot{{
				Pool: poolAddr, User: userAddr, ReserveIndex: 1,
				SupplyBTokens: "999", CollateralBTokens: "888", LiabilityDTokens: "777",
				LedgerNumber: 7,
			}}))
		})

		row, ok = getPosition(t, ctx, pool, poolAddr, userAddr, 1)
		require.True(t, ok)
		assert.Equal(t, "999", row.SupplyB)
		assert.Equal(t, "888", row.CollateralB)
		assert.Equal(t, "777", row.LiabilityD)
		assert.Equal(t, int32(7), row.LastModifiedLedger)
		// net_supplied/net_borrowed must remain untouched by the snapshot upsert.
		assert.Equal(t, "777", row.NetSupplied)
		assert.Equal(t, "888", row.NetBorrowed)
	})

	t.Run("is a no-op when no rows are staged", func(t *testing.T) {
		require.NoError(t, m.BatchUpsertSnapshots(ctx, nil, nil))
	})
}

func TestPositionModel_ZeroAbsentReserves(t *testing.T) {
	ctx, pool, m, cleanup := newPositionsFixture(t)
	defer cleanup()

	poolAddr := keypair.MustRandom().Address()

	t.Run("zeroes only the absent index, leaving present ones untouched", func(t *testing.T) {
		userAddr := keypair.MustRandom().Address()
		insertPosition(t, ctx, pool, poolAddr, userAddr, 0, "10", "20", "30", "1", "2")
		insertPosition(t, ctx, pool, poolAddr, userAddr, 1, "11", "21", "31", "3", "4")
		insertPosition(t, ctx, pool, poolAddr, userAddr, 2, "12", "22", "32", "5", "6")

		// A different user's rows must be untouched by this call.
		otherUser := keypair.MustRandom().Address()
		insertPosition(t, ctx, pool, poolAddr, otherUser, 1, "77", "78", "79", "1", "1")

		runInTx(t, ctx, pool, func(tx pgx.Tx) {
			require.NoError(t, m.ZeroAbsentReserves(ctx, tx, []blend.PositionPresence{{
				Pool: poolAddr, User: userAddr, PresentIndexes: []int32{0, 2}, LedgerNumber: 99,
			}}))
		})

		row0, ok := getPosition(t, ctx, pool, poolAddr, userAddr, 0)
		require.True(t, ok)
		assert.Equal(t, "10", row0.SupplyB, "present index 0 must be untouched")
		assert.Equal(t, "20", row0.CollateralB)
		assert.Equal(t, "30", row0.LiabilityD)
		assert.Equal(t, int32(insertPositionLedger), row0.LastModifiedLedger)

		row1, ok := getPosition(t, ctx, pool, poolAddr, userAddr, 1)
		require.True(t, ok)
		assert.Equal(t, "0", row1.SupplyB, "absent index 1 must be zeroed")
		assert.Equal(t, "0", row1.CollateralB)
		assert.Equal(t, "0", row1.LiabilityD)
		assert.Equal(t, int32(99), row1.LastModifiedLedger)
		// net columns are cost basis and must never be touched by zeroing.
		assert.Equal(t, "3", row1.NetSupplied)
		assert.Equal(t, "4", row1.NetBorrowed)

		row2, ok := getPosition(t, ctx, pool, poolAddr, userAddr, 2)
		require.True(t, ok)
		assert.Equal(t, "12", row2.SupplyB, "present index 2 must be untouched")

		otherRow, ok := getPosition(t, ctx, pool, poolAddr, otherUser, 1)
		require.True(t, ok)
		assert.Equal(t, "77", otherRow.SupplyB, "a different user's rows must be untouched")
	})

	t.Run("empty PresentIndexes zeroes every reserve for that user", func(t *testing.T) {
		userAddr := keypair.MustRandom().Address()
		insertPosition(t, ctx, pool, poolAddr, userAddr, 0, "10", "20", "30", "1", "2")
		insertPosition(t, ctx, pool, poolAddr, userAddr, 1, "11", "21", "31", "3", "4")

		runInTx(t, ctx, pool, func(tx pgx.Tx) {
			require.NoError(t, m.ZeroAbsentReserves(ctx, tx, []blend.PositionPresence{{
				Pool: poolAddr, User: userAddr, PresentIndexes: nil, LedgerNumber: 50,
			}}))
		})

		row0, ok := getPosition(t, ctx, pool, poolAddr, userAddr, 0)
		require.True(t, ok)
		assert.Equal(t, "0", row0.SupplyB)
		assert.Equal(t, "0", row0.CollateralB)
		assert.Equal(t, "0", row0.LiabilityD)

		row1, ok := getPosition(t, ctx, pool, poolAddr, userAddr, 1)
		require.True(t, ok)
		assert.Equal(t, "0", row1.SupplyB)
	})

	t.Run("is a no-op when no presences are staged", func(t *testing.T) {
		require.NoError(t, m.ZeroAbsentReserves(ctx, nil, nil))
	})
}

func TestPositionModel_BatchApplyNetDeltas(t *testing.T) {
	ctx, pool, m, cleanup := newPositionsFixture(t)
	defer cleanup()

	poolAddr := keypair.MustRandom().Address()
	assetAddr := keypair.MustRandom().Address()
	insertReserve(t, ctx, pool, poolAddr, assetAddr, 3, "1100000000000", "1050000000000")

	t.Run("adds server-side to existing net values", func(t *testing.T) {
		userAddr := keypair.MustRandom().Address()
		insertPosition(t, ctx, pool, poolAddr, userAddr, 3, "0", "0", "0", "1000", "500")

		runInTx(t, ctx, pool, func(tx pgx.Tx) {
			require.NoError(t, m.BatchApplyNetDeltas(ctx, tx, []blend.PositionNetDelta{{
				Pool: poolAddr, User: userAddr, Asset: assetAddr,
				NetSuppliedDelta: "250", NetBorrowedDelta: "100", LedgerNumber: 42,
			}}))
		})

		row, ok := getPosition(t, ctx, pool, poolAddr, userAddr, 3)
		require.True(t, ok)
		assert.Equal(t, "1250", row.NetSupplied)
		assert.Equal(t, "600", row.NetBorrowed)
		assert.Equal(t, int32(42), row.LastModifiedLedger)
	})

	t.Run("ZeroBorrowed replaces net_borrowed instead of adding", func(t *testing.T) {
		userAddr := keypair.MustRandom().Address()
		insertPosition(t, ctx, pool, poolAddr, userAddr, 3, "0", "0", "0", "1000", "9999999")

		runInTx(t, ctx, pool, func(tx pgx.Tx) {
			require.NoError(t, m.BatchApplyNetDeltas(ctx, tx, []blend.PositionNetDelta{{
				Pool: poolAddr, User: userAddr, Asset: assetAddr,
				NetSuppliedDelta: "0", NetBorrowedDelta: "0", ZeroBorrowed: true, LedgerNumber: 43,
			}}))
		})

		row, ok := getPosition(t, ctx, pool, poolAddr, userAddr, 3)
		require.True(t, ok)
		assert.Equal(t, "0", row.NetBorrowed, "bad-debt fold must replace, not add")
	})

	t.Run("unknown asset with no blend_reserves row changes nothing and errors nothing", func(t *testing.T) {
		userAddr := keypair.MustRandom().Address()
		insertPosition(t, ctx, pool, poolAddr, userAddr, 3, "0", "0", "0", "1000", "500")
		unknownAsset := keypair.MustRandom().Address()

		runInTx(t, ctx, pool, func(tx pgx.Tx) {
			require.NoError(t, m.BatchApplyNetDeltas(ctx, tx, []blend.PositionNetDelta{{
				Pool: poolAddr, User: userAddr, Asset: unknownAsset,
				NetSuppliedDelta: "250", NetBorrowedDelta: "100", LedgerNumber: 44,
			}}))
		})

		row, ok := getPosition(t, ctx, pool, poolAddr, userAddr, 3)
		require.True(t, ok)
		assert.Equal(t, "1000", row.NetSupplied, "no matching reserve row: nothing should change")
		assert.Equal(t, "500", row.NetBorrowed)
		assert.Equal(t, int32(insertPositionLedger), row.LastModifiedLedger)
	})

	t.Run("is a no-op when no deltas are staged", func(t *testing.T) {
		require.NoError(t, m.BatchApplyNetDeltas(ctx, nil, nil))
	})

	t.Run("rejects duplicate (pool, user, asset) keys", func(t *testing.T) {
		// UPDATE ... FROM would apply only one of the two source rows, and
		// merging them here would be order-dependent because of ZeroBorrowed —
		// the model demands pre-aggregated input instead.
		userAddr := keypair.MustRandom().Address()
		tx, err := pool.Begin(ctx)
		require.NoError(t, err)
		defer func() { _ = tx.Rollback(ctx) }()
		err = m.BatchApplyNetDeltas(ctx, tx, []blend.PositionNetDelta{
			{Pool: poolAddr, User: userAddr, Asset: assetAddr, NetSuppliedDelta: "10", NetBorrowedDelta: "0", LedgerNumber: 5},
			{Pool: poolAddr, User: userAddr, Asset: assetAddr, NetSuppliedDelta: "20", NetBorrowedDelta: "0", LedgerNumber: 5},
		})
		require.ErrorContains(t, err, "pre-aggregated")
	})
}

func TestPositionModel_ApplyAuctionAdjustments(t *testing.T) {
	ctx, pool, m, cleanup := newPositionsFixture(t)
	defer cleanup()

	poolAddr := keypair.MustRandom().Address()
	assetAddr := keypair.MustRandom().Address()
	// b_rate = 1.1 (scaled by 1e12), d_rate = 1.05 (scaled by 1e12).
	insertReserve(t, ctx, pool, poolAddr, assetAddr, 4, "1100000000000", "1050000000000")

	t.Run("values protocol-token deltas at current rates", func(t *testing.T) {
		userAddr := keypair.MustRandom().Address()
		insertPosition(t, ctx, pool, poolAddr, userAddr, 4, "0", "0", "0", "0", "0")

		runInTx(t, ctx, pool, func(tx pgx.Tx) {
			require.NoError(t, m.ApplyAuctionAdjustments(ctx, tx, []blend.PositionAuctionAdjustment{{
				Pool: poolAddr, User: userAddr, Asset: assetAddr,
				LotBTokensDelta: "1000", BidDTokensDelta: "1000", LedgerNumber: 77,
			}}))
		})

		row, ok := getPosition(t, ctx, pool, poolAddr, userAddr, 4)
		require.True(t, ok)
		// 1000 lot b-tokens * 1.1 b_rate = 1100 underlying added to net_supplied.
		assert.Equal(t, "1100.0000000000000000", row.NetSupplied)
		// 1000 bid d-tokens * 1.05 d_rate = 1050 underlying added to net_borrowed.
		assert.Equal(t, "1050.0000000000000000", row.NetBorrowed)
		assert.Equal(t, int32(77), row.LastModifiedLedger)
	})

	t.Run("sums duplicate (pool, user, asset) adjustments before applying", func(t *testing.T) {
		userAddr := keypair.MustRandom().Address()
		insertPosition(t, ctx, pool, poolAddr, userAddr, 4, "0", "0", "0", "0", "0")

		// Two fills against the same position in one batch: without the
		// GROUP BY pre-aggregation, UPDATE ... FROM would apply only one.
		runInTx(t, ctx, pool, func(tx pgx.Tx) {
			require.NoError(t, m.ApplyAuctionAdjustments(ctx, tx, []blend.PositionAuctionAdjustment{
				{Pool: poolAddr, User: userAddr, Asset: assetAddr, LotBTokensDelta: "1000", BidDTokensDelta: "1000", LedgerNumber: 77},
				{Pool: poolAddr, User: userAddr, Asset: assetAddr, LotBTokensDelta: "500", BidDTokensDelta: "500", LedgerNumber: 78},
			}))
		})

		row, ok := getPosition(t, ctx, pool, poolAddr, userAddr, 4)
		require.True(t, ok)
		// (1000 + 500) lot b-tokens * 1.1 b_rate = 1650 underlying.
		assert.Equal(t, "1650.0000000000000000", row.NetSupplied)
		// (1000 + 500) bid d-tokens * 1.05 d_rate = 1575 underlying.
		assert.Equal(t, "1575.0000000000000000", row.NetBorrowed)
		assert.Equal(t, int32(78), row.LastModifiedLedger, "ledger takes the MAX across the batch")
	})

	t.Run("is a no-op when no adjustments are staged", func(t *testing.T) {
		require.NoError(t, m.ApplyAuctionAdjustments(ctx, nil, nil))
	})
}

func TestPositionModel_DeleteByPoolUser(t *testing.T) {
	ctx, pool, m, cleanup := newPositionsFixture(t)
	defer cleanup()

	poolA := keypair.MustRandom().Address()
	poolB := keypair.MustRandom().Address()
	userA := keypair.MustRandom().Address()
	userB := keypair.MustRandom().Address()

	insertPosition(t, ctx, pool, poolA, userA, 0, "1", "1", "1", "1", "1")
	insertPosition(t, ctx, pool, poolA, userA, 1, "2", "2", "2", "2", "2")
	insertPosition(t, ctx, pool, poolA, userB, 0, "3", "3", "3", "3", "3")
	insertPosition(t, ctx, pool, poolB, userA, 0, "4", "4", "4", "4", "4")

	runInTx(t, ctx, pool, func(tx pgx.Tx) {
		require.NoError(t, m.DeleteByPoolUser(ctx, tx, []blend.PoolUserKey{{Pool: poolA, User: userA}}))
	})

	_, ok := getPosition(t, ctx, pool, poolA, userA, 0)
	assert.False(t, ok, "(poolA, userA) reserve 0 should be deleted")
	_, ok = getPosition(t, ctx, pool, poolA, userA, 1)
	assert.False(t, ok, "(poolA, userA) reserve 1 should be deleted")

	_, ok = getPosition(t, ctx, pool, poolA, userB, 0)
	assert.True(t, ok, "a different user in the same pool must be untouched")
	_, ok = getPosition(t, ctx, pool, poolB, userA, 0)
	assert.True(t, ok, "the same user in a different pool must be untouched")

	t.Run("is a no-op when no keys are staged", func(t *testing.T) {
		require.NoError(t, m.DeleteByPoolUser(ctx, nil, nil))
	})
}
