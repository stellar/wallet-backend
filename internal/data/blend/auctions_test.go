// Unit tests for the Blend v2 AuctionModel.
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

func newAuctionsFixture(t *testing.T) (context.Context, *pgxpool.Pool, *blend.AuctionModel, func()) {
	t.Helper()
	ctx := context.Background()

	dbt := dbtest.Open(t)
	pool, err := db.OpenDBConnectionPool(ctx, dbt.DSN)
	require.NoError(t, err)

	m := &blend.AuctionModel{
		DB:      pool,
		Metrics: metrics.NewMetrics(prometheus.NewRegistry()).DB,
	}

	cleanup := func() {
		pool.Close()
		dbt.Close()
	}
	return ctx, pool, m, cleanup
}

type auctionRow struct {
	Bid                map[string]string
	Lot                map[string]string
	StartBlock         int32
	LastModifiedLedger int32
}

func getAuction(t *testing.T, ctx context.Context, pool *pgxpool.Pool, poolAddr, userAddr string, auctionType int32) (auctionRow, bool) {
	t.Helper()
	var bidRaw, lotRaw []byte
	var startBlock, ledger int32
	err := pool.QueryRow(ctx, `
		SELECT bid, lot, start_block, last_modified_ledger
		FROM blend_auctions WHERE pool_contract_id = $1 AND user_account_id = $2 AND auction_type = $3
	`, types.AddressBytea(poolAddr), types.AddressBytea(userAddr), auctionType).Scan(&bidRaw, &lotRaw, &startBlock, &ledger)
	if err != nil {
		return auctionRow{}, false
	}
	var bid, lot map[string]string
	require.NoError(t, json.Unmarshal(bidRaw, &bid))
	require.NoError(t, json.Unmarshal(lotRaw, &lot))
	return auctionRow{Bid: bid, Lot: lot, StartBlock: startBlock, LastModifiedLedger: ledger}, true
}

func TestAuctionModel_BatchUpsert(t *testing.T) {
	ctx, pool, m, cleanup := newAuctionsFixture(t)
	defer cleanup()

	poolAddr := keypair.MustRandom().Address()

	t.Run("round-trips bid/lot maps through JSONB", func(t *testing.T) {
		userAddr := keypair.MustRandom().Address()
		assetA := keypair.MustRandom().Address()
		assetB := keypair.MustRandom().Address()
		runInTx(t, ctx, pool, func(tx pgx.Tx) {
			require.NoError(t, m.BatchUpsert(ctx, tx, []blend.Auction{{
				Pool:        types.AddressBytea(poolAddr),
				User:        types.AddressBytea(userAddr),
				AuctionType: 0,
				Bid: map[string]string{
					assetA: "100",
					assetB: "200",
				},
				Lot: map[string]string{
					assetA: "300",
					assetB: "400",
				},
				StartBlock:         1000,
				LastModifiedLedger: 5,
			}}))
		})

		row, ok := getAuction(t, ctx, pool, poolAddr, userAddr, 0)
		require.True(t, ok)
		require.Len(t, row.Bid, 2)
		assert.Equal(t, "100", row.Bid[assetA])
		assert.Equal(t, "200", row.Bid[assetB])
		require.Len(t, row.Lot, 2)
		assert.Equal(t, "300", row.Lot[assetA])
		assert.Equal(t, "400", row.Lot[assetB])
		assert.Equal(t, int32(1000), row.StartBlock)
		assert.Equal(t, int32(5), row.LastModifiedLedger)
	})

	t.Run("re-upsert with fewer assets fully replaces bid/lot", func(t *testing.T) {
		userAddr := keypair.MustRandom().Address()
		assetA := keypair.MustRandom().Address()
		assetB := keypair.MustRandom().Address()
		assetC := keypair.MustRandom().Address()
		runInTx(t, ctx, pool, func(tx pgx.Tx) {
			require.NoError(t, m.BatchUpsert(ctx, tx, []blend.Auction{{
				Pool:        types.AddressBytea(poolAddr),
				User:        types.AddressBytea(userAddr),
				AuctionType: 0,
				Bid:         map[string]string{assetA: "100", assetB: "200"},
				Lot:         map[string]string{assetA: "300", assetB: "400"},
				StartBlock:  1000,
			}}))
		})
		runInTx(t, ctx, pool, func(tx pgx.Tx) {
			require.NoError(t, m.BatchUpsert(ctx, tx, []blend.Auction{{
				Pool:               types.AddressBytea(poolAddr),
				User:               types.AddressBytea(userAddr),
				AuctionType:        0,
				Bid:                map[string]string{assetC: "50"},
				Lot:                map[string]string{assetC: "70"},
				StartBlock:         1000,
				LastModifiedLedger: 10,
			}}))
		})

		row, ok := getAuction(t, ctx, pool, poolAddr, userAddr, 0)
		require.True(t, ok)
		require.Len(t, row.Bid, 1)
		assert.Equal(t, "50", row.Bid[assetC])
		_, hasOldAsset := row.Bid[assetA]
		assert.False(t, hasOldAsset, "old bid asset keys must be gone after full replace")
		require.Len(t, row.Lot, 1)
		assert.Equal(t, "70", row.Lot[assetC])
		assert.Equal(t, int32(10), row.LastModifiedLedger)
	})

	t.Run("is a no-op when no rows are staged", func(t *testing.T) {
		require.NoError(t, m.BatchUpsert(ctx, nil, nil))
	})
}

func TestAuctionModel_DeleteByKey(t *testing.T) {
	ctx, pool, m, cleanup := newAuctionsFixture(t)
	defer cleanup()

	poolX := keypair.MustRandom().Address()
	userA := keypair.MustRandom().Address()
	userB := keypair.MustRandom().Address()

	runInTx(t, ctx, pool, func(tx pgx.Tx) {
		require.NoError(t, m.BatchUpsert(ctx, tx, []blend.Auction{
			{Pool: types.AddressBytea(poolX), User: types.AddressBytea(userA), AuctionType: 0, Bid: map[string]string{}, Lot: map[string]string{}, StartBlock: 1},
			{Pool: types.AddressBytea(poolX), User: types.AddressBytea(userA), AuctionType: 2, Bid: map[string]string{}, Lot: map[string]string{}, StartBlock: 1},
			{Pool: types.AddressBytea(poolX), User: types.AddressBytea(userB), AuctionType: 0, Bid: map[string]string{}, Lot: map[string]string{}, StartBlock: 1},
		}))
	})

	runInTx(t, ctx, pool, func(tx pgx.Tx) {
		require.NoError(t, m.DeleteByKey(ctx, tx, []blend.AuctionKey{
			{Pool: types.AddressBytea(poolX), User: types.AddressBytea(userA), AuctionType: 0},
		}))
	})

	_, ok := getAuction(t, ctx, pool, poolX, userA, 0)
	assert.False(t, ok, "(poolX, userA, type0) should be deleted")
	_, ok = getAuction(t, ctx, pool, poolX, userA, 2)
	assert.True(t, ok, "a different auction_type for the same user must be untouched")
	_, ok = getAuction(t, ctx, pool, poolX, userB, 0)
	assert.True(t, ok, "a different user must be untouched")

	t.Run("deleting a non-existent key is a no-op", func(t *testing.T) {
		runInTx(t, ctx, pool, func(tx pgx.Tx) {
			require.NoError(t, m.DeleteByKey(ctx, tx, []blend.AuctionKey{
				{Pool: types.AddressBytea(poolX), User: types.AddressBytea(userB), AuctionType: 2},
			}))
		})
		_, ok := getAuction(t, ctx, pool, poolX, userB, 0)
		assert.True(t, ok, "unrelated row must remain untouched by a no-match delete")
	})

	t.Run("is a no-op when no keys are staged", func(t *testing.T) {
		require.NoError(t, m.DeleteByKey(ctx, nil, nil))
	})
}

func TestAuctionModel_GetByAccount(t *testing.T) {
	ctx, pool, m, cleanup := newAuctionsFixture(t)
	defer cleanup()

	poolA := keypair.MustRandom().Address()
	poolB := keypair.MustRandom().Address()
	userA := keypair.MustRandom().Address()
	userB := keypair.MustRandom().Address()
	assetX := keypair.MustRandom().Address()
	assetY := keypair.MustRandom().Address()

	runInTx(t, ctx, pool, func(tx pgx.Tx) {
		require.NoError(t, m.BatchUpsert(ctx, tx, []blend.Auction{
			{
				Pool: types.AddressBytea(poolA), User: types.AddressBytea(userA), AuctionType: 0,
				Bid: map[string]string{assetX: "100"}, Lot: map[string]string{assetY: "200"},
				StartBlock: 10, LastModifiedLedger: 1,
			},
			{
				Pool: types.AddressBytea(poolB), User: types.AddressBytea(userA), AuctionType: 1,
				Bid: map[string]string{assetX: "300"}, Lot: map[string]string{assetY: "400"},
				StartBlock: 20, LastModifiedLedger: 1,
			},
			{
				Pool: types.AddressBytea(poolA), User: types.AddressBytea(userB), AuctionType: 0,
				Bid: map[string]string{}, Lot: map[string]string{},
				StartBlock: 30, LastModifiedLedger: 1,
			},
		}))
	})

	got, err := m.GetByAccount(ctx, userA)
	require.NoError(t, err)
	require.Len(t, got, 2, "only userA's rows across both pools")
	for _, a := range got {
		assert.Equal(t, types.AddressBytea(userA), a.User)
	}
	assertOrderedByAddrThen(t, got,
		func(a blend.Auction) types.AddressBytea { return a.Pool },
		func(a blend.Auction) int32 { return a.AuctionType },
	)

	byPool := map[types.AddressBytea]blend.Auction{}
	for _, a := range got {
		byPool[a.Pool] = a
	}
	aA, ok := byPool[types.AddressBytea(poolA)]
	require.True(t, ok)
	assert.EqualValues(t, 0, aA.AuctionType)
	require.Len(t, aA.Bid, 1)
	assert.Equal(t, "100", aA.Bid[assetX])
	require.Len(t, aA.Lot, 1)
	assert.Equal(t, "200", aA.Lot[assetY])
	assert.Equal(t, uint32(10), aA.StartBlock)

	aB, ok := byPool[types.AddressBytea(poolB)]
	require.True(t, ok)
	assert.EqualValues(t, 1, aB.AuctionType)
	assert.Equal(t, "300", aB.Bid[assetX])
	assert.Equal(t, "400", aB.Lot[assetY])

	t.Run("unknown account returns an empty, non-nil slice", func(t *testing.T) {
		got, err := m.GetByAccount(ctx, keypair.MustRandom().Address())
		require.NoError(t, err)
		require.NotNil(t, got)
		require.Empty(t, got)
	})
}
