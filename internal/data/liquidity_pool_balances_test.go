// Unit tests for LiquidityPoolBalanceModel and LiquidityPoolModel.
package data

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stellar/go-stellar-sdk/keypair"
	"github.com/stretchr/testify/require"

	"github.com/stellar/wallet-backend/internal/db"
	"github.com/stellar/wallet-backend/internal/db/dbtest"
	"github.com/stellar/wallet-backend/internal/indexer/types"
	"github.com/stellar/wallet-backend/internal/metrics"
)

// poolIDHex builds a deterministic 64-character hex pool id from a single byte, so
// ordering in tests is predictable (0x01 < 0x02 < 0x03).
func poolIDHex(b byte) string {
	return strings.Repeat(fmt.Sprintf("%02x", b), 32)
}

func TestLiquidityPoolBalanceModel_GetByAccount(t *testing.T) {
	ctx := context.Background()

	dbt := dbtest.Open(t)
	defer dbt.Close()
	dbConnectionPool, err := db.OpenDBConnectionPool(ctx, dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	pool1, pool2, pool3 := poolIDHex(1), poolIDHex(2), poolIDHex(3)
	_, err = dbConnectionPool.Exec(ctx, `
		INSERT INTO liquidity_pools (pool_id, asset_a, amount_a, asset_b, amount_b, last_modified_ledger) VALUES
		($1, 'native', 100, 'USDC:ISSUER1', 200, 10),
		($2, 'BTC:ISSUER2', 300, 'ETH:ISSUER3', 400, 11),
		($3, 'native', 500, 'EURC:ISSUER4', 600, 12)
	`, pool1, pool2, pool3)
	require.NoError(t, err)

	cleanUpDB := func() {
		_, err = dbConnectionPool.Exec(ctx, `DELETE FROM liquidity_pool_balances`)
		require.NoError(t, err)
	}

	dbMetrics := metrics.NewMetrics(prometheus.NewRegistry()).DB
	m := &LiquidityPoolBalanceModel{DB: dbConnectionPool, Metrics: dbMetrics}

	t.Run("returns error for empty account address", func(t *testing.T) {
		balances, err := m.GetByAccount(ctx, "", nil, nil, ASC)
		require.Error(t, err)
		require.Nil(t, balances)
		require.Contains(t, err.Error(), "empty account address")
	})

	t.Run("returns empty slice for account with no pool shares", func(t *testing.T) {
		cleanUpDB()
		balances, err := m.GetByAccount(ctx, keypair.MustRandom().Address(), nil, nil, ASC)
		require.NoError(t, err)
		require.Empty(t, balances)
	})

	t.Run("returns single pool share with reserves from JOIN", func(t *testing.T) {
		cleanUpDB()
		accountAddr := keypair.MustRandom().Address()
		_, err := dbConnectionPool.Exec(ctx, `
			INSERT INTO liquidity_pool_balances (account_id, pool_id, shares, last_modified_ledger)
			VALUES ($1, $2, 1000, 12345)
		`, types.AddressBytea(accountAddr), pool1)
		require.NoError(t, err)

		balances, err := m.GetByAccount(ctx, accountAddr, nil, nil, ASC)
		require.NoError(t, err)
		require.Len(t, balances, 1)

		require.Equal(t, types.AddressBytea(accountAddr), balances[0].AccountID)
		require.Equal(t, pool1, balances[0].PoolID)
		require.Equal(t, int64(1000), balances[0].Shares)
		require.Equal(t, uint32(12345), balances[0].LedgerNumber)
		// JOINed reserves:
		require.Equal(t, "native", balances[0].AssetA)
		require.Equal(t, int64(100), balances[0].AmountA)
		require.Equal(t, "USDC:ISSUER1", balances[0].AssetB)
		require.Equal(t, int64(200), balances[0].AmountB)
	})

	t.Run("omits pool shares with no matching pool row (INNER JOIN)", func(t *testing.T) {
		cleanUpDB()
		accountAddr := keypair.MustRandom().Address()
		_, err := dbConnectionPool.Exec(ctx, `
			INSERT INTO liquidity_pool_balances (account_id, pool_id, shares, last_modified_ledger)
			VALUES ($1, $2, 1000, 1)
		`, types.AddressBytea(accountAddr), poolIDHex(9))
		require.NoError(t, err)

		balances, err := m.GetByAccount(ctx, accountAddr, nil, nil, ASC)
		require.NoError(t, err)
		require.Empty(t, balances)
	})

	t.Run("paginates with limit and cursor in ASC and DESC order", func(t *testing.T) {
		cleanUpDB()
		accountAddr := keypair.MustRandom().Address()
		_, err := dbConnectionPool.Exec(ctx, `
			INSERT INTO liquidity_pool_balances (account_id, pool_id, shares, last_modified_ledger) VALUES
			($1, $2, 1000, 100),
			($1, $3, 2000, 101),
			($1, $4, 3000, 102)
		`, types.AddressBytea(accountAddr), pool1, pool2, pool3)
		require.NoError(t, err)

		limit := int32(2)
		page, err := m.GetByAccount(ctx, accountAddr, &limit, nil, ASC)
		require.NoError(t, err)
		require.Len(t, page, 2)
		require.Equal(t, pool1, page[0].PoolID)
		require.Equal(t, pool2, page[1].PoolID)

		cursor := page[1].PoolID
		nextPage, err := m.GetByAccount(ctx, accountAddr, &limit, &cursor, ASC)
		require.NoError(t, err)
		require.Len(t, nextPage, 1)
		require.Equal(t, pool3, nextPage[0].PoolID)

		descPage, err := m.GetByAccount(ctx, accountAddr, &limit, nil, DESC)
		require.NoError(t, err)
		require.Len(t, descPage, 2)
		require.Equal(t, pool3, descPage[0].PoolID)
		require.Equal(t, pool2, descPage[1].PoolID)
	})
}

func TestLiquidityPoolBalanceModel_BatchUpsertAndCopy(t *testing.T) {
	ctx := context.Background()

	dbt := dbtest.Open(t)
	defer dbt.Close()
	dbConnectionPool, err := db.OpenDBConnectionPool(ctx, dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	dbMetrics := metrics.NewMetrics(prometheus.NewRegistry()).DB
	m := &LiquidityPoolBalanceModel{DB: dbConnectionPool, Metrics: dbMetrics}

	cleanUpDB := func() {
		_, err = dbConnectionPool.Exec(ctx, `DELETE FROM liquidity_pool_balances`)
		require.NoError(t, err)
	}

	sharesFor := func(account, poolID string) (int64, bool) {
		var shares int64
		row := dbConnectionPool.QueryRow(ctx, `SELECT shares FROM liquidity_pool_balances WHERE account_id = $1 AND pool_id = $2`, types.AddressBytea(account), poolID)
		if scanErr := row.Scan(&shares); scanErr != nil {
			return 0, false
		}
		return shares, true
	}

	t.Run("insert, update, and delete via BatchUpsert", func(t *testing.T) {
		cleanUpDB()
		account := keypair.MustRandom().Address()
		pool := poolIDHex(1)

		// Insert
		tx, err := dbConnectionPool.Begin(ctx)
		require.NoError(t, err)
		require.NoError(t, m.BatchUpsert(ctx, tx, []LiquidityPoolBalance{
			{AccountID: types.AddressBytea(account), PoolID: pool, Shares: 1000, LedgerNumber: 10},
		}, nil))
		require.NoError(t, tx.Commit(ctx))
		got, ok := sharesFor(account, pool)
		require.True(t, ok)
		require.Equal(t, int64(1000), got)

		// Update on conflict
		tx, err = dbConnectionPool.Begin(ctx)
		require.NoError(t, err)
		require.NoError(t, m.BatchUpsert(ctx, tx, []LiquidityPoolBalance{
			{AccountID: types.AddressBytea(account), PoolID: pool, Shares: 7777, LedgerNumber: 11},
		}, nil))
		require.NoError(t, tx.Commit(ctx))
		got, ok = sharesFor(account, pool)
		require.True(t, ok)
		require.Equal(t, int64(7777), got)

		// Delete
		tx, err = dbConnectionPool.Begin(ctx)
		require.NoError(t, err)
		require.NoError(t, m.BatchUpsert(ctx, tx, nil, []LiquidityPoolBalance{
			{AccountID: types.AddressBytea(account), PoolID: pool},
		}))
		require.NoError(t, tx.Commit(ctx))
		_, ok = sharesFor(account, pool)
		require.False(t, ok)
	})

	t.Run("BatchCopy bulk inserts", func(t *testing.T) {
		cleanUpDB()
		account := keypair.MustRandom().Address()
		tx, err := dbConnectionPool.Begin(ctx)
		require.NoError(t, err)
		require.NoError(t, m.BatchCopy(ctx, tx, []LiquidityPoolBalance{
			{AccountID: types.AddressBytea(account), PoolID: poolIDHex(1), Shares: 100, LedgerNumber: 1},
			{AccountID: types.AddressBytea(account), PoolID: poolIDHex(2), Shares: 200, LedgerNumber: 2},
		}))
		require.NoError(t, tx.Commit(ctx))

		var count int
		require.NoError(t, dbConnectionPool.QueryRow(ctx, `SELECT COUNT(*) FROM liquidity_pool_balances WHERE account_id = $1`, types.AddressBytea(account)).Scan(&count))
		require.Equal(t, 2, count)
	})
}

func TestLiquidityPoolModel_BatchUpsertAndCopy(t *testing.T) {
	ctx := context.Background()

	dbt := dbtest.Open(t)
	defer dbt.Close()
	dbConnectionPool, err := db.OpenDBConnectionPool(ctx, dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	dbMetrics := metrics.NewMetrics(prometheus.NewRegistry()).DB
	m := &LiquidityPoolModel{DB: dbConnectionPool, Metrics: dbMetrics}

	cleanUpDB := func() {
		_, err = dbConnectionPool.Exec(ctx, `DELETE FROM liquidity_pools`)
		require.NoError(t, err)
	}

	readPool := func(poolID string) (LiquidityPool, bool) {
		var lp LiquidityPool
		row := dbConnectionPool.QueryRow(ctx, `SELECT pool_id, asset_a, amount_a, asset_b, amount_b, last_modified_ledger FROM liquidity_pools WHERE pool_id = $1`, poolID)
		if scanErr := row.Scan(&lp.PoolID, &lp.AssetA, &lp.AmountA, &lp.AssetB, &lp.AmountB, &lp.LedgerNumber); scanErr != nil {
			return LiquidityPool{}, false
		}
		return lp, true
	}

	t.Run("insert, update reserves, and delete via BatchUpsert", func(t *testing.T) {
		cleanUpDB()
		pool := poolIDHex(1)

		tx, err := dbConnectionPool.Begin(ctx)
		require.NoError(t, err)
		require.NoError(t, m.BatchUpsert(ctx, tx, []LiquidityPool{
			{PoolID: pool, AssetA: "native", AmountA: 100, AssetB: "USDC:ISSUER1", AmountB: 200, LedgerNumber: 10},
		}, nil))
		require.NoError(t, tx.Commit(ctx))
		lp, ok := readPool(pool)
		require.True(t, ok)
		require.Equal(t, int64(100), lp.AmountA)
		require.Equal(t, int64(200), lp.AmountB)

		// Update reserves on conflict
		tx, err = dbConnectionPool.Begin(ctx)
		require.NoError(t, err)
		require.NoError(t, m.BatchUpsert(ctx, tx, []LiquidityPool{
			{PoolID: pool, AssetA: "native", AmountA: 999, AssetB: "USDC:ISSUER1", AmountB: 888, LedgerNumber: 11},
		}, nil))
		require.NoError(t, tx.Commit(ctx))
		lp, ok = readPool(pool)
		require.True(t, ok)
		require.Equal(t, int64(999), lp.AmountA)
		require.Equal(t, int64(888), lp.AmountB)

		// Delete
		tx, err = dbConnectionPool.Begin(ctx)
		require.NoError(t, err)
		require.NoError(t, m.BatchUpsert(ctx, tx, nil, []LiquidityPool{{PoolID: pool}}))
		require.NoError(t, tx.Commit(ctx))
		_, ok = readPool(pool)
		require.False(t, ok)
	})

	t.Run("BatchCopy bulk inserts", func(t *testing.T) {
		cleanUpDB()
		tx, err := dbConnectionPool.Begin(ctx)
		require.NoError(t, err)
		require.NoError(t, m.BatchCopy(ctx, tx, []LiquidityPool{
			{PoolID: poolIDHex(1), AssetA: "native", AmountA: 1, AssetB: "USDC:ISSUER1", AmountB: 2, LedgerNumber: 1},
			{PoolID: poolIDHex(2), AssetA: "BTC:ISSUER2", AmountA: 3, AssetB: "ETH:ISSUER3", AmountB: 4, LedgerNumber: 2},
		}))
		require.NoError(t, tx.Commit(ctx))

		var count int
		require.NoError(t, dbConnectionPool.QueryRow(ctx, `SELECT COUNT(*) FROM liquidity_pools`).Scan(&count))
		require.Equal(t, 2, count)
	})
}
