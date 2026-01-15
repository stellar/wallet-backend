// Unit tests for NativeBalanceModel.
package data

import (
	"context"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/stellar/wallet-backend/internal/db"
	"github.com/stellar/wallet-backend/internal/db/dbtest"
	"github.com/stellar/wallet-backend/internal/metrics"
)

func TestNativeBalanceModel_GetByAccount(t *testing.T) {
	ctx := context.Background()

	dbt := dbtest.Open(t)
	defer dbt.Close()
	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	cleanUpDB := func() {
		_, err = dbConnectionPool.ExecContext(ctx, `DELETE FROM native_balances`)
		require.NoError(t, err)
	}

	t.Run("returns error for empty account address", func(t *testing.T) {
		mockMetricsService := metrics.NewMockMetricsService()
		defer mockMetricsService.AssertExpectations(t)

		m := &NativeBalanceModel{
			DB:             dbConnectionPool,
			MetricsService: mockMetricsService,
		}

		balance, err := m.GetByAccount(ctx, "")
		require.Error(t, err)
		require.Nil(t, balance)
		require.Contains(t, err.Error(), "empty account address")
	})

	t.Run("returns nil for account not found", func(t *testing.T) {
		cleanUpDB()
		mockMetricsService := metrics.NewMockMetricsService()
		mockMetricsService.On("ObserveDBQueryDuration", "GetByAccount", "native_balances", mock.Anything).Return()
		mockMetricsService.On("IncDBQuery", "GetByAccount", "native_balances").Return()
		defer mockMetricsService.AssertExpectations(t)

		m := &NativeBalanceModel{
			DB:             dbConnectionPool,
			MetricsService: mockMetricsService,
		}

		balance, err := m.GetByAccount(ctx, "GNOTEXIST")
		require.NoError(t, err)
		require.Nil(t, balance)
	})

	t.Run("returns balance with correct data", func(t *testing.T) {
		cleanUpDB()
		mockMetricsService := metrics.NewMockMetricsService()
		mockMetricsService.On("ObserveDBQueryDuration", "GetByAccount", "native_balances", mock.Anything).Return()
		mockMetricsService.On("IncDBQuery", "GetByAccount", "native_balances").Return()
		defer mockMetricsService.AssertExpectations(t)

		m := &NativeBalanceModel{
			DB:             dbConnectionPool,
			MetricsService: mockMetricsService,
		}

		accountAddr := "GACCOUNT1"
		_, err := dbConnectionPool.ExecContext(ctx, `
			INSERT INTO native_balances
			(account_address, balance, buying_liabilities, selling_liabilities, last_modified_ledger)
			VALUES ($1, 1000000000, 100, 200, 12345)
		`, accountAddr)
		require.NoError(t, err)

		balance, err := m.GetByAccount(ctx, accountAddr)
		require.NoError(t, err)
		require.NotNil(t, balance)

		require.Equal(t, accountAddr, balance.AccountAddress)
		require.Equal(t, int64(1000000000), balance.Balance)
		require.Equal(t, int64(100), balance.BuyingLiabilities)
		require.Equal(t, int64(200), balance.SellingLiabilities)
		require.Equal(t, uint32(12345), balance.LedgerNumber)
	})
}

func TestNativeBalanceModel_BatchUpsert(t *testing.T) {
	ctx := context.Background()

	dbt := dbtest.Open(t)
	defer dbt.Close()
	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	cleanUpDB := func() {
		_, err = dbConnectionPool.ExecContext(ctx, `DELETE FROM native_balances`)
		require.NoError(t, err)
	}

	t.Run("returns nil for empty upserts and deletes", func(t *testing.T) {
		mockMetricsService := metrics.NewMockMetricsService()
		defer mockMetricsService.AssertExpectations(t)

		m := &NativeBalanceModel{
			DB:             dbConnectionPool,
			MetricsService: mockMetricsService,
		}

		pgxTx, err := dbConnectionPool.PgxPool().Begin(ctx)
		require.NoError(t, err)
		defer pgxTx.Rollback(ctx) //nolint:errcheck

		err = m.BatchUpsert(ctx, pgxTx, nil, nil)
		require.NoError(t, err)
	})

	t.Run("inserts new native balance", func(t *testing.T) {
		cleanUpDB()
		mockMetricsService := metrics.NewMockMetricsService()
		mockMetricsService.On("ObserveDBQueryDuration", "BatchUpsert", "native_balances", mock.Anything).Return()
		mockMetricsService.On("IncDBQuery", "BatchUpsert", "native_balances").Return()
		defer mockMetricsService.AssertExpectations(t)

		m := &NativeBalanceModel{
			DB:             dbConnectionPool,
			MetricsService: mockMetricsService,
		}

		pgxTx, err := dbConnectionPool.PgxPool().Begin(ctx)
		require.NoError(t, err)

		upserts := []NativeBalance{
			{
				AccountAddress:     "GACCOUNT1",
				Balance:            1000000000,
				BuyingLiabilities:  100,
				SellingLiabilities: 200,
				LedgerNumber:       12345,
			},
		}

		err = m.BatchUpsert(ctx, pgxTx, upserts, nil)
		require.NoError(t, err)
		require.NoError(t, pgxTx.Commit(ctx))

		// Verify insert
		var count int
		err = dbConnectionPool.PgxPool().QueryRow(ctx, `SELECT COUNT(*) FROM native_balances WHERE account_address = $1`, "GACCOUNT1").Scan(&count)
		require.NoError(t, err)
		require.Equal(t, 1, count)
	})

	t.Run("updates existing balance on conflict", func(t *testing.T) {
		cleanUpDB()
		mockMetricsService := metrics.NewMockMetricsService()
		mockMetricsService.On("ObserveDBQueryDuration", "BatchUpsert", "native_balances", mock.Anything).Return().Times(2)
		mockMetricsService.On("IncDBQuery", "BatchUpsert", "native_balances").Return().Times(2)
		defer mockMetricsService.AssertExpectations(t)

		m := &NativeBalanceModel{
			DB:             dbConnectionPool,
			MetricsService: mockMetricsService,
		}

		// First insert
		pgxTx1, err := dbConnectionPool.PgxPool().Begin(ctx)
		require.NoError(t, err)
		upserts := []NativeBalance{
			{
				AccountAddress: "GACCOUNT1",
				Balance:        1000,
				LedgerNumber:   100,
			},
		}
		err = m.BatchUpsert(ctx, pgxTx1, upserts, nil)
		require.NoError(t, err)
		require.NoError(t, pgxTx1.Commit(ctx))

		// Update with new values
		pgxTx2, err := dbConnectionPool.PgxPool().Begin(ctx)
		require.NoError(t, err)
		upserts[0].Balance = 2000
		upserts[0].LedgerNumber = 200
		err = m.BatchUpsert(ctx, pgxTx2, upserts, nil)
		require.NoError(t, err)
		require.NoError(t, pgxTx2.Commit(ctx))

		// Verify update
		var balance int64
		var ledger uint32
		err = dbConnectionPool.PgxPool().QueryRow(ctx,
			`SELECT balance, last_modified_ledger FROM native_balances WHERE account_address = $1`,
			"GACCOUNT1").Scan(&balance, &ledger)
		require.NoError(t, err)
		require.Equal(t, int64(2000), balance)
		require.Equal(t, uint32(200), ledger)
	})

	t.Run("deletes native balance", func(t *testing.T) {
		cleanUpDB()
		mockMetricsService := metrics.NewMockMetricsService()
		mockMetricsService.On("ObserveDBQueryDuration", "BatchUpsert", "native_balances", mock.Anything).Return().Times(2)
		mockMetricsService.On("IncDBQuery", "BatchUpsert", "native_balances").Return().Times(2)
		defer mockMetricsService.AssertExpectations(t)

		m := &NativeBalanceModel{
			DB:             dbConnectionPool,
			MetricsService: mockMetricsService,
		}

		// First insert
		pgxTx1, err := dbConnectionPool.PgxPool().Begin(ctx)
		require.NoError(t, err)
		upserts := []NativeBalance{
			{AccountAddress: "GACCOUNT1", Balance: 1000, LedgerNumber: 100},
		}
		err = m.BatchUpsert(ctx, pgxTx1, upserts, nil)
		require.NoError(t, err)
		require.NoError(t, pgxTx1.Commit(ctx))

		// Delete
		pgxTx2, err := dbConnectionPool.PgxPool().Begin(ctx)
		require.NoError(t, err)
		deletes := []string{"GACCOUNT1"}
		err = m.BatchUpsert(ctx, pgxTx2, nil, deletes)
		require.NoError(t, err)
		require.NoError(t, pgxTx2.Commit(ctx))

		// Verify delete
		var count int
		err = dbConnectionPool.PgxPool().QueryRow(ctx, `SELECT COUNT(*) FROM native_balances WHERE account_address = $1`, "GACCOUNT1").Scan(&count)
		require.NoError(t, err)
		require.Equal(t, 0, count)
	})

	t.Run("handles combined upserts and deletes", func(t *testing.T) {
		cleanUpDB()
		mockMetricsService := metrics.NewMockMetricsService()
		mockMetricsService.On("ObserveDBQueryDuration", "BatchUpsert", "native_balances", mock.Anything).Return().Times(2)
		mockMetricsService.On("IncDBQuery", "BatchUpsert", "native_balances").Return().Times(2)
		defer mockMetricsService.AssertExpectations(t)

		m := &NativeBalanceModel{
			DB:             dbConnectionPool,
			MetricsService: mockMetricsService,
		}

		// Insert two balances
		pgxTx1, err := dbConnectionPool.PgxPool().Begin(ctx)
		require.NoError(t, err)
		upserts := []NativeBalance{
			{AccountAddress: "GACCOUNT1", Balance: 1000, LedgerNumber: 100},
			{AccountAddress: "GACCOUNT2", Balance: 2000, LedgerNumber: 100},
		}
		err = m.BatchUpsert(ctx, pgxTx1, upserts, nil)
		require.NoError(t, err)
		require.NoError(t, pgxTx1.Commit(ctx))

		// Update one, delete one, add new one
		pgxTx2, err := dbConnectionPool.PgxPool().Begin(ctx)
		require.NoError(t, err)
		newUpserts := []NativeBalance{
			{AccountAddress: "GACCOUNT1", Balance: 1500, LedgerNumber: 200}, // update
			{AccountAddress: "GACCOUNT3", Balance: 3000, LedgerNumber: 200}, // new
		}
		deletes := []string{"GACCOUNT2"} // delete
		err = m.BatchUpsert(ctx, pgxTx2, newUpserts, deletes)
		require.NoError(t, err)
		require.NoError(t, pgxTx2.Commit(ctx))

		// Verify results
		var count int
		err = dbConnectionPool.PgxPool().QueryRow(ctx, `SELECT COUNT(*) FROM native_balances`).Scan(&count)
		require.NoError(t, err)
		require.Equal(t, 2, count) // GACCOUNT1 (updated) + GACCOUNT3 (new)

		var balance int64
		err = dbConnectionPool.PgxPool().QueryRow(ctx,
			`SELECT balance FROM native_balances WHERE account_address = $1`,
			"GACCOUNT1").Scan(&balance)
		require.NoError(t, err)
		require.Equal(t, int64(1500), balance)
	})
}

func TestNativeBalanceModel_BatchCopy(t *testing.T) {
	ctx := context.Background()

	dbt := dbtest.Open(t)
	defer dbt.Close()
	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	cleanUpDB := func() {
		_, err = dbConnectionPool.ExecContext(ctx, `DELETE FROM native_balances`)
		require.NoError(t, err)
	}

	t.Run("returns nil for empty input", func(t *testing.T) {
		mockMetricsService := metrics.NewMockMetricsService()
		defer mockMetricsService.AssertExpectations(t)

		m := &NativeBalanceModel{
			DB:             dbConnectionPool,
			MetricsService: mockMetricsService,
		}

		pgxTx, err := dbConnectionPool.PgxPool().Begin(ctx)
		require.NoError(t, err)
		defer pgxTx.Rollback(ctx) //nolint:errcheck

		err = m.BatchCopy(ctx, pgxTx, nil)
		require.NoError(t, err)
	})

	t.Run("inserts single balance via COPY", func(t *testing.T) {
		cleanUpDB()
		mockMetricsService := metrics.NewMockMetricsService()
		mockMetricsService.On("ObserveDBQueryDuration", "BatchCopy", "native_balances", mock.Anything).Return()
		mockMetricsService.On("IncDBQuery", "BatchCopy", "native_balances").Return()
		defer mockMetricsService.AssertExpectations(t)

		m := &NativeBalanceModel{
			DB:             dbConnectionPool,
			MetricsService: mockMetricsService,
		}

		pgxTx, err := dbConnectionPool.PgxPool().Begin(ctx)
		require.NoError(t, err)

		balances := []NativeBalance{
			{
				AccountAddress:     "GACCOUNT1",
				Balance:            1000000000,
				BuyingLiabilities:  100,
				SellingLiabilities: 200,
				LedgerNumber:       12345,
			},
		}

		err = m.BatchCopy(ctx, pgxTx, balances)
		require.NoError(t, err)
		require.NoError(t, pgxTx.Commit(ctx))

		// Verify all fields
		var b NativeBalance
		err = dbConnectionPool.PgxPool().QueryRow(ctx, `
			SELECT account_address, balance, buying_liabilities, selling_liabilities, last_modified_ledger
			FROM native_balances WHERE account_address = $1
		`, "GACCOUNT1").Scan(&b.AccountAddress, &b.Balance, &b.BuyingLiabilities, &b.SellingLiabilities, &b.LedgerNumber)
		require.NoError(t, err)
		require.Equal(t, balances[0].AccountAddress, b.AccountAddress)
		require.Equal(t, balances[0].Balance, b.Balance)
		require.Equal(t, balances[0].BuyingLiabilities, b.BuyingLiabilities)
		require.Equal(t, balances[0].SellingLiabilities, b.SellingLiabilities)
		require.Equal(t, balances[0].LedgerNumber, b.LedgerNumber)
	})

	t.Run("inserts multiple balances via COPY", func(t *testing.T) {
		cleanUpDB()
		mockMetricsService := metrics.NewMockMetricsService()
		mockMetricsService.On("ObserveDBQueryDuration", "BatchCopy", "native_balances", mock.Anything).Return()
		mockMetricsService.On("IncDBQuery", "BatchCopy", "native_balances").Return()
		defer mockMetricsService.AssertExpectations(t)

		m := &NativeBalanceModel{
			DB:             dbConnectionPool,
			MetricsService: mockMetricsService,
		}

		pgxTx, err := dbConnectionPool.PgxPool().Begin(ctx)
		require.NoError(t, err)

		balances := []NativeBalance{
			{AccountAddress: "GACCOUNT1", Balance: 1000, LedgerNumber: 100},
			{AccountAddress: "GACCOUNT2", Balance: 2000, LedgerNumber: 100},
			{AccountAddress: "GACCOUNT3", Balance: 3000, LedgerNumber: 100},
		}

		err = m.BatchCopy(ctx, pgxTx, balances)
		require.NoError(t, err)
		require.NoError(t, pgxTx.Commit(ctx))

		// Verify count
		var count int
		err = dbConnectionPool.PgxPool().QueryRow(ctx, `SELECT COUNT(*) FROM native_balances`).Scan(&count)
		require.NoError(t, err)
		require.Equal(t, 3, count)
	})
}
