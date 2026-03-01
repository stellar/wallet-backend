// Unit tests for TrustlineBalanceModel.
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

func TestTrustlineBalanceModel_GetByAccount(t *testing.T) {
	ctx := context.Background()

	dbt := dbtest.Open(t)
	defer dbt.Close()
	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	// Insert test assets for foreign key references
	assetID1 := DeterministicAssetID("USDC", "ISSUER1")
	assetID2 := DeterministicAssetID("EURC", "ISSUER2")
	_, err = dbConnectionPool.Pool().Exec(ctx, `
		INSERT INTO trustline_assets (id, code, issuer) VALUES
		($1, 'USDC', 'ISSUER1'),
		($2, 'EURC', 'ISSUER2')
	`, assetID1, assetID2)
	require.NoError(t, err)

	cleanUpDB := func() {
		_, err = dbConnectionPool.Pool().Exec(ctx, `DELETE FROM trustline_balances`)
		require.NoError(t, err)
	}

	t.Run("returns error for empty account address", func(t *testing.T) {
		mockMetricsService := metrics.NewMockMetricsService()
		defer mockMetricsService.AssertExpectations(t)

		m := &TrustlineBalanceModel{
			DB:             dbConnectionPool,
			MetricsService: mockMetricsService,
		}

		balances, err := m.GetByAccount(ctx, "")
		require.Error(t, err)
		require.Nil(t, balances)
		require.Contains(t, err.Error(), "empty account address")
	})

	t.Run("returns empty slice for account with no trustlines", func(t *testing.T) {
		cleanUpDB()
		mockMetricsService := metrics.NewMockMetricsService()
		mockMetricsService.On("ObserveDBQueryDuration", "GetByAccount", "trustline_balances", mock.Anything).Return()
		mockMetricsService.On("IncDBQuery", "GetByAccount", "trustline_balances").Return()
		defer mockMetricsService.AssertExpectations(t)

		m := &TrustlineBalanceModel{
			DB:             dbConnectionPool,
			MetricsService: mockMetricsService,
		}

		balances, err := m.GetByAccount(ctx, "GNOTEXIST")
		require.NoError(t, err)
		require.Empty(t, balances)
	})

	t.Run("returns single trustline with correct data from JOIN", func(t *testing.T) {
		cleanUpDB()
		mockMetricsService := metrics.NewMockMetricsService()
		mockMetricsService.On("ObserveDBQueryDuration", "GetByAccount", "trustline_balances", mock.Anything).Return()
		mockMetricsService.On("IncDBQuery", "GetByAccount", "trustline_balances").Return()
		defer mockMetricsService.AssertExpectations(t)

		m := &TrustlineBalanceModel{
			DB:             dbConnectionPool,
			MetricsService: mockMetricsService,
		}

		accountAddr := "GACCOUNT1"
		_, err := dbConnectionPool.Pool().Exec(ctx, `
			INSERT INTO trustline_balances
			(account_address, asset_id, balance, trust_limit, buying_liabilities, selling_liabilities, flags, last_modified_ledger)
			VALUES ($1, $2, 1000, 10000, 100, 50, 1, 12345)
		`, accountAddr, assetID1)
		require.NoError(t, err)

		balances, err := m.GetByAccount(ctx, accountAddr)
		require.NoError(t, err)
		require.Len(t, balances, 1)

		// Verify all fields including JOIN data
		require.Equal(t, accountAddr, balances[0].AccountAddress)
		require.Equal(t, assetID1, balances[0].AssetID)
		require.Equal(t, "USDC", balances[0].Code)
		require.Equal(t, "ISSUER1", balances[0].Issuer)
		require.Equal(t, int64(1000), balances[0].Balance)
		require.Equal(t, int64(10000), balances[0].Limit)
		require.Equal(t, int64(100), balances[0].BuyingLiabilities)
		require.Equal(t, int64(50), balances[0].SellingLiabilities)
		require.Equal(t, uint32(1), balances[0].Flags)
		require.Equal(t, uint32(12345), balances[0].LedgerNumber)
	})

	t.Run("returns multiple trustlines for account", func(t *testing.T) {
		cleanUpDB()
		mockMetricsService := metrics.NewMockMetricsService()
		mockMetricsService.On("ObserveDBQueryDuration", "GetByAccount", "trustline_balances", mock.Anything).Return()
		mockMetricsService.On("IncDBQuery", "GetByAccount", "trustline_balances").Return()
		defer mockMetricsService.AssertExpectations(t)

		m := &TrustlineBalanceModel{
			DB:             dbConnectionPool,
			MetricsService: mockMetricsService,
		}

		accountAddr := "GACCOUNT2"
		_, err := dbConnectionPool.Pool().Exec(ctx, `
			INSERT INTO trustline_balances
			(account_address, asset_id, balance, trust_limit, buying_liabilities, selling_liabilities, flags, last_modified_ledger)
			VALUES
			($1, $2, 1000, 10000, 0, 0, 0, 100),
			($1, $3, 2000, 20000, 0, 0, 0, 101)
		`, accountAddr, assetID1, assetID2)
		require.NoError(t, err)

		balances, err := m.GetByAccount(ctx, accountAddr)
		require.NoError(t, err)
		require.Len(t, balances, 2)

		// Verify both trustlines belong to the correct account
		for _, b := range balances {
			require.Equal(t, accountAddr, b.AccountAddress)
		}
	})
}

func TestTrustlineBalanceModel_BatchUpsert(t *testing.T) {
	ctx := context.Background()

	dbt := dbtest.Open(t)
	defer dbt.Close()
	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	// Insert test assets for foreign key references
	assetID1 := DeterministicAssetID("USDC", "ISSUER1")
	assetID2 := DeterministicAssetID("EURC", "ISSUER2")
	_, err = dbConnectionPool.Pool().Exec(ctx, `
		INSERT INTO trustline_assets (id, code, issuer) VALUES
		($1, 'USDC', 'ISSUER1'),
		($2, 'EURC', 'ISSUER2')
	`, assetID1, assetID2)
	require.NoError(t, err)

	cleanUpDB := func() {
		_, err = dbConnectionPool.Pool().Exec(ctx, `DELETE FROM trustline_balances`)
		require.NoError(t, err)
	}

	t.Run("returns nil for empty upserts and deletes", func(t *testing.T) {
		mockMetricsService := metrics.NewMockMetricsService()
		defer mockMetricsService.AssertExpectations(t)

		m := &TrustlineBalanceModel{
			DB:             dbConnectionPool,
			MetricsService: mockMetricsService,
		}

		pgxTx, err := dbConnectionPool.Pool().Begin(ctx)
		require.NoError(t, err)
		defer pgxTx.Rollback(ctx) //nolint:errcheck

		err = m.BatchUpsert(ctx, pgxTx, nil, nil)
		require.NoError(t, err)
	})

	t.Run("inserts new trustline balance", func(t *testing.T) {
		cleanUpDB()
		mockMetricsService := metrics.NewMockMetricsService()
		mockMetricsService.On("ObserveDBQueryDuration", "BatchUpsert", "trustline_balances", mock.Anything).Return()
		mockMetricsService.On("IncDBQuery", "BatchUpsert", "trustline_balances").Return()
		defer mockMetricsService.AssertExpectations(t)

		m := &TrustlineBalanceModel{
			DB:             dbConnectionPool,
			MetricsService: mockMetricsService,
		}

		pgxTx, err := dbConnectionPool.Pool().Begin(ctx)
		require.NoError(t, err)

		upserts := []TrustlineBalance{
			{
				AccountAddress:     "GACCOUNT1",
				AssetID:            assetID1,
				Balance:            1000,
				Limit:              10000,
				BuyingLiabilities:  100,
				SellingLiabilities: 50,
				Flags:              1,
				LedgerNumber:       12345,
			},
		}

		err = m.BatchUpsert(ctx, pgxTx, upserts, nil)
		require.NoError(t, err)
		require.NoError(t, pgxTx.Commit(ctx))

		// Verify insert
		var count int
		err = dbConnectionPool.Pool().QueryRow(ctx, `SELECT COUNT(*) FROM trustline_balances WHERE account_address = $1`, "GACCOUNT1").Scan(&count)
		require.NoError(t, err)
		require.Equal(t, 1, count)
	})

	t.Run("updates existing trustline balance on conflict", func(t *testing.T) {
		cleanUpDB()
		mockMetricsService := metrics.NewMockMetricsService()
		mockMetricsService.On("ObserveDBQueryDuration", "BatchUpsert", "trustline_balances", mock.Anything).Return().Times(2)
		mockMetricsService.On("IncDBQuery", "BatchUpsert", "trustline_balances").Return().Times(2)
		defer mockMetricsService.AssertExpectations(t)

		m := &TrustlineBalanceModel{
			DB:             dbConnectionPool,
			MetricsService: mockMetricsService,
		}

		// First insert
		pgxTx1, err := dbConnectionPool.Pool().Begin(ctx)
		require.NoError(t, err)
		upserts := []TrustlineBalance{
			{
				AccountAddress: "GACCOUNT1",
				AssetID:        assetID1,
				Balance:        1000,
				Limit:          10000,
				LedgerNumber:   100,
			},
		}
		err = m.BatchUpsert(ctx, pgxTx1, upserts, nil)
		require.NoError(t, err)
		require.NoError(t, pgxTx1.Commit(ctx))

		// Update with new values
		pgxTx2, err := dbConnectionPool.Pool().Begin(ctx)
		require.NoError(t, err)
		upserts[0].Balance = 2000
		upserts[0].LedgerNumber = 200
		err = m.BatchUpsert(ctx, pgxTx2, upserts, nil)
		require.NoError(t, err)
		require.NoError(t, pgxTx2.Commit(ctx))

		// Verify update
		var balance int64
		var ledger uint32
		err = dbConnectionPool.Pool().QueryRow(ctx,
			`SELECT balance, last_modified_ledger FROM trustline_balances WHERE account_address = $1 AND asset_id = $2`,
			"GACCOUNT1", assetID1).Scan(&balance, &ledger)
		require.NoError(t, err)
		require.Equal(t, int64(2000), balance)
		require.Equal(t, uint32(200), ledger)
	})

	t.Run("deletes trustline balance", func(t *testing.T) {
		cleanUpDB()
		mockMetricsService := metrics.NewMockMetricsService()
		mockMetricsService.On("ObserveDBQueryDuration", "BatchUpsert", "trustline_balances", mock.Anything).Return().Times(2)
		mockMetricsService.On("IncDBQuery", "BatchUpsert", "trustline_balances").Return().Times(2)
		defer mockMetricsService.AssertExpectations(t)

		m := &TrustlineBalanceModel{
			DB:             dbConnectionPool,
			MetricsService: mockMetricsService,
		}

		// First insert
		pgxTx1, err := dbConnectionPool.Pool().Begin(ctx)
		require.NoError(t, err)
		upserts := []TrustlineBalance{
			{AccountAddress: "GACCOUNT1", AssetID: assetID1, Balance: 1000, LedgerNumber: 100},
		}
		err = m.BatchUpsert(ctx, pgxTx1, upserts, nil)
		require.NoError(t, err)
		require.NoError(t, pgxTx1.Commit(ctx))

		// Delete
		pgxTx2, err := dbConnectionPool.Pool().Begin(ctx)
		require.NoError(t, err)
		deletes := []TrustlineBalance{
			{AccountAddress: "GACCOUNT1", AssetID: assetID1},
		}
		err = m.BatchUpsert(ctx, pgxTx2, nil, deletes)
		require.NoError(t, err)
		require.NoError(t, pgxTx2.Commit(ctx))

		// Verify delete
		var count int
		err = dbConnectionPool.Pool().QueryRow(ctx, `SELECT COUNT(*) FROM trustline_balances WHERE account_address = $1`, "GACCOUNT1").Scan(&count)
		require.NoError(t, err)
		require.Equal(t, 0, count)
	})

	t.Run("handles combined upserts and deletes", func(t *testing.T) {
		cleanUpDB()
		mockMetricsService := metrics.NewMockMetricsService()
		mockMetricsService.On("ObserveDBQueryDuration", "BatchUpsert", "trustline_balances", mock.Anything).Return().Times(2)
		mockMetricsService.On("IncDBQuery", "BatchUpsert", "trustline_balances").Return().Times(2)
		defer mockMetricsService.AssertExpectations(t)

		m := &TrustlineBalanceModel{
			DB:             dbConnectionPool,
			MetricsService: mockMetricsService,
		}

		// Insert two trustlines
		pgxTx1, err := dbConnectionPool.Pool().Begin(ctx)
		require.NoError(t, err)
		upserts := []TrustlineBalance{
			{AccountAddress: "GACCOUNT1", AssetID: assetID1, Balance: 1000, LedgerNumber: 100},
			{AccountAddress: "GACCOUNT1", AssetID: assetID2, Balance: 2000, LedgerNumber: 100},
		}
		err = m.BatchUpsert(ctx, pgxTx1, upserts, nil)
		require.NoError(t, err)
		require.NoError(t, pgxTx1.Commit(ctx))

		// Update one, delete one, add new one for different account
		pgxTx2, err := dbConnectionPool.Pool().Begin(ctx)
		require.NoError(t, err)
		newUpserts := []TrustlineBalance{
			{AccountAddress: "GACCOUNT1", AssetID: assetID1, Balance: 1500, LedgerNumber: 200}, // update
			{AccountAddress: "GACCOUNT2", AssetID: assetID1, Balance: 3000, LedgerNumber: 200}, // new
		}
		deletes := []TrustlineBalance{
			{AccountAddress: "GACCOUNT1", AssetID: assetID2}, // delete
		}
		err = m.BatchUpsert(ctx, pgxTx2, newUpserts, deletes)
		require.NoError(t, err)
		require.NoError(t, pgxTx2.Commit(ctx))

		// Verify results
		var count int
		err = dbConnectionPool.Pool().QueryRow(ctx, `SELECT COUNT(*) FROM trustline_balances`).Scan(&count)
		require.NoError(t, err)
		require.Equal(t, 2, count) // GACCOUNT1:assetID1 (updated) + GACCOUNT2:assetID1 (new)

		var balance int64
		err = dbConnectionPool.Pool().QueryRow(ctx,
			`SELECT balance FROM trustline_balances WHERE account_address = $1 AND asset_id = $2`,
			"GACCOUNT1", assetID1).Scan(&balance)
		require.NoError(t, err)
		require.Equal(t, int64(1500), balance)
	})
}

func TestTrustlineBalanceModel_BatchCopy(t *testing.T) {
	ctx := context.Background()

	dbt := dbtest.Open(t)
	defer dbt.Close()
	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	// Insert test assets for foreign key references
	assetID1 := DeterministicAssetID("USDC", "ISSUER1")
	assetID2 := DeterministicAssetID("EURC", "ISSUER2")
	_, err = dbConnectionPool.Pool().Exec(ctx, `
		INSERT INTO trustline_assets (id, code, issuer) VALUES
		($1, 'USDC', 'ISSUER1'),
		($2, 'EURC', 'ISSUER2')
	`, assetID1, assetID2)
	require.NoError(t, err)

	cleanUpDB := func() {
		_, err = dbConnectionPool.Pool().Exec(ctx, `DELETE FROM trustline_balances`)
		require.NoError(t, err)
	}

	t.Run("returns nil for empty input", func(t *testing.T) {
		mockMetricsService := metrics.NewMockMetricsService()
		defer mockMetricsService.AssertExpectations(t)

		m := &TrustlineBalanceModel{
			DB:             dbConnectionPool,
			MetricsService: mockMetricsService,
		}

		pgxTx, err := dbConnectionPool.Pool().Begin(ctx)
		require.NoError(t, err)
		defer pgxTx.Rollback(ctx) //nolint:errcheck

		err = m.BatchCopy(ctx, pgxTx, nil)
		require.NoError(t, err)
	})

	t.Run("inserts single balance via COPY", func(t *testing.T) {
		cleanUpDB()
		mockMetricsService := metrics.NewMockMetricsService()
		mockMetricsService.On("ObserveDBQueryDuration", "BatchCopy", "trustline_balances", mock.Anything).Return()
		mockMetricsService.On("IncDBQuery", "BatchCopy", "trustline_balances").Return()
		defer mockMetricsService.AssertExpectations(t)

		m := &TrustlineBalanceModel{
			DB:             dbConnectionPool,
			MetricsService: mockMetricsService,
		}

		pgxTx, err := dbConnectionPool.Pool().Begin(ctx)
		require.NoError(t, err)

		balances := []TrustlineBalance{
			{
				AccountAddress:     "GACCOUNT1",
				AssetID:            assetID1,
				Balance:            1000,
				Limit:              10000,
				BuyingLiabilities:  100,
				SellingLiabilities: 50,
				Flags:              1,
				LedgerNumber:       12345,
			},
		}

		err = m.BatchCopy(ctx, pgxTx, balances)
		require.NoError(t, err)
		require.NoError(t, pgxTx.Commit(ctx))

		// Verify all fields
		var b TrustlineBalance
		err = dbConnectionPool.Pool().QueryRow(ctx, `
			SELECT account_address, asset_id, balance, trust_limit, buying_liabilities, selling_liabilities, flags, last_modified_ledger
			FROM trustline_balances WHERE account_address = $1
		`, "GACCOUNT1").Scan(&b.AccountAddress, &b.AssetID, &b.Balance, &b.Limit, &b.BuyingLiabilities, &b.SellingLiabilities, &b.Flags, &b.LedgerNumber)
		require.NoError(t, err)
		require.Equal(t, balances[0].AccountAddress, b.AccountAddress)
		require.Equal(t, balances[0].AssetID, b.AssetID)
		require.Equal(t, balances[0].Balance, b.Balance)
		require.Equal(t, balances[0].Limit, b.Limit)
		require.Equal(t, balances[0].BuyingLiabilities, b.BuyingLiabilities)
		require.Equal(t, balances[0].SellingLiabilities, b.SellingLiabilities)
		require.Equal(t, balances[0].Flags, b.Flags)
		require.Equal(t, balances[0].LedgerNumber, b.LedgerNumber)
	})

	t.Run("inserts multiple balances via COPY", func(t *testing.T) {
		cleanUpDB()
		mockMetricsService := metrics.NewMockMetricsService()
		mockMetricsService.On("ObserveDBQueryDuration", "BatchCopy", "trustline_balances", mock.Anything).Return()
		mockMetricsService.On("IncDBQuery", "BatchCopy", "trustline_balances").Return()
		defer mockMetricsService.AssertExpectations(t)

		m := &TrustlineBalanceModel{
			DB:             dbConnectionPool,
			MetricsService: mockMetricsService,
		}

		pgxTx, err := dbConnectionPool.Pool().Begin(ctx)
		require.NoError(t, err)

		balances := []TrustlineBalance{
			{AccountAddress: "GACCOUNT1", AssetID: assetID1, Balance: 1000, Limit: 10000, LedgerNumber: 100},
			{AccountAddress: "GACCOUNT1", AssetID: assetID2, Balance: 2000, Limit: 20000, LedgerNumber: 100},
			{AccountAddress: "GACCOUNT2", AssetID: assetID1, Balance: 3000, Limit: 30000, LedgerNumber: 100},
		}

		err = m.BatchCopy(ctx, pgxTx, balances)
		require.NoError(t, err)
		require.NoError(t, pgxTx.Commit(ctx))

		// Verify count
		var count int
		err = dbConnectionPool.Pool().QueryRow(ctx, `SELECT COUNT(*) FROM trustline_balances`).Scan(&count)
		require.NoError(t, err)
		require.Equal(t, 3, count)
	})
}
