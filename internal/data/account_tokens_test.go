// Unit tests for AccountTokensModel.
package data

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/stellar/wallet-backend/internal/db"
	"github.com/stellar/wallet-backend/internal/db/dbtest"
	"github.com/stellar/wallet-backend/internal/metrics"
)

func TestAccountTokensModel_GetTrustlineAssetIDs(t *testing.T) {
	ctx := context.Background()

	dbt := dbtest.Open(t)
	defer dbt.Close()
	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	cleanUpDB := func() {
		_, err = dbConnectionPool.ExecContext(ctx, `DELETE FROM account_trustlines`)
		require.NoError(t, err)
	}

	t.Run("returns error for empty account address", func(t *testing.T) {
		mockMetricsService := metrics.NewMockMetricsService()
		defer mockMetricsService.AssertExpectations(t)

		m := &AccountTokensModel{
			DB:             dbConnectionPool,
			MetricsService: mockMetricsService,
		}

		ids, err := m.GetTrustlineAssetIDs(ctx, "")
		require.Error(t, err)
		require.Contains(t, err.Error(), "empty account address")
		require.Nil(t, ids)
	})

	t.Run("returns empty for non-existent account", func(t *testing.T) {
		cleanUpDB()
		mockMetricsService := metrics.NewMockMetricsService()
		mockMetricsService.On("ObserveDBQueryDuration", "GetTrustlineAssetIDs", "account_trustlines", mock.Anything).Return()
		mockMetricsService.On("IncDBQuery", "GetTrustlineAssetIDs", "account_trustlines").Return()
		defer mockMetricsService.AssertExpectations(t)

		m := &AccountTokensModel{
			DB:             dbConnectionPool,
			MetricsService: mockMetricsService,
		}

		ids, err := m.GetTrustlineAssetIDs(ctx, "GBBD47IF6LWK7P7MDEVSCWR7DPUWV3NY3DTQEVFL4NAT4AQH3ZLLFLA5")
		require.NoError(t, err)
		require.Empty(t, ids)
	})

	t.Run("returns asset IDs for existing account", func(t *testing.T) {
		cleanUpDB()
		mockMetricsService := metrics.NewMockMetricsService()
		mockMetricsService.On("ObserveDBQueryDuration", "BulkInsertTrustlines", "account_trustlines", mock.Anything).Return()
		mockMetricsService.On("IncDBQuery", "BulkInsertTrustlines", "account_trustlines").Return()
		mockMetricsService.On("ObserveDBQueryDuration", "GetTrustlineAssetIDs", "account_trustlines", mock.Anything).Return()
		mockMetricsService.On("IncDBQuery", "GetTrustlineAssetIDs", "account_trustlines").Return()
		defer mockMetricsService.AssertExpectations(t)

		m := &AccountTokensModel{
			DB:             dbConnectionPool,
			MetricsService: mockMetricsService,
		}

		// Insert test data
		accountAddress := "GBBD47IF6LWK7P7MDEVSCWR7DPUWV3NY3DTQEVFL4NAT4AQH3ZLLFLA5"
		expectedIDs := []int64{1, 2, 3}
		err = db.RunInPgxTransaction(ctx, dbConnectionPool, func(dbTx pgx.Tx) error {
			return m.BulkInsertTrustlines(ctx, dbTx, map[string][]int64{accountAddress: expectedIDs})
		})
		require.NoError(t, err)

		// Retrieve and verify
		ids, err := m.GetTrustlineAssetIDs(ctx, accountAddress)
		require.NoError(t, err)
		require.ElementsMatch(t, expectedIDs, ids)

		cleanUpDB()
	})
}

func TestAccountTokensModel_BatchGetTrustlineAssetIDs(t *testing.T) {
	ctx := context.Background()

	dbt := dbtest.Open(t)
	defer dbt.Close()
	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	cleanUpDB := func() {
		_, err = dbConnectionPool.ExecContext(ctx, `DELETE FROM account_trustlines`)
		require.NoError(t, err)
	}

	t.Run("returns empty map for empty input", func(t *testing.T) {
		mockMetricsService := metrics.NewMockMetricsService()
		defer mockMetricsService.AssertExpectations(t)

		m := &AccountTokensModel{
			DB:             dbConnectionPool,
			MetricsService: mockMetricsService,
		}

		result, err := m.BatchGetTrustlineAssetIDs(ctx, []string{})
		require.NoError(t, err)
		require.Empty(t, result)
	})

	t.Run("returns asset IDs for multiple accounts", func(t *testing.T) {
		cleanUpDB()
		mockMetricsService := metrics.NewMockMetricsService()
		mockMetricsService.On("ObserveDBQueryDuration", "BulkInsertTrustlines", "account_trustlines", mock.Anything).Return()
		mockMetricsService.On("IncDBQuery", "BulkInsertTrustlines", "account_trustlines").Return()
		mockMetricsService.On("ObserveDBQueryDuration", "BatchGetTrustlineAssetIDs", "account_trustlines", mock.Anything).Return()
		mockMetricsService.On("IncDBQuery", "BatchGetTrustlineAssetIDs", "account_trustlines").Return()
		defer mockMetricsService.AssertExpectations(t)

		m := &AccountTokensModel{
			DB:             dbConnectionPool,
			MetricsService: mockMetricsService,
		}

		// Insert test data
		account1 := "GBBD47IF6LWK7P7MDEVSCWR7DPUWV3NY3DTQEVFL4NAT4AQH3ZLLFLA5"
		account2 := "GCQYG3MNNPFNFUBWXF5IDNNC7V3ZDLWLKSQVHFZEBWNPPQ4XVRCVHWQJ"
		ids1 := []int64{1, 2}
		ids2 := []int64{3, 4, 5}

		err = db.RunInPgxTransaction(ctx, dbConnectionPool, func(dbTx pgx.Tx) error {
			return m.BulkInsertTrustlines(ctx, dbTx, map[string][]int64{
				account1: ids1,
				account2: ids2,
			})
		})
		require.NoError(t, err)

		// Retrieve and verify
		result, err := m.BatchGetTrustlineAssetIDs(ctx, []string{account1, account2})
		require.NoError(t, err)
		require.Len(t, result, 2)
		require.ElementsMatch(t, ids1, result[account1])
		require.ElementsMatch(t, ids2, result[account2])

		cleanUpDB()
	})
}

func TestAccountTokensModel_GetContractIDs(t *testing.T) {
	ctx := context.Background()

	dbt := dbtest.Open(t)
	defer dbt.Close()
	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	cleanUpDB := func() {
		_, err = dbConnectionPool.ExecContext(ctx, `DELETE FROM account_contracts`)
		require.NoError(t, err)
	}

	t.Run("returns error for empty account address", func(t *testing.T) {
		mockMetricsService := metrics.NewMockMetricsService()
		defer mockMetricsService.AssertExpectations(t)

		m := &AccountTokensModel{
			DB:             dbConnectionPool,
			MetricsService: mockMetricsService,
		}

		ids, err := m.GetContractIDs(ctx, "")
		require.Error(t, err)
		require.Contains(t, err.Error(), "empty account address")
		require.Nil(t, ids)
	})

	t.Run("returns empty for non-existent account", func(t *testing.T) {
		cleanUpDB()
		mockMetricsService := metrics.NewMockMetricsService()
		mockMetricsService.On("ObserveDBQueryDuration", "GetContractIDs", "account_contracts", mock.Anything).Return()
		mockMetricsService.On("IncDBQuery", "GetContractIDs", "account_contracts").Return()
		defer mockMetricsService.AssertExpectations(t)

		m := &AccountTokensModel{
			DB:             dbConnectionPool,
			MetricsService: mockMetricsService,
		}

		ids, err := m.GetContractIDs(ctx, "GBBD47IF6LWK7P7MDEVSCWR7DPUWV3NY3DTQEVFL4NAT4AQH3ZLLFLA5")
		require.NoError(t, err)
		require.Empty(t, ids)
	})

	t.Run("returns contract IDs for existing account", func(t *testing.T) {
		cleanUpDB()
		mockMetricsService := metrics.NewMockMetricsService()
		mockMetricsService.On("ObserveDBQueryDuration", "BulkInsertContracts", "account_contracts", mock.Anything).Return()
		mockMetricsService.On("IncDBQuery", "BulkInsertContracts", "account_contracts").Return()
		mockMetricsService.On("ObserveDBQueryDuration", "GetContractIDs", "account_contracts", mock.Anything).Return()
		mockMetricsService.On("IncDBQuery", "GetContractIDs", "account_contracts").Return()
		defer mockMetricsService.AssertExpectations(t)

		m := &AccountTokensModel{
			DB:             dbConnectionPool,
			MetricsService: mockMetricsService,
		}

		// Insert test data
		accountAddress := "GBBD47IF6LWK7P7MDEVSCWR7DPUWV3NY3DTQEVFL4NAT4AQH3ZLLFLA5"
		expectedContracts := []int64{1, 2, 3}
		err = db.RunInPgxTransaction(ctx, dbConnectionPool, func(dbTx pgx.Tx) error {
			return m.BulkInsertContracts(ctx, dbTx, map[string][]int64{accountAddress: expectedContracts})
		})
		require.NoError(t, err)

		// Retrieve and verify
		ids, err := m.GetContractIDs(ctx, accountAddress)
		require.NoError(t, err)
		require.ElementsMatch(t, expectedContracts, ids)

		cleanUpDB()
	})
}

func TestAccountTokensModel_BatchAddContracts(t *testing.T) {
	ctx := context.Background()

	dbt := dbtest.Open(t)
	defer dbt.Close()
	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	cleanUpDB := func() {
		_, err = dbConnectionPool.ExecContext(ctx, `DELETE FROM account_contracts`)
		require.NoError(t, err)
	}

	t.Run("returns nil for empty input", func(t *testing.T) {
		mockMetricsService := metrics.NewMockMetricsService()
		defer mockMetricsService.AssertExpectations(t)

		m := &AccountTokensModel{
			DB:             dbConnectionPool,
			MetricsService: mockMetricsService,
		}

		err := db.RunInPgxTransaction(ctx, dbConnectionPool, func(dbTx pgx.Tx) error {
			return m.BatchAddContracts(ctx, dbTx, map[string][]int64{})
		})
		require.NoError(t, err)
	})

	t.Run("adds contracts to new account", func(t *testing.T) {
		cleanUpDB()
		mockMetricsService := metrics.NewMockMetricsService()
		mockMetricsService.On("ObserveDBQueryDuration", "BatchAddContracts", "account_contracts", mock.Anything).Return()
		mockMetricsService.On("IncDBQuery", "BatchAddContracts", "account_contracts").Return()
		mockMetricsService.On("ObserveDBQueryDuration", "GetContractIDs", "account_contracts", mock.Anything).Return()
		mockMetricsService.On("IncDBQuery", "GetContractIDs", "account_contracts").Return()
		defer mockMetricsService.AssertExpectations(t)

		m := &AccountTokensModel{
			DB:             dbConnectionPool,
			MetricsService: mockMetricsService,
		}

		accountAddress := "GBBD47IF6LWK7P7MDEVSCWR7DPUWV3NY3DTQEVFL4NAT4AQH3ZLLFLA5"
		contracts := []int64{1, 2}

		err = db.RunInPgxTransaction(ctx, dbConnectionPool, func(dbTx pgx.Tx) error {
			return m.BatchAddContracts(ctx, dbTx, map[string][]int64{accountAddress: contracts})
		})
		require.NoError(t, err)

		// Verify
		result, err := m.GetContractIDs(ctx, accountAddress)
		require.NoError(t, err)
		require.ElementsMatch(t, contracts, result)

		cleanUpDB()
	})

	t.Run("appends contracts to existing account without duplicates", func(t *testing.T) {
		cleanUpDB()
		mockMetricsService := metrics.NewMockMetricsService()
		mockMetricsService.On("ObserveDBQueryDuration", "BatchAddContracts", "account_contracts", mock.Anything).Return()
		mockMetricsService.On("IncDBQuery", "BatchAddContracts", "account_contracts").Return()
		mockMetricsService.On("ObserveDBQueryDuration", "GetContractIDs", "account_contracts", mock.Anything).Return()
		mockMetricsService.On("IncDBQuery", "GetContractIDs", "account_contracts").Return()
		defer mockMetricsService.AssertExpectations(t)

		m := &AccountTokensModel{
			DB:             dbConnectionPool,
			MetricsService: mockMetricsService,
		}

		accountAddress := "GBBD47IF6LWK7P7MDEVSCWR7DPUWV3NY3DTQEVFL4NAT4AQH3ZLLFLA5"

		// First add
		err = db.RunInPgxTransaction(ctx, dbConnectionPool, func(dbTx pgx.Tx) error {
			return m.BatchAddContracts(ctx, dbTx, map[string][]int64{accountAddress: {1, 2}})
		})
		require.NoError(t, err)

		// Second add with overlap
		err = db.RunInPgxTransaction(ctx, dbConnectionPool, func(dbTx pgx.Tx) error {
			return m.BatchAddContracts(ctx, dbTx, map[string][]int64{accountAddress: {2, 3}})
		})
		require.NoError(t, err)

		// Verify - should have all 3 without duplicates
		result, err := m.GetContractIDs(ctx, accountAddress)
		require.NoError(t, err)
		require.Len(t, result, 3)
		require.ElementsMatch(t, []int64{1, 2, 3}, result)

		cleanUpDB()
	})
}

func TestAccountTokensModel_BulkInsertTrustlines(t *testing.T) {
	ctx := context.Background()

	dbt := dbtest.Open(t)
	defer dbt.Close()
	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	cleanUpDB := func() {
		_, err = dbConnectionPool.ExecContext(ctx, `DELETE FROM account_trustlines`)
		require.NoError(t, err)
	}

	t.Run("returns nil for empty input", func(t *testing.T) {
		mockMetricsService := metrics.NewMockMetricsService()
		defer mockMetricsService.AssertExpectations(t)

		m := &AccountTokensModel{
			DB:             dbConnectionPool,
			MetricsService: mockMetricsService,
		}

		err := db.RunInPgxTransaction(ctx, dbConnectionPool, func(dbTx pgx.Tx) error {
			return m.BulkInsertTrustlines(ctx, dbTx, map[string][]int64{})
		})
		require.NoError(t, err)
	})

	t.Run("inserts trustlines for multiple accounts", func(t *testing.T) {
		cleanUpDB()
		mockMetricsService := metrics.NewMockMetricsService()
		mockMetricsService.On("ObserveDBQueryDuration", "BulkInsertTrustlines", "account_trustlines", mock.Anything).Return()
		mockMetricsService.On("IncDBQuery", "BulkInsertTrustlines", "account_trustlines").Return()
		mockMetricsService.On("ObserveDBQueryDuration", "GetTrustlineAssetIDs", "account_trustlines", mock.Anything).Return()
		mockMetricsService.On("IncDBQuery", "GetTrustlineAssetIDs", "account_trustlines").Return()
		defer mockMetricsService.AssertExpectations(t)

		m := &AccountTokensModel{
			DB:             dbConnectionPool,
			MetricsService: mockMetricsService,
		}

		account1 := "GBBD47IF6LWK7P7MDEVSCWR7DPUWV3NY3DTQEVFL4NAT4AQH3ZLLFLA5"
		account2 := "GCQYG3MNNPFNFUBWXF5IDNNC7V3ZDLWLKSQVHFZEBWNPPQ4XVRCVHWQJ"
		ids1 := []int64{1, 2, 3}
		ids2 := []int64{4, 5}

		err = db.RunInPgxTransaction(ctx, dbConnectionPool, func(dbTx pgx.Tx) error {
			return m.BulkInsertTrustlines(ctx, dbTx, map[string][]int64{
				account1: ids1,
				account2: ids2,
			})
		})
		require.NoError(t, err)

		// Verify account1
		result1, err := m.GetTrustlineAssetIDs(ctx, account1)
		require.NoError(t, err)
		require.ElementsMatch(t, ids1, result1)

		// Verify account2
		result2, err := m.GetTrustlineAssetIDs(ctx, account2)
		require.NoError(t, err)
		require.ElementsMatch(t, ids2, result2)

		cleanUpDB()
	})

	t.Run("overwrites existing trustlines on conflict", func(t *testing.T) {
		cleanUpDB()
		mockMetricsService := metrics.NewMockMetricsService()
		mockMetricsService.On("ObserveDBQueryDuration", "BulkInsertTrustlines", "account_trustlines", mock.Anything).Return()
		mockMetricsService.On("IncDBQuery", "BulkInsertTrustlines", "account_trustlines").Return()
		mockMetricsService.On("ObserveDBQueryDuration", "GetTrustlineAssetIDs", "account_trustlines", mock.Anything).Return()
		mockMetricsService.On("IncDBQuery", "GetTrustlineAssetIDs", "account_trustlines").Return()
		defer mockMetricsService.AssertExpectations(t)

		m := &AccountTokensModel{
			DB:             dbConnectionPool,
			MetricsService: mockMetricsService,
		}

		accountAddress := "GBBD47IF6LWK7P7MDEVSCWR7DPUWV3NY3DTQEVFL4NAT4AQH3ZLLFLA5"

		// First insert
		err = db.RunInPgxTransaction(ctx, dbConnectionPool, func(dbTx pgx.Tx) error {
			return m.BulkInsertTrustlines(ctx, dbTx, map[string][]int64{accountAddress: {1, 2}})
		})
		require.NoError(t, err)

		// Second insert should overwrite
		err = db.RunInPgxTransaction(ctx, dbConnectionPool, func(dbTx pgx.Tx) error {
			return m.BulkInsertTrustlines(ctx, dbTx, map[string][]int64{accountAddress: {3, 4, 5}})
		})
		require.NoError(t, err)

		// Verify - should only have the new IDs
		result, err := m.GetTrustlineAssetIDs(ctx, accountAddress)
		require.NoError(t, err)
		require.ElementsMatch(t, []int64{3, 4, 5}, result)

		cleanUpDB()
	})
}

func TestAccountTokensModel_BulkInsertContracts(t *testing.T) {
	ctx := context.Background()

	dbt := dbtest.Open(t)
	defer dbt.Close()
	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	cleanUpDB := func() {
		_, err = dbConnectionPool.ExecContext(ctx, `DELETE FROM account_contracts`)
		require.NoError(t, err)
	}

	t.Run("returns nil for empty input", func(t *testing.T) {
		mockMetricsService := metrics.NewMockMetricsService()
		defer mockMetricsService.AssertExpectations(t)

		m := &AccountTokensModel{
			DB:             dbConnectionPool,
			MetricsService: mockMetricsService,
		}

		err := db.RunInPgxTransaction(ctx, dbConnectionPool, func(dbTx pgx.Tx) error {
			return m.BulkInsertContracts(ctx, dbTx, map[string][]int64{})
		})
		require.NoError(t, err)
	})

	t.Run("inserts contracts for multiple accounts", func(t *testing.T) {
		cleanUpDB()
		mockMetricsService := metrics.NewMockMetricsService()
		mockMetricsService.On("ObserveDBQueryDuration", "BulkInsertContracts", "account_contracts", mock.Anything).Return()
		mockMetricsService.On("IncDBQuery", "BulkInsertContracts", "account_contracts").Return()
		mockMetricsService.On("ObserveDBQueryDuration", "GetContractIDs", "account_contracts", mock.Anything).Return()
		mockMetricsService.On("IncDBQuery", "GetContractIDs", "account_contracts").Return()
		defer mockMetricsService.AssertExpectations(t)

		m := &AccountTokensModel{
			DB:             dbConnectionPool,
			MetricsService: mockMetricsService,
		}

		account1 := "GBBD47IF6LWK7P7MDEVSCWR7DPUWV3NY3DTQEVFL4NAT4AQH3ZLLFLA5"
		account2 := "GCQYG3MNNPFNFUBWXF5IDNNC7V3ZDLWLKSQVHFZEBWNPPQ4XVRCVHWQJ"
		contracts1 := []int64{1, 2}
		contracts2 := []int64{3}

		err = db.RunInPgxTransaction(ctx, dbConnectionPool, func(dbTx pgx.Tx) error {
			return m.BulkInsertContracts(ctx, dbTx, map[string][]int64{
				account1: contracts1,
				account2: contracts2,
			})
		})
		require.NoError(t, err)

		// Verify account1
		result1, err := m.GetContractIDs(ctx, account1)
		require.NoError(t, err)
		require.ElementsMatch(t, contracts1, result1)

		// Verify account2
		result2, err := m.GetContractIDs(ctx, account2)
		require.NoError(t, err)
		require.ElementsMatch(t, contracts2, result2)

		cleanUpDB()
	})
}
