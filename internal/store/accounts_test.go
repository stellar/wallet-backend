package store

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/stellar/wallet-backend/internal/data"
	"github.com/stellar/wallet-backend/internal/db"
	"github.com/stellar/wallet-backend/internal/db/dbtest"
	"github.com/stellar/wallet-backend/internal/metrics"
)

func TestAccountsStore_Add(t *testing.T) {
	ctx := context.Background()

	dbt := dbtest.Open(t)
	defer dbt.Close()
	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	cleanUpDB := func() {
		_, cleanupErr := dbConnectionPool.ExecContext(ctx, `DELETE FROM accounts`)
		require.NoError(t, cleanupErr)
	}

	t.Run("successfully adds account to cache", func(t *testing.T) {
		mockMetricsService := metrics.NewMockMetricsService()
		mockMetricsService.On("ObserveDBQueryDuration", "SELECT", "accounts", mock.Anything).Return()
		mockMetricsService.On("IncDBQuery", "SELECT", "accounts").Return()
		defer mockMetricsService.AssertExpectations(t)

		models, err := data.NewModels(dbConnectionPool, mockMetricsService)
		require.NoError(t, err)

		store, err := NewAccountsStore(models.Account)
		require.NoError(t, err)

		accountID := "GAFOZZL77R57WMGES6BO6WJDEIFJ6662GMCVEX6ZESULRX3FRBGSSV5N"

		// Add account to cache
		store.Add(accountID)

		// Verify account exists in cache
		exists := store.Exists(accountID)
		assert.True(t, exists)

		cleanUpDB()
	})

	t.Run("adding same account multiple times is idempotent", func(t *testing.T) {
		mockMetricsService := metrics.NewMockMetricsService()
		mockMetricsService.On("ObserveDBQueryDuration", "SELECT", "accounts", mock.Anything).Return()
		mockMetricsService.On("IncDBQuery", "SELECT", "accounts").Return()
		defer mockMetricsService.AssertExpectations(t)

		models, err := data.NewModels(dbConnectionPool, mockMetricsService)
		require.NoError(t, err)

		store, err := NewAccountsStore(models.Account)
		require.NoError(t, err)

		accountID := "GAFOZZL77R57WMGES6BO6WJDEIFJ6662GMCVEX6ZESULRX3FRBGSSV5N"

		// Add account multiple times
		store.Add(accountID)
		store.Add(accountID)
		store.Add(accountID)

		// Should still exist only once
		exists := store.Exists(accountID)
		assert.True(t, exists)

		cleanUpDB()
	})
}

func TestAccountsStore_Remove(t *testing.T) {
	ctx := context.Background()

	dbt := dbtest.Open(t)
	defer dbt.Close()
	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	cleanUpDB := func() {
		_, cleanupErr := dbConnectionPool.ExecContext(ctx, `DELETE FROM accounts`)
		require.NoError(t, cleanupErr)
	}

	t.Run("successfully removes account from cache", func(t *testing.T) {
		mockMetricsService := metrics.NewMockMetricsService()
		mockMetricsService.On("ObserveDBQueryDuration", "SELECT", "accounts", mock.Anything).Return()
		mockMetricsService.On("IncDBQuery", "SELECT", "accounts").Return()
		defer mockMetricsService.AssertExpectations(t)

		models, err := data.NewModels(dbConnectionPool, mockMetricsService)
		require.NoError(t, err)

		store, err := NewAccountsStore(models.Account)
		require.NoError(t, err)

		accountID := "GAFOZZL77R57WMGES6BO6WJDEIFJ6662GMCVEX6ZESULRX3FRBGSSV5N"

		// Add account first
		store.Add(accountID)
		exists := store.Exists(accountID)
		assert.True(t, exists)

		// Remove account
		store.Remove(accountID)

		// Verify account no longer exists in cache
		exists = store.Exists(accountID)
		assert.False(t, exists)

		cleanUpDB()
	})

	t.Run("removing non-existent account is safe", func(t *testing.T) {
		mockMetricsService := metrics.NewMockMetricsService()
		mockMetricsService.On("ObserveDBQueryDuration", "SELECT", "accounts", mock.Anything).Return()
		mockMetricsService.On("IncDBQuery", "SELECT", "accounts").Return()
		defer mockMetricsService.AssertExpectations(t)

		models, err := data.NewModels(dbConnectionPool, mockMetricsService)
		require.NoError(t, err)

		store, err := NewAccountsStore(models.Account)
		require.NoError(t, err)

		accountID := "NONEXISTENT_ACCOUNT"

		// Remove account that doesn't exist - should not panic
		store.Remove(accountID)

		// Verify account still doesn't exist
		exists := store.Exists(accountID)
		assert.False(t, exists)

		cleanUpDB()
	})
}

func TestAccountsStore_CachePopulationOnInit(t *testing.T) {
	ctx := context.Background()

	dbt := dbtest.Open(t)
	defer dbt.Close()
	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	cleanUpDB := func() {
		_, cleanupErr := dbConnectionPool.ExecContext(ctx, `DELETE FROM accounts`)
		require.NoError(t, cleanupErr)
	}

	t.Run("cache is populated with existing accounts from database", func(t *testing.T) {
		mockMetricsService := metrics.NewMockMetricsService()
		// Two INSERT calls for pre-populating + one GetAll call during store initialization
		mockMetricsService.On("ObserveDBQueryDuration", "INSERT", "accounts", mock.Anything).Return().Twice()
		mockMetricsService.On("IncDBQuery", "INSERT", "accounts").Return().Twice()
		mockMetricsService.On("ObserveDBQueryDuration", "SELECT", "accounts", mock.Anything).Return().Once()
		mockMetricsService.On("IncDBQuery", "SELECT", "accounts").Return().Once()
		defer mockMetricsService.AssertExpectations(t)

		models, err := data.NewModels(dbConnectionPool, mockMetricsService)
		require.NoError(t, err)

		// Pre-populate database with accounts
		accounts := []string{
			"GAFOZZL77R57WMGES6BO6WJDEIFJ6662GMCVEX6ZESULRX3FRBGSSV5N",
			"GC2BRLN55MHAW6QPKJBTXARC35IWK55DX6OGDPRTANYWXVLS3LPY5BWR",
		}

		for _, account := range accounts {
			err = models.Account.Insert(ctx, account)
			require.NoError(t, err)
		}

		// Create new store - it should populate cache from database
		store, err := NewAccountsStore(models.Account)
		require.NoError(t, err)

		// Verify all accounts are in cache
		for _, account := range accounts {
			exists := store.Exists(account)
			assert.True(t, exists, "Account %s should exist in cache after initialization", account)
		}

		cleanUpDB()
	})

	t.Run("empty database results in empty cache", func(t *testing.T) {
		mockMetricsService := metrics.NewMockMetricsService()
		mockMetricsService.On("ObserveDBQueryDuration", "SELECT", "accounts", mock.Anything).Return().Once()
		mockMetricsService.On("IncDBQuery", "SELECT", "accounts").Return().Once()
		defer mockMetricsService.AssertExpectations(t)

		models, err := data.NewModels(dbConnectionPool, mockMetricsService)
		require.NoError(t, err)

		// Create store with empty database
		store, err := NewAccountsStore(models.Account)
		require.NoError(t, err)

		// Verify cache is empty
		testAccount := "GAFOZZL77R57WMGES6BO6WJDEIFJ6662GMCVEX6ZESULRX3FRBGSSV5N"
		exists := store.Exists(testAccount)
		assert.False(t, exists)

		cleanUpDB()
	})
}

func TestAccountsStore_MultipleAccounts(t *testing.T) {
	ctx := context.Background()

	dbt := dbtest.Open(t)
	defer dbt.Close()
	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	cleanUpDB := func() {
		_, cleanupErr := dbConnectionPool.ExecContext(ctx, `DELETE FROM accounts`)
		require.NoError(t, cleanupErr)
	}

	mockMetricsService := metrics.NewMockMetricsService()
	mockMetricsService.On("ObserveDBQueryDuration", "SELECT", "accounts", mock.Anything).Return().Once()
	mockMetricsService.On("IncDBQuery", "SELECT", "accounts").Return().Once()
	defer mockMetricsService.AssertExpectations(t)

	models, err := data.NewModels(dbConnectionPool, mockMetricsService)
	require.NoError(t, err)

	store, err := NewAccountsStore(models.Account)
	require.NoError(t, err)

	// Define multiple test accounts
	accounts := []string{
		"GAFOZZL77R57WMGES6BO6WJDEIFJ6662GMCVEX6ZESULRX3FRBGSSV5N",
		"GC2BRLN55MHAW6QPKJBTXARC35IWK55DX6OGDPRTANYWXVLS3LPY5BWR",
		"GDQJUTQYK2MQX2VGDR2FYWLIYAQIEGXTQVTFEMGH2BEWFG4BRUY4CKI7",
	}

	// Add all accounts
	for _, account := range accounts {
		store.Add(account)
	}

	// Verify all accounts exist
	for _, account := range accounts {
		exists := store.Exists(account)
		assert.True(t, exists, "Account %s should exist", account)
	}

	// Remove one account
	store.Remove(accounts[1])

	// Verify the removed account no longer exists
	exists := store.Exists(accounts[1])
	assert.False(t, exists, "Removed account should not exist")

	// Verify other accounts still exist
	exists = store.Exists(accounts[0])
	assert.True(t, exists, "First account should still exist")
	exists = store.Exists(accounts[2])
	assert.True(t, exists, "Third account should still exist")

	cleanUpDB()
}

func TestAccountsStore_ConcurrentAccess(t *testing.T) {
	ctx := context.Background()

	dbt := dbtest.Open(t)
	defer dbt.Close()
	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	cleanUpDB := func() {
		_, cleanupErr := dbConnectionPool.ExecContext(ctx, `DELETE FROM accounts`)
		require.NoError(t, cleanupErr)
	}

	mockMetricsService := metrics.NewMockMetricsService()
	mockMetricsService.On("ObserveDBQueryDuration", "SELECT", "accounts", mock.Anything).Return().Once()
	mockMetricsService.On("IncDBQuery", "SELECT", "accounts").Return().Once()
	defer mockMetricsService.AssertExpectations(t)

	models, err := data.NewModels(dbConnectionPool, mockMetricsService)
	require.NoError(t, err)

	t.Run("concurrent reads and writes", func(t *testing.T) {
		store, err := NewAccountsStore(models.Account)
		require.NoError(t, err)

		// Pre-populate with some accounts
		baseAccounts := []string{
			"GAFOZZL77R57WMGES6BO6WJDEIFJ6662GMCVEX6ZESULRX3FRBGSSV5N",
			"GC2BRLN55MHAW6QPKJBTXARC35IWK55DX6OGDPRTANYWXVLS3LPY5BWR",
			"GDQJUTQYK2MQX2VGDR2FYWLIYAQIEGXTQVTFEMGH2BEWFG4BRUY4CKI7",
		}
		for _, account := range baseAccounts {
			store.Add(account)
		}

		const numReaders = 10
		const numWriters = 5
		const numOperations = 50

		var wg sync.WaitGroup
		wg.Add(numReaders + numWriters)

		// Start reader goroutines
		for i := 0; i < numReaders; i++ {
			go func(readerID int) {
				defer wg.Done()
				for j := 0; j < numOperations; j++ {
					// Read random accounts
					account := baseAccounts[j%len(baseAccounts)]
					_ = store.Exists(account)

					// Also try reading non-existent accounts
					nonExistentAccount := fmt.Sprintf("NONEXISTENT_%d_%d", readerID, j)
					_ = store.Exists(nonExistentAccount)
				}
			}(i)
		}

		// Start writer goroutines
		for i := 0; i < numWriters; i++ {
			go func(writerID int) {
				defer wg.Done()
				for j := 0; j < numOperations; j++ {
					// Add and remove accounts
					account := fmt.Sprintf("WRITER_%d_ACCOUNT_%d", writerID, j)
					store.Add(account)

					// Verify it was added
					exists := store.Exists(account)
					assert.True(t, exists)

					// Remove it
					store.Remove(account)

					// Verify it was removed
					exists = store.Exists(account)
					assert.False(t, exists)
				}
			}(i)
		}

		// Wait for all goroutines to complete
		wg.Wait()

		// Verify base accounts still exist
		for _, account := range baseAccounts {
			exists := store.Exists(account)
			assert.True(t, exists, "Base account %s should still exist after concurrent operations", account)
		}

		cleanUpDB()
	})
}
