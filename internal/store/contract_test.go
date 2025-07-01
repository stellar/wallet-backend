package store

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/stellar/wallet-backend/internal/data"
	"github.com/stellar/wallet-backend/internal/db"
	"github.com/stellar/wallet-backend/internal/db/dbtest"
	"github.com/stellar/wallet-backend/internal/metrics"
)

func TestContractStore_UpsertWithTx(t *testing.T) {
	ctx := context.Background()

	dbt := dbtest.Open(t)
	defer dbt.Close()
	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	cleanUpDB := func() {
		_, err := dbConnectionPool.ExecContext(ctx, `DELETE FROM token_contracts`)
		require.NoError(t, err)
	}

	t.Run("successfully inserts a new contract in DB and cache", func(t *testing.T) {
		mockMetricsService := metrics.NewMockMetricsService()
		mockMetricsService.On("ObserveDBQueryDuration", "INSERT", "token_contracts", mock.Anything).Return()
		mockMetricsService.On("IncDBQuery", "INSERT", "token_contracts").Return()
		mockMetricsService.On("ObserveDBQueryDuration", "SELECT", "token_contracts", mock.Anything).Return()
		mockMetricsService.On("IncDBQuery", "SELECT", "token_contracts").Return()
		defer mockMetricsService.AssertExpectations(t)

		models, err := data.NewModels(dbConnectionPool, mockMetricsService)
		require.NoError(t, err)

		store, err := NewTokenContractStore(models.Contract)
		require.NoError(t, err)

		contractID := "CONTRACT123"
		name := "Test Token"
		symbol := "TEST"

		// Upsert contract with transaction
		err = store.UpsertWithTx(ctx, contractID, name, symbol)
		require.NoError(t, err)

		// Verify the data was stored correctly in cache
		storedName, err := store.Name(ctx, contractID)
		require.NoError(t, err)
		assert.Equal(t, name, storedName)

		storedSymbol, err := store.Symbol(ctx, contractID)
		require.NoError(t, err)
		assert.Equal(t, symbol, storedSymbol)

		// Verify the data exists in database
		contract, err := models.Contract.GetByID(ctx, contractID)
		require.NoError(t, err)
		assert.Equal(t, name, contract.Name)
		assert.Equal(t, symbol, contract.Symbol)

		cleanUpDB()
	})

	t.Run("updates an existing contract in the DB and cache", func(t *testing.T) {
		mockMetricsService := metrics.NewMockMetricsService()
		mockMetricsService.On("ObserveDBQueryDuration", "SELECT", "token_contracts", mock.Anything).Return().Times(3)
		mockMetricsService.On("IncDBQuery", "SELECT", "token_contracts").Return().Times(3)
		mockMetricsService.On("ObserveDBQueryDuration", "INSERT", "token_contracts", mock.Anything).Return().Once()
		mockMetricsService.On("IncDBQuery", "INSERT", "token_contracts").Return().Once()
		mockMetricsService.On("ObserveDBQueryDuration", "UPDATE", "token_contracts", mock.Anything).Return().Once()
		mockMetricsService.On("IncDBQuery", "UPDATE", "token_contracts").Return().Once()
		defer mockMetricsService.AssertExpectations(t)

		models, err := data.NewModels(dbConnectionPool, mockMetricsService)
		require.NoError(t, err)

		store, err := NewTokenContractStore(models.Contract)
		require.NoError(t, err)

		contractID := "CONTRACT123"
		name := "Test Token"
		symbol := "TEST"

		// First upsert should succeed
		err = store.UpsertWithTx(ctx, contractID, name, symbol)
		require.NoError(t, err)

		// Second upsert with same ID should succeed and update
		err = store.UpsertWithTx(ctx, contractID, "Another Name", "ANTH")
		require.NoError(t, err)

		// Verify the data was updated
		storedName, err := store.Name(ctx, contractID)
		require.NoError(t, err)
		assert.Equal(t, "Another Name", storedName)

		storedSymbol, err := store.Symbol(ctx, contractID)
		require.NoError(t, err)
		assert.Equal(t, "ANTH", storedSymbol)

		cleanUpDB()
	})
}

func TestContractStore_Name(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()

	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	ctx := context.Background()

	cleanUpDB := func() {
		_, err := dbConnectionPool.ExecContext(ctx, `DELETE FROM token_contracts`)
		require.NoError(t, err)
	}

	t.Run("success - from cache", func(t *testing.T) {
		mockMetricsService := metrics.NewMockMetricsService()
		mockMetricsService.On("ObserveDBQueryDuration", "INSERT", "token_contracts", mock.Anything).Return().Once()
		mockMetricsService.On("IncDBQuery", "INSERT", "token_contracts").Return().Once()
		mockMetricsService.On("ObserveDBQueryDuration", "SELECT", "token_contracts", mock.Anything).Return().Twice() // GetAll + UpsertWithTx
		mockMetricsService.On("IncDBQuery", "SELECT", "token_contracts").Return().Twice()
		defer mockMetricsService.AssertExpectations(t)

		models, err := data.NewModels(dbConnectionPool, mockMetricsService)
		require.NoError(t, err)

		store, err := NewTokenContractStore(models.Contract)
		require.NoError(t, err)

		contractID := "CONTRACT123"
		expectedName := "Test Token"
		symbol := "TEST"

		// Insert contract
		err = store.UpsertWithTx(ctx, contractID, expectedName, symbol)
		require.NoError(t, err)

		// Get name from cache
		name, err := store.Name(ctx, contractID)
		require.NoError(t, err)
		assert.Equal(t, expectedName, name)

		cleanUpDB()
	})

	t.Run("not found in cache", func(t *testing.T) {
		mockMetricsService := metrics.NewMockMetricsService()
		mockMetricsService.On("ObserveDBQueryDuration", "SELECT", "token_contracts", mock.Anything).Return().Once() // GetAll only
		mockMetricsService.On("IncDBQuery", "SELECT", "token_contracts").Return().Once()
		defer mockMetricsService.AssertExpectations(t)

		models, err := data.NewModels(dbConnectionPool, mockMetricsService)
		require.NoError(t, err)

		store, err := NewTokenContractStore(models.Contract)
		require.NoError(t, err)

		contractID := "NONEXISTENT"

		name, err := store.Name(ctx, contractID)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "contract not found")
		assert.Empty(t, name)
	})
}

func TestContractStore_Symbol(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()

	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	ctx := context.Background()

	cleanUpDB := func() {
		_, err := dbConnectionPool.ExecContext(ctx, `DELETE FROM token_contracts`)
		require.NoError(t, err)
	}

	t.Run("success - from cache", func(t *testing.T) {
		mockMetricsService := metrics.NewMockMetricsService()
		mockMetricsService.On("ObserveDBQueryDuration", "INSERT", "token_contracts", mock.Anything).Return().Once()
		mockMetricsService.On("IncDBQuery", "INSERT", "token_contracts").Return().Once()
		mockMetricsService.On("ObserveDBQueryDuration", "SELECT", "token_contracts", mock.Anything).Return().Twice() // GetAll + UpsertWithTx
		mockMetricsService.On("IncDBQuery", "SELECT", "token_contracts").Return().Twice()
		defer mockMetricsService.AssertExpectations(t)

		models, err := data.NewModels(dbConnectionPool, mockMetricsService)
		require.NoError(t, err)

		store, err := NewTokenContractStore(models.Contract)
		require.NoError(t, err)

		contractID := "CONTRACT123"
		name := "Test Token"
		expectedSymbol := "TEST"

		// Insert contract
		err = store.UpsertWithTx(ctx, contractID, name, expectedSymbol)
		require.NoError(t, err)

		// Get symbol from cache
		symbol, err := store.Symbol(ctx, contractID)
		require.NoError(t, err)
		assert.Equal(t, expectedSymbol, symbol)

		cleanUpDB()
	})

	t.Run("not found in cache", func(t *testing.T) {
		mockMetricsService := metrics.NewMockMetricsService()
		mockMetricsService.On("ObserveDBQueryDuration", "SELECT", "token_contracts", mock.Anything).Return().Once() // GetAll only
		mockMetricsService.On("IncDBQuery", "SELECT", "token_contracts").Return().Once()
		defer mockMetricsService.AssertExpectations(t)

		models, err := data.NewModels(dbConnectionPool, mockMetricsService)
		require.NoError(t, err)

		store, err := NewTokenContractStore(models.Contract)
		require.NoError(t, err)

		contractID := "NONEXISTENT"

		symbol, err := store.Symbol(ctx, contractID)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "contract not found")
		assert.Empty(t, symbol)
	})
}

func TestContractStore_Exists(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()

	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	ctx := context.Background()

	cleanUpDB := func() {
		_, err := dbConnectionPool.ExecContext(ctx, `DELETE FROM token_contracts`)
		require.NoError(t, err)
	}

	t.Run("exists in cache", func(t *testing.T) {
		mockMetricsService := metrics.NewMockMetricsService()
		mockMetricsService.On("ObserveDBQueryDuration", "SELECT", "token_contracts", mock.Anything).Return().Twice() // GetAll + UpsertWithTx
		mockMetricsService.On("IncDBQuery", "SELECT", "token_contracts").Return().Twice()
		mockMetricsService.On("ObserveDBQueryDuration", "INSERT", "token_contracts", mock.Anything).Return().Once()
		mockMetricsService.On("IncDBQuery", "INSERT", "token_contracts").Return().Once()
		defer mockMetricsService.AssertExpectations(t)

		models, err := data.NewModels(dbConnectionPool, mockMetricsService)
		require.NoError(t, err)

		store, err := NewTokenContractStore(models.Contract)
		require.NoError(t, err)

		contractID := "CONTRACT123"
		name := "Test Token"
		symbol := "TEST"

		// Insert contract
		err = store.UpsertWithTx(ctx, contractID, name, symbol)
		require.NoError(t, err)

		exists := store.Exists(ctx, contractID)
		assert.True(t, exists)

		cleanUpDB()
	})

	t.Run("does not exist in cache", func(t *testing.T) {
		mockMetricsService := metrics.NewMockMetricsService()
		mockMetricsService.On("ObserveDBQueryDuration", "SELECT", "token_contracts", mock.Anything).Return().Once() // GetAll only
		mockMetricsService.On("IncDBQuery", "SELECT", "token_contracts").Return().Once()
		defer mockMetricsService.AssertExpectations(t)

		models, err := data.NewModels(dbConnectionPool, mockMetricsService)
		require.NoError(t, err)

		store, err := NewTokenContractStore(models.Contract)
		require.NoError(t, err)

		contractID := "NONEXISTENT"

		exists := store.Exists(ctx, contractID)
		assert.False(t, exists)
	})

	t.Run("cache expiration triggers refresh workflow", func(t *testing.T) {
		mockMetricsService := metrics.NewMockMetricsService()
		mockMetricsService.On("ObserveDBQueryDuration", "SELECT", "token_contracts", mock.Anything).Return().Times(3) // GetAll + 2 UpsertWithTx
		mockMetricsService.On("IncDBQuery", "SELECT", "token_contracts").Return().Times(3)
		mockMetricsService.On("ObserveDBQueryDuration", "INSERT", "token_contracts", mock.Anything).Return().Once()
		mockMetricsService.On("IncDBQuery", "INSERT", "token_contracts").Return().Once()
		mockMetricsService.On("ObserveDBQueryDuration", "UPDATE", "token_contracts", mock.Anything).Return().Once()
		mockMetricsService.On("IncDBQuery", "UPDATE", "token_contracts").Return().Once()
		defer mockMetricsService.AssertExpectations(t)

		models, err := data.NewModels(dbConnectionPool, mockMetricsService)
		require.NoError(t, err)

		store, err := NewTokenContractStore(models.Contract)
		require.NoError(t, err)

		contractID := "CONTRACT_EXPIRY_TEST"
		name := "Original Name"
		symbol := "ORIG"

		// Initial upsert
		err = store.UpsertWithTx(ctx, contractID, name, symbol)
		require.NoError(t, err)

		// Verify data is in cache
		exists := store.Exists(ctx, contractID)
		assert.True(t, exists)

		storedName, err := store.Name(ctx, contractID)
		require.NoError(t, err)
		assert.Equal(t, name, storedName)

		// Simulate cache expiration by manually deleting from cache
		store.(*tokenContractStore).cache.Delete(contractID)

		// Now Exists should return false (cache miss)
		exists = store.Exists(ctx, contractID)
		assert.False(t, exists, "Exists should return false when cache expires")

		// Name should also fail since cache is empty
		_, err = store.Name(ctx, contractID)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "contract not found")

		// This is where calling code would refresh with new metadata
		newName := "Updated Name"
		newSymbol := "UPD"
		err = store.UpsertWithTx(ctx, contractID, newName, newSymbol)
		require.NoError(t, err)

		// Now everything should work with fresh data
		exists = store.Exists(ctx, contractID)
		assert.True(t, exists)

		storedName, err = store.Name(ctx, contractID)
		require.NoError(t, err)
		assert.Equal(t, newName, storedName)

		storedSymbol, err := store.Symbol(ctx, contractID)
		require.NoError(t, err)
		assert.Equal(t, newSymbol, storedSymbol)

		cleanUpDB()
	})
}

func TestContractStore_MultipleContracts(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()

	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	mockMetricsService := metrics.NewMockMetricsService()
	// This test calls UpsertWithTx 3 times + 1 GetAll + 3 GetByID calls to verify = 7 total SELECT operations
	mockMetricsService.On("ObserveDBQueryDuration", "INSERT", "token_contracts", mock.Anything).Return().Times(3)
	mockMetricsService.On("IncDBQuery", "INSERT", "token_contracts").Return().Times(3)
	mockMetricsService.On("ObserveDBQueryDuration", "SELECT", "token_contracts", mock.Anything).Return().Times(7)
	mockMetricsService.On("IncDBQuery", "SELECT", "token_contracts").Return().Times(7)
	defer mockMetricsService.AssertExpectations(t)

	models, err := data.NewModels(dbConnectionPool, mockMetricsService)
	require.NoError(t, err)

	ctx := context.Background()
	store, err := NewTokenContractStore(models.Contract)
	require.NoError(t, err)

	// Insert multiple contracts
	contracts := []struct {
		id     string
		name   string
		symbol string
	}{
		{"CONTRACT1", "Token One", "TK1"},
		{"CONTRACT2", "Token Two", "TK2"},
		{"CONTRACT3", "Token Three", "TK3"},
	}

	for _, c := range contracts {
		err = store.UpsertWithTx(ctx, c.id, c.name, c.symbol)
		require.NoError(t, err)
	}

	// Verify all contracts exist and have correct data in cache
	var exists bool
	var name, symbol string
	for _, c := range contracts {
		exists = store.Exists(ctx, c.id)
		assert.True(t, exists)

		name, err = store.Name(ctx, c.id)
		require.NoError(t, err)
		assert.Equal(t, c.name, name)

		symbol, err = store.Symbol(ctx, c.id)
		require.NoError(t, err)
		assert.Equal(t, c.symbol, symbol)
	}

	// Verify all contracts exist in database
	var contract *data.Contract
	for _, c := range contracts {
		contract, err = models.Contract.GetByID(ctx, c.id)
		require.NoError(t, err)
		assert.Equal(t, c.name, contract.Name)
		assert.Equal(t, c.symbol, contract.Symbol)
	}

	// Clean up
	_, err = dbConnectionPool.ExecContext(ctx, `DELETE FROM token_contracts`)
	require.NoError(t, err)
}

func TestContractStore_CachePopulationOnInit(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()

	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	mockMetricsService := metrics.NewMockMetricsService()
	// This test inserts 2 contracts directly to DB and then creates store
	// 2 INSERT calls + 1 GetAll call during store initialization
	mockMetricsService.On("ObserveDBQueryDuration", "INSERT", "token_contracts", mock.Anything).Return().Twice()
	mockMetricsService.On("IncDBQuery", "INSERT", "token_contracts").Return().Twice()
	mockMetricsService.On("ObserveDBQueryDuration", "SELECT", "token_contracts", mock.Anything).Return().Once()
	mockMetricsService.On("IncDBQuery", "SELECT", "token_contracts").Return().Once()
	defer mockMetricsService.AssertExpectations(t)

	models, err := data.NewModels(dbConnectionPool, mockMetricsService)
	require.NoError(t, err)

	ctx := context.Background()

	// First, insert some contracts directly into the database
	contracts := []struct {
		id     string
		name   string
		symbol string
	}{
		{"CONTRACT1", "Token One", "TK1"},
		{"CONTRACT2", "Token Two", "TK2"},
	}

	for _, c := range contracts {
		err = db.RunInTransaction(ctx, dbConnectionPool, nil, func(dbTx db.Transaction) error {
			contract := &data.Contract{
				ID:     c.id,
				Name:   c.name,
				Symbol: c.symbol,
			}
			return models.Contract.Insert(ctx, dbTx, contract)
		})
		require.NoError(t, err)
	}

	// Now create a new store - it should populate cache from database
	store, err := NewTokenContractStore(models.Contract)
	require.NoError(t, err)

	// Verify all contracts are in cache
	var exists bool
	var name, symbol string
	for _, c := range contracts {
		exists = store.Exists(ctx, c.id)
		assert.True(t, exists, "Contract %s should exist in cache after initialization", c.id)

		name, err = store.Name(ctx, c.id)
		require.NoError(t, err)
		assert.Equal(t, c.name, name)

		symbol, err = store.Symbol(ctx, c.id)
		require.NoError(t, err)
		assert.Equal(t, c.symbol, symbol)
	}

	// Clean up
	_, err = dbConnectionPool.ExecContext(ctx, `DELETE FROM token_contracts`)
	require.NoError(t, err)
}

func TestContractStore_ConcurrentAccess(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()

	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	mockMetricsService := metrics.NewMockMetricsService()
	// This test has lots of concurrent operations, we need to be generous with expectations
	// 1 GetAll + 3 initial inserts + 10 writers * 50 operations each = 504 total SELECT operations
	mockMetricsService.On("ObserveDBQueryDuration", "INSERT", "token_contracts", mock.Anything).Return().Times(3)
	mockMetricsService.On("IncDBQuery", "INSERT", "token_contracts").Return().Times(3)
	mockMetricsService.On("ObserveDBQueryDuration", "SELECT", "token_contracts", mock.Anything).Return().Times(504)
	mockMetricsService.On("IncDBQuery", "SELECT", "token_contracts").Return().Times(504)
	mockMetricsService.On("ObserveDBQueryDuration", "UPDATE", "token_contracts", mock.Anything).Return().Times(500)
	mockMetricsService.On("IncDBQuery", "UPDATE", "token_contracts").Return().Times(500)
	defer mockMetricsService.AssertExpectations(t)

	models, err := data.NewModels(dbConnectionPool, mockMetricsService)
	require.NoError(t, err)

	ctx := context.Background()

	t.Run("concurrent reads and writes on same contracts", func(t *testing.T) {
		store, err := NewTokenContractStore(models.Contract)
		require.NoError(t, err)

		// Pre-populate some contracts
		sharedContracts := []string{"SHARED_1", "SHARED_2", "SHARED_3"}
		for i, id := range sharedContracts {
			err = store.UpsertWithTx(ctx, id, fmt.Sprintf("Initial %d", i), fmt.Sprintf("INIT%d", i))
			require.NoError(t, err)
		}

		const numReaders = 20
		const numWriters = 10
		done := make(chan bool, numReaders+numWriters)

		// Start reader goroutines
		for range numReaders {
			go func() {
				defer func() { done <- true }()

				// Repeatedly read random contracts
				for j := range 100 {
					contractID := sharedContracts[j%len(sharedContracts)]

					_, _ = store.Name(ctx, contractID)   //nolint:errcheck
					_, _ = store.Symbol(ctx, contractID) //nolint:errcheck
					_ = store.Exists(ctx, contractID)    //nolint:errcheck
				}
			}()
		}

		// Start writer goroutines
		for i := range numWriters {
			go func(writerID int) {
				defer func() { done <- true }()

				// Repeatedly update random contracts
				for j := range 50 {
					contractID := sharedContracts[j%len(sharedContracts)]
					name := fmt.Sprintf("Updated by %d at %d", writerID, j)
					symbol := fmt.Sprintf("UPD%d", writerID)

					err := store.UpsertWithTx(ctx, contractID, name, symbol)
					assert.NoError(t, err)
				}
			}(i)
		}

		// Wait for completion
		for range numReaders + numWriters {
			<-done
		}

		// Verify all contracts still exist and are valid
		for _, id := range sharedContracts {
			exists := store.Exists(ctx, id)
			assert.True(t, exists)
		}
	})
}
