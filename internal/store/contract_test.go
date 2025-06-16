// Package store provides storage interfaces and implementations for wallet-backend.
// This file contains tests for the contract store implementation.
package store

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/wallet-backend/internal/data"
	"github.com/stellar/wallet-backend/internal/db"
	"github.com/stellar/wallet-backend/internal/db/dbtest"
	"github.com/stellar/wallet-backend/internal/metrics"
)

func TestContractStore_UpsertWithTx(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()

	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	models, err := data.NewModels(dbConnectionPool, metrics.NewMockMetricsService())
	require.NoError(t, err)

	ctx := context.Background()

	cleanUpDB := func() {
		_, err := dbConnectionPool.ExecContext(ctx, `DELETE FROM contracts`)
		require.NoError(t, err)
	}

	t.Run("successfully inserts a new contract in DB and cache", func(t *testing.T) {
		store := NewContractStore(models.Contract)

		contractID := "CONTRACT123"
		name := "Test Token"
		symbol := "TEST"

		// Upsert contract with transaction
		err := store.UpsertWithTx(ctx, contractID, name, symbol)
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
		store := NewContractStore(models.Contract)

		contractID := "CONTRACT123"
		name := "Test Token"
		symbol := "TEST"

		// First upsert should succeed
		err := store.UpsertWithTx(ctx, contractID, name, symbol)
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

	models, err := data.NewModels(dbConnectionPool, metrics.NewMockMetricsService())
	require.NoError(t, err)

	ctx := context.Background()

	cleanUpDB := func() {
		_, err := dbConnectionPool.ExecContext(ctx, `DELETE FROM contracts`)
		require.NoError(t, err)
	}

	t.Run("success - from cache", func(t *testing.T) {
		store := NewContractStore(models.Contract)

		contractID := "CONTRACT123"
		expectedName := "Test Token"
		symbol := "TEST"

		// Insert contract
		err := store.UpsertWithTx(ctx, contractID, expectedName, symbol)
		require.NoError(t, err)

		// Get name from cache
		name, err := store.Name(ctx, contractID)
		require.NoError(t, err)
		assert.Equal(t, expectedName, name)

		cleanUpDB()
	})

	t.Run("not found in cache", func(t *testing.T) {
		store := NewContractStore(models.Contract)

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

	models, err := data.NewModels(dbConnectionPool, metrics.NewMockMetricsService())
	require.NoError(t, err)

	ctx := context.Background()

	cleanUpDB := func() {
		_, err := dbConnectionPool.ExecContext(ctx, `DELETE FROM contracts`)
		require.NoError(t, err)
	}

	t.Run("success - from cache", func(t *testing.T) {
		store := NewContractStore(models.Contract)

		contractID := "CONTRACT123"
		name := "Test Token"
		expectedSymbol := "TEST"

		// Insert contract
		err := store.UpsertWithTx(ctx, contractID, name, expectedSymbol)
		require.NoError(t, err)

		// Get symbol from cache
		symbol, err := store.Symbol(ctx, contractID)
		require.NoError(t, err)
		assert.Equal(t, expectedSymbol, symbol)

		cleanUpDB()
	})

	t.Run("not found in cache", func(t *testing.T) {
		store := NewContractStore(models.Contract)

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

	models, err := data.NewModels(dbConnectionPool, metrics.NewMockMetricsService())
	require.NoError(t, err)

	ctx := context.Background()

	cleanUpDB := func() {
		_, err := dbConnectionPool.ExecContext(ctx, `DELETE FROM contracts`)
		require.NoError(t, err)
	}

	t.Run("exists in cache", func(t *testing.T) {
		store := NewContractStore(models.Contract)

		contractID := "CONTRACT123"
		name := "Test Token"
		symbol := "TEST"

		// Insert contract
		err := store.UpsertWithTx(ctx, contractID, name, symbol)
		require.NoError(t, err)

		exists, err := store.Exists(ctx, contractID)
		require.NoError(t, err)
		assert.True(t, exists)

		cleanUpDB()
	})

	t.Run("does not exist in cache", func(t *testing.T) {
		store := NewContractStore(models.Contract)

		contractID := "NONEXISTENT"

		exists, err := store.Exists(ctx, contractID)
		require.NoError(t, err)
		assert.False(t, exists)
	})

	t.Run("cache expiration triggers refresh workflow", func(t *testing.T) {
		store := NewContractStore(models.Contract)

		contractID := "CONTRACT_EXPIRY_TEST"
		name := "Original Name"
		symbol := "ORIG"

		// Initial upsert
		err := store.UpsertWithTx(ctx, contractID, name, symbol)
		require.NoError(t, err)

		// Verify data is in cache
		exists, err := store.Exists(ctx, contractID)
		require.NoError(t, err)
		assert.True(t, exists)

		storedName, err := store.Name(ctx, contractID)
		require.NoError(t, err)
		assert.Equal(t, name, storedName)

		// Simulate cache expiration by manually deleting from cache
		store.(*contractStore).cache.Delete(contractID)

		// Now Exists should return false (cache miss)
		exists, err = store.Exists(ctx, contractID)
		require.NoError(t, err)
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
		exists, err = store.Exists(ctx, contractID)
		require.NoError(t, err)
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

	models, err := data.NewModels(dbConnectionPool, metrics.NewMockMetricsService())
	require.NoError(t, err)

	ctx := context.Background()
	store := NewContractStore(models.Contract)

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
	for _, c := range contracts {
		var exists bool
		exists, err = store.Exists(ctx, c.id)
		require.NoError(t, err)
		assert.True(t, exists)

		var name string
		name, err = store.Name(ctx, c.id)
		require.NoError(t, err)
		assert.Equal(t, c.name, name)

		var symbol string
		symbol, err = store.Symbol(ctx, c.id)
		require.NoError(t, err)
		assert.Equal(t, c.symbol, symbol)
	}

	// Verify all contracts exist in database
	for _, c := range contracts {
		var contract *data.Contract
		contract, err = models.Contract.GetByID(ctx, c.id)
		require.NoError(t, err)
		assert.Equal(t, c.name, contract.Name)
		assert.Equal(t, c.symbol, contract.Symbol)
	}

	// Clean up
	_, err = dbConnectionPool.ExecContext(ctx, `DELETE FROM contracts`)
	require.NoError(t, err)
}

func TestContractStore_CachePopulationOnInit(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()

	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	models, err := data.NewModels(dbConnectionPool, metrics.NewMockMetricsService())
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
	store := NewContractStore(models.Contract)

	// Verify all contracts are in cache
	for _, c := range contracts {
		var exists bool
		exists, err = store.Exists(ctx, c.id)
		require.NoError(t, err)
		assert.True(t, exists, "Contract %s should exist in cache after initialization", c.id)

		var name string
		name, err = store.Name(ctx, c.id)
		require.NoError(t, err)
		assert.Equal(t, c.name, name)

		var symbol string
		symbol, err = store.Symbol(ctx, c.id)
		require.NoError(t, err)
		assert.Equal(t, c.symbol, symbol)
	}

	// Clean up
	_, err = dbConnectionPool.ExecContext(ctx, `DELETE FROM contracts`)
	require.NoError(t, err)
}

func TestContractStore_ConcurrentAccess(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()

	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	models, err := data.NewModels(dbConnectionPool, metrics.NewMockMetricsService())
	require.NoError(t, err)

	ctx := context.Background()

	t.Run("concurrent upserts and reads", func(t *testing.T) {
		store := NewContractStore(models.Contract)

		// Start multiple goroutines doing upserts and reads
		const numGoroutines = 10
		done := make(chan bool, numGoroutines)

		for i := 0; i < numGoroutines; i++ {
			go func(id int) {
				defer func() { done <- true }()

				contractID := fmt.Sprintf("CONCURRENT_%d", id)
				name := fmt.Sprintf("Concurrent Token %d", id)
				symbol := fmt.Sprintf("CT%d", id)

				// Upsert
				upsertErr := store.UpsertWithTx(ctx, contractID, name, symbol)
				require.NoError(t, upsertErr)

				// Read back
				var readName string
				var readSymbol string
				var exists bool
				var readErr error

				readName, readErr = store.Name(ctx, contractID)
				require.NoError(t, readErr)
				assert.Equal(t, name, readName)

				readSymbol, readErr = store.Symbol(ctx, contractID)
				require.NoError(t, readErr)
				assert.Equal(t, symbol, readSymbol)

				exists, readErr = store.Exists(ctx, contractID)
				require.NoError(t, readErr)
				assert.True(t, exists)
			}(i)
		}

		// Wait for all goroutines to complete
		for i := 0; i < numGoroutines; i++ {
			<-done
		}

		// Clean up
		_, err = dbConnectionPool.ExecContext(ctx, `DELETE FROM contracts`)
		require.NoError(t, err)
	})
}
