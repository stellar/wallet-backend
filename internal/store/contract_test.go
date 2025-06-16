// Package store provides storage interfaces and implementations for wallet-backend.
// This file contains tests for the contract store implementation.
package store

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/wallet-backend/internal/data"
	"github.com/stellar/wallet-backend/internal/db"
	"github.com/stellar/wallet-backend/internal/db/dbtest"
	"github.com/stellar/wallet-backend/internal/metrics"
)

func TestContractStore_InsertWithTx(t *testing.T) {
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

	t.Run("success", func(t *testing.T) {
		store := NewContractStore(models.Contract)

		contractID := "CONTRACT123"
		name := "Test Token"
		symbol := "TEST"

		// Insert contract with transaction
		err := store.InsertWithTx(ctx, contractID, name, symbol)
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

	t.Run("insert duplicate fails", func(t *testing.T) {
		store := NewContractStore(models.Contract)

		contractID := "CONTRACT123"
		name := "Test Token"
		symbol := "TEST"

		// First insert should succeed
		err := store.InsertWithTx(ctx, contractID, name, symbol)
		require.NoError(t, err)

		// Second insert with same ID should fail
		err = store.InsertWithTx(ctx, contractID, "Another Name", "ANTH")
		assert.Error(t, err)

		cleanUpDB()
	})
}

func TestContractStore_UpdateWithTx(t *testing.T) {
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

	t.Run("success", func(t *testing.T) {
		store := NewContractStore(models.Contract)

		contractID := "CONTRACT123"
		name1 := "Test Token"
		symbol1 := "TEST"
		name2 := "Updated Token"
		symbol2 := "UPDT"

		// Insert initial contract
		err := store.InsertWithTx(ctx, contractID, name1, symbol1)
		require.NoError(t, err)

		// Update contract
		err = store.UpdateWithTx(ctx, contractID, name2, symbol2)
		require.NoError(t, err)

		// Verify the data was updated in cache
		storedName, err := store.Name(ctx, contractID)
		require.NoError(t, err)
		assert.Equal(t, name2, storedName)

		storedSymbol, err := store.Symbol(ctx, contractID)
		require.NoError(t, err)
		assert.Equal(t, symbol2, storedSymbol)

		// Verify the data was updated in database
		contract, err := models.Contract.GetByID(ctx, contractID)
		require.NoError(t, err)
		assert.Equal(t, name2, contract.Name)
		assert.Equal(t, symbol2, contract.Symbol)

		cleanUpDB()
	})

	t.Run("update non-existent contract fails", func(t *testing.T) {
		store := NewContractStore(models.Contract)

		// Try to update non-existent contract
		err := store.UpdateWithTx(ctx, "NONEXISTENT", "Name", "SYM")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "getting contract from database")

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
		err := store.InsertWithTx(ctx, contractID, expectedName, symbol)
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
		assert.Contains(t, err.Error(), "getting contract data: contract not found")
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
		err := store.InsertWithTx(ctx, contractID, name, expectedSymbol)
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
		assert.Contains(t, err.Error(), "getting contract data: contract not found")
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
		err := store.InsertWithTx(ctx, contractID, name, symbol)
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
		err := store.InsertWithTx(ctx, c.id, c.name, c.symbol)
		require.NoError(t, err)
	}

	// Verify all contracts exist and have correct data in cache
	for _, c := range contracts {
		exists, err := store.Exists(ctx, c.id)
		require.NoError(t, err)
		assert.True(t, exists)

		name, err := store.Name(ctx, c.id)
		require.NoError(t, err)
		assert.Equal(t, c.name, name)

		symbol, err := store.Symbol(ctx, c.id)
		require.NoError(t, err)
		assert.Equal(t, c.symbol, symbol)
	}

	// Verify all contracts exist in database
	for _, c := range contracts {
		contract, err := models.Contract.GetByID(ctx, c.id)
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
		err := db.RunInTransaction(ctx, dbConnectionPool, nil, func(dbTx db.Transaction) error {
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
		exists, err := store.Exists(ctx, c.id)
		require.NoError(t, err)
		assert.True(t, exists, "Contract %s should exist in cache after initialization", c.id)

		name, err := store.Name(ctx, c.id)
		require.NoError(t, err)
		assert.Equal(t, c.name, name)

		symbol, err := store.Symbol(ctx, c.id)
		require.NoError(t, err)
		assert.Equal(t, c.symbol, symbol)
	}

	// Clean up
	_, err = dbConnectionPool.ExecContext(ctx, `DELETE FROM contracts`)
	require.NoError(t, err)
}