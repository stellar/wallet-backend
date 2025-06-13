// Package store provides storage interfaces and implementations for wallet-backend.
// This file contains tests for the in-memory contract store implementation.
package store

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMemoryContractStore_Set(t *testing.T) {
	ctx := context.Background()

	t.Run("success", func(t *testing.T) {
		store := NewContractStore()

		contractID := "CONTRACT123"
		name := "Test Token"
		symbol := "TEST"

		err := store.Set(ctx, contractID, name, symbol)
		require.NoError(t, err)

		// Verify the data was stored correctly
		storedName, err := store.Name(ctx, contractID)
		require.NoError(t, err)
		assert.Equal(t, name, storedName)

		storedSymbol, err := store.Symbol(ctx, contractID)
		require.NoError(t, err)
		assert.Equal(t, symbol, storedSymbol)
	})

	t.Run("overwrite existing values", func(t *testing.T) {
		store := NewContractStore()

		contractID := "CONTRACT123"
		name1 := "Test Token"
		symbol1 := "TEST"
		name2 := "Updated Token"
		symbol2 := "UPDT"

		// Set initial values
		err := store.Set(ctx, contractID, name1, symbol1)
		require.NoError(t, err)

		// Overwrite with new values
		err = store.Set(ctx, contractID, name2, symbol2)
		require.NoError(t, err)

		// Verify the data was updated
		storedName, err := store.Name(ctx, contractID)
		require.NoError(t, err)
		assert.Equal(t, name2, storedName)

		storedSymbol, err := store.Symbol(ctx, contractID)
		require.NoError(t, err)
		assert.Equal(t, symbol2, storedSymbol)
	})
}

func TestMemoryContractStore_Name(t *testing.T) {
	ctx := context.Background()

	t.Run("success", func(t *testing.T) {
		store := NewContractStore()

		contractID := "CONTRACT123"
		expectedName := "Test Token"
		symbol := "TEST"

		err := store.Set(ctx, contractID, expectedName, symbol)
		require.NoError(t, err)

		name, err := store.Name(ctx, contractID)
		require.NoError(t, err)
		assert.Equal(t, expectedName, name)
	})

	t.Run("not found", func(t *testing.T) {
		store := NewContractStore()

		contractID := "NONEXISTENT"

		name, err := store.Name(ctx, contractID)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "getting contract data: contract not found")
		assert.Empty(t, name)
	})
}

func TestMemoryContractStore_Symbol(t *testing.T) {
	ctx := context.Background()

	t.Run("success", func(t *testing.T) {
		store := NewContractStore()

		contractID := "CONTRACT123"
		name := "Test Token"
		expectedSymbol := "TEST"

		err := store.Set(ctx, contractID, name, expectedSymbol)
		require.NoError(t, err)

		symbol, err := store.Symbol(ctx, contractID)
		require.NoError(t, err)
		assert.Equal(t, expectedSymbol, symbol)
	})

	t.Run("not found", func(t *testing.T) {
		store := NewContractStore()

		contractID := "NONEXISTENT"

		symbol, err := store.Symbol(ctx, contractID)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "getting contract data: contract not found")
		assert.Empty(t, symbol)
	})
}

func TestMemoryContractStore_Exists(t *testing.T) {
	ctx := context.Background()

	t.Run("exists", func(t *testing.T) {
		store := NewContractStore()

		contractID := "CONTRACT123"
		name := "Test Token"
		symbol := "TEST"

		err := store.Set(ctx, contractID, name, symbol)
		require.NoError(t, err)

		exists, err := store.Exists(ctx, contractID)
		require.NoError(t, err)
		assert.True(t, exists)
	})

	t.Run("does not exist", func(t *testing.T) {
		store := NewContractStore()

		contractID := "NONEXISTENT"

		exists, err := store.Exists(ctx, contractID)
		require.NoError(t, err)
		assert.False(t, exists)
	})
}

func TestMemoryContractStore_MultipleContracts(t *testing.T) {
	ctx := context.Background()
	store := NewContractStore()

	// Set multiple contracts
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
		err := store.Set(ctx, c.id, c.name, c.symbol)
		require.NoError(t, err)
	}

	// Verify all contracts exist and have correct data
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
}
