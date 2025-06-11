package store

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestContractStore_Set(t *testing.T) {
	ctx := context.Background()

	t.Run("success", func(t *testing.T) {
		mockRedis := new(MockRedisClient)
		store := NewContractStore(mockRedis)

		contractID := "CONTRACT123"
		name := "Test Token"
		symbol := "TEST"

		mockRedis.On("HSet", ctx, contractID, contractNameKey, name, defaultExpiration).Return(nil).Once()
		mockRedis.On("HSet", ctx, contractID, contractSymbolKey, symbol, defaultExpiration).Return(nil).Once()

		err := store.Set(ctx, contractID, name, symbol)
		require.NoError(t, err)

		mockRedis.AssertExpectations(t)
	})

	t.Run("error_setting_name", func(t *testing.T) {
		mockRedis := new(MockRedisClient)
		store := NewContractStore(mockRedis)

		contractID := "CONTRACT123"
		name := "Test Token"
		symbol := "TEST"
		expectedErr := errors.New("redis error")

		mockRedis.On("HSet", ctx, contractID, contractNameKey, name, defaultExpiration).Return(expectedErr).Once()

		err := store.Set(ctx, contractID, name, symbol)
		assert.Error(t, err)
		assert.Equal(t, expectedErr, err)

		mockRedis.AssertExpectations(t)
	})

	t.Run("error_setting_symbol", func(t *testing.T) {
		mockRedis := new(MockRedisClient)
		store := NewContractStore(mockRedis)

		contractID := "CONTRACT123"
		name := "Test Token"
		symbol := "TEST"
		expectedErr := errors.New("redis error")

		mockRedis.On("HSet", ctx, contractID, contractNameKey, name, defaultExpiration).Return(nil).Once()
		mockRedis.On("HSet", ctx, contractID, contractSymbolKey, symbol, defaultExpiration).Return(expectedErr).Once()

		err := store.Set(ctx, contractID, name, symbol)
		assert.Error(t, err)
		assert.Equal(t, expectedErr, err)

		mockRedis.AssertExpectations(t)
	})
}

func TestContractStore_Name(t *testing.T) {
	ctx := context.Background()

	t.Run("success", func(t *testing.T) {
		mockRedis := new(MockRedisClient)
		store := NewContractStore(mockRedis)

		contractID := "CONTRACT123"
		expectedName := "Test Token"

		mockRedis.On("HGet", ctx, contractID, contractNameKey).Return(expectedName, nil).Once()

		name, err := store.Name(ctx, contractID)
		require.NoError(t, err)
		assert.Equal(t, expectedName, name)

		mockRedis.AssertExpectations(t)
	})

	t.Run("error", func(t *testing.T) {
		mockRedis := new(MockRedisClient)
		store := NewContractStore(mockRedis)

		contractID := "CONTRACT123"
		expectedErr := errors.New("redis error")

		mockRedis.On("HGet", ctx, contractID, contractNameKey).Return("", expectedErr).Once()

		name, err := store.Name(ctx, contractID)
		assert.Error(t, err)
		assert.Equal(t, expectedErr, err)
		assert.Empty(t, name)

		mockRedis.AssertExpectations(t)
	})
}

func TestContractStore_Symbol(t *testing.T) {
	ctx := context.Background()

	t.Run("success", func(t *testing.T) {
		mockRedis := new(MockRedisClient)
		store := NewContractStore(mockRedis)

		contractID := "CONTRACT123"
		expectedSymbol := "TEST"

		mockRedis.On("HGet", ctx, contractID, contractSymbolKey).Return(expectedSymbol, nil).Once()

		symbol, err := store.Symbol(ctx, contractID)
		require.NoError(t, err)
		assert.Equal(t, expectedSymbol, symbol)

		mockRedis.AssertExpectations(t)
	})

	t.Run("error", func(t *testing.T) {
		mockRedis := new(MockRedisClient)
		store := NewContractStore(mockRedis)

		contractID := "CONTRACT123"
		expectedErr := errors.New("redis error")

		mockRedis.On("HGet", ctx, contractID, contractSymbolKey).Return("", expectedErr).Once()

		symbol, err := store.Symbol(ctx, contractID)
		assert.Error(t, err)
		assert.Equal(t, expectedErr, err)
		assert.Empty(t, symbol)

		mockRedis.AssertExpectations(t)
	})
}

func TestContractStore_Exists(t *testing.T) {
	ctx := context.Background()

	t.Run("exists", func(t *testing.T) {
		mockRedis := new(MockRedisClient)
		store := NewContractStore(mockRedis)

		contractID := "CONTRACT123"

		mockRedis.On("Exists", ctx, []string{contractID}).Return(int64(1), nil).Once()

		exists, err := store.Exists(ctx, contractID)
		require.NoError(t, err)
		assert.True(t, exists)

		mockRedis.AssertExpectations(t)
	})

	t.Run("does_not_exist", func(t *testing.T) {
		mockRedis := new(MockRedisClient)
		store := NewContractStore(mockRedis)

		contractID := "CONTRACT123"

		mockRedis.On("Exists", ctx, []string{contractID}).Return(int64(0), nil).Once()

		exists, err := store.Exists(ctx, contractID)
		require.NoError(t, err)
		assert.False(t, exists)

		mockRedis.AssertExpectations(t)
	})

	t.Run("error", func(t *testing.T) {
		mockRedis := new(MockRedisClient)
		store := NewContractStore(mockRedis)

		contractID := "CONTRACT123"
		expectedErr := errors.New("redis error")

		mockRedis.On("Exists", ctx, []string{contractID}).Return(int64(0), expectedErr).Once()

		exists, err := store.Exists(ctx, contractID)
		assert.Error(t, err)
		assert.Equal(t, expectedErr, err)
		assert.False(t, exists)

		mockRedis.AssertExpectations(t)
	})
}
