package services

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	"github.com/stellar/go/keypair"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/stellar/wallet-backend/internal/data"
	"github.com/stellar/wallet-backend/internal/db"
	"github.com/stellar/wallet-backend/internal/db/dbtest"
	"github.com/stellar/wallet-backend/internal/metrics"
)

func TestAccountRegister(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()

	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	t.Run("successful registration", func(t *testing.T) {
		mockMetricsService := metrics.NewMockMetricsService()
		mockMetricsService.On("IncActiveAccount").Return().Once()
		mockMetricsService.On("ObserveDBQueryDuration", "INSERT", "accounts", mock.Anything).Return().Once()
		mockMetricsService.On("IncDBQuery", "INSERT", "accounts").Return().Once()
		defer mockMetricsService.AssertExpectations(t)

		models, err := data.NewModels(dbConnectionPool, mockMetricsService)
		require.NoError(t, err)
		accountService, err := NewAccountService(models, mockMetricsService)
		require.NoError(t, err)

		ctx := context.Background()
		address := keypair.MustRandom().Address()
		err = accountService.RegisterAccount(ctx, address)
		require.NoError(t, err)

		var dbAddress sql.NullString
		err = dbConnectionPool.GetContext(ctx, &dbAddress, "SELECT stellar_address FROM accounts WHERE stellar_address = $1", address)
		require.NoError(t, err)

		assert.True(t, dbAddress.Valid)
		assert.Equal(t, address, dbAddress.String)
	})

	t.Run("duplicate registration fails", func(t *testing.T) {
		mockMetricsService := metrics.NewMockMetricsService()
		// First registration succeeds
		mockMetricsService.On("IncActiveAccount").Return().Once()
		mockMetricsService.On("ObserveDBQueryDuration", "INSERT", "accounts", mock.Anything).Return().Times(2)
		mockMetricsService.On("IncDBQuery", "INSERT", "accounts").Return().Times(1)
		mockMetricsService.On("IncDBQueryError", "INSERT", "accounts", mock.Anything).Return().Times(1)
		defer mockMetricsService.AssertExpectations(t)

		models, err := data.NewModels(dbConnectionPool, mockMetricsService)
		require.NoError(t, err)
		accountService, err := NewAccountService(models, mockMetricsService)
		require.NoError(t, err)

		ctx := context.Background()
		address := keypair.MustRandom().Address()

		// First registration should succeed
		err = accountService.RegisterAccount(ctx, address)
		require.NoError(t, err)

		// Second registration should fail
		err = accountService.RegisterAccount(ctx, address)
		require.Error(t, err)
		assert.True(t, errors.Is(err, data.ErrAccountAlreadyExists))
	})

	t.Run("invalid address fails", func(t *testing.T) {
		mockMetricsService := metrics.NewMockMetricsService()
		defer mockMetricsService.AssertExpectations(t)

		models, err := data.NewModels(dbConnectionPool, mockMetricsService)
		require.NoError(t, err)
		accountService, err := NewAccountService(models, mockMetricsService)
		require.NoError(t, err)

		ctx := context.Background()

		// Test with invalid address
		err = accountService.RegisterAccount(ctx, "invalid-address")
		require.Error(t, err)
		assert.True(t, errors.Is(err, ErrInvalidAddress))

		// Test with empty address
		err = accountService.RegisterAccount(ctx, "")
		require.Error(t, err)
		assert.True(t, errors.Is(err, ErrInvalidAddress))
	})
}

func TestAccountDeregister(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()

	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	t.Run("successful deregistration", func(t *testing.T) {
		mockMetricsService := metrics.NewMockMetricsService()
		mockMetricsService.On("DecActiveAccount").Return().Once()
		mockMetricsService.On("ObserveDBQueryDuration", "DELETE", "accounts", mock.Anything).Return().Once()
		mockMetricsService.On("IncDBQuery", "DELETE", "accounts").Return().Once()
		defer mockMetricsService.AssertExpectations(t)

		models, err := data.NewModels(dbConnectionPool, mockMetricsService)
		require.NoError(t, err)
		accountService, err := NewAccountService(models, mockMetricsService)
		require.NoError(t, err)

		ctx := context.Background()
		address := keypair.MustRandom().Address()
		result, err := dbConnectionPool.ExecContext(ctx, "INSERT INTO accounts (stellar_address) VALUES ($1)", address)
		require.NoError(t, err)
		rowAffected, err := result.RowsAffected()
		require.NoError(t, err)
		require.Equal(t, int64(1), rowAffected)

		err = accountService.DeregisterAccount(ctx, address)
		require.NoError(t, err)

		var dbAddress sql.NullString
		err = dbConnectionPool.GetContext(ctx, &dbAddress, "SELECT stellar_address FROM accounts LIMIT 1")
		assert.ErrorIs(t, err, sql.ErrNoRows)
	})

	t.Run("deregister non-existent account fails", func(t *testing.T) {
		mockMetricsService := metrics.NewMockMetricsService()
		mockMetricsService.On("ObserveDBQueryDuration", "DELETE", "accounts", mock.Anything).Return().Once()
		defer mockMetricsService.AssertExpectations(t)

		models, err := data.NewModels(dbConnectionPool, mockMetricsService)
		require.NoError(t, err)
		accountService, err := NewAccountService(models, mockMetricsService)
		require.NoError(t, err)

		ctx := context.Background()
		nonExistentAddress := keypair.MustRandom().Address()

		err = accountService.DeregisterAccount(ctx, nonExistentAddress)
		require.Error(t, err)
		assert.True(t, errors.Is(err, data.ErrAccountNotFound))
	})
}
