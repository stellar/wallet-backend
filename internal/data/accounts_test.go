package data

import (
	"context"
	"database/sql"
	"testing"

	"github.com/stellar/go/keypair"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/stellar/wallet-backend/internal/db"
	"github.com/stellar/wallet-backend/internal/db/dbtest"
	"github.com/stellar/wallet-backend/internal/metrics"
)

func TestAccountModel_BatchGetByIDs(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()
	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	mockMetricsService := metrics.NewMockMetricsService()
	defer mockMetricsService.AssertExpectations(t)

	accountModel := &AccountModel{
		DB:             dbConnectionPool,
		MetricsService: mockMetricsService,
	}

	ctx := context.Background()

	t.Run("empty input returns empty result", func(t *testing.T) {
		result, err := accountModel.BatchGetByIDs(ctx, []string{})
		require.NoError(t, err)
		assert.Empty(t, result)
	})

	t.Run("returns existing accounts only", func(t *testing.T) {
		// Insert some test accounts
		_, err := dbConnectionPool.ExecContext(ctx, "INSERT INTO accounts (stellar_address) VALUES ($1), ($2)", "account1", "account2")
		require.NoError(t, err)

		// Test with mix of existing and non-existing accounts
		mockMetricsService.On("ObserveDBQueryDuration", "SELECT", "accounts", mock.Anything).Return().Once()
		mockMetricsService.On("IncDBQuery", "SELECT", "accounts").Return().Once()

		result, err := accountModel.BatchGetByIDs(ctx, []string{"account1", "nonexistent", "account2", "another_nonexistent"})
		require.NoError(t, err)

		// Should only return the existing accounts
		assert.Len(t, result, 2)
		assert.Contains(t, result, "account1")
		assert.Contains(t, result, "account2")
		assert.NotContains(t, result, "nonexistent")
		assert.NotContains(t, result, "another_nonexistent")
	})

	t.Run("returns empty when no accounts exist", func(t *testing.T) {
		// Clean up the table
		_, err := dbConnectionPool.ExecContext(ctx, "DELETE FROM accounts")
		require.NoError(t, err)

		mockMetricsService.On("ObserveDBQueryDuration", "SELECT", "accounts", mock.Anything).Return().Once()
		mockMetricsService.On("IncDBQuery", "SELECT", "accounts").Return().Once()

		result, err := accountModel.BatchGetByIDs(ctx, []string{"nonexistent1", "nonexistent2"})
		require.NoError(t, err)
		assert.Empty(t, result)
	})
}

func TestAccountModel_Insert(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()
	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	mockMetricsService := metrics.NewMockMetricsService()
	mockMetricsService.On("ObserveDBQueryDuration", "INSERT", "accounts", mock.Anything).Return()
	mockMetricsService.On("IncDBQuery", "INSERT", "accounts").Return()
	defer mockMetricsService.AssertExpectations(t)

	m := &AccountModel{
		DB:             dbConnectionPool,
		MetricsService: mockMetricsService,
	}

	ctx := context.Background()
	address := keypair.MustRandom().Address()
	err = m.Insert(ctx, address)
	require.NoError(t, err)

	var dbAddress sql.NullString
	err = m.DB.GetContext(ctx, &dbAddress, "SELECT stellar_address FROM accounts LIMIT 1")
	require.NoError(t, err)

	assert.True(t, dbAddress.Valid)
	assert.Equal(t, address, dbAddress.String)
}

func TestAccountModel_Delete(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()
	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	mockMetricsService := metrics.NewMockMetricsService()
	mockMetricsService.On("ObserveDBQueryDuration", "DELETE", "accounts", mock.Anything).Return()
	mockMetricsService.On("IncDBQuery", "DELETE", "accounts").Return()
	defer mockMetricsService.AssertExpectations(t)

	m := &AccountModel{
		DB:             dbConnectionPool,
		MetricsService: mockMetricsService,
	}

	ctx := context.Background()
	address := keypair.MustRandom().Address()
	result, err := m.DB.ExecContext(ctx, "INSERT INTO accounts (stellar_address) VALUES ($1)", address)
	require.NoError(t, err)
	rowAffected, err := result.RowsAffected()
	require.NoError(t, err)
	require.Equal(t, int64(1), rowAffected)

	err = m.Delete(ctx, address)
	require.NoError(t, err)

	var dbAddress sql.NullString
	err = m.DB.GetContext(ctx, &dbAddress, "SELECT stellar_address FROM accounts LIMIT 1")
	assert.ErrorIs(t, err, sql.ErrNoRows)
}

func TestAccountModel_IsAccountFeeBumpEligible(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()
	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	mockMetricsService := metrics.NewMockMetricsService()
	mockMetricsService.On("IncDBQuery", "SELECT", "accounts").Return()
	mockMetricsService.On("ObserveDBQueryDuration", "SELECT", "accounts", mock.Anything).Return()
	defer mockMetricsService.AssertExpectations(t)

	m := &AccountModel{
		DB:             dbConnectionPool,
		MetricsService: mockMetricsService,
	}

	ctx := context.Background()
	address := keypair.MustRandom().Address()

	isFeeBumpEligible, err := m.IsAccountFeeBumpEligible(ctx, address)
	require.NoError(t, err)
	assert.False(t, isFeeBumpEligible)

	result, err := m.DB.ExecContext(ctx, "INSERT INTO accounts (stellar_address) VALUES ($1)", address)
	require.NoError(t, err)
	rowAffected, err := result.RowsAffected()
	require.NoError(t, err)
	require.Equal(t, int64(1), rowAffected)

	isFeeBumpEligible, err = m.IsAccountFeeBumpEligible(ctx, address)
	require.NoError(t, err)
	assert.True(t, isFeeBumpEligible)
}
