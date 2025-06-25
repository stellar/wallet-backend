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

func TestAccountModelInsert(t *testing.T) {
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

func TestAccountModelGetAll(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()
	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	ctx := context.Background()

	mockMetricsService := metrics.NewMockMetricsService()
	mockMetricsService.On("ObserveDBQueryDuration", "SELECT", "accounts", mock.Anything).Twice()
	mockMetricsService.On("IncDBQuery", "SELECT", "accounts").Twice()
	defer mockMetricsService.AssertExpectations(t)

	m := &AccountModel{
		DB:             dbConnectionPool,
		MetricsService: mockMetricsService,
	}

	// Intitially, there are no accounts in the database.
	addresses, err := m.GetAll(ctx)
	require.NoError(t, err)
	require.Empty(t, addresses)

	// Insert three accounts into the database.
	address1 := keypair.MustRandom().Address()
	address2 := keypair.MustRandom().Address()
	address3 := keypair.MustRandom().Address()
	_, err = m.DB.ExecContext(ctx, "INSERT INTO accounts (stellar_address) VALUES ($1), ($2), ($3)", address1, address2, address3)
	require.NoError(t, err)

	// Get all accounts from the database.
	addresses, err = m.GetAll(ctx)
	require.NoError(t, err)
	require.ElementsMatch(t, []string{address1, address2, address3}, addresses)
}

func TestAccountModelDelete(t *testing.T) {
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

func TestAccountModelIsAccountFeeBumpEligible(t *testing.T) {
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
