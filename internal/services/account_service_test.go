package services

import (
	"context"
	"database/sql"
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

// MockAccountsStore is a mock implementation of AccountsStore
type MockAccountsStore struct {
	mock.Mock
}

func (m *MockAccountsStore) Add(accountID string) {
	m.Called(accountID)
}

func (m *MockAccountsStore) Remove(accountID string) {
	m.Called(accountID)
}

func (m *MockAccountsStore) Exists(ctx context.Context, accountID string) bool {
	args := m.Called(ctx, accountID)
	return args.Bool(0)
}

func TestAccountRegister(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()

	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()
	mockMetricsService := metrics.NewMockMetricsService()
	mockMetricsService.On("IncActiveAccount").Return().Once()
	mockMetricsService.On("ObserveDBQueryDuration", "INSERT", "accounts", mock.Anything).Return().Once()
	mockMetricsService.On("IncDBQuery", "INSERT", "accounts").Return().Once()
	defer mockMetricsService.AssertExpectations(t)

	models, err := data.NewModels(dbConnectionPool, mockMetricsService)
	require.NoError(t, err)

	// Create mock accounts store
	mockAccountsStore := &MockAccountsStore{}
	mockAccountsStore.On("Add", mock.AnythingOfType("string")).Return()

	accountService, err := NewAccountService(models, mockAccountsStore, mockMetricsService)
	require.NoError(t, err)

	ctx := context.Background()
	address := keypair.MustRandom().Address()
	err = accountService.RegisterAccount(ctx, address)
	require.NoError(t, err)

	var dbAddress sql.NullString
	err = dbConnectionPool.GetContext(ctx, &dbAddress, "SELECT stellar_address FROM accounts LIMIT 1")
	require.NoError(t, err)

	assert.True(t, dbAddress.Valid)
	assert.Equal(t, address, dbAddress.String)
}

func TestAccountDeregister(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()

	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()
	mockMetricsService := metrics.NewMockMetricsService()
	mockMetricsService.On("DecActiveAccount").Return().Once()
	mockMetricsService.On("ObserveDBQueryDuration", "DELETE", "accounts", mock.Anything).Return().Once()
	mockMetricsService.On("IncDBQuery", "DELETE", "accounts").Return().Once()
	defer mockMetricsService.AssertExpectations(t)

	models, err := data.NewModels(dbConnectionPool, mockMetricsService)
	require.NoError(t, err)

	// Create mock accounts store
	mockAccountsStore := &MockAccountsStore{}
	mockAccountsStore.On("Remove", mock.AnythingOfType("string")).Return()

	accountService, err := NewAccountService(models, mockAccountsStore, mockMetricsService)
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
}
