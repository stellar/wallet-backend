package data

import (
	"context"
	"database/sql"
	"testing"

	"github.com/lib/pq"
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

func Test_AccountModel_GetExisting(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()
	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	ctx := context.Background()
	existingAddress1 := keypair.MustRandom().Address()
	existingAddress2 := keypair.MustRandom().Address()
	existingAddresses := []string{existingAddress1, existingAddress2}
	nonExistingAddress := keypair.MustRandom().Address()

	// Insert addresses to test against
	_, err = dbConnectionPool.ExecContext(ctx, "INSERT INTO accounts (stellar_address) SELECT unnest($1::text[])", pq.Array(existingAddresses))
	require.NoError(t, err)

	testCases := []struct {
		name            string
		inputValues     []string
		useDBTx         bool
		wantResults     []string
		wantErrContains string
	}{
		{
			name:            "🟢all_addresses_found",
			inputValues:     existingAddresses,
			useDBTx:         false,
			wantResults:     existingAddresses,
			wantErrContains: "",
		},
		{
			name:            "🟢all_addresses_found_with_db_transaction",
			inputValues:     existingAddresses,
			useDBTx:         true,
			wantResults:     existingAddresses,
			wantErrContains: "",
		},
		{
			name:            "🟢no_addresses_found",
			inputValues:     []string{nonExistingAddress},
			useDBTx:         false,
			wantResults:     nil,
			wantErrContains: "",
		},
		{
			name:            "🟢mixed_addresses",
			inputValues:     []string{existingAddress1, existingAddress2, nonExistingAddress},
			useDBTx:         false,
			wantResults:     []string{existingAddress1, existingAddress2},
			wantErrContains: "",
		},
		{
			name:            "🟢empty_input",
			inputValues:     []string{},
			useDBTx:         false,
			wantResults:     nil,
			wantErrContains: "",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Create fresh mock for each test case
			mockMetricsService := metrics.NewMockMetricsService()
			mockMetricsService.
				On("ObserveDBQueryDuration", "SELECT", "accounts", mock.Anything).Return().
				On("IncDBQuery", "SELECT", "accounts").Return()
			defer mockMetricsService.AssertExpectations(t)

			m := &AccountModel{
				DB:             dbConnectionPool,
				MetricsService: mockMetricsService,
			}

			var dbTx db.Transaction
			if tc.useDBTx {
				dbTx, err = dbConnectionPool.BeginTxx(ctx, nil)
				require.NoError(t, err)
				defer dbTx.Rollback()
			}

			got, err := m.GetExisting(ctx, dbTx, tc.inputValues)

			if tc.wantErrContains != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tc.wantErrContains)
				assert.Nil(t, got)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tc.wantResults, got)
			}
		})
	}
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
