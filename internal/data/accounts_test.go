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

	t.Run("successful insert", func(t *testing.T) {
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
		err = m.DB.GetContext(ctx, &dbAddress, "SELECT stellar_address FROM accounts WHERE stellar_address = $1", address)
		require.NoError(t, err)

		assert.True(t, dbAddress.Valid)
		assert.Equal(t, address, dbAddress.String)
	})

	t.Run("duplicate insert fails", func(t *testing.T) {
		mockMetricsService := metrics.NewMockMetricsService()
		mockMetricsService.On("ObserveDBQueryDuration", "INSERT", "accounts", mock.Anything).Return().Times(2)
		mockMetricsService.On("IncDBQuery", "INSERT", "accounts").Return().Times(2)
		defer mockMetricsService.AssertExpectations(t)

		m := &AccountModel{
			DB:             dbConnectionPool,
			MetricsService: mockMetricsService,
		}

		ctx := context.Background()
		address := keypair.MustRandom().Address()
		
		// First insert should succeed
		err = m.Insert(ctx, address)
		require.NoError(t, err)

		// Second insert should fail
		err = m.Insert(ctx, address)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "already registered")
	})
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

func TestAccountModelGet(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()
	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	mockMetricsService := metrics.NewMockMetricsService()
	mockMetricsService.On("ObserveDBQueryDuration", "SELECT", "accounts", mock.Anything).Return()
	mockMetricsService.On("IncDBQuery", "SELECT", "accounts").Return()
	defer mockMetricsService.AssertExpectations(t)

	m := &AccountModel{
		DB:             dbConnectionPool,
		MetricsService: mockMetricsService,
	}

	ctx := context.Background()
	address := keypair.MustRandom().Address()

	// Insert test account
	result, err := m.DB.ExecContext(ctx, "INSERT INTO accounts (stellar_address) VALUES ($1)", address)
	require.NoError(t, err)
	rowAffected, err := result.RowsAffected()
	require.NoError(t, err)
	require.Equal(t, int64(1), rowAffected)

	// Test Get function
	account, err := m.Get(ctx, address)
	require.NoError(t, err)
	assert.Equal(t, address, account.StellarAddress)
}

func TestAccountModelBatchGetByTxHashes(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()
	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	mockMetricsService := metrics.NewMockMetricsService()
	mockMetricsService.On("ObserveDBQueryDuration", "SELECT", "accounts", mock.Anything).Return()
	mockMetricsService.On("IncDBQuery", "SELECT", "accounts").Return()
	defer mockMetricsService.AssertExpectations(t)

	m := &AccountModel{
		DB:             dbConnectionPool,
		MetricsService: mockMetricsService,
	}

	ctx := context.Background()
	address1 := keypair.MustRandom().Address()
	address2 := keypair.MustRandom().Address()
	txHash1 := "tx1"
	txHash2 := "tx2"

	// Insert test accounts
	_, err = m.DB.ExecContext(ctx, "INSERT INTO accounts (stellar_address) VALUES ($1), ($2)", address1, address2)
	require.NoError(t, err)

	// Insert test transactions first
	_, err = m.DB.ExecContext(ctx, "INSERT INTO transactions (hash, to_id, envelope_xdr, result_xdr, meta_xdr, ledger_number, ledger_created_at) VALUES ($1, 1, 'env1', 'res1', 'meta1', 1, NOW()), ($2, 2, 'env2', 'res2', 'meta2', 2, NOW())", txHash1, txHash2)
	require.NoError(t, err)

	// Insert test transactions_accounts links
	_, err = m.DB.ExecContext(ctx, "INSERT INTO transactions_accounts (tx_hash, account_id) VALUES ($1, $2), ($3, $4)", txHash1, address1, txHash2, address2)
	require.NoError(t, err)

	// Test BatchGetByTxHash function
	accounts, err := m.BatchGetByTxHashes(ctx, []string{txHash1, txHash2})
	require.NoError(t, err)
	assert.Len(t, accounts, 2)

	// Verify accounts are returned with correct tx_hash
	addressSet := make(map[string]string)
	for _, acc := range accounts {
		addressSet[acc.StellarAddress] = acc.TxHash
	}
	assert.Equal(t, txHash1, addressSet[address1])
	assert.Equal(t, txHash2, addressSet[address2])
}

func TestAccountModelBatchGetByOperationIDs(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()
	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	mockMetricsService := metrics.NewMockMetricsService()
	mockMetricsService.On("ObserveDBQueryDuration", "SELECT", "accounts", mock.Anything).Return()
	mockMetricsService.On("IncDBQuery", "SELECT", "accounts").Return()
	defer mockMetricsService.AssertExpectations(t)

	m := &AccountModel{
		DB:             dbConnectionPool,
		MetricsService: mockMetricsService,
	}

	ctx := context.Background()
	address1 := keypair.MustRandom().Address()
	address2 := keypair.MustRandom().Address()
	operationID1 := int64(123)
	operationID2 := int64(456)

	// Insert test accounts
	_, err = m.DB.ExecContext(ctx, "INSERT INTO accounts (stellar_address) VALUES ($1), ($2)", address1, address2)
	require.NoError(t, err)

	// Insert test transactions first
	_, err = m.DB.ExecContext(ctx, "INSERT INTO transactions (hash, to_id, envelope_xdr, result_xdr, meta_xdr, ledger_number, ledger_created_at) VALUES ('tx1', 1, 'env1', 'res1', 'meta1', 1, NOW()), ('tx2', 2, 'env2', 'res2', 'meta2', 2, NOW())")
	require.NoError(t, err)

	// Insert test operations first
	_, err = m.DB.ExecContext(ctx, "INSERT INTO operations (id, tx_hash, operation_type, operation_xdr, ledger_number, ledger_created_at) VALUES ($1, 'tx1', 'payment', 'xdr1', 1, NOW()), ($2, 'tx2', 'payment', 'xdr2', 2	, NOW())", operationID1, operationID2)
	require.NoError(t, err)

	// Insert test operations_accounts links
	_, err = m.DB.ExecContext(ctx, "INSERT INTO operations_accounts (operation_id, account_id) VALUES ($1, $2), ($3, $4)", operationID1, address1, operationID2, address2)
	require.NoError(t, err)

	// Test BatchGetByOperationID function
	accounts, err := m.BatchGetByOperationIDs(ctx, []int64{operationID1, operationID2})
	require.NoError(t, err)
	assert.Len(t, accounts, 2)

	// Verify accounts are returned with correct operation_id
	addressSet := make(map[string]int64)
	for _, acc := range accounts {
		addressSet[acc.StellarAddress] = acc.OperationID
	}
	assert.Equal(t, operationID1, addressSet[address1])
	assert.Equal(t, operationID2, addressSet[address2])
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
