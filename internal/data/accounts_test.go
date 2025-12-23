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
	"github.com/stellar/wallet-backend/internal/indexer/types"
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
		// Insert some test accounts with valid addresses
		addr1 := keypair.MustRandom().Address()
		addr2 := keypair.MustRandom().Address()
		nonExistent1 := keypair.MustRandom().Address()
		nonExistent2 := keypair.MustRandom().Address()

		_, err := dbConnectionPool.ExecContext(ctx, "INSERT INTO accounts (stellar_address) VALUES ($1), ($2)",
			types.StellarAddress(addr1), types.StellarAddress(addr2))
		require.NoError(t, err)

		// Test with mix of existing and non-existing accounts
		mockMetricsService.On("ObserveDBQueryDuration", "BatchGetByIDs", "accounts", mock.Anything).Return().Once()
		mockMetricsService.On("IncDBQuery", "BatchGetByIDs", "accounts").Return().Once()
		mockMetricsService.On("ObserveDBBatchSize", "BatchGetByIDs", "accounts", mock.Anything).Return().Once()

		result, err := accountModel.BatchGetByIDs(ctx, []string{addr1, nonExistent1, addr2, nonExistent2})
		require.NoError(t, err)

		// Should only return the existing accounts
		assert.Len(t, result, 2)
		assert.Contains(t, result, addr1)
		assert.Contains(t, result, addr2)
		assert.NotContains(t, result, nonExistent1)
		assert.NotContains(t, result, nonExistent2)
	})

	t.Run("returns empty when no accounts exist", func(t *testing.T) {
		// Clean up the table
		_, err := dbConnectionPool.ExecContext(ctx, "DELETE FROM accounts")
		require.NoError(t, err)

		mockMetricsService.On("ObserveDBQueryDuration", "BatchGetByIDs", "accounts", mock.Anything).Return().Once()
		mockMetricsService.On("IncDBQuery", "BatchGetByIDs", "accounts").Return().Once()
		mockMetricsService.On("ObserveDBBatchSize", "BatchGetByIDs", "accounts", mock.Anything).Return().Once()

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

	t.Run("successful insert", func(t *testing.T) {
		mockMetricsService := metrics.NewMockMetricsService()
		mockMetricsService.On("ObserveDBQueryDuration", "Insert", "accounts", mock.Anything).Return()
		mockMetricsService.On("IncDBQuery", "Insert", "accounts").Return()
		defer mockMetricsService.AssertExpectations(t)

		m := &AccountModel{
			DB:             dbConnectionPool,
			MetricsService: mockMetricsService,
		}

		ctx := context.Background()
		address := keypair.MustRandom().Address()
		err = m.Insert(ctx, address)
		require.NoError(t, err)

		var dbAddress types.StellarAddress
		err = m.DB.GetContext(ctx, &dbAddress, "SELECT stellar_address FROM accounts WHERE stellar_address = $1", types.StellarAddress(address))
		require.NoError(t, err)

		assert.Equal(t, address, string(dbAddress))
	})

	t.Run("duplicate insert fails", func(t *testing.T) {
		mockMetricsService := metrics.NewMockMetricsService()
		mockMetricsService.On("ObserveDBQueryDuration", "Insert", "accounts", mock.Anything).Return().Times(2)
		mockMetricsService.On("IncDBQuery", "Insert", "accounts").Return().Times(1)
		mockMetricsService.On("IncDBQueryError", "Insert", "accounts", mock.Anything).Return().Times(1)
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
		assert.ErrorIs(t, err, ErrAccountAlreadyExists)
	})
}

func TestAccountModel_Delete(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()
	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	t.Run("successful deletion", func(t *testing.T) {
		mockMetricsService := metrics.NewMockMetricsService()
		mockMetricsService.On("ObserveDBQueryDuration", "Delete", "accounts", mock.Anything).Return()
		mockMetricsService.On("IncDBQuery", "Delete", "accounts").Return()
		defer mockMetricsService.AssertExpectations(t)

		m := &AccountModel{
			DB:             dbConnectionPool,
			MetricsService: mockMetricsService,
		}

		ctx := context.Background()
		address := keypair.MustRandom().Address()
		result, insertErr := m.DB.ExecContext(ctx, "INSERT INTO accounts (stellar_address) VALUES ($1)", types.StellarAddress(address))
		require.NoError(t, insertErr)
		rowAffected, rowErr := result.RowsAffected()
		require.NoError(t, rowErr)
		require.Equal(t, int64(1), rowAffected)

		delErr := m.Delete(ctx, address)
		require.NoError(t, delErr)

		var dbAddress types.StellarAddress
		getErr := m.DB.GetContext(ctx, &dbAddress, "SELECT stellar_address FROM accounts LIMIT 1")
		assert.ErrorIs(t, getErr, sql.ErrNoRows)
	})

	t.Run("delete non-existent account fails", func(t *testing.T) {
		mockMetricsService := metrics.NewMockMetricsService()
		mockMetricsService.On("ObserveDBQueryDuration", "Delete", "accounts", mock.Anything).Return()
		defer mockMetricsService.AssertExpectations(t)

		m := &AccountModel{
			DB:             dbConnectionPool,
			MetricsService: mockMetricsService,
		}

		ctx := context.Background()
		nonExistentAddress := keypair.MustRandom().Address()

		err = m.Delete(ctx, nonExistentAddress)
		require.Error(t, err)
		assert.ErrorIs(t, err, ErrAccountNotFound)
	})
}

func TestAccountModelGet(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()
	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	mockMetricsService := metrics.NewMockMetricsService()
	mockMetricsService.On("ObserveDBQueryDuration", "Get", "accounts", mock.Anything).Return()
	mockMetricsService.On("IncDBQuery", "Get", "accounts").Return()
	defer mockMetricsService.AssertExpectations(t)

	m := &AccountModel{
		DB:             dbConnectionPool,
		MetricsService: mockMetricsService,
	}

	ctx := context.Background()
	address := keypair.MustRandom().Address()

	// Insert test account
	result, err := m.DB.ExecContext(ctx, "INSERT INTO accounts (stellar_address) VALUES ($1)", types.StellarAddress(address))
	require.NoError(t, err)
	rowAffected, err := result.RowsAffected()
	require.NoError(t, err)
	require.Equal(t, int64(1), rowAffected)

	// Test Get function
	account, err := m.Get(ctx, address)
	require.NoError(t, err)
	assert.Equal(t, address, string(account.StellarAddress))
}

func TestAccountModelBatchGetByTxHashes(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()
	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	mockMetricsService := metrics.NewMockMetricsService()
	mockMetricsService.On("ObserveDBQueryDuration", "BatchGetByTxHashes", "accounts", mock.Anything).Return()
	mockMetricsService.On("IncDBQuery", "BatchGetByTxHashes", "accounts").Return()
	mockMetricsService.On("ObserveDBBatchSize", "BatchGetByTxHashes", "accounts", mock.Anything).Return().Maybe()
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
	_, err = m.DB.ExecContext(ctx, "INSERT INTO accounts (stellar_address) VALUES ($1), ($2)", types.StellarAddress(address1), types.StellarAddress(address2))
	require.NoError(t, err)

	// Insert test transactions first (to_id encodes ledger via TOID)
	_, err = m.DB.ExecContext(ctx, "INSERT INTO transactions (hash, to_id, envelope_xdr, result_xdr, meta_xdr, ledger_created_at) VALUES ($1, 1, 'env1', 'res1', 'meta1', NOW()), ($2, 2, 'env2', 'res2', 'meta2', NOW())", txHash1, txHash2)
	require.NoError(t, err)

	// Insert test transactions_accounts links (uses tx_id which references to_id)
	_, err = m.DB.ExecContext(ctx, "INSERT INTO transactions_accounts (tx_id, account_id, ledger_created_at) VALUES (1, $1, NOW()), (2, $2, NOW())", types.StellarAddress(address1), types.StellarAddress(address2))
	require.NoError(t, err)

	// Test BatchGetByTxHash function
	accounts, err := m.BatchGetByTxHashes(ctx, []string{txHash1, txHash2}, "")
	require.NoError(t, err)
	assert.Len(t, accounts, 2)

	// Verify accounts are returned with correct tx_hash
	addressSet := make(map[string]string)
	for _, acc := range accounts {
		addressSet[string(acc.StellarAddress)] = acc.TxHash
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
	mockMetricsService.On("ObserveDBQueryDuration", "BatchGetByOperationIDs", "accounts", mock.Anything).Return()
	mockMetricsService.On("IncDBQuery", "BatchGetByOperationIDs", "accounts").Return()
	mockMetricsService.On("ObserveDBBatchSize", "BatchGetByOperationIDs", "accounts", mock.Anything).Return().Maybe()
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
	_, err = m.DB.ExecContext(ctx, "INSERT INTO accounts (stellar_address) VALUES ($1), ($2)", types.StellarAddress(address1), types.StellarAddress(address2))
	require.NoError(t, err)

	// Insert test transactions first (to_id encodes ledger via TOID)
	_, err = m.DB.ExecContext(ctx, "INSERT INTO transactions (hash, to_id, envelope_xdr, result_xdr, meta_xdr, ledger_created_at) VALUES ('tx1', 1, 'env1', 'res1', 'meta1', NOW()), ('tx2', 2, 'env2', 'res2', 'meta2', NOW())")
	require.NoError(t, err)

	// Insert test operations first (operation_type is SMALLINT: 2=PAYMENT)
	_, err = m.DB.ExecContext(ctx, "INSERT INTO operations (id, operation_type, operation_xdr, ledger_created_at) VALUES ($1, 2, 'xdr1', NOW()), ($2, 2, 'xdr2', NOW())", operationID1, operationID2)
	require.NoError(t, err)

	// Insert test operations_accounts links
	_, err = m.DB.ExecContext(ctx, "INSERT INTO operations_accounts (operation_id, account_id, ledger_created_at) VALUES ($1, $2, NOW()), ($3, $4, NOW())", operationID1, types.StellarAddress(address1), operationID2, types.StellarAddress(address2))
	require.NoError(t, err)

	// Test BatchGetByOperationID function
	accounts, err := m.BatchGetByOperationIDs(ctx, []int64{operationID1, operationID2}, "")
	require.NoError(t, err)
	assert.Len(t, accounts, 2)

	// Verify accounts are returned with correct operation_id
	addressSet := make(map[string]int64)
	for _, acc := range accounts {
		addressSet[string(acc.StellarAddress)] = acc.OperationID
	}
	assert.Equal(t, operationID1, addressSet[address1])
	assert.Equal(t, operationID2, addressSet[address2])
}

func TestAccountModel_IsAccountFeeBumpEligible(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()
	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	mockMetricsService := metrics.NewMockMetricsService()
	mockMetricsService.On("IncDBQuery", "IsAccountFeeBumpEligible", "accounts").Return()
	mockMetricsService.On("ObserveDBQueryDuration", "IsAccountFeeBumpEligible", "accounts", mock.Anything).Return()
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

	result, err := m.DB.ExecContext(ctx, "INSERT INTO accounts (stellar_address) VALUES ($1)", types.StellarAddress(address))
	require.NoError(t, err)
	rowAffected, err := result.RowsAffected()
	require.NoError(t, err)
	require.Equal(t, int64(1), rowAffected)

	isFeeBumpEligible, err = m.IsAccountFeeBumpEligible(ctx, address)
	require.NoError(t, err)
	assert.True(t, isFeeBumpEligible)
}

func TestAccountModelBatchGetByStateChangeIDs(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()
	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	mockMetricsService := metrics.NewMockMetricsService()
	mockMetricsService.On("ObserveDBQueryDuration", "BatchGetByStateChangeIDs", "accounts", mock.Anything).Return()
	mockMetricsService.On("IncDBQuery", "BatchGetByStateChangeIDs", "accounts").Return()
	mockMetricsService.On("ObserveDBBatchSize", "BatchGetByStateChangeIDs", "accounts", mock.Anything).Return().Maybe()
	defer mockMetricsService.AssertExpectations(t)

	m := &AccountModel{
		DB:             dbConnectionPool,
		MetricsService: mockMetricsService,
	}

	ctx := context.Background()
	address1 := keypair.MustRandom().Address()
	address2 := keypair.MustRandom().Address()
	toID1 := int64(100)
	toID2 := int64(200)
	stateChangeOrder1 := int64(1)
	stateChangeOrder2 := int64(1)

	// Insert test accounts
	_, err = m.DB.ExecContext(ctx, "INSERT INTO accounts (stellar_address) VALUES ($1), ($2)", types.StellarAddress(address1), types.StellarAddress(address2))
	require.NoError(t, err)

	// Insert test transactions first (to_id encodes ledger via TOID)
	_, err = m.DB.ExecContext(ctx, "INSERT INTO transactions (hash, to_id, envelope_xdr, result_xdr, meta_xdr, ledger_created_at) VALUES ('tx1', 1, 'env1', 'res1', 'meta1', NOW()), ('tx2', 2, 'env2', 'res2', 'meta2', NOW())")
	require.NoError(t, err)

	// Insert test operations first (operation_type is SMALLINT: 2=PAYMENT)
	_, err = m.DB.ExecContext(ctx, "INSERT INTO operations (id, operation_type, operation_xdr, ledger_created_at) VALUES (1, 2, 'xdr1', NOW()), (2, 2, 'xdr2', NOW())")
	require.NoError(t, err)

	// Insert test state changes that reference the accounts
	// state_change_category: 1=BALANCE, state_change_reason: 4=CREDIT, 3=DEBIT
	_, err = m.DB.ExecContext(ctx, `
		INSERT INTO state_changes (
			to_id, state_change_order, state_change_category, state_change_reason, ledger_created_at, account_id
		) VALUES
		($1, $2, 1, 4, NOW(), $3),
		($4, $5, 1, 3, NOW(), $6)
	`, toID1, stateChangeOrder1, types.StellarAddress(address1), toID2, stateChangeOrder2, types.StellarAddress(address2))
	require.NoError(t, err)

	// Test BatchGetByStateChangeIDs function
	scToIDs := []int64{toID1, toID2}
	scOrders := []int64{stateChangeOrder1, stateChangeOrder2}
	accounts, err := m.BatchGetByStateChangeIDs(ctx, scToIDs, scOrders, "")
	require.NoError(t, err)
	assert.Len(t, accounts, 2)

	// Verify accounts are returned with correct state_change_id
	addressSet := make(map[string]string)
	for _, acc := range accounts {
		addressSet[string(acc.StellarAddress)] = acc.StateChangeID
	}
	assert.Equal(t, "100-1", addressSet[address1])
	assert.Equal(t, "200-1", addressSet[address2])
}
