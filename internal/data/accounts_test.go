package data

import (
	"context"
	"database/sql"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/stellar/go-stellar-sdk/keypair"
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

	// Generate test addresses
	account1 := keypair.MustRandom().Address()
	account2 := keypair.MustRandom().Address()
	nonexistent1 := keypair.MustRandom().Address()
	nonexistent2 := keypair.MustRandom().Address()

	t.Run("empty input returns empty result", func(t *testing.T) {
		var result []string
		err := db.RunInPgxTransaction(ctx, dbConnectionPool, func(tx pgx.Tx) error {
			result, err = accountModel.BatchGetByIDs(ctx, tx, []string{})
			return err
		})
		require.NoError(t, err)
		assert.Empty(t, result)
	})

	t.Run("returns existing accounts only", func(t *testing.T) {
		// Insert some test accounts using StellarAddress for BYTEA conversion
		_, err := dbConnectionPool.ExecContext(ctx, "INSERT INTO accounts (stellar_address) VALUES ($1), ($2)",
			types.AddressBytea(account1), types.AddressBytea(account2))
		require.NoError(t, err)

		// Test with mix of existing and non-existing accounts
		mockMetricsService.On("ObserveDBQueryDuration", "BatchGetByIDs", "accounts", mock.Anything).Return().Once()
		mockMetricsService.On("IncDBQuery", "BatchGetByIDs", "accounts").Return().Once()
		mockMetricsService.On("ObserveDBBatchSize", "BatchGetByIDs", "accounts", mock.Anything).Return().Once()

		var result []string
		err = db.RunInPgxTransaction(ctx, dbConnectionPool, func(tx pgx.Tx) error {
			result, err = accountModel.BatchGetByIDs(ctx, tx, []string{account1, nonexistent1, account2, nonexistent2})
			return err
		})
		require.NoError(t, err)

		// Should only return the existing accounts
		assert.Len(t, result, 2)
		assert.Contains(t, result, account1)
		assert.Contains(t, result, account2)
		assert.NotContains(t, result, nonexistent1)
		assert.NotContains(t, result, nonexistent2)
	})

	t.Run("returns empty when no accounts exist", func(t *testing.T) {
		// Clean up the table
		_, err := dbConnectionPool.ExecContext(ctx, "DELETE FROM accounts")
		require.NoError(t, err)

		mockMetricsService.On("ObserveDBQueryDuration", "BatchGetByIDs", "accounts", mock.Anything).Return().Once()
		mockMetricsService.On("IncDBQuery", "BatchGetByIDs", "accounts").Return().Once()
		mockMetricsService.On("ObserveDBBatchSize", "BatchGetByIDs", "accounts", mock.Anything).Return().Once()

		var result []string
		err = db.RunInPgxTransaction(ctx, dbConnectionPool, func(tx pgx.Tx) error {
			result, err = accountModel.BatchGetByIDs(ctx, tx, []string{nonexistent1, nonexistent2})
			return err
		})
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

		var dbAddress types.AddressBytea
		err = m.DB.GetContext(ctx, &dbAddress, "SELECT stellar_address FROM accounts WHERE stellar_address = $1", types.AddressBytea(address))
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
		result, insertErr := m.DB.ExecContext(ctx, "INSERT INTO accounts (stellar_address) VALUES ($1)", types.AddressBytea(address))
		require.NoError(t, insertErr)
		rowAffected, err := result.RowsAffected()
		require.NoError(t, err)
		require.Equal(t, int64(1), rowAffected)

		err = m.Delete(ctx, address)
		require.NoError(t, err)

		var dbAddress types.AddressBytea
		err = m.DB.GetContext(ctx, &dbAddress, "SELECT stellar_address FROM accounts LIMIT 1")
		assert.ErrorIs(t, err, sql.ErrNoRows)
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
	result, err := m.DB.ExecContext(ctx, "INSERT INTO accounts (stellar_address) VALUES ($1)", types.AddressBytea(address))
	require.NoError(t, err)
	rowAffected, err := result.RowsAffected()
	require.NoError(t, err)
	require.Equal(t, int64(1), rowAffected)

	// Test Get function
	account, err := m.Get(ctx, address)
	require.NoError(t, err)
	assert.Equal(t, address, string(account.StellarAddress))
}

func TestAccountModelBatchGetByToIDs(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()
	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	mockMetricsService := metrics.NewMockMetricsService()
	mockMetricsService.On("ObserveDBQueryDuration", "BatchGetByToIDs", "accounts", mock.Anything).Return()
	mockMetricsService.On("IncDBQuery", "BatchGetByToIDs", "accounts").Return()
	mockMetricsService.On("ObserveDBBatchSize", "BatchGetByToIDs", "accounts", mock.Anything).Return().Maybe()
	defer mockMetricsService.AssertExpectations(t)

	m := &AccountModel{
		DB:             dbConnectionPool,
		MetricsService: mockMetricsService,
	}

	ctx := context.Background()
	address1 := keypair.MustRandom().Address()
	address2 := keypair.MustRandom().Address()
	toID1 := int64(1)
	toID2 := int64(2)

	// Insert test accounts
	_, err = m.DB.ExecContext(ctx, "INSERT INTO accounts (stellar_address) VALUES ($1), ($2)",
		types.AddressBytea(address1), types.AddressBytea(address2))
	require.NoError(t, err)

	// Insert test transactions first (hash is BYTEA, using valid 64-char hex strings)
	testHash1 := types.HashBytea("0000000000000000000000000000000000000000000000000000000000000001")
	testHash2 := types.HashBytea("0000000000000000000000000000000000000000000000000000000000000002")
	_, err = m.DB.ExecContext(ctx, "INSERT INTO transactions (hash, to_id, envelope_xdr, fee_charged, result_code, meta_xdr, ledger_number, ledger_created_at) VALUES ($1, $2, 'env1', 100, 'TransactionResultCodeTxSuccess', 'meta1', 1, NOW()), ($3, $4, 'env2', 200, 'TransactionResultCodeTxSuccess', 'meta2', 2, NOW())", testHash1, toID1, testHash2, toID2)
	require.NoError(t, err)

	// Insert test transactions_accounts links
	_, err = m.DB.ExecContext(ctx, "INSERT INTO transactions_accounts (tx_to_id, account_id) VALUES ($1, $2), ($3, $4)",
		toID1, types.AddressBytea(address1), toID2, types.AddressBytea(address2))
	require.NoError(t, err)

	// Test BatchGetByToIDs function
	accounts, err := m.BatchGetByToIDs(ctx, []int64{toID1, toID2}, "")
	require.NoError(t, err)
	assert.Len(t, accounts, 2)

	// Verify accounts are returned with correct to_id
	addressSet := make(map[string]int64)
	for _, acc := range accounts {
		addressSet[string(acc.StellarAddress)] = acc.ToID
	}
	assert.Equal(t, toID1, addressSet[address1])
	assert.Equal(t, toID2, addressSet[address2])
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

	// Insert test accounts (stellar_address is BYTEA)
	_, err = m.DB.ExecContext(ctx, "INSERT INTO accounts (stellar_address) VALUES ($1), ($2)",
		types.AddressBytea(address1), types.AddressBytea(address2))
	require.NoError(t, err)

	// Insert test transactions first (hash is BYTEA, using valid 64-char hex strings)
	testHash1 := types.HashBytea("0000000000000000000000000000000000000000000000000000000000000001")
	testHash2 := types.HashBytea("0000000000000000000000000000000000000000000000000000000000000002")
	_, err = m.DB.ExecContext(ctx, "INSERT INTO transactions (hash, to_id, envelope_xdr, fee_charged, result_code, meta_xdr, ledger_number, ledger_created_at) VALUES ($1, 4096, 'env1', 100, 'TransactionResultCodeTxSuccess', 'meta1', 1, NOW()), ($2, 8192, 'env2', 200, 'TransactionResultCodeTxSuccess', 'meta2', 2, NOW())", testHash1, testHash2)
	require.NoError(t, err)

	// Insert test operations (IDs don't need to be in TOID range here since we're just testing operations_accounts links)
	_, err = m.DB.ExecContext(ctx, "INSERT INTO operations (id, operation_type, operation_xdr, result_code, successful, ledger_number, ledger_created_at) VALUES ($1, 'PAYMENT', 'xdr1', 'op_success', true, 1, NOW()), ($2, 'PAYMENT', 'xdr2', 'op_success', true, 2, NOW())", operationID1, operationID2)
	require.NoError(t, err)

	// Insert test operations_accounts links (account_id is BYTEA)
	_, err = m.DB.ExecContext(ctx, "INSERT INTO operations_accounts (operation_id, account_id) VALUES ($1, $2), ($3, $4)",
		operationID1, types.AddressBytea(address1), operationID2, types.AddressBytea(address2))
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

	result, err := m.DB.ExecContext(ctx, "INSERT INTO accounts (stellar_address) VALUES ($1)", types.AddressBytea(address))
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
	toID1 := int64(4096)
	toID2 := int64(8192)
	stateChangeOrder1 := int64(1)
	stateChangeOrder2 := int64(1)

	// Insert test accounts (stellar_address is BYTEA)
	_, err = m.DB.ExecContext(ctx, "INSERT INTO accounts (stellar_address) VALUES ($1), ($2)",
		types.AddressBytea(address1), types.AddressBytea(address2))
	require.NoError(t, err)

	// Insert test transactions first (hash is BYTEA, using valid 64-char hex strings)
	testHash1 := types.HashBytea("0000000000000000000000000000000000000000000000000000000000000001")
	testHash2 := types.HashBytea("0000000000000000000000000000000000000000000000000000000000000002")
	_, err = m.DB.ExecContext(ctx, "INSERT INTO transactions (hash, to_id, envelope_xdr, fee_charged, result_code, meta_xdr, ledger_number, ledger_created_at) VALUES ($1, 4096, 'env1', 100, 'TransactionResultCodeTxSuccess', 'meta1', 1, NOW()), ($2, 8192, 'env2', 200, 'TransactionResultCodeTxSuccess', 'meta2', 2, NOW())", testHash1, testHash2)
	require.NoError(t, err)

	// Insert test operations (IDs must be in TOID range for each transaction)
	_, err = m.DB.ExecContext(ctx, "INSERT INTO operations (id, operation_type, operation_xdr, result_code, successful, ledger_number, ledger_created_at) VALUES (4097, 'PAYMENT', 'xdr1', 'op_success', true, 1, NOW()), (8193, 'PAYMENT', 'xdr2', 'op_success', true, 2, NOW())")
	require.NoError(t, err)

	// Insert test state changes that reference the accounts (state_changes.account_id is TEXT)
	_, err = m.DB.ExecContext(ctx, `
		INSERT INTO state_changes (
			to_id, state_change_order, state_change_category, ledger_created_at,
			ledger_number, account_id, operation_id
		) VALUES
		($1, $2, 'BALANCE', NOW(), 1, $3, 4097),
		($4, $5, 'BALANCE', NOW(), 2, $6, 8193)
	`, toID1, stateChangeOrder1, types.AddressBytea(address1), toID2, stateChangeOrder2, types.AddressBytea(address2))
	require.NoError(t, err)

	// Test BatchGetByStateChangeIDs function
	scToIDs := []int64{toID1, toID2}
	scOpIDs := []int64{4097, 8193}
	scOrders := []int64{stateChangeOrder1, stateChangeOrder2}
	accounts, err := m.BatchGetByStateChangeIDs(ctx, scToIDs, scOpIDs, scOrders, "")
	require.NoError(t, err)
	assert.Len(t, accounts, 2)

	// Verify accounts are returned with correct state_change_id (format: to_id-operation_id-state_change_order)
	addressSet := make(map[string]string)
	for _, acc := range accounts {
		addressSet[string(acc.StellarAddress)] = acc.StateChangeID
	}
	assert.Equal(t, "4096-4097-1", addressSet[address1])
	assert.Equal(t, "8192-8193-1", addressSet[address2])
}
