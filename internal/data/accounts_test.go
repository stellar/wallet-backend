package data

import (
	"context"
	"testing"

	"github.com/stellar/go-stellar-sdk/keypair"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/stellar/wallet-backend/internal/db"
	"github.com/stellar/wallet-backend/internal/db/dbtest"
	"github.com/stellar/wallet-backend/internal/indexer/types"
	"github.com/stellar/wallet-backend/internal/metrics"
)

func TestAccountModelBatchGetByToIDs(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()
	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	mockMetricsService := metrics.NewMockMetricsService()
	mockMetricsService.On("ObserveDBQueryDuration", "BatchGetByToIDs", "transactions_accounts", mock.Anything).Return()
	mockMetricsService.On("IncDBQuery", "BatchGetByToIDs", "transactions_accounts").Return()
	mockMetricsService.On("ObserveDBBatchSize", "BatchGetByToIDs", "transactions_accounts", mock.Anything).Return().Maybe()
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

	// Insert test transactions first (hash is BYTEA, using valid 64-char hex strings)
	testHash1 := types.HashBytea("0000000000000000000000000000000000000000000000000000000000000001")
	testHash2 := types.HashBytea("0000000000000000000000000000000000000000000000000000000000000002")
	_, err = m.DB.Pool().Exec(ctx, "INSERT INTO transactions (hash, to_id, envelope_xdr, fee_charged, result_code, meta_xdr, ledger_number, ledger_created_at) VALUES ($1, $2, 'env1', 100, 'TransactionResultCodeTxSuccess', 'meta1', 1, NOW()), ($3, $4, 'env2', 200, 'TransactionResultCodeTxSuccess', 'meta2', 2, NOW())", testHash1, toID1, testHash2, toID2)
	require.NoError(t, err)

	// Insert test transactions_accounts links
	_, err = m.DB.Pool().Exec(ctx, "INSERT INTO transactions_accounts (ledger_created_at, tx_to_id, account_id) VALUES (NOW(), $1, $2), (NOW(), $3, $4)",
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
	mockMetricsService.On("ObserveDBQueryDuration", "BatchGetByOperationIDs", "operations_accounts", mock.Anything).Return()
	mockMetricsService.On("IncDBQuery", "BatchGetByOperationIDs", "operations_accounts").Return()
	mockMetricsService.On("ObserveDBBatchSize", "BatchGetByOperationIDs", "operations_accounts", mock.Anything).Return().Maybe()
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

	// Insert test transactions first (hash is BYTEA, using valid 64-char hex strings)
	testHash1 := types.HashBytea("0000000000000000000000000000000000000000000000000000000000000001")
	testHash2 := types.HashBytea("0000000000000000000000000000000000000000000000000000000000000002")
	_, err = m.DB.Pool().Exec(ctx, "INSERT INTO transactions (hash, to_id, envelope_xdr, fee_charged, result_code, meta_xdr, ledger_number, ledger_created_at) VALUES ($1, 4096, 'env1', 100, 'TransactionResultCodeTxSuccess', 'meta1', 1, NOW()), ($2, 8192, 'env2', 200, 'TransactionResultCodeTxSuccess', 'meta2', 2, NOW())", testHash1, testHash2)
	require.NoError(t, err)

	// Insert test operations (IDs don't need to be in TOID range here since we're just testing operations_accounts links)
	xdr1 := types.XDRBytea([]byte("xdr1"))
	xdr2 := types.XDRBytea([]byte("xdr2"))
	_, err = m.DB.Pool().Exec(ctx, "INSERT INTO operations (id, operation_type, operation_xdr, result_code, successful, ledger_number, ledger_created_at) VALUES ($1, 'PAYMENT', $3, 'op_success', true, 1, NOW()), ($2, 'PAYMENT', $4, 'op_success', true, 2, NOW())", operationID1, operationID2, xdr1, xdr2)
	require.NoError(t, err)

	// Insert test operations_accounts links
	_, err = m.DB.Pool().Exec(ctx, "INSERT INTO operations_accounts (ledger_created_at, operation_id, account_id) VALUES (NOW(), $1, $2), (NOW(), $3, $4)",
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
	mockMetricsService.On("IncDBQuery", "IsAccountFeeBumpEligible", "channel_accounts").Return()
	mockMetricsService.On("ObserveDBQueryDuration", "IsAccountFeeBumpEligible", "channel_accounts", mock.Anything).Return()
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

	// Insert into channel_accounts since IsAccountFeeBumpEligible only checks that table
	_, err = m.DB.Pool().Exec(ctx, "INSERT INTO channel_accounts (public_key, encrypted_private_key) VALUES ($1, 'encrypted')", address)
	require.NoError(t, err)

	isFeeBumpEligible, err = m.IsAccountFeeBumpEligible(ctx, address)
	require.NoError(t, err)
	assert.True(t, isFeeBumpEligible)
}
