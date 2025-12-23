package data

import (
	"context"
	"testing"
	"time"

	set "github.com/deckarep/golang-set/v2"
	"github.com/stellar/go/keypair"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/stellar/wallet-backend/internal/db"
	"github.com/stellar/wallet-backend/internal/db/dbtest"
	"github.com/stellar/wallet-backend/internal/indexer/types"
	"github.com/stellar/wallet-backend/internal/metrics"
)

func Test_TransactionModel_BatchInsert(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()
	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	ctx := context.Background()
	now := time.Now()

	// Create test data
	kp1 := keypair.MustRandom()
	kp2 := keypair.MustRandom()
	const q = "INSERT INTO accounts (stellar_address) SELECT UNNEST(ARRAY[$1, $2])"
	_, err = dbConnectionPool.ExecContext(ctx, q, kp1.Address(), kp2.Address())
	require.NoError(t, err)

	meta1, meta2 := "meta1", "meta2"
	envelope1, envelope2 := "envelope1", "envelope2"
	tx1 := types.Transaction{
		Hash:            "tx1",
		ToID:            1,
		EnvelopeXDR:     &envelope1,
		ResultXDR:       "result1",
		MetaXDR:         &meta1,
		LedgerNumber:    1,
		LedgerCreatedAt: now,
	}
	tx2 := types.Transaction{
		Hash:            "tx2",
		ToID:            2,
		EnvelopeXDR:     &envelope2,
		ResultXDR:       "result2",
		MetaXDR:         &meta2,
		LedgerNumber:    2,
		LedgerCreatedAt: now,
	}

	testCases := []struct {
		name                   string
		useDBTx                bool
		txs                    []*types.Transaction
		stellarAddressesByHash map[string]set.Set[string]
		wantAccountLinks       map[string][]string
		wantErrContains        string
		wantHashes             []string
	}{
		{
			name:                   "🟢successful_insert_without_dbTx",
			useDBTx:                false,
			txs:                    []*types.Transaction{&tx1, &tx2},
			stellarAddressesByHash: map[string]set.Set[string]{tx1.Hash: set.NewSet(kp1.Address()), tx2.Hash: set.NewSet(kp2.Address())},
			wantAccountLinks:       map[string][]string{tx1.Hash: {kp1.Address()}, tx2.Hash: {kp2.Address()}},
			wantErrContains:        "",
			wantHashes:             []string{tx1.Hash, tx2.Hash},
		},
		{
			name:                   "🟢successful_insert_with_dbTx",
			useDBTx:                true,
			txs:                    []*types.Transaction{&tx1},
			stellarAddressesByHash: map[string]set.Set[string]{tx1.Hash: set.NewSet(kp1.Address())},
			wantAccountLinks:       map[string][]string{tx1.Hash: {kp1.Address()}},
			wantErrContains:        "",
			wantHashes:             []string{tx1.Hash},
		},
		{
			name:                   "🟢empty_input",
			useDBTx:                false,
			txs:                    []*types.Transaction{},
			stellarAddressesByHash: map[string]set.Set[string]{},
			wantAccountLinks:       map[string][]string{},
			wantErrContains:        "",
			wantHashes:             nil,
		},
		{
			name:                   "🟡duplicate_transaction",
			useDBTx:                false,
			txs:                    []*types.Transaction{&tx1, &tx1},
			stellarAddressesByHash: map[string]set.Set[string]{tx1.Hash: set.NewSet(kp1.Address())},
			wantAccountLinks:       map[string][]string{tx1.Hash: {kp1.Address()}},
			wantErrContains:        "",
			wantHashes:             []string{tx1.Hash},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Clear the database before each test
			_, err = dbConnectionPool.ExecContext(ctx, "TRUNCATE transactions, transactions_accounts CASCADE")
			require.NoError(t, err)

			// Create fresh mock for each test case
			mockMetricsService := metrics.NewMockMetricsService()
			// The implementation always loops through both tables and calls ObserveDBQueryDuration for each
			mockMetricsService.
				On("ObserveDBQueryDuration", "BatchInsert", "transactions", mock.Anything).Return().Once().
				On("ObserveDBQueryDuration", "BatchInsert", "transactions_accounts", mock.Anything).Return().Once()
			// ObserveDBBatchSize is only called for transactions table (not transactions_accounts)
			mockMetricsService.On("ObserveDBBatchSize", "BatchInsert", "transactions", mock.Anything).Return().Once()
			// IncDBQuery is called for both tables on success
			mockMetricsService.
				On("IncDBQuery", "BatchInsert", "transactions").Return().Once().
				On("IncDBQuery", "BatchInsert", "transactions_accounts").Return().Once()
			defer mockMetricsService.AssertExpectations(t)

			m := &TransactionModel{
				DB:             dbConnectionPool,
				MetricsService: mockMetricsService,
			}

			var sqlExecuter db.SQLExecuter = dbConnectionPool
			if tc.useDBTx {
				tx, err := dbConnectionPool.BeginTxx(ctx, nil)
				require.NoError(t, err)
				defer tx.Rollback()
				sqlExecuter = tx
			}

			gotInsertedHashes, err := m.BatchInsert(ctx, sqlExecuter, tc.txs, tc.stellarAddressesByHash)

			if tc.wantErrContains != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tc.wantErrContains)
				return
			}

			// Verify the results
			require.NoError(t, err)
			var dbInsertedHashes []string
			err = sqlExecuter.SelectContext(ctx, &dbInsertedHashes, "SELECT hash FROM transactions")
			require.NoError(t, err)
			assert.ElementsMatch(t, tc.wantHashes, dbInsertedHashes)
			assert.ElementsMatch(t, tc.wantHashes, gotInsertedHashes)

			// Verify the account links
			if len(tc.wantAccountLinks) > 0 {
				var accountLinks []struct {
					TxHash    string `db:"tx_hash"`
					AccountID string `db:"account_id"`
				}
				err = sqlExecuter.SelectContext(ctx, &accountLinks, "SELECT tx_hash, account_id FROM transactions_accounts ORDER BY tx_hash, account_id")
				require.NoError(t, err)

				// Create a map of tx_hash -> set of account_ids for O(1) lookups
				accountLinksMap := make(map[string][]string)
				for _, link := range accountLinks {
					accountLinksMap[link.TxHash] = append(accountLinksMap[link.TxHash], link.AccountID)
				}

				// Verify each transaction has its expected account links
				require.Equal(t, len(tc.wantAccountLinks), len(accountLinksMap), "number of elements in the maps don't match")
				for key, expectedSlice := range tc.wantAccountLinks {
					actualSlice, exists := accountLinksMap[key]
					require.True(t, exists, "key %s not found in actual map", key)
					assert.ElementsMatch(t, expectedSlice, actualSlice, "slices for key %s don't match", key)
				}
			}
		})
	}
}

func TestTransactionModel_GetByHash(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()
	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	mockMetricsService := metrics.NewMockMetricsService()
	mockMetricsService.On("ObserveDBQueryDuration", "GetByHash", "transactions", mock.Anything).Return()
	mockMetricsService.On("IncDBQuery", "GetByHash", "transactions").Return()
	defer mockMetricsService.AssertExpectations(t)

	m := &TransactionModel{
		DB:             dbConnectionPool,
		MetricsService: mockMetricsService,
	}

	ctx := context.Background()
	now := time.Now()

	// Create test transaction
	txHash := "test_tx_hash"
	_, err = dbConnectionPool.ExecContext(ctx, `
		INSERT INTO transactions (hash, to_id, envelope_xdr, result_xdr, meta_xdr, ledger_number, ledger_created_at)
		VALUES ($1, 1, 'envelope', 'result', 'meta', 1, $2)
	`, txHash, now)
	require.NoError(t, err)

	// Test GetByHash
	transaction, err := m.GetByHash(ctx, txHash, "")
	require.NoError(t, err)
	assert.Equal(t, txHash, transaction.Hash)
	assert.Equal(t, int64(1), transaction.ToID)
	require.NotNil(t, transaction.EnvelopeXDR)
	assert.Equal(t, "envelope", *transaction.EnvelopeXDR)
}

func TestTransactionModel_GetAll(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()
	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	mockMetricsService := metrics.NewMockMetricsService()
	mockMetricsService.On("ObserveDBQueryDuration", "GetAll", "transactions", mock.Anything).Return()
	mockMetricsService.On("IncDBQuery", "GetAll", "transactions").Return()
	defer mockMetricsService.AssertExpectations(t)

	m := &TransactionModel{
		DB:             dbConnectionPool,
		MetricsService: mockMetricsService,
	}

	ctx := context.Background()
	now := time.Now()

	// Create test transactions
	_, err = dbConnectionPool.ExecContext(ctx, `
		INSERT INTO transactions (hash, to_id, envelope_xdr, result_xdr, meta_xdr, ledger_number, ledger_created_at)
		VALUES
			('tx1', 1, 'envelope1', 'result1', 'meta1', 1, $1),
			('tx2', 2, 'envelope2', 'result2', 'meta2', 2, $1),
			('tx3', 3, 'envelope3', 'result3', 'meta3', 3, $1)
	`, now)
	require.NoError(t, err)

	// Test GetAll without specifying cursor and limit (gets all transactions)
	transactions, err := m.GetAll(ctx, "", nil, nil, ASC)
	require.NoError(t, err)
	assert.Len(t, transactions, 3)
	assert.Equal(t, int64(1), transactions[0].Cursor)
	assert.Equal(t, int64(2), transactions[1].Cursor)
	assert.Equal(t, int64(3), transactions[2].Cursor)

	// Test GetAll with smaller limit
	limit := int32(2)
	transactions, err = m.GetAll(ctx, "", &limit, nil, ASC)
	require.NoError(t, err)
	assert.Len(t, transactions, 2)
	assert.Equal(t, int64(1), transactions[0].Cursor)
	assert.Equal(t, int64(2), transactions[1].Cursor)
}

func TestTransactionModel_BatchGetByAccountAddress(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()
	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	mockMetricsService := metrics.NewMockMetricsService()
	mockMetricsService.On("ObserveDBQueryDuration", "BatchGetByAccountAddress", "transactions", mock.Anything).Return()
	mockMetricsService.On("IncDBQuery", "BatchGetByAccountAddress", "transactions").Return()
	defer mockMetricsService.AssertExpectations(t)

	m := &TransactionModel{
		DB:             dbConnectionPool,
		MetricsService: mockMetricsService,
	}

	ctx := context.Background()
	now := time.Now()

	// Create test accounts
	address1 := keypair.MustRandom().Address()
	address2 := keypair.MustRandom().Address()
	_, err = dbConnectionPool.ExecContext(ctx, "INSERT INTO accounts (stellar_address) VALUES ($1), ($2)", address1, address2)
	require.NoError(t, err)

	// Create test transactions
	_, err = dbConnectionPool.ExecContext(ctx, `
		INSERT INTO transactions (hash, to_id, envelope_xdr, result_xdr, meta_xdr, ledger_number, ledger_created_at)
		VALUES
			('tx1', 1, 'envelope1', 'result1', 'meta1', 1, $1),
			('tx2', 2, 'envelope2', 'result2', 'meta2', 2, $1),
			('tx3', 3, 'envelope3', 'result3', 'meta3', 3, $1)
	`, now)
	require.NoError(t, err)

	// Create test transactions_accounts links
	_, err = dbConnectionPool.ExecContext(ctx, `
		INSERT INTO transactions_accounts (tx_hash, account_id)
		VALUES
			('tx1', $1),
			('tx2', $1),
			('tx3', $2)
	`, address1, address2)
	require.NoError(t, err)

	// Test BatchGetByAccount
	transactions, err := m.BatchGetByAccountAddress(ctx, address1, "", nil, nil, "ASC")
	require.NoError(t, err)
	assert.Len(t, transactions, 2)

	assert.Equal(t, int64(1), transactions[0].Cursor)
	assert.Equal(t, int64(2), transactions[1].Cursor)
}

func TestTransactionModel_BatchGetByOperationIDs(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()
	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	mockMetricsService := metrics.NewMockMetricsService()
	mockMetricsService.On("ObserveDBQueryDuration", "BatchGetByOperationIDs", "transactions", mock.Anything).Return()
	mockMetricsService.On("ObserveDBBatchSize", "BatchGetByOperationIDs", "transactions", mock.Anything).Return()
	mockMetricsService.On("IncDBQuery", "BatchGetByOperationIDs", "transactions").Return()
	defer mockMetricsService.AssertExpectations(t)

	m := &TransactionModel{
		DB:             dbConnectionPool,
		MetricsService: mockMetricsService,
	}

	ctx := context.Background()
	now := time.Now()

	// Create test transactions
	_, err = dbConnectionPool.ExecContext(ctx, `
		INSERT INTO transactions (hash, to_id, envelope_xdr, result_xdr, meta_xdr, ledger_number, ledger_created_at)
		VALUES
			('tx1', 1, 'envelope1', 'result1', 'meta1', 1, $1),
			('tx2', 2, 'envelope2', 'result2', 'meta2', 2, $1),
			('tx3', 3, 'envelope3', 'result3', 'meta3', 3, $1)
	`, now)
	require.NoError(t, err)

	// Create test operations
	_, err = dbConnectionPool.ExecContext(ctx, `
		INSERT INTO operations (id, tx_hash, operation_type, operation_xdr, ledger_number, ledger_created_at)
		VALUES
			(1, 'tx1', 'payment', 'xdr1', 1, $1),
			(2, 'tx2', 'create_account', 'xdr2', 2, $1),
			(3, 'tx1', 'payment', 'xdr3', 3, $1)
	`, now)
	require.NoError(t, err)

	// Test BatchGetByOperationID
	transactions, err := m.BatchGetByOperationIDs(ctx, []int64{1, 2, 3}, "")
	require.NoError(t, err)
	assert.Len(t, transactions, 3)

	// Verify transactions are for correct operation IDs
	operationIDsFound := make(map[int64]string)
	for _, tx := range transactions {
		operationIDsFound[tx.OperationID] = tx.Hash
	}
	assert.Equal(t, "tx1", operationIDsFound[1])
	assert.Equal(t, "tx2", operationIDsFound[2])
	assert.Equal(t, "tx1", operationIDsFound[3])
}

func TestTransactionModel_BatchGetByStateChangeIDs(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()
	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	mockMetricsService := metrics.NewMockMetricsService()
	mockMetricsService.On("ObserveDBQueryDuration", "BatchGetByStateChangeIDs", "transactions", mock.Anything).Return()
	mockMetricsService.On("ObserveDBBatchSize", "BatchGetByStateChangeIDs", "transactions", mock.Anything).Return()
	mockMetricsService.On("IncDBQuery", "BatchGetByStateChangeIDs", "transactions").Return()
	defer mockMetricsService.AssertExpectations(t)

	m := &TransactionModel{
		DB:             dbConnectionPool,
		MetricsService: mockMetricsService,
	}

	ctx := context.Background()
	now := time.Now()

	// Create test account
	address := keypair.MustRandom().Address()
	_, err = dbConnectionPool.ExecContext(ctx, "INSERT INTO accounts (stellar_address) VALUES ($1)", address)
	require.NoError(t, err)

	// Create test transactions
	_, err = dbConnectionPool.ExecContext(ctx, `
		INSERT INTO transactions (hash, to_id, envelope_xdr, result_xdr, meta_xdr, ledger_number, ledger_created_at)
		VALUES
			('tx1', 1, 'envelope1', 'result1', 'meta1', 1, $1),
			('tx2', 2, 'envelope2', 'result2', 'meta2', 2, $1),
			('tx3', 3, 'envelope3', 'result3', 'meta3', 3, $1)
	`, now)
	require.NoError(t, err)

	// Create test state changes
	_, err = dbConnectionPool.ExecContext(ctx, `
		INSERT INTO state_changes (to_id, state_change_order, state_change_category, ledger_created_at, ledger_number, account_id, operation_id, tx_hash)
		VALUES
			(1, 1, 'credit', $1, 1, $2, 1, 'tx1'),
			(2, 1, 'debit', $1, 2, $2, 2, 'tx2'),
			(3, 1, 'credit', $1, 3, $2, 3, 'tx1')
	`, now, address)
	require.NoError(t, err)

	// Test BatchGetByStateChangeID
	transactions, err := m.BatchGetByStateChangeIDs(ctx, []int64{1, 2, 3}, []int64{1, 1, 1}, "")
	require.NoError(t, err)
	assert.Len(t, transactions, 3)

	// Verify transactions are for correct state change IDs
	stateChangeIDsFound := make(map[string]string)
	for _, tx := range transactions {
		stateChangeIDsFound[tx.StateChangeID] = tx.Hash
	}
	assert.Equal(t, "tx1", stateChangeIDsFound["1-1"])
	assert.Equal(t, "tx2", stateChangeIDsFound["2-1"])
	assert.Equal(t, "tx1", stateChangeIDsFound["3-1"])
}
