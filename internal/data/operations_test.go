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

func Test_OperationModel_BatchInsert(t *testing.T) {
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

	// Create referenced transactions first
	tx1 := types.Transaction{
		Hash:            "tx1",
		ToID:            1,
		EnvelopeXDR:     "envelope1",
		ResultXDR:       "result1",
		MetaXDR:         "meta1",
		LedgerNumber:    1,
		LedgerCreatedAt: now,
	}
	tx2 := types.Transaction{
		Hash:            "tx2",
		ToID:            2,
		EnvelopeXDR:     "envelope2",
		ResultXDR:       "result2",
		MetaXDR:         "meta2",
		LedgerNumber:    2,
		LedgerCreatedAt: now,
	}

	// Insert transactions
	sqlxDB, err := dbConnectionPool.SqlxDB(ctx)
	require.NoError(t, err)
	txModel := &TransactionModel{DB: dbConnectionPool, MetricsService: metrics.NewMetricsService(sqlxDB)}
	_, err = txModel.BatchInsert(ctx, nil, []types.Transaction{tx1, tx2}, map[string]set.Set[string]{
		tx1.Hash: set.NewSet(kp1.Address()),
		tx2.Hash: set.NewSet(kp2.Address()),
	})
	require.NoError(t, err)

	op1 := types.Operation{
		ID:              1,
		TxHash:          tx1.Hash,
		OperationType:   types.OperationTypePayment,
		OperationXDR:    "operation1",
		LedgerCreatedAt: now,
	}
	op2 := types.Operation{
		ID:              2,
		TxHash:          tx2.Hash,
		OperationType:   types.OperationTypeCreateAccount,
		OperationXDR:    "operation2",
		LedgerCreatedAt: now,
	}

	testCases := []struct {
		name                   string
		useDBTx                bool
		operations             []types.Operation
		stellarAddressesByOpID map[int64]set.Set[string]
		wantAccountLinks       map[int64][]string
		wantErrContains        string
		wantIDs                []int64
	}{
		{
			name:                   "🟢successful_insert_without_dbTx",
			useDBTx:                false,
			operations:             []types.Operation{op1, op2},
			stellarAddressesByOpID: map[int64]set.Set[string]{op1.ID: set.NewSet(kp1.Address(), kp1.Address(), kp1.Address(), kp1.Address()), op2.ID: set.NewSet(kp2.Address(), kp2.Address())},
			wantAccountLinks:       map[int64][]string{op1.ID: {kp1.Address()}, op2.ID: {kp2.Address()}},
			wantErrContains:        "",
			wantIDs:                []int64{op1.ID, op2.ID},
		},
		{
			name:                   "🟢successful_insert_with_dbTx",
			useDBTx:                true,
			operations:             []types.Operation{op1},
			stellarAddressesByOpID: map[int64]set.Set[string]{op1.ID: set.NewSet(kp1.Address())},
			wantAccountLinks:       map[int64][]string{op1.ID: {kp1.Address()}},
			wantErrContains:        "",
			wantIDs:                []int64{op1.ID},
		},
		{
			name:                   "🟢empty_input",
			useDBTx:                false,
			operations:             []types.Operation{},
			stellarAddressesByOpID: map[int64]set.Set[string]{},
			wantAccountLinks:       map[int64][]string{},
			wantErrContains:        "",
			wantIDs:                nil,
		},
		{
			name:                   "🟡duplicate_operation",
			useDBTx:                false,
			operations:             []types.Operation{op1, op1},
			stellarAddressesByOpID: map[int64]set.Set[string]{op1.ID: set.NewSet(kp1.Address())},
			wantAccountLinks:       map[int64][]string{op1.ID: {kp1.Address()}},
			wantErrContains:        "",
			wantIDs:                []int64{op1.ID},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Clear the database before each test
			_, err = dbConnectionPool.ExecContext(ctx, "TRUNCATE operations, operations_accounts CASCADE")
			require.NoError(t, err)

			// Create fresh mock for each test case
			mockMetricsService := metrics.NewMockMetricsService()
			mockMetricsService.
				On("ObserveDBQueryDuration", "INSERT", "operations", mock.Anything).Return().Once().
				On("ObserveDBQueryDuration", "INSERT", "operations_accounts", mock.Anything).Return().Once().
				On("ObserveDBBatchSize", "INSERT", "operations", mock.Anything).Return().Once().
				On("IncDBQuery", "INSERT", "operations").Return().Once().
				On("IncDBQuery", "INSERT", "operations_accounts").Return().Once()
			defer mockMetricsService.AssertExpectations(t)

			m := &OperationModel{
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

			gotInsertedIDs, err := m.BatchInsert(ctx, sqlExecuter, tc.operations, tc.stellarAddressesByOpID)

			if tc.wantErrContains != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tc.wantErrContains)
				return
			}

			// Verify the results
			require.NoError(t, err)
			var dbInsertedIDs []int64
			err = sqlExecuter.SelectContext(ctx, &dbInsertedIDs, "SELECT id FROM operations")
			require.NoError(t, err)
			assert.ElementsMatch(t, tc.wantIDs, dbInsertedIDs)
			assert.ElementsMatch(t, tc.wantIDs, gotInsertedIDs)

			// Verify the account links
			if len(tc.wantAccountLinks) > 0 {
				var accountLinks []struct {
					OperationID int64  `db:"operation_id"`
					AccountID   string `db:"account_id"`
				}
				err = sqlExecuter.SelectContext(ctx, &accountLinks, "SELECT operation_id, account_id FROM operations_accounts ORDER BY operation_id, account_id")
				require.NoError(t, err)

				// Create a map of operation_id -> set of account_ids for O(1) lookups
				accountLinksMap := make(map[int64][]string)
				for _, link := range accountLinks {
					accountLinksMap[link.OperationID] = append(accountLinksMap[link.OperationID], link.AccountID)
				}

				// Verify each operation has its expected account links
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

func TestOperationModel_GetAll(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()
	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	mockMetricsService := metrics.NewMockMetricsService()
	mockMetricsService.On("ObserveDBQueryDuration", "SELECT", "operations", mock.Anything).Return()
	mockMetricsService.On("IncDBQuery", "SELECT", "operations").Return()
	defer mockMetricsService.AssertExpectations(t)

	m := &OperationModel{
		DB:             dbConnectionPool,
		MetricsService: mockMetricsService,
	}

	ctx := context.Background()
	now := time.Now()

	// Create test transactions first
	_, err = dbConnectionPool.ExecContext(ctx, `
		INSERT INTO transactions (hash, to_id, envelope_xdr, result_xdr, meta_xdr, ledger_number, ledger_created_at)
		VALUES 
			('tx1', 1, 'env1', 'res1', 'meta1', 1, $1),
			('tx2', 2, 'env2', 'res2', 'meta2', 2, $1),
			('tx3', 3, 'env3', 'res3', 'meta3', 3, $1)
	`, now)
	require.NoError(t, err)

	// Create test operations
	_, err = dbConnectionPool.ExecContext(ctx, `
		INSERT INTO operations (id, tx_hash, operation_type, operation_xdr, ledger_number, ledger_created_at)
		VALUES 
			(1, 'tx1', 'payment', 'xdr1', 1, $1),
			(2, 'tx2', 'create_account', 'xdr2', 2, $1),
			(3, 'tx3', 'payment', 'xdr3', 3, $1)
	`, now)
	require.NoError(t, err)

	// Test GetAll without limit (gets all operations)
	operations, err := m.GetAll(ctx, "", nil, nil, ASC)
	require.NoError(t, err)
	assert.Len(t, operations, 3)
	assert.Equal(t, int64(1), operations[0].Cursor)
	assert.Equal(t, int64(2), operations[1].Cursor)
	assert.Equal(t, int64(3), operations[2].Cursor)

	// Test GetAll with smaller limit
	limit := int32(2)
	operations, err = m.GetAll(ctx, "", &limit, nil, ASC)
	require.NoError(t, err)
	assert.Len(t, operations, 2)
	assert.Equal(t, int64(1), operations[0].Cursor)
	assert.Equal(t, int64(2), operations[1].Cursor)
}

func TestOperationModel_BatchGetByTxHashes(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()
	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	ctx := context.Background()
	now := time.Now()

	// Create test transactions first
	_, err = dbConnectionPool.ExecContext(ctx, `
		INSERT INTO transactions (hash, to_id, envelope_xdr, result_xdr, meta_xdr, ledger_number, ledger_created_at)
		VALUES 
			('tx1', 1, 'env1', 'res1', 'meta1', 1, $1),
			('tx2', 2, 'env2', 'res2', 'meta2', 2, $1),
			('tx3', 3, 'env3', 'res3', 'meta3', 3, $1)
	`, now)
	require.NoError(t, err)

	// Create test operations - multiple operations per transaction to test ranking
	_, err = dbConnectionPool.ExecContext(ctx, `
		INSERT INTO operations (id, tx_hash, operation_type, operation_xdr, ledger_number, ledger_created_at)
		VALUES 
			(1, 'tx1', 'payment', 'xdr1', 1, $1),
			(2, 'tx2', 'create_account', 'xdr2', 2, $1),
			(3, 'tx1', 'payment', 'xdr3', 3, $1),
			(4, 'tx1', 'manage_offer', 'xdr4', 4, $1),
			(5, 'tx2', 'payment', 'xdr5', 5, $1),
			(6, 'tx3', 'trust_line', 'xdr6', 6, $1)
	`, now)
	require.NoError(t, err)

	testCases := []struct {
		name              string
		txHashes          []string
		limit             *int32
		sortOrder         SortOrder
		expectedCount     int
		expectedTxCounts  map[string]int
		expectMetricCalls int
	}{
		{
			name:              "🟢 basic functionality with multiple tx hashes",
			txHashes:          []string{"tx1", "tx2"},
			limit:             nil,
			sortOrder:         ASC,
			expectedCount:     5, // 3 ops for tx1 + 2 ops for tx2
			expectedTxCounts:  map[string]int{"tx1": 3, "tx2": 2},
			expectMetricCalls: 1,
		},
		{
			name:              "🟢 with limit parameter",
			txHashes:          []string{"tx1", "tx2"},
			limit:             int32Ptr(2),
			sortOrder:         ASC,
			expectedCount:     4, // 2 ops per tx hash (limited by ROW_NUMBER)
			expectedTxCounts:  map[string]int{"tx1": 2, "tx2": 2},
			expectMetricCalls: 1,
		},
		{
			name:              "🟢 DESC sort order",
			txHashes:          []string{"tx1"},
			limit:             nil,
			sortOrder:         DESC,
			expectedCount:     3,
			expectedTxCounts:  map[string]int{"tx1": 3},
			expectMetricCalls: 1,
		},
		{
			name:              "🟢 single transaction",
			txHashes:          []string{"tx3"},
			limit:             nil,
			sortOrder:         ASC,
			expectedCount:     1,
			expectedTxCounts:  map[string]int{"tx3": 1},
			expectMetricCalls: 1,
		},
		{
			name:              "🟡 empty tx hashes array",
			txHashes:          []string{},
			limit:             nil,
			sortOrder:         ASC,
			expectedCount:     0,
			expectedTxCounts:  map[string]int{},
			expectMetricCalls: 1,
		},
		{
			name:              "🟡 non-existent transaction hash",
			txHashes:          []string{"nonexistent"},
			limit:             nil,
			sortOrder:         ASC,
			expectedCount:     0,
			expectedTxCounts:  map[string]int{},
			expectMetricCalls: 1,
		},
		{
			name:              "🟡 mixed existing and non-existent hashes",
			txHashes:          []string{"tx1", "nonexistent", "tx2"},
			limit:             nil,
			sortOrder:         ASC,
			expectedCount:     5,
			expectedTxCounts:  map[string]int{"tx1": 3, "tx2": 2},
			expectMetricCalls: 1,
		},
		{
			name:              "🟢 limit smaller than operations per transaction",
			txHashes:          []string{"tx1"},
			limit:             int32Ptr(1),
			sortOrder:         ASC,
			expectedCount:     1, // Only first operation due to ROW_NUMBER ranking
			expectedTxCounts:  map[string]int{"tx1": 1},
			expectMetricCalls: 1,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mockMetricsService := metrics.NewMockMetricsService()
			mockMetricsService.On("ObserveDBQueryDuration", "SELECT", "operations", mock.Anything).Return().Times(tc.expectMetricCalls)
			mockMetricsService.On("ObserveDBBatchSize", "SELECT", "operations", mock.Anything).Return().Times(tc.expectMetricCalls)
			mockMetricsService.On("IncDBQuery", "SELECT", "operations").Return().Times(tc.expectMetricCalls)
			defer mockMetricsService.AssertExpectations(t)

			m := &OperationModel{
				DB:             dbConnectionPool,
				MetricsService: mockMetricsService,
			}

			operations, err := m.BatchGetByTxHashes(ctx, tc.txHashes, "", tc.limit, tc.sortOrder)
			require.NoError(t, err)
			assert.Len(t, operations, tc.expectedCount)

			// Verify operations are for correct tx hashes
			txHashesFound := make(map[string]int)
			for _, op := range operations {
				txHashesFound[op.TxHash]++
			}
			assert.Equal(t, tc.expectedTxCounts, txHashesFound)

			// Verify within-transaction ordering
			// The CTE uses ROW_NUMBER() OVER (PARTITION BY o.tx_hash ORDER BY o.id %s)
			// This means operations within each transaction should be ordered by ID
			if len(operations) > 0 {
				operationsByTxHash := make(map[string][]*types.OperationWithCursor)
				for _, op := range operations {
					operationsByTxHash[op.TxHash] = append(operationsByTxHash[op.TxHash], op)
				}

				// Verify ordering within each transaction
				for txHash, txOperations := range operationsByTxHash {
					if len(txOperations) > 1 {
						for i := 1; i < len(txOperations); i++ {
							prevID := txOperations[i-1].ID
							currID := txOperations[i].ID
							// After final transformation, operations should be in ascending ID order within each tx
							assert.True(t, prevID <= currID,
								"operations within tx %s should be ordered by ID: prev=%d, curr=%d",
								txHash, prevID, currID)
						}
					}
				}
			}

			// Verify limit behavior when specified
			if tc.limit != nil && len(tc.expectedTxCounts) > 0 {
				for txHash, count := range tc.expectedTxCounts {
					assert.True(t, count <= int(*tc.limit), "number of operations for %s should not exceed limit %d", txHash, *tc.limit)
				}
			}
		})
	}
}

func int32Ptr(v int32) *int32 {
	return &v
}

func TestOperationModel_BatchGetByTxHash(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()
	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	mockMetricsService := metrics.NewMockMetricsService()
	mockMetricsService.On("ObserveDBQueryDuration", "SELECT", "operations", mock.Anything).Return()
	mockMetricsService.On("IncDBQuery", "SELECT", "operations").Return()
	defer mockMetricsService.AssertExpectations(t)

	m := &OperationModel{
		DB:             dbConnectionPool,
		MetricsService: mockMetricsService,
	}

	ctx := context.Background()
	now := time.Now()

	// Create test transactions first
	_, err = dbConnectionPool.ExecContext(ctx, `
		INSERT INTO transactions (hash, to_id, envelope_xdr, result_xdr, meta_xdr, ledger_number, ledger_created_at)
		VALUES 
			('tx1', 1, 'env1', 'res1', 'meta1', 1, $1),
			('tx2', 2, 'env2', 'res2', 'meta2', 2, $1)
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

	// Test BatchGetByTxHash
	operations, err := m.BatchGetByTxHash(ctx, "tx1", "", nil, nil, ASC)
	require.NoError(t, err)
	assert.Len(t, operations, 2)
	assert.Equal(t, "xdr1", operations[0].OperationXDR)
	assert.Equal(t, "xdr3", operations[1].OperationXDR)
}

func TestOperationModel_BatchGetByAccountAddresses(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()
	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	mockMetricsService := metrics.NewMockMetricsService()
	mockMetricsService.On("ObserveDBQueryDuration", "SELECT", "operations", mock.Anything).Return()
	mockMetricsService.On("IncDBQuery", "SELECT", "operations").Return()
	defer mockMetricsService.AssertExpectations(t)

	m := &OperationModel{
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

	// Create test transactions first
	_, err = dbConnectionPool.ExecContext(ctx, `
		INSERT INTO transactions (hash, to_id, envelope_xdr, result_xdr, meta_xdr, ledger_number, ledger_created_at)
		VALUES 
			('tx1', 1, 'env1', 'res1', 'meta1', 1, $1),
			('tx2', 2, 'env2', 'res2', 'meta2', 2, $1),
			('tx3', 3, 'env3', 'res3', 'meta3', 3, $1)
	`, now)
	require.NoError(t, err)

	// Create test operations
	_, err = dbConnectionPool.ExecContext(ctx, `
		INSERT INTO operations (id, tx_hash, operation_type, operation_xdr, ledger_number, ledger_created_at)
		VALUES 
			(1, 'tx1', 'payment', 'xdr1', 1, $1),
			(2, 'tx2', 'create_account', 'xdr2', 2, $1),
			(3, 'tx3', 'payment', 'xdr3', 3, $1)
	`, now)
	require.NoError(t, err)

	// Create test operations_accounts links
	_, err = dbConnectionPool.ExecContext(ctx, `
		INSERT INTO operations_accounts (operation_id, account_id)
		VALUES 
			(1, $1),
			(2, $1),
			(3, $2)
	`, address1, address2)
	require.NoError(t, err)

	// Test BatchGetByAccount
	operations, err := m.BatchGetByAccountAddress(ctx, address1, "", nil, nil, "ASC")
	require.NoError(t, err)
	assert.Len(t, operations, 2)
	assert.Equal(t, int64(1), operations[0].Operation.ID)
	assert.Equal(t, int64(2), operations[1].Operation.ID)
}

func TestOperationModel_GetByID(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()
	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	ctx := context.Background()
	now := time.Now()

	// Create test transactions first
	_, err = dbConnectionPool.ExecContext(ctx, `
		INSERT INTO transactions (hash, to_id, envelope_xdr, result_xdr, meta_xdr, ledger_number, ledger_created_at)
		VALUES 
			('tx1', 1, 'env1', 'res1', 'meta1', 1, $1),
			('tx2', 2, 'env2', 'res2', 'meta2', 2, $1)
	`, now)
	require.NoError(t, err)

	// Create test operations
	_, err = dbConnectionPool.ExecContext(ctx, `
		INSERT INTO operations (id, tx_hash, operation_type, operation_xdr, ledger_number, ledger_created_at)
		VALUES 
			(1, 'tx1', 'payment', 'xdr1', 1, $1),
			(2, 'tx2', 'create_account', 'xdr2', 2, $1)
	`, now)
	require.NoError(t, err)

	mockMetricsService := metrics.NewMockMetricsService()
	mockMetricsService.On("ObserveDBQueryDuration", "SELECT", "operations", mock.Anything).Return()
	mockMetricsService.On("IncDBQuery", "SELECT", "operations").Return()
	defer mockMetricsService.AssertExpectations(t)

	m := &OperationModel{
		DB:             dbConnectionPool,
		MetricsService: mockMetricsService,
	}

	operation, err := m.GetByID(ctx, 1, "")
	require.NoError(t, err)
	assert.Equal(t, int64(1), operation.ID)
	assert.Equal(t, "tx1", operation.TxHash)
	assert.Equal(t, "xdr1", operation.OperationXDR)
	assert.Equal(t, uint32(1), operation.LedgerNumber)
	assert.WithinDuration(t, now, operation.LedgerCreatedAt, time.Second)
}

func TestOperationModel_BatchGetByStateChangeIDs(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()
	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	mockMetricsService := metrics.NewMockMetricsService()
	mockMetricsService.On("ObserveDBQueryDuration", "SELECT", "operations", mock.Anything).Return()
	mockMetricsService.On("ObserveDBBatchSize", "SELECT", "operations", mock.Anything).Return()
	mockMetricsService.On("IncDBQuery", "SELECT", "operations").Return()
	defer mockMetricsService.AssertExpectations(t)

	m := &OperationModel{
		DB:             dbConnectionPool,
		MetricsService: mockMetricsService,
	}

	ctx := context.Background()
	now := time.Now()

	// Create test account
	address := keypair.MustRandom().Address()
	_, err = dbConnectionPool.ExecContext(ctx, "INSERT INTO accounts (stellar_address) VALUES ($1)", address)
	require.NoError(t, err)

	// Create test transactions first
	_, err = dbConnectionPool.ExecContext(ctx, `
		INSERT INTO transactions (hash, to_id, envelope_xdr, result_xdr, meta_xdr, ledger_number, ledger_created_at)
		VALUES 
			('tx1', 1, 'env1', 'res1', 'meta1', 1, $1),
			('tx2', 2, 'env2', 'res2', 'meta2', 2, $1),
			('tx3', 3, 'env3', 'res3', 'meta3', 3, $1)
	`, now)
	require.NoError(t, err)

	// Create test operations
	_, err = dbConnectionPool.ExecContext(ctx, `
		INSERT INTO operations (id, tx_hash, operation_type, operation_xdr, ledger_number, ledger_created_at)
		VALUES 
			(1, 'tx1', 'payment', 'xdr1', 1, $1),
			(2, 'tx2', 'create_account', 'xdr2', 2, $1),
			(3, 'tx3', 'payment', 'xdr3', 3, $1)
	`, now)
	require.NoError(t, err)

	// Create test state changes
	_, err = dbConnectionPool.ExecContext(ctx, `
		INSERT INTO state_changes (to_id, state_change_order, state_change_category, ledger_created_at, ledger_number, account_id, operation_id, tx_hash)
		VALUES 
			(1, 1, 'credit', $1, 1, $2, 1, 'tx1'),
			(2, 1, 'debit', $1, 2, $2, 2, 'tx2'),
			(3, 1, 'credit', $1, 3, $2, 1, 'tx3')
	`, now, address)
	require.NoError(t, err)

	// Test BatchGetByStateChangeID
	operations, err := m.BatchGetByStateChangeIDs(ctx, []int64{1, 2, 3}, []int64{1, 1, 1}, "")
	require.NoError(t, err)
	assert.Len(t, operations, 3)

	// Verify operations are for correct state change IDs
	stateChangeIDsFound := make(map[string]int64)
	for _, op := range operations {
		stateChangeIDsFound[op.StateChangeID] = op.ID
	}
	assert.Equal(t, int64(1), stateChangeIDsFound["1-1"])
	assert.Equal(t, int64(2), stateChangeIDsFound["2-1"])
	assert.Equal(t, int64(1), stateChangeIDsFound["3-1"])
}
