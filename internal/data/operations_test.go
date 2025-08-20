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
	nonExistingAccount := keypair.MustRandom()

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
		{
			name:                   "🟡op_with_all_non_existing_accounts_is_ignored",
			useDBTx:                false,
			operations:             []types.Operation{op1},
			stellarAddressesByOpID: map[int64]set.Set[string]{op1.ID: set.NewSet(nonExistingAccount.Address())},
			wantAccountLinks:       map[int64][]string{},
			wantErrContains:        "",
			wantIDs:                nil,
		},
		{
			name:                   "🟡non_existing_account_is_ignored_but_op_and_other_accounts_links_are_inserted",
			useDBTx:                false,
			operations:             []types.Operation{op1},
			stellarAddressesByOpID: map[int64]set.Set[string]{op1.ID: set.NewSet(kp1.Address(), kp2.Address(), nonExistingAccount.Address())},
			wantAccountLinks:       map[int64][]string{op1.ID: {kp1.Address(), kp2.Address()}},
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
	operations, err := m.BatchGetByTxHashes(ctx, []string{"tx1", "tx2"}, "")
	require.NoError(t, err)
	assert.Len(t, operations, 3)

	// Verify operations are for correct tx hashes
	txHashesFound := make(map[string]int)
	for _, op := range operations {
		txHashesFound[op.TxHash]++
	}
	assert.Equal(t, 2, txHashesFound["tx1"])
	assert.Equal(t, 1, txHashesFound["tx2"])
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

func TestOperationModel_BatchGetByStateChangeIDs(t *testing.T) {
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
