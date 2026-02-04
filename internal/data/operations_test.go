package data

import (
	"context"
	"fmt"
	"testing"
	"time"

	set "github.com/deckarep/golang-set/v2"
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

// generateTestOperations creates n test operations for benchmarking.
// It returns a map of operation IDs to addresses.
func generateTestOperations(n int, startID int64) ([]*types.Operation, map[int64]set.Set[string]) {
	ops := make([]*types.Operation, n)
	addressesByOpID := make(map[int64]set.Set[string])
	now := time.Now()

	for i := 0; i < n; i++ {
		opID := startID + int64(i)
		address := keypair.MustRandom().Address()

		ops[i] = &types.Operation{
			ID:              opID,
			OperationType:   types.OperationTypePayment,
			OperationXDR:    fmt.Sprintf("operation_xdr_%d", i),
			LedgerNumber:    uint32(i + 1),
			LedgerCreatedAt: now,
		}
		addressesByOpID[opID] = set.NewSet(address)
	}

	return ops, addressesByOpID
}

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

	// Create referenced transactions first with specific ToIDs
	// Operations IDs must be in TOID range for each transaction: (to_id, to_id + 4096)
	meta1, meta2 := "meta1", "meta2"
	envelope1, envelope2 := "envelope1", "envelope2"
	tx1 := types.Transaction{
		Hash:            "tx1",
		ToID:            4096,
		EnvelopeXDR:     &envelope1,
		FeeCharged:      100,
		ResultCode:      "TransactionResultCodeTxSuccess",
		MetaXDR:         &meta1,
		LedgerNumber:    1,
		LedgerCreatedAt: now,
		IsFeeBump:       false,
	}
	tx2 := types.Transaction{
		Hash:            "tx2",
		ToID:            8192,
		EnvelopeXDR:     &envelope2,
		FeeCharged:      200,
		ResultCode:      "TransactionResultCodeTxSuccess",
		MetaXDR:         &meta2,
		LedgerNumber:    2,
		LedgerCreatedAt: now,
		IsFeeBump:       true,
	}

	// Insert transactions
	sqlxDB, err := dbConnectionPool.SqlxDB(ctx)
	require.NoError(t, err)
	txModel := &TransactionModel{DB: dbConnectionPool, MetricsService: metrics.NewMetricsService(sqlxDB)}
	_, err = txModel.BatchInsert(ctx, nil, []*types.Transaction{&tx1, &tx2}, map[int64]set.Set[string]{
		tx1.ToID: set.NewSet(kp1.Address()),
		tx2.ToID: set.NewSet(kp2.Address()),
	})
	require.NoError(t, err)

	// Operations IDs must be in TOID range: (to_id, to_id + 4096)
	op1 := types.Operation{
		ID:              4097, // in range (4096, 8192)
		OperationType:   types.OperationTypePayment,
		OperationXDR:    "operation1",
		LedgerCreatedAt: now,
	}
	op2 := types.Operation{
		ID:              8193, // in range (8192, 12288)
		OperationType:   types.OperationTypeCreateAccount,
		OperationXDR:    "operation2",
		LedgerCreatedAt: now,
	}

	testCases := []struct {
		name                   string
		useDBTx                bool
		operations             []*types.Operation
		stellarAddressesByOpID map[int64]set.Set[string]
		wantAccountLinks       map[int64][]string
		wantErrContains        string
		wantIDs                []int64
	}{
		{
			name:                   "🟢successful_insert_without_dbTx",
			useDBTx:                false,
			operations:             []*types.Operation{&op1, &op2},
			stellarAddressesByOpID: map[int64]set.Set[string]{op1.ID: set.NewSet(kp1.Address(), kp1.Address(), kp1.Address(), kp1.Address()), op2.ID: set.NewSet(kp2.Address(), kp2.Address())},
			wantAccountLinks:       map[int64][]string{op1.ID: {kp1.Address()}, op2.ID: {kp2.Address()}},
			wantErrContains:        "",
			wantIDs:                []int64{op1.ID, op2.ID},
		},
		{
			name:                   "🟢successful_insert_with_dbTx",
			useDBTx:                true,
			operations:             []*types.Operation{&op1},
			stellarAddressesByOpID: map[int64]set.Set[string]{op1.ID: set.NewSet(kp1.Address())},
			wantAccountLinks:       map[int64][]string{op1.ID: {kp1.Address()}},
			wantErrContains:        "",
			wantIDs:                []int64{op1.ID},
		},
		{
			name:                   "🟢empty_input",
			useDBTx:                false,
			operations:             []*types.Operation{},
			stellarAddressesByOpID: map[int64]set.Set[string]{},
			wantAccountLinks:       map[int64][]string{},
			wantErrContains:        "",
			wantIDs:                nil,
		},
		{
			name:                   "🟡duplicate_operation",
			useDBTx:                false,
			operations:             []*types.Operation{&op1, &op1},
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
				On("ObserveDBQueryDuration", "BatchInsert", "operations", mock.Anything).Return().Once().
				On("ObserveDBQueryDuration", "BatchInsert", "operations_accounts", mock.Anything).Return().Once().
				On("ObserveDBBatchSize", "BatchInsert", "operations", mock.Anything).Return().Once().
				On("IncDBQuery", "BatchInsert", "operations").Return().Once().
				On("IncDBQuery", "BatchInsert", "operations_accounts").Return().Once()
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
					OperationID int64              `db:"operation_id"`
					AccountID   types.AddressBytea `db:"account_id"`
				}
				err = sqlExecuter.SelectContext(ctx, &accountLinks, "SELECT operation_id, account_id FROM operations_accounts ORDER BY operation_id, account_id")
				require.NoError(t, err)

				// Create a map of operation_id -> set of account_ids for O(1) lookups
				accountLinksMap := make(map[int64][]string)
				for _, link := range accountLinks {
					accountLinksMap[link.OperationID] = append(accountLinksMap[link.OperationID], string(link.AccountID))
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

func Test_OperationModel_BatchCopy(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()
	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	ctx := context.Background()
	now := time.Now()

	// Create test accounts
	kp1 := keypair.MustRandom()
	kp2 := keypair.MustRandom()
	const q = "INSERT INTO accounts (stellar_address) SELECT UNNEST(ARRAY[$1, $2])"
	_, err = dbConnectionPool.ExecContext(ctx, q, kp1.Address(), kp2.Address())
	require.NoError(t, err)

	// Create referenced transactions first with specific ToIDs
	// Operations IDs must be in TOID range for each transaction: (to_id, to_id + 4096)
	meta1, meta2 := "meta1", "meta2"
	envelope1, envelope2 := "envelope1", "envelope2"
	tx1 := types.Transaction{
		Hash:            "tx1",
		ToID:            4096,
		EnvelopeXDR:     &envelope1,
		FeeCharged:      100,
		ResultCode:      "TransactionResultCodeTxSuccess",
		MetaXDR:         &meta1,
		LedgerNumber:    1,
		LedgerCreatedAt: now,
		IsFeeBump:       false,
	}
	tx2 := types.Transaction{
		Hash:            "tx2",
		ToID:            8192,
		EnvelopeXDR:     &envelope2,
		FeeCharged:      200,
		ResultCode:      "TransactionResultCodeTxSuccess",
		MetaXDR:         &meta2,
		LedgerNumber:    2,
		LedgerCreatedAt: now,
		IsFeeBump:       true,
	}

	// Insert transactions
	sqlxDB, err := dbConnectionPool.SqlxDB(ctx)
	require.NoError(t, err)
	txModel := &TransactionModel{DB: dbConnectionPool, MetricsService: metrics.NewMetricsService(sqlxDB)}
	_, err = txModel.BatchInsert(ctx, nil, []*types.Transaction{&tx1, &tx2}, map[int64]set.Set[string]{
		tx1.ToID: set.NewSet(kp1.Address()),
		tx2.ToID: set.NewSet(kp2.Address()),
	})
	require.NoError(t, err)

	// Operations IDs must be in TOID range: (to_id, to_id + 4096)
	op1 := types.Operation{
		ID:              4097, // in range (4096, 8192)
		OperationType:   types.OperationTypePayment,
		OperationXDR:    "operation1",
		LedgerCreatedAt: now,
	}
	op2 := types.Operation{
		ID:              8193, // in range (8192, 12288)
		OperationType:   types.OperationTypeCreateAccount,
		OperationXDR:    "operation2",
		LedgerCreatedAt: now,
	}

	testCases := []struct {
		name                   string
		operations             []*types.Operation
		stellarAddressesByOpID map[int64]set.Set[string]
		wantCount              int
		wantErrContains        string
	}{
		{
			name:                   "🟢successful_insert_multiple",
			operations:             []*types.Operation{&op1, &op2},
			stellarAddressesByOpID: map[int64]set.Set[string]{op1.ID: set.NewSet(kp1.Address()), op2.ID: set.NewSet(kp2.Address())},
			wantCount:              2,
		},
		{
			name:                   "🟢empty_input",
			operations:             []*types.Operation{},
			stellarAddressesByOpID: map[int64]set.Set[string]{},
			wantCount:              0,
		},
		{
			name:                   "🟢no_participants",
			operations:             []*types.Operation{&op1},
			stellarAddressesByOpID: map[int64]set.Set[string]{},
			wantCount:              1,
		},
	}

	// Create pgx connection for BatchCopy (requires pgx.Tx, not sqlx.Tx)
	conn, err := pgx.Connect(ctx, dbt.DSN)
	require.NoError(t, err)
	defer conn.Close(ctx)

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Clear the database before each test
			_, err = dbConnectionPool.ExecContext(ctx, "TRUNCATE operations, operations_accounts CASCADE")
			require.NoError(t, err)

			// Create fresh mock for each test case
			mockMetricsService := metrics.NewMockMetricsService()
			// Only set up metric expectations if we have operations to insert
			if len(tc.operations) > 0 {
				mockMetricsService.
					On("ObserveDBQueryDuration", "BatchCopy", "operations", mock.Anything).Return().Once()
				mockMetricsService.
					On("ObserveDBBatchSize", "BatchCopy", "operations", mock.Anything).Return().Once()
				mockMetricsService.
					On("IncDBQuery", "BatchCopy", "operations").Return().Once()
				if len(tc.stellarAddressesByOpID) > 0 {
					mockMetricsService.
						On("IncDBQuery", "BatchCopy", "operations_accounts").Return().Once()
				}
			}
			defer mockMetricsService.AssertExpectations(t)

			m := &OperationModel{
				DB:             dbConnectionPool,
				MetricsService: mockMetricsService,
			}

			// BatchCopy requires a pgx transaction
			pgxTx, err := conn.Begin(ctx)
			require.NoError(t, err)

			gotCount, err := m.BatchCopy(ctx, pgxTx, tc.operations, tc.stellarAddressesByOpID)

			if tc.wantErrContains != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tc.wantErrContains)
				pgxTx.Rollback(ctx)
				return
			}

			require.NoError(t, err)
			require.NoError(t, pgxTx.Commit(ctx))
			assert.Equal(t, tc.wantCount, gotCount)

			// Verify from DB
			var dbInsertedIDs []int64
			err = dbConnectionPool.SelectContext(ctx, &dbInsertedIDs, "SELECT id FROM operations ORDER BY id")
			require.NoError(t, err)
			assert.Len(t, dbInsertedIDs, tc.wantCount)

			// Verify account links if expected
			if len(tc.stellarAddressesByOpID) > 0 && tc.wantCount > 0 {
				var accountLinks []struct {
					OperationID int64  `db:"operation_id"`
					AccountID   string `db:"account_id"`
				}
				err = dbConnectionPool.SelectContext(ctx, &accountLinks, "SELECT operation_id, account_id FROM operations_accounts ORDER BY operation_id, account_id")
				require.NoError(t, err)

				// Create a map of operation_id -> set of account_ids
				accountLinksMap := make(map[int64][]string)
				for _, link := range accountLinks {
					accountLinksMap[link.OperationID] = append(accountLinksMap[link.OperationID], link.AccountID)
				}

				// Verify each expected operation has its account links
				for opID, expectedAddresses := range tc.stellarAddressesByOpID {
					actualAddresses := accountLinksMap[opID]
					assert.ElementsMatch(t, expectedAddresses.ToSlice(), actualAddresses, "account links for op %d don't match", opID)
				}
			}
		})
	}
}

func Test_OperationModel_BatchCopy_DuplicateFails(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()
	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	ctx := context.Background()
	now := time.Now()

	// Create test accounts
	kp1 := keypair.MustRandom()
	const q = "INSERT INTO accounts (stellar_address) VALUES ($1)"
	_, err = dbConnectionPool.ExecContext(ctx, q, kp1.Address())
	require.NoError(t, err)

	// Create a parent transaction that the operation will reference
	_, err = dbConnectionPool.ExecContext(ctx, `
		INSERT INTO transactions (hash, to_id, envelope_xdr, fee_charged, result_code, meta_xdr, ledger_number, ledger_created_at, is_fee_bump)
		VALUES ('tx_for_dup_test', 1, 'env', 100, 'TransactionResultCodeTxSuccess', 'meta', 1, $1, false)
	`, now)
	require.NoError(t, err)

	op1 := types.Operation{
		ID:              999,
		OperationType:   types.OperationTypePayment,
		OperationXDR:    "operation_xdr_dup_test",
		LedgerNumber:    1,
		LedgerCreatedAt: now,
	}

	// Pre-insert the operation using BatchInsert (which uses ON CONFLICT DO NOTHING)
	sqlxDB, err := dbConnectionPool.SqlxDB(ctx)
	require.NoError(t, err)
	opModel := &OperationModel{DB: dbConnectionPool, MetricsService: metrics.NewMetricsService(sqlxDB)}
	_, err = opModel.BatchInsert(ctx, nil, []*types.Operation{&op1}, map[int64]set.Set[string]{
		op1.ID: set.NewSet(kp1.Address()),
	})
	require.NoError(t, err)

	// Verify the operation was inserted
	var count int
	err = dbConnectionPool.GetContext(ctx, &count, "SELECT COUNT(*) FROM operations WHERE id = $1", op1.ID)
	require.NoError(t, err)
	require.Equal(t, 1, count)

	// Now try to insert the same operation using BatchCopy - this should FAIL
	// because COPY does not support ON CONFLICT handling
	mockMetricsService := metrics.NewMockMetricsService()
	mockMetricsService.On("IncDBQueryError", "BatchCopy", "operations", mock.Anything).Return().Once()
	defer mockMetricsService.AssertExpectations(t)

	m := &OperationModel{
		DB:             dbConnectionPool,
		MetricsService: mockMetricsService,
	}

	conn, err := pgx.Connect(ctx, dbt.DSN)
	require.NoError(t, err)
	defer conn.Close(ctx)

	pgxTx, err := conn.Begin(ctx)
	require.NoError(t, err)

	_, err = m.BatchCopy(ctx, pgxTx, []*types.Operation{&op1}, map[int64]set.Set[string]{
		op1.ID: set.NewSet(kp1.Address()),
	})

	// BatchCopy should fail with a unique constraint violation
	require.Error(t, err)
	assert.Contains(t, err.Error(), "pgx CopyFrom operations: ERROR: duplicate key value violates unique constraint \"operations_pkey\"")

	// Rollback the failed transaction
	require.NoError(t, pgxTx.Rollback(ctx))
}

func TestOperationModel_GetAll(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()
	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	mockMetricsService := metrics.NewMockMetricsService()
	mockMetricsService.On("ObserveDBQueryDuration", "GetAll", "operations", mock.Anything).Return()
	mockMetricsService.On("IncDBQuery", "GetAll", "operations").Return()
	defer mockMetricsService.AssertExpectations(t)

	m := &OperationModel{
		DB:             dbConnectionPool,
		MetricsService: mockMetricsService,
	}

	ctx := context.Background()
	now := time.Now()

	// Create test transactions first
	_, err = dbConnectionPool.ExecContext(ctx, `
		INSERT INTO transactions (hash, to_id, envelope_xdr, fee_charged, result_code, meta_xdr, ledger_number, ledger_created_at, is_fee_bump)
		VALUES
			('tx1', 1, 'env1', 100, 'TransactionResultCodeTxSuccess', 'meta1', 1, $1, false),
			('tx2', 2, 'env2', 200, 'TransactionResultCodeTxSuccess', 'meta2', 2, $1, true),
			('tx3', 3, 'env3', 300, 'TransactionResultCodeTxSuccess', 'meta3', 3, $1, false)
	`, now)
	require.NoError(t, err)

	// Create test operations (IDs must be in TOID range for each transaction: (to_id, to_id + 4096))
	_, err = dbConnectionPool.ExecContext(ctx, `
		INSERT INTO operations (id, operation_type, operation_xdr, result_code, successful, ledger_number, ledger_created_at)
		VALUES
			(2, 'PAYMENT', 'xdr1', 'op_success', true, 1, $1),
			(4098, 'CREATE_ACCOUNT', 'xdr2', 'op_success', true, 2, $1),
			(8194, 'PAYMENT', 'xdr3', 'op_success', true, 3, $1)
	`, now)
	require.NoError(t, err)

	// Test GetAll without limit (gets all operations)
	operations, err := m.GetAll(ctx, "", nil, nil, ASC)
	require.NoError(t, err)
	assert.Len(t, operations, 3)
	assert.Equal(t, int64(2), operations[0].Cursor)
	assert.Equal(t, int64(4098), operations[1].Cursor)
	assert.Equal(t, int64(8194), operations[2].Cursor)

	// Test GetAll with smaller limit
	limit := int32(2)
	operations, err = m.GetAll(ctx, "", &limit, nil, ASC)
	require.NoError(t, err)
	assert.Len(t, operations, 2)
	assert.Equal(t, int64(2), operations[0].Cursor)
	assert.Equal(t, int64(4098), operations[1].Cursor)
}

func TestOperationModel_BatchGetByToIDs(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()
	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	ctx := context.Background()
	now := time.Now()

	// Create test transactions first with specific ToIDs
	// ToID encoding: operations for a tx with to_id are in range (to_id, to_id + 4096)
	// Using to_id values: 4096, 8192, 12288 (multiples of 4096 for clarity)
	_, err = dbConnectionPool.ExecContext(ctx, `
		INSERT INTO transactions (hash, to_id, envelope_xdr, fee_charged, result_code, meta_xdr, ledger_number, ledger_created_at, is_fee_bump)
		VALUES
			('tx1', 4096, 'env1', 100, 'TransactionResultCodeTxSuccess', 'meta1', 1, $1, false),
			('tx2', 8192, 'env2', 200, 'TransactionResultCodeTxSuccess', 'meta2', 2, $1, true),
			('tx3', 12288, 'env3', 300, 'TransactionResultCodeTxSuccess', 'meta3', 3, $1, false)
	`, now)
	require.NoError(t, err)

	// Create test operations - IDs must be in TOID range for each transaction
	// For tx1 (to_id=4096): ops 4097, 4098, 4099
	// For tx2 (to_id=8192): ops 8193, 8194
	// For tx3 (to_id=12288): op 12289
	_, err = dbConnectionPool.ExecContext(ctx, `
		INSERT INTO operations (id, operation_type, operation_xdr, result_code, successful, ledger_number, ledger_created_at)
		VALUES
			(4097, 'PAYMENT', 'xdr1', 'op_success', true, 1, $1),
			(8193, 'CREATE_ACCOUNT', 'xdr2', 'op_success', true, 2, $1),
			(4098, 'PAYMENT', 'xdr3', 'op_success', true, 3, $1),
			(4099, 'MANAGE_SELL_OFFER', 'xdr4', 'op_success', true, 4, $1),
			(8194, 'PAYMENT', 'xdr5', 'op_success', true, 5, $1),
			(12289, 'CHANGE_TRUST', 'xdr6', 'op_success', true, 6, $1)
	`, now)
	require.NoError(t, err)

	testCases := []struct {
		name               string
		toIDs              []int64
		limit              *int32
		sortOrder          SortOrder
		expectedCount      int
		expectedToIDCounts map[int64]int // Maps tx_to_id to expected op count
		expectMetricCalls  int
	}{
		{
			name:               "🟢 basic functionality with multiple ToIDs",
			toIDs:              []int64{4096, 8192},
			limit:              nil,
			sortOrder:          ASC,
			expectedCount:      5, // 3 ops for tx1 + 2 ops for tx2
			expectedToIDCounts: map[int64]int{4096: 3, 8192: 2},
			expectMetricCalls:  1,
		},
		{
			name:               "🟢 with limit parameter",
			toIDs:              []int64{4096, 8192},
			limit:              int32Ptr(2),
			sortOrder:          ASC,
			expectedCount:      4, // 2 ops per ToID (limited by ROW_NUMBER)
			expectedToIDCounts: map[int64]int{4096: 2, 8192: 2},
			expectMetricCalls:  1,
		},
		{
			name:               "🟢 DESC sort order",
			toIDs:              []int64{4096},
			limit:              nil,
			sortOrder:          DESC,
			expectedCount:      3,
			expectedToIDCounts: map[int64]int{4096: 3},
			expectMetricCalls:  1,
		},
		{
			name:               "🟢 single transaction",
			toIDs:              []int64{12288},
			limit:              nil,
			sortOrder:          ASC,
			expectedCount:      1,
			expectedToIDCounts: map[int64]int{12288: 1},
			expectMetricCalls:  1,
		},
		{
			name:               "🟡 empty ToIDs array",
			toIDs:              []int64{},
			limit:              nil,
			sortOrder:          ASC,
			expectedCount:      0,
			expectedToIDCounts: map[int64]int{},
			expectMetricCalls:  1,
		},
		{
			name:               "🟡 non-existent ToID",
			toIDs:              []int64{99999},
			limit:              nil,
			sortOrder:          ASC,
			expectedCount:      0,
			expectedToIDCounts: map[int64]int{},
			expectMetricCalls:  1,
		},
		{
			name:               "🟡 mixed existing and non-existent ToIDs",
			toIDs:              []int64{4096, 99999, 8192},
			limit:              nil,
			sortOrder:          ASC,
			expectedCount:      5,
			expectedToIDCounts: map[int64]int{4096: 3, 8192: 2},
			expectMetricCalls:  1,
		},
		{
			name:               "🟢 limit smaller than operations per transaction",
			toIDs:              []int64{4096},
			limit:              int32Ptr(1),
			sortOrder:          ASC,
			expectedCount:      1, // Only first operation due to ROW_NUMBER ranking
			expectedToIDCounts: map[int64]int{4096: 1},
			expectMetricCalls:  1,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mockMetricsService := metrics.NewMockMetricsService()
			mockMetricsService.On("ObserveDBQueryDuration", "BatchGetByToIDs", "operations", mock.Anything).Return().Times(tc.expectMetricCalls)
			mockMetricsService.On("ObserveDBBatchSize", "BatchGetByToIDs", "operations", mock.Anything).Return().Times(tc.expectMetricCalls)
			mockMetricsService.On("IncDBQuery", "BatchGetByToIDs", "operations").Return().Times(tc.expectMetricCalls)
			defer mockMetricsService.AssertExpectations(t)

			m := &OperationModel{
				DB:             dbConnectionPool,
				MetricsService: mockMetricsService,
			}

			operations, err := m.BatchGetByToIDs(ctx, tc.toIDs, "", tc.limit, tc.sortOrder)
			require.NoError(t, err)
			assert.Len(t, operations, tc.expectedCount)

			// Verify operations are for correct ToIDs by deriving tx_to_id from operation ID
			toIDsFound := make(map[int64]int)
			for _, op := range operations {
				txToID := op.ID &^ 0xFFF // Derive tx_to_id using TOID bit masking
				toIDsFound[txToID]++
			}
			assert.Equal(t, tc.expectedToIDCounts, toIDsFound)

			// Verify within-transaction ordering
			if len(operations) > 0 {
				operationsByToID := make(map[int64][]*types.OperationWithCursor)
				for _, op := range operations {
					txToID := op.ID &^ 0xFFF
					operationsByToID[txToID] = append(operationsByToID[txToID], op)
				}

				// Verify ordering within each transaction
				for toID, txOperations := range operationsByToID {
					if len(txOperations) > 1 {
						for i := 1; i < len(txOperations); i++ {
							prevID := txOperations[i-1].ID
							currID := txOperations[i].ID
							// After final transformation, operations should be in ascending ID order within each tx
							assert.True(t, prevID <= currID,
								"operations within tx (to_id=%d) should be ordered by ID: prev=%d, curr=%d",
								toID, prevID, currID)
						}
					}
				}
			}

			// Verify limit behavior when specified
			if tc.limit != nil && len(tc.expectedToIDCounts) > 0 {
				for toID, count := range tc.expectedToIDCounts {
					assert.True(t, count <= int(*tc.limit), "number of operations for to_id=%d should not exceed limit %d", toID, *tc.limit)
				}
			}
		})
	}
}

func int32Ptr(v int32) *int32 {
	return &v
}

func TestOperationModel_BatchGetByToID(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()
	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	mockMetricsService := metrics.NewMockMetricsService()
	mockMetricsService.On("ObserveDBQueryDuration", "BatchGetByToID", "operations", mock.Anything).Return()
	mockMetricsService.On("IncDBQuery", "BatchGetByToID", "operations").Return()
	defer mockMetricsService.AssertExpectations(t)

	m := &OperationModel{
		DB:             dbConnectionPool,
		MetricsService: mockMetricsService,
	}

	ctx := context.Background()
	now := time.Now()

	// Create test transactions first with specific ToIDs
	_, err = dbConnectionPool.ExecContext(ctx, `
		INSERT INTO transactions (hash, to_id, envelope_xdr, fee_charged, result_code, meta_xdr, ledger_number, ledger_created_at, is_fee_bump)
		VALUES
			('tx1', 4096, 'env1', 100, 'TransactionResultCodeTxSuccess', 'meta1', 1, $1, false),
			('tx2', 8192, 'env2', 200, 'TransactionResultCodeTxSuccess', 'meta2', 2, $1, true)
	`, now)
	require.NoError(t, err)

	// Create test operations - IDs must be in TOID range for each transaction
	// For tx1 (to_id=4096): ops 4097, 4098
	// For tx2 (to_id=8192): op 8193
	_, err = dbConnectionPool.ExecContext(ctx, `
		INSERT INTO operations (id, operation_type, operation_xdr, result_code, successful, ledger_number, ledger_created_at)
		VALUES
			(4097, 'PAYMENT', 'xdr1', 'op_success', true, 1, $1),
			(8193, 'CREATE_ACCOUNT', 'xdr2', 'op_success', true, 2, $1),
			(4098, 'PAYMENT', 'xdr3', 'op_success', true, 3, $1)
	`, now)
	require.NoError(t, err)

	// Test BatchGetByToID
	operations, err := m.BatchGetByToID(ctx, 4096, "", nil, nil, ASC)
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
	mockMetricsService.On("ObserveDBQueryDuration", "BatchGetByAccountAddress", "operations", mock.Anything).Return()
	mockMetricsService.On("IncDBQuery", "BatchGetByAccountAddress", "operations").Return()
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
		INSERT INTO transactions (hash, to_id, envelope_xdr, fee_charged, result_code, meta_xdr, ledger_number, ledger_created_at, is_fee_bump)
		VALUES
			('tx1', 4096, 'env1', 100, 'TransactionResultCodeTxSuccess', 'meta1', 1, $1, false),
			('tx2', 8192, 'env2', 200, 'TransactionResultCodeTxSuccess', 'meta2', 2, $1, true),
			('tx3', 12288, 'env3', 300, 'TransactionResultCodeTxSuccess', 'meta3', 3, $1, false)
	`, now)
	require.NoError(t, err)

	// Create test operations (IDs must be in TOID range for each transaction)
	_, err = dbConnectionPool.ExecContext(ctx, `
		INSERT INTO operations (id, operation_type, operation_xdr, result_code, successful, ledger_number, ledger_created_at)
		VALUES
			(4097, 'PAYMENT', 'xdr1', 'op_success', true, 1, $1),
			(8193, 'CREATE_ACCOUNT', 'xdr2', 'op_success', true, 2, $1),
			(12289, 'PAYMENT', 'xdr3', 'op_success', true, 3, $1)
	`, now)
	require.NoError(t, err)

	// Create test operations_accounts links
	_, err = dbConnectionPool.ExecContext(ctx, `
		INSERT INTO operations_accounts (operation_id, account_id)
		VALUES
			(4097, $1),
			(8193, $1),
			(12289, $2)
	`, address1, address2)
	require.NoError(t, err)

	// Test BatchGetByAccount
	operations, err := m.BatchGetByAccountAddress(ctx, address1, "", nil, nil, "ASC")
	require.NoError(t, err)
	assert.Len(t, operations, 2)
	assert.Equal(t, int64(4097), operations[0].Operation.ID)
	assert.Equal(t, int64(8193), operations[1].Operation.ID)
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
		INSERT INTO transactions (hash, to_id, envelope_xdr, fee_charged, result_code, meta_xdr, ledger_number, ledger_created_at, is_fee_bump)
		VALUES
			('tx1', 4096, 'env1', 100, 'TransactionResultCodeTxSuccess', 'meta1', 1, $1, false),
			('tx2', 8192, 'env2', 200, 'TransactionResultCodeTxSuccess', 'meta2', 2, $1, true)
	`, now)
	require.NoError(t, err)

	// Create test operations (IDs must be in TOID range for each transaction)
	_, err = dbConnectionPool.ExecContext(ctx, `
		INSERT INTO operations (id, operation_type, operation_xdr, result_code, successful, ledger_number, ledger_created_at)
		VALUES
			(4097, 'PAYMENT', 'xdr1', 'op_success', true, 1, $1),
			(8193, 'CREATE_ACCOUNT', 'xdr2', 'op_success', true, 2, $1)
	`, now)
	require.NoError(t, err)

	mockMetricsService := metrics.NewMockMetricsService()
	mockMetricsService.On("ObserveDBQueryDuration", "GetByID", "operations", mock.Anything).Return()
	mockMetricsService.On("IncDBQuery", "GetByID", "operations").Return()
	defer mockMetricsService.AssertExpectations(t)

	m := &OperationModel{
		DB:             dbConnectionPool,
		MetricsService: mockMetricsService,
	}

	operation, err := m.GetByID(ctx, 4097, "")
	require.NoError(t, err)
	assert.Equal(t, int64(4097), operation.ID)
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
	mockMetricsService.On("ObserveDBQueryDuration", "BatchGetByStateChangeIDs", "operations", mock.Anything).Return()
	mockMetricsService.On("ObserveDBBatchSize", "BatchGetByStateChangeIDs", "operations", mock.Anything).Return()
	mockMetricsService.On("IncDBQuery", "BatchGetByStateChangeIDs", "operations").Return()
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
		INSERT INTO transactions (hash, to_id, envelope_xdr, fee_charged, result_code, meta_xdr, ledger_number, ledger_created_at, is_fee_bump)
		VALUES
			('tx1', 4096, 'env1', 100, 'TransactionResultCodeTxSuccess', 'meta1', 1, $1, false),
			('tx2', 8192, 'env2', 200, 'TransactionResultCodeTxSuccess', 'meta2', 2, $1, true),
			('tx3', 12288, 'env3', 300, 'TransactionResultCodeTxSuccess', 'meta3', 3, $1, false)
	`, now)
	require.NoError(t, err)

	// Create test operations (IDs must be in TOID range for each transaction)
	_, err = dbConnectionPool.ExecContext(ctx, `
		INSERT INTO operations (id, operation_type, operation_xdr, result_code, successful, ledger_number, ledger_created_at)
		VALUES
			(4097, 'PAYMENT', 'xdr1', 'op_success', true, 1, $1),
			(8193, 'CREATE_ACCOUNT', 'xdr2', 'op_success', true, 2, $1),
			(12289, 'PAYMENT', 'xdr3', 'op_success', true, 3, $1)
	`, now)
	require.NoError(t, err)

	// Create test state changes
	_, err = dbConnectionPool.ExecContext(ctx, `
		INSERT INTO state_changes (to_id, state_change_order, state_change_category, ledger_created_at, ledger_number, account_id, operation_id)
		VALUES
			(4096, 1, 'BALANCE', $1, 1, $2, 4097),
			(8192, 1, 'BALANCE', $1, 2, $2, 8193),
			(12288, 1, 'BALANCE', $1, 3, $2, 4097)
	`, now, address)
	require.NoError(t, err)

	// Test BatchGetByStateChangeID
	operations, err := m.BatchGetByStateChangeIDs(ctx, []int64{4096, 8192, 12288}, []int64{4097, 8193, 4097}, []int64{1, 1, 1}, "")
	require.NoError(t, err)
	assert.Len(t, operations, 3)

	// Verify operations are for correct state change IDs (format: to_id-operation_id-state_change_order)
	stateChangeIDsFound := make(map[string]int64)
	for _, op := range operations {
		stateChangeIDsFound[op.StateChangeID] = op.ID
	}
	assert.Equal(t, int64(4097), stateChangeIDsFound["4096-4097-1"])
	assert.Equal(t, int64(8193), stateChangeIDsFound["8192-8193-1"])
	assert.Equal(t, int64(4097), stateChangeIDsFound["12288-4097-1"])
}

func BenchmarkOperationModel_BatchInsert(b *testing.B) {
	dbt := dbtest.OpenB(b)
	defer dbt.Close()
	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	if err != nil {
		b.Fatalf("failed to open db connection pool: %v", err)
	}
	defer dbConnectionPool.Close()

	ctx := context.Background()
	sqlxDB, err := dbConnectionPool.SqlxDB(ctx)
	if err != nil {
		b.Fatalf("failed to get sqlx db: %v", err)
	}
	metricsService := metrics.NewMetricsService(sqlxDB)

	m := &OperationModel{
		DB:             dbConnectionPool,
		MetricsService: metricsService,
	}

	batchSizes := []int{1000, 5000, 10000, 50000, 100000}

	for _, size := range batchSizes {
		b.Run(fmt.Sprintf("size=%d", size), func(b *testing.B) {
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				b.StopTimer()
				// Clean up operations before each iteration
				//nolint:errcheck // truncate is best-effort cleanup in benchmarks
				dbConnectionPool.ExecContext(ctx, "TRUNCATE operations, operations_accounts CASCADE")
				// Generate fresh test data for each iteration
				ops, addressesByOpID := generateTestOperations(size, int64(i*size))
				b.StartTimer()

				_, err := m.BatchInsert(ctx, nil, ops, addressesByOpID)
				if err != nil {
					b.Fatalf("BatchInsert failed: %v", err)
				}
			}
		})
	}
}

// BenchmarkOperationModel_BatchCopy benchmarks bulk insert using pgx's binary COPY protocol.
func BenchmarkOperationModel_BatchCopy(b *testing.B) {
	dbt := dbtest.OpenB(b)
	defer dbt.Close()
	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	if err != nil {
		b.Fatalf("failed to open db connection pool: %v", err)
	}
	defer dbConnectionPool.Close()

	ctx := context.Background()
	sqlxDB, err := dbConnectionPool.SqlxDB(ctx)
	if err != nil {
		b.Fatalf("failed to get sqlx db: %v", err)
	}
	metricsService := metrics.NewMetricsService(sqlxDB)

	m := &OperationModel{
		DB:             dbConnectionPool,
		MetricsService: metricsService,
	}

	// Create pgx connection for BatchCopy
	conn, err := pgx.Connect(ctx, dbt.DSN)
	if err != nil {
		b.Fatalf("failed to connect with pgx: %v", err)
	}
	defer conn.Close(ctx)

	batchSizes := []int{1000, 5000, 10000, 50000, 100000}

	for _, size := range batchSizes {
		b.Run(fmt.Sprintf("size=%d", size), func(b *testing.B) {
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				b.StopTimer()
				// Clean up operations before each iteration
				_, err = conn.Exec(ctx, "TRUNCATE operations, operations_accounts CASCADE")
				if err != nil {
					b.Fatalf("failed to truncate: %v", err)
				}

				// Generate fresh test data for each iteration
				ops, addressesByOpID := generateTestOperations(size, int64(i*size))

				// Start a pgx transaction
				pgxTx, err := conn.Begin(ctx)
				if err != nil {
					b.Fatalf("failed to begin transaction: %v", err)
				}
				b.StartTimer()

				_, err = m.BatchCopy(ctx, pgxTx, ops, addressesByOpID)
				if err != nil {
					pgxTx.Rollback(ctx)
					b.Fatalf("BatchCopy failed: %v", err)
				}

				b.StopTimer()
				if err := pgxTx.Commit(ctx); err != nil {
					b.Fatalf("failed to commit transaction: %v", err)
				}
				b.StartTimer()
			}
		})
	}
}
