package data

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stellar/go-stellar-sdk/keypair"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/wallet-backend/internal/db"
	"github.com/stellar/wallet-backend/internal/db/dbtest"
	"github.com/stellar/wallet-backend/internal/indexer/types"
	"github.com/stellar/wallet-backend/internal/metrics"
)

// generateTestOperations creates n test operations for benchmarking.
// It returns a map of operation IDs to addresses.
func generateTestOperations(n int, startID int64) ([]*types.Operation, map[int64]types.ParticipantSet) {
	ops := make([]*types.Operation, n)
	addressesByOpID := make(map[int64]types.ParticipantSet)
	now := time.Now()

	for i := 0; i < n; i++ {
		opID := startID + int64(i)
		address := keypair.MustRandom().Address()

		ops[i] = &types.Operation{
			ID:              opID,
			OperationType:   types.OperationTypePayment,
			OperationXDR:    types.XDRBytea([]byte(fmt.Sprintf("operation_xdr_%d", i))),
			LedgerNumber:    uint32(i + 1),
			LedgerCreatedAt: now,
		}
		addressesByOpID[opID] = types.NewParticipantSet(address)
	}

	return ops, addressesByOpID
}

func Test_OperationModel_BatchCopy(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()
	ctx := context.Background()
	dbConnectionPool, err := db.OpenDBConnectionPool(ctx, dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()
	now := time.Now()

	kp1 := keypair.MustRandom()
	kp2 := keypair.MustRandom()

	// Create referenced transactions first with specific ToIDs
	// Operations IDs must be in TOID range for each transaction: (to_id, to_id + 4096)
	tx1 := types.Transaction{
		Hash:            "d176b7b0133690fbfb2de8fa9ca2273cb4f2e29447e0cf0e14a5f82d0daa4877",
		ToID:            4096,
		FeeCharged:      100,
		ResultCode:      "TransactionResultCodeTxSuccess",
		LedgerNumber:    1,
		LedgerCreatedAt: now,
		IsFeeBump:       false,
	}
	tx2 := types.Transaction{
		Hash:            "e176b7b0133690fbfb2de8fa9ca2273cb4f2e29447e0cf0e14a5f82d0daa4877",
		ToID:            8192,
		FeeCharged:      200,
		ResultCode:      "TransactionResultCodeTxSuccess",
		LedgerNumber:    2,
		LedgerCreatedAt: now,
		IsFeeBump:       true,
	}

	// Insert transactions using direct SQL
	_, err = dbConnectionPool.Exec(ctx, `
		INSERT INTO transactions (hash, to_id, fee_charged, result_code, ledger_number, ledger_created_at, is_fee_bump)
		VALUES ($1, $2, $3, $4, $5, $6, $7), ($8, $9, $10, $11, $12, $13, $14)
	`, tx1.Hash, tx1.ToID, tx1.FeeCharged, tx1.ResultCode, tx1.LedgerNumber, tx1.LedgerCreatedAt, tx1.IsFeeBump,
		tx2.Hash, tx2.ToID, tx2.FeeCharged, tx2.ResultCode, tx2.LedgerNumber, tx2.LedgerCreatedAt, tx2.IsFeeBump)
	require.NoError(t, err)

	// Operations IDs must be in TOID range: (to_id, to_id + 4096)
	op1 := types.Operation{
		ID:              4097, // in range (4096, 8192)
		OperationType:   types.OperationTypePayment,
		OperationXDR:    types.XDRBytea([]byte("operation1")),
		LedgerCreatedAt: now,
	}
	op2 := types.Operation{
		ID:              8193, // in range (8192, 12288)
		OperationType:   types.OperationTypeCreateAccount,
		OperationXDR:    types.XDRBytea([]byte("operation2")),
		LedgerCreatedAt: now,
	}

	testCases := []struct {
		name                   string
		operations             []*types.Operation
		stellarAddressesByOpID map[int64]types.ParticipantSet
		wantCount              int
		wantErrContains        string
	}{
		{
			name:                   "🟢successful_insert_multiple",
			operations:             []*types.Operation{&op1, &op2},
			stellarAddressesByOpID: map[int64]types.ParticipantSet{op1.ID: types.NewParticipantSet(kp1.Address()), op2.ID: types.NewParticipantSet(kp2.Address())},
			wantCount:              2,
		},
		{
			name:                   "🟢empty_input",
			operations:             []*types.Operation{},
			stellarAddressesByOpID: map[int64]types.ParticipantSet{},
			wantCount:              0,
		},
		{
			name:                   "🟢no_participants",
			operations:             []*types.Operation{&op1},
			stellarAddressesByOpID: map[int64]types.ParticipantSet{},
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
			_, err = dbConnectionPool.Exec(ctx, "TRUNCATE operations, operations_accounts CASCADE")
			require.NoError(t, err)

			reg := prometheus.NewRegistry()
			dbMetrics := metrics.NewMetrics(reg).DB

			m := &OperationModel{
				DB:      dbConnectionPool,
				Metrics: dbMetrics,
			}

			// BatchCopy requires a pgx transaction
			pgxTx, err := conn.Begin(ctx)
			require.NoError(t, err)

			gotCount, err := m.BatchCopy(ctx, pgxTx, tc.operations)

			if tc.wantErrContains != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tc.wantErrContains)
				pgxTx.Rollback(ctx)
				return
			}

			require.NoError(t, err)
			require.NoError(t, m.BatchCopyAccounts(ctx, pgxTx, tc.operations, tc.stellarAddressesByOpID))
			require.NoError(t, pgxTx.Commit(ctx))
			assert.Equal(t, tc.wantCount, gotCount)

			// Verify from DB
			dbInsertedIDs, err := db.QueryMany[int64](ctx, dbConnectionPool, "SELECT id FROM operations ORDER BY id")
			require.NoError(t, err)
			assert.Len(t, dbInsertedIDs, tc.wantCount)

			// Verify account links if expected
			if len(tc.stellarAddressesByOpID) > 0 && tc.wantCount > 0 {
				type operationAccountLink struct {
					OperationID int64              `db:"operation_id"`
					AccountID   types.AddressBytea `db:"account_id"`
				}
				accountLinks, err := db.QueryMany[operationAccountLink](ctx, dbConnectionPool, "SELECT operation_id, account_id FROM operations_accounts ORDER BY operation_id, account_id")
				require.NoError(t, err)

				// Create a map of operation_id -> set of account_ids
				accountLinksMap := make(map[int64][]string)
				for _, link := range accountLinks {
					accountLinksMap[link.OperationID] = append(accountLinksMap[link.OperationID], string(link.AccountID))
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

func TestOperationModel_GetAll(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()
	ctx := context.Background()
	dbConnectionPool, err := db.OpenDBConnectionPool(ctx, dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	reg := prometheus.NewRegistry()
	dbMetrics := metrics.NewMetrics(reg).DB

	m := &OperationModel{
		DB:      dbConnectionPool,
		Metrics: dbMetrics,
	}

	now := time.Now()

	// Create test transactions first (hash is BYTEA, using valid 64-char hex strings)
	testHash1 := types.HashBytea("0000000000000000000000000000000000000000000000000000000000000001")
	testHash2 := types.HashBytea("0000000000000000000000000000000000000000000000000000000000000002")
	testHash3 := types.HashBytea("0000000000000000000000000000000000000000000000000000000000000003")
	_, err = dbConnectionPool.Exec(ctx, `
		INSERT INTO transactions (hash, to_id, fee_charged, result_code, ledger_number, ledger_created_at, is_fee_bump)
		VALUES
			($2, 1, 100, 'TransactionResultCodeTxSuccess', 1, $1, false),
			($3, 2, 200, 'TransactionResultCodeTxSuccess', 2, $1, true),
			($4, 3, 300, 'TransactionResultCodeTxSuccess', 3, $1, false)
	`, now, testHash1, testHash2, testHash3)
	require.NoError(t, err)

	// Create test operations (IDs must be in TOID range for each transaction: (to_id, to_id + 4096))
	xdr1 := types.XDRBytea([]byte("xdr1"))
	xdr2 := types.XDRBytea([]byte("xdr2"))
	xdr3 := types.XDRBytea([]byte("xdr3"))
	_, err = dbConnectionPool.Exec(ctx, `
		INSERT INTO operations (id, operation_type, operation_xdr, result_code, successful, ledger_number, ledger_created_at)
		VALUES
			(2, 'PAYMENT', $2, 'op_success', true, 1, $1),
			(4098, 'CREATE_ACCOUNT', $3, 'op_success', true, 2, $1),
			(8194, 'PAYMENT', $4, 'op_success', true, 3, $1)
	`, now, xdr1, xdr2, xdr3)
	require.NoError(t, err)

	// Test GetAll without limit (gets all operations)
	operations, err := m.GetAll(ctx, "", nil, nil, ASC)
	require.NoError(t, err)
	assert.Len(t, operations, 3)
	assert.Equal(t, int64(2), operations[0].CompositeCursor.ID)
	assert.Equal(t, int64(4098), operations[1].CompositeCursor.ID)
	assert.Equal(t, int64(8194), operations[2].CompositeCursor.ID)

	// Test GetAll with smaller limit
	limit := int32(2)
	operations, err = m.GetAll(ctx, "", &limit, nil, ASC)
	require.NoError(t, err)
	assert.Len(t, operations, 2)
	assert.Equal(t, int64(2), operations[0].CompositeCursor.ID)
	assert.Equal(t, int64(4098), operations[1].CompositeCursor.ID)
}

func TestOperationModel_BatchGetByToIDs(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()
	ctx := context.Background()
	dbConnectionPool, err := db.OpenDBConnectionPool(ctx, dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	now := time.Now()

	// Create test transactions first with specific ToIDs
	// ToID encoding: operations for a tx with to_id are in range (to_id, to_id + 4096)
	// Using to_id values: 4096, 8192, 12288 (multiples of 4096 for clarity)
	testHash1 := types.HashBytea("0000000000000000000000000000000000000000000000000000000000000001")
	testHash2 := types.HashBytea("0000000000000000000000000000000000000000000000000000000000000002")
	testHash3 := types.HashBytea("0000000000000000000000000000000000000000000000000000000000000003")
	_, err = dbConnectionPool.Exec(ctx, `
		INSERT INTO transactions (hash, to_id, fee_charged, result_code, ledger_number, ledger_created_at, is_fee_bump)
		VALUES
			($2, 4096, 100, 'TransactionResultCodeTxSuccess', 1, $1, false),
			($3, 8192, 200, 'TransactionResultCodeTxSuccess', 2, $1, true),
			($4, 12288, 300, 'TransactionResultCodeTxSuccess', 3, $1, false)
	`, now, testHash1, testHash2, testHash3)
	require.NoError(t, err)

	// Create test operations - IDs must be in TOID range for each transaction
	// For tx1 (to_id=4096): ops 4097, 4098, 4099
	// For tx2 (to_id=8192): ops 8193, 8194
	// For tx3 (to_id=12288): op 12289
	xdr1 := types.XDRBytea([]byte("xdr1"))
	xdr2 := types.XDRBytea([]byte("xdr2"))
	xdr3 := types.XDRBytea([]byte("xdr3"))
	xdr4 := types.XDRBytea([]byte("xdr4"))
	xdr5 := types.XDRBytea([]byte("xdr5"))
	xdr6 := types.XDRBytea([]byte("xdr6"))
	_, err = dbConnectionPool.Exec(ctx, `
		INSERT INTO operations (id, operation_type, operation_xdr, result_code, successful, ledger_number, ledger_created_at)
		VALUES
			(4097, 'PAYMENT', $2, 'op_success', true, 1, $1),
			(8193, 'CREATE_ACCOUNT', $3, 'op_success', true, 2, $1),
			(4098, 'PAYMENT', $4, 'op_success', true, 3, $1),
			(4099, 'MANAGE_SELL_OFFER', $5, 'op_success', true, 4, $1),
			(8194, 'PAYMENT', $6, 'op_success', true, 5, $1),
			(12289, 'CHANGE_TRUST', $7, 'op_success', true, 6, $1)
	`, now, xdr1, xdr2, xdr3, xdr4, xdr5, xdr6)
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
			reg := prometheus.NewRegistry()
			dbMetrics := metrics.NewMetrics(reg).DB

			m := &OperationModel{
				DB:      dbConnectionPool,
				Metrics: dbMetrics,
			}

			operations, err := m.BatchGetByToIDs(ctx, tc.toIDs, "", tc.limit, tc.sortOrder)
			require.NoError(t, err)
			assert.Len(t, operations, tc.expectedCount)

			// Verify operations are for correct ToIDs by deriving tx_to_id from operation ID
			toIDsFound := make(map[int64]int)
			for _, op := range operations {
				txToID := op.Operation.ID &^ 0xFFF // Derive tx_to_id using TOID bit masking
				toIDsFound[txToID]++
			}
			assert.Equal(t, tc.expectedToIDCounts, toIDsFound)

			// Verify within-transaction ordering
			if len(operations) > 0 {
				operationsByToID := make(map[int64][]*types.OperationWithCursor)
				for _, op := range operations {
					txToID := op.Operation.ID &^ 0xFFF
					operationsByToID[txToID] = append(operationsByToID[txToID], op)
				}

				// Verify ordering within each transaction
				for toID, txOperations := range operationsByToID {
					if len(txOperations) > 1 {
						for i := 1; i < len(txOperations); i++ {
							prevID := txOperations[i-1].Operation.ID
							currID := txOperations[i].Operation.ID
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
	ctx := context.Background()
	dbConnectionPool, err := db.OpenDBConnectionPool(ctx, dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	reg := prometheus.NewRegistry()
	dbMetrics := metrics.NewMetrics(reg).DB

	m := &OperationModel{
		DB:      dbConnectionPool,
		Metrics: dbMetrics,
	}

	now := time.Now()

	// Create test transactions first with specific ToIDs (hash is BYTEA)
	testHash1 := types.HashBytea("0000000000000000000000000000000000000000000000000000000000000001")
	testHash2 := types.HashBytea("0000000000000000000000000000000000000000000000000000000000000002")
	_, err = dbConnectionPool.Exec(ctx, `
		INSERT INTO transactions (hash, to_id, fee_charged, result_code, ledger_number, ledger_created_at, is_fee_bump)
		VALUES
			($2, 4096, 100, 'TransactionResultCodeTxSuccess', 1, $1, false),
			($3, 8192, 200, 'TransactionResultCodeTxSuccess', 2, $1, true)
	`, now, testHash1, testHash2)
	require.NoError(t, err)

	// Create test operations - IDs must be in TOID range for each transaction
	// For tx1 (to_id=4096): ops 4097, 4098
	// For tx2 (to_id=8192): op 8193
	xdr1 := types.XDRBytea([]byte("xdr1"))
	xdr2 := types.XDRBytea([]byte("xdr2"))
	xdr3 := types.XDRBytea([]byte("xdr3"))
	_, err = dbConnectionPool.Exec(ctx, `
		INSERT INTO operations (id, operation_type, operation_xdr, result_code, successful, ledger_number, ledger_created_at)
		VALUES
			(4097, 'PAYMENT', $2, 'op_success', true, 1, $1),
			(8193, 'CREATE_ACCOUNT', $3, 'op_success', true, 2, $1),
			(4098, 'PAYMENT', $4, 'op_success', true, 3, $1)
	`, now, xdr1, xdr2, xdr3)
	require.NoError(t, err)

	// Test BatchGetByToID
	operations, err := m.BatchGetByToID(ctx, 4096, "", nil, nil, ASC)
	require.NoError(t, err)
	assert.Len(t, operations, 2)
	assert.Equal(t, xdr1.String(), operations[0].OperationXDR.String())
	assert.Equal(t, xdr3.String(), operations[1].OperationXDR.String())
}

func TestOperationModel_BatchGetByAccountAddresses(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()
	ctx := context.Background()
	dbConnectionPool, err := db.OpenDBConnectionPool(ctx, dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	reg := prometheus.NewRegistry()
	dbMetrics := metrics.NewMetrics(reg).DB

	m := &OperationModel{
		DB:      dbConnectionPool,
		Metrics: dbMetrics,
	}

	now := time.Now()

	address1 := keypair.MustRandom().Address()
	address2 := keypair.MustRandom().Address()

	// Create test transactions first (hash is BYTEA)
	testHash1 := types.HashBytea("0000000000000000000000000000000000000000000000000000000000000001")
	testHash2 := types.HashBytea("0000000000000000000000000000000000000000000000000000000000000002")
	testHash3 := types.HashBytea("0000000000000000000000000000000000000000000000000000000000000003")
	_, err = dbConnectionPool.Exec(ctx, `
		INSERT INTO transactions (hash, to_id, fee_charged, result_code, ledger_number, ledger_created_at, is_fee_bump)
		VALUES
			($2, 4096, 100, 'TransactionResultCodeTxSuccess', 1, $1, false),
			($3, 8192, 200, 'TransactionResultCodeTxSuccess', 2, $1, true),
			($4, 12288, 300, 'TransactionResultCodeTxSuccess', 3, $1, false)
	`, now, testHash1, testHash2, testHash3)
	require.NoError(t, err)

	// Create test operations (IDs must be in TOID range for each transaction)
	xdr1 := types.XDRBytea([]byte("xdr1"))
	xdr2 := types.XDRBytea([]byte("xdr2"))
	xdr3 := types.XDRBytea([]byte("xdr3"))
	_, err = dbConnectionPool.Exec(ctx, `
		INSERT INTO operations (id, operation_type, operation_xdr, result_code, successful, ledger_number, ledger_created_at)
		VALUES
			(4097, 'PAYMENT', $2, 'op_success', true, 1, $1),
			(8193, 'CREATE_ACCOUNT', $3, 'op_success', true, 2, $1),
			(12289, 'PAYMENT', $4, 'op_success', true, 3, $1)
	`, now, xdr1, xdr2, xdr3)
	require.NoError(t, err)

	// Create test operations_accounts links
	_, err = dbConnectionPool.Exec(ctx, `
		INSERT INTO operations_accounts (ledger_created_at, operation_id, account_id)
		VALUES
			($3, 4097, $1),
			($3, 8193, $1),
			($3, 12289, $2)
	`, types.AddressBytea(address1), types.AddressBytea(address2), now)
	require.NoError(t, err)

	// Test BatchGetByAccount
	operations, err := m.BatchGetByAccountAddress(ctx, address1, "", nil, nil, ASC, nil)
	require.NoError(t, err)
	assert.Len(t, operations, 2)
	assert.Equal(t, int64(4097), operations[0].Operation.ID)
	assert.Equal(t, int64(8193), operations[1].Operation.ID)
}

func TestOperationModel_GetByID(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()
	ctx := context.Background()
	dbConnectionPool, err := db.OpenDBConnectionPool(ctx, dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	now := time.Now()

	// Create test transactions first (hash is BYTEA)
	testHash1 := types.HashBytea("0000000000000000000000000000000000000000000000000000000000000001")
	testHash2 := types.HashBytea("0000000000000000000000000000000000000000000000000000000000000002")
	_, err = dbConnectionPool.Exec(ctx, `
		INSERT INTO transactions (hash, to_id, fee_charged, result_code, ledger_number, ledger_created_at, is_fee_bump)
		VALUES
			($2, 4096, 100, 'TransactionResultCodeTxSuccess', 1, $1, false),
			($3, 8192, 200, 'TransactionResultCodeTxSuccess', 2, $1, true)
	`, now, testHash1, testHash2)
	require.NoError(t, err)

	// Create test operations (IDs must be in TOID range for each transaction)
	opXdr1 := types.XDRBytea([]byte("xdr1"))
	opXdr2 := types.XDRBytea([]byte("xdr2"))
	_, err = dbConnectionPool.Exec(ctx, `
		INSERT INTO operations (id, operation_type, operation_xdr, result_code, successful, ledger_number, ledger_created_at)
		VALUES
			(4097, 'PAYMENT', $2, 'op_success', true, 1, $1),
			(8193, 'CREATE_ACCOUNT', $3, 'op_success', true, 2, $1)
	`, now, opXdr1, opXdr2)
	require.NoError(t, err)

	reg := prometheus.NewRegistry()
	dbMetrics := metrics.NewMetrics(reg).DB

	m := &OperationModel{
		DB:      dbConnectionPool,
		Metrics: dbMetrics,
	}

	operation, err := m.GetByID(ctx, 4097, "")
	require.NoError(t, err)
	assert.Equal(t, int64(4097), operation.ID)
	assert.Equal(t, opXdr1.String(), operation.OperationXDR.String())
	assert.Equal(t, uint32(1), operation.LedgerNumber)
	assert.WithinDuration(t, now, operation.LedgerCreatedAt, time.Second)
}

func TestOperationModel_BatchGetByStateChangeIDs(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()
	ctx := context.Background()
	dbConnectionPool, err := db.OpenDBConnectionPool(ctx, dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	reg := prometheus.NewRegistry()
	dbMetrics := metrics.NewMetrics(reg).DB

	m := &OperationModel{
		DB:      dbConnectionPool,
		Metrics: dbMetrics,
	}

	now := time.Now()

	address := keypair.MustRandom().Address()

	// Create test transactions first (hash is BYTEA)
	testHash1 := types.HashBytea("0000000000000000000000000000000000000000000000000000000000000001")
	testHash2 := types.HashBytea("0000000000000000000000000000000000000000000000000000000000000002")
	testHash3 := types.HashBytea("0000000000000000000000000000000000000000000000000000000000000003")
	_, err = dbConnectionPool.Exec(ctx, `
		INSERT INTO transactions (hash, to_id, fee_charged, result_code, ledger_number, ledger_created_at, is_fee_bump)
		VALUES
			($2, 4096, 100, 'TransactionResultCodeTxSuccess', 1, $1, false),
			($3, 8192, 200, 'TransactionResultCodeTxSuccess', 2, $1, true),
			($4, 12288, 300, 'TransactionResultCodeTxSuccess', 3, $1, false)
	`, now, testHash1, testHash2, testHash3)
	require.NoError(t, err)

	// Create test operations (IDs must be in TOID range for each transaction)
	xdr1 := types.XDRBytea([]byte("xdr1"))
	xdr2 := types.XDRBytea([]byte("xdr2"))
	xdr3 := types.XDRBytea([]byte("xdr3"))
	_, err = dbConnectionPool.Exec(ctx, `
		INSERT INTO operations (id, operation_type, operation_xdr, result_code, successful, ledger_number, ledger_created_at)
		VALUES
			(4097, 'PAYMENT', $2, 'op_success', true, 1, $1),
			(8193, 'CREATE_ACCOUNT', $3, 'op_success', true, 2, $1),
			(12289, 'PAYMENT', $4, 'op_success', true, 3, $1)
	`, now, xdr1, xdr2, xdr3)
	require.NoError(t, err)

	// Create test state changes
	_, err = dbConnectionPool.Exec(ctx, `
		INSERT INTO state_changes (to_id, state_change_id, state_change_category, state_change_reason, ledger_created_at, ledger_number, account_id, operation_id)
		VALUES
			(4096, 1, 'BALANCE', 'CREDIT', $1, 1, $2, 4097),
			(8192, 1, 'BALANCE', 'CREDIT', $1, 2, $2, 8193),
			(12288, 1, 'BALANCE', 'CREDIT', $1, 3, $2, 4097)
	`, now, address)
	require.NoError(t, err)

	// Test BatchGetByStateChangeID
	operations, err := m.BatchGetByStateChangeIDs(ctx, []int64{4096, 8192, 12288}, []int64{4097, 8193, 4097}, []int64{1, 1, 1}, "")
	require.NoError(t, err)
	assert.Len(t, operations, 3)

	// Verify operations are for correct state change IDs (format: to_id-operation_id-state_change_id)
	stateChangeIDsFound := make(map[string]int64)
	for _, op := range operations {
		stateChangeIDsFound[op.StateChangeID] = op.ID
	}
	assert.Equal(t, int64(4097), stateChangeIDsFound["4096-4097-1"])
	assert.Equal(t, int64(8193), stateChangeIDsFound["8192-8193-1"])
	assert.Equal(t, int64(4097), stateChangeIDsFound["12288-4097-1"])
}

// BenchmarkOperationModel_BatchCopy benchmarks bulk insert using pgx's binary COPY protocol.
func BenchmarkOperationModel_BatchCopy(b *testing.B) {
	dbt := dbtest.OpenB(b)
	defer dbt.Close()
	ctx := context.Background()
	dbConnectionPool, err := db.OpenDBConnectionPool(ctx, dbt.DSN)
	if err != nil {
		b.Fatalf("failed to open db connection pool: %v", err)
	}
	defer dbConnectionPool.Close()

	reg := prometheus.NewRegistry()
	dbMetrics := metrics.NewMetrics(reg).DB

	m := &OperationModel{
		DB:      dbConnectionPool,
		Metrics: dbMetrics,
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

				_, err = m.BatchCopy(ctx, pgxTx, ops)
				if err != nil {
					pgxTx.Rollback(ctx)
					b.Fatalf("BatchCopy failed: %v", err)
				}
				if err = m.BatchCopyAccounts(ctx, pgxTx, ops, addressesByOpID); err != nil {
					pgxTx.Rollback(ctx)
					b.Fatalf("BatchCopyAccounts failed: %v", err)
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
