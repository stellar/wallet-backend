package data

import (
	"context"
	"database/sql"
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

// generateTestStateChanges creates n test state changes for benchmarking.
// Populates all fields to provide an upper-bound benchmark.
func generateTestStateChanges(n int, txHash string, accountID string, startToID int64) []types.StateChange {
	scs := make([]types.StateChange, n)
	now := time.Now()
	reason := types.StateChangeReasonCredit

	for i := 0; i < n; i++ {
		scs[i] = types.StateChange{
			ToID:                startToID + int64(i),
			StateChangeOrder:    1,
			StateChangeCategory: types.StateChangeCategoryBalance,
			StateChangeReason:   &reason,
			LedgerCreatedAt:     now,
			LedgerNumber:        uint32(i + 1),
			AccountID:           accountID,
			OperationID:         int64(i + 1),
			TxHash:              txHash,
			// sql.NullString fields
			TokenID:            sql.NullString{String: fmt.Sprintf("token_%d", i), Valid: true},
			Amount:             sql.NullString{String: fmt.Sprintf("%d", (i+1)*100), Valid: true},
			OfferID:            sql.NullString{String: fmt.Sprintf("offer_%d", i), Valid: true},
			SignerAccountID:    sql.NullString{String: fmt.Sprintf("GSIGNER%032d", i), Valid: true},
			SpenderAccountID:   sql.NullString{String: fmt.Sprintf("GSPENDER%031d", i), Valid: true},
			SponsoredAccountID: sql.NullString{String: fmt.Sprintf("GSPONSORED%028d", i), Valid: true},
			SponsorAccountID:   sql.NullString{String: fmt.Sprintf("GSPONSOR%030d", i), Valid: true},
			DeployerAccountID:  sql.NullString{String: fmt.Sprintf("GDEPLOYER%029d", i), Valid: true},
			FunderAccountID:    sql.NullString{String: fmt.Sprintf("GFUNDER%031d", i), Valid: true},
			// Typed fields (previously JSONB)
			SignerWeightOld:   sql.NullInt16{Int16: int16(i), Valid: true},
			SignerWeightNew:   sql.NullInt16{Int16: int16(i + 1), Valid: true},
			ThresholdOld:      sql.NullInt16{Int16: 1, Valid: true},
			ThresholdNew:      sql.NullInt16{Int16: 2, Valid: true},
			TrustlineLimitOld: sql.NullString{String: fmt.Sprintf("%d", i*1000), Valid: true},
			TrustlineLimitNew: sql.NullString{String: fmt.Sprintf("%d", (i+1)*1000), Valid: true},
			Flags:             sql.NullInt16{Int16: 6, Valid: true}, // Bitmask for auth_required (2) | auth_revocable (4)
			KeyValue:          types.NullableJSONB{"key": fmt.Sprintf("data_key_%d", i), "value": fmt.Sprintf("data_value_%d", i)},
		}
	}

	return scs
}

func TestStateChangeModel_BatchInsert(t *testing.T) {
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
	meta1, meta2 := "meta1", "meta2"
	envelope1, envelope2 := "envelope1", "envelope2"
	tx1 := types.Transaction{
		Hash:            "tx1",
		ToID:            1,
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
		ToID:            2,
		EnvelopeXDR:     &envelope2,
		FeeCharged:      200,
		ResultCode:      "TransactionResultCodeTxSuccess",
		MetaXDR:         &meta2,
		LedgerNumber:    2,
		LedgerCreatedAt: now,
		IsFeeBump:       true,
	}
	sqlxDB, err := dbConnectionPool.SqlxDB(ctx)
	require.NoError(t, err)
	txModel := &TransactionModel{DB: dbConnectionPool, MetricsService: metrics.NewMetricsService(sqlxDB)}
	_, err = txModel.BatchInsert(ctx, nil, []*types.Transaction{&tx1, &tx2}, map[string]set.Set[string]{
		tx1.Hash: set.NewSet(kp1.Address()),
		tx2.Hash: set.NewSet(kp2.Address()),
	})
	require.NoError(t, err)

	reason := types.StateChangeReasonAdd
	sc1 := types.StateChange{
		ToID:                1,
		StateChangeOrder:    1,
		StateChangeCategory: types.StateChangeCategoryBalance,
		StateChangeReason:   &reason,
		LedgerCreatedAt:     now,
		LedgerNumber:        1,
		AccountID:           kp1.Address(),
		OperationID:         123,
		TxHash:              tx1.Hash,
		TokenID:             sql.NullString{String: "token1", Valid: true},
		Amount:              sql.NullString{String: "100", Valid: true},
	}
	sc2 := types.StateChange{
		ToID:                2,
		StateChangeOrder:    1,
		StateChangeCategory: types.StateChangeCategoryBalance,
		StateChangeReason:   &reason,
		LedgerCreatedAt:     now,
		LedgerNumber:        2,
		AccountID:           kp2.Address(),
		OperationID:         456,
		TxHash:              tx2.Hash,
	}

	testCases := []struct {
		name            string
		useDBTx         bool
		stateChanges    []types.StateChange
		wantIDs         []string
		wantErrContains string
	}{
		{
			name:         "🟢successful_insert_without_dbTx",
			useDBTx:      false,
			stateChanges: []types.StateChange{sc1, sc2},
			wantIDs:      []string{fmt.Sprintf("%d-%d", sc1.ToID, sc1.StateChangeOrder), fmt.Sprintf("%d-%d", sc2.ToID, sc2.StateChangeOrder)},
		},
		{
			name:         "🟢successful_insert_with_dbTx",
			useDBTx:      true,
			stateChanges: []types.StateChange{sc1},
			wantIDs:      []string{fmt.Sprintf("%d-%d", sc1.ToID, sc1.StateChangeOrder)},
		},
		{
			name:         "🟢empty_input",
			useDBTx:      false,
			stateChanges: []types.StateChange{},
			wantIDs:      nil,
		},
		{
			name:         "🟡duplicate_state_change",
			useDBTx:      false,
			stateChanges: []types.StateChange{sc1, sc1},
			wantIDs:      []string{fmt.Sprintf("%d-%d", sc1.ToID, sc1.StateChangeOrder)},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			_, err = dbConnectionPool.ExecContext(ctx, "TRUNCATE state_changes CASCADE")
			require.NoError(t, err)

			mockMetricsService := metrics.NewMockMetricsService()
			mockMetricsService.
				On("ObserveDBQueryDuration", "BatchInsert", "state_changes", mock.Anything).Return().Once()
			mockMetricsService.
				On("ObserveDBBatchSize", "BatchInsert", "state_changes", mock.Anything).Return().Once()
			mockMetricsService.
				On("IncDBQuery", "BatchInsert", "state_changes").Return().Once()

			m := &StateChangeModel{
				DB:             dbConnectionPool,
				MetricsService: mockMetricsService,
			}

			var sqlExecuter db.SQLExecuter = dbConnectionPool
			if tc.useDBTx {
				tx, err := dbConnectionPool.BeginTxx(ctx, nil)
				require.NoError(t, err)
				defer tx.Rollback() // nolint: errcheck
				sqlExecuter = tx
			}

			gotInsertedIDs, err := m.BatchInsert(ctx, sqlExecuter, tc.stateChanges)

			if tc.wantErrContains != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tc.wantErrContains)
				return
			}

			require.NoError(t, err)
			assert.ElementsMatch(t, tc.wantIDs, gotInsertedIDs)

			// Verify from DB
			var dbInsertedIDs []string
			err = sqlExecuter.SelectContext(ctx, &dbInsertedIDs, "SELECT CONCAT(to_id, '-', state_change_order) FROM state_changes")
			require.NoError(t, err)
			assert.ElementsMatch(t, tc.wantIDs, dbInsertedIDs)

			mockMetricsService.AssertExpectations(t)
		})
	}
}

func TestStateChangeModel_BatchCopy(t *testing.T) {
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

	// Create referenced transactions first
	meta1, meta2 := "meta1", "meta2"
	envelope1, envelope2 := "envelope1", "envelope2"
	tx1 := types.Transaction{
		Hash:            "tx1",
		ToID:            1,
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
		ToID:            2,
		EnvelopeXDR:     &envelope2,
		FeeCharged:      200,
		ResultCode:      "TransactionResultCodeTxSuccess",
		MetaXDR:         &meta2,
		LedgerNumber:    2,
		LedgerCreatedAt: now,
		IsFeeBump:       true,
	}
	sqlxDB, err := dbConnectionPool.SqlxDB(ctx)
	require.NoError(t, err)
	txModel := &TransactionModel{DB: dbConnectionPool, MetricsService: metrics.NewMetricsService(sqlxDB)}
	_, err = txModel.BatchInsert(ctx, nil, []*types.Transaction{&tx1, &tx2}, map[string]set.Set[string]{
		tx1.Hash: set.NewSet(kp1.Address()),
		tx2.Hash: set.NewSet(kp2.Address()),
	})
	require.NoError(t, err)

	reason := types.StateChangeReasonAdd
	sc1 := types.StateChange{
		ToID:                1,
		StateChangeOrder:    1,
		StateChangeCategory: types.StateChangeCategoryBalance,
		StateChangeReason:   &reason,
		LedgerCreatedAt:     now,
		LedgerNumber:        1,
		AccountID:           kp1.Address(),
		OperationID:         123,
		TxHash:              tx1.Hash,
		TokenID:             sql.NullString{String: "token1", Valid: true},
		Amount:              sql.NullString{String: "100", Valid: true},
	}
	sc2 := types.StateChange{
		ToID:                2,
		StateChangeOrder:    1,
		StateChangeCategory: types.StateChangeCategoryBalance,
		StateChangeReason:   &reason,
		LedgerCreatedAt:     now,
		LedgerNumber:        2,
		AccountID:           kp2.Address(),
		OperationID:         456,
		TxHash:              tx2.Hash,
	}
	// State change with typed signer/threshold fields
	sc3 := types.StateChange{
		ToID:                3,
		StateChangeOrder:    1,
		StateChangeCategory: types.StateChangeCategorySigner,
		StateChangeReason:   nil,
		LedgerCreatedAt:     now,
		LedgerNumber:        3,
		AccountID:           kp1.Address(),
		OperationID:         789,
		TxHash:              tx1.Hash,
		SignerWeightOld:     sql.NullInt16{Int16: 0, Valid: true},
		SignerWeightNew:     sql.NullInt16{Int16: 10, Valid: true},
		ThresholdOld:        sql.NullInt16{Int16: 1, Valid: true},
		ThresholdNew:        sql.NullInt16{Int16: 3, Valid: true},
	}

	testCases := []struct {
		name            string
		stateChanges    []types.StateChange
		wantCount       int
		wantErrContains string
	}{
		{
			name:         "🟢successful_insert_multiple",
			stateChanges: []types.StateChange{sc1, sc2},
			wantCount:    2,
		},
		{
			name:         "🟢empty_input",
			stateChanges: []types.StateChange{},
			wantCount:    0,
		},
		{
			name:         "🟢nullable_fields",
			stateChanges: []types.StateChange{sc2},
			wantCount:    1,
		},
		{
			name:         "🟢jsonb_fields",
			stateChanges: []types.StateChange{sc3},
			wantCount:    1,
		},
	}

	// Create pgx connection for BatchCopy (requires pgx.Tx, not sqlx.Tx)
	conn, err := pgx.Connect(ctx, dbt.DSN)
	require.NoError(t, err)
	defer conn.Close(ctx)

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Clear the database before each test
			_, err = dbConnectionPool.ExecContext(ctx, "TRUNCATE state_changes CASCADE")
			require.NoError(t, err)

			// Create fresh mock for each test case
			mockMetricsService := metrics.NewMockMetricsService()
			// Only set up metric expectations if we have state changes to insert
			if len(tc.stateChanges) > 0 {
				mockMetricsService.
					On("ObserveDBQueryDuration", "BatchCopy", "state_changes", mock.Anything).Return().Once()
				mockMetricsService.
					On("ObserveDBBatchSize", "BatchCopy", "state_changes", mock.Anything).Return().Once()
				mockMetricsService.
					On("IncDBQuery", "BatchCopy", "state_changes").Return().Once()
			}
			defer mockMetricsService.AssertExpectations(t)

			m := &StateChangeModel{
				DB:             dbConnectionPool,
				MetricsService: mockMetricsService,
			}

			// BatchCopy requires a pgx transaction
			pgxTx, err := conn.Begin(ctx)
			require.NoError(t, err)

			gotCount, err := m.BatchCopy(ctx, pgxTx, tc.stateChanges)

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
			var dbInsertedIDs []string
			err = dbConnectionPool.SelectContext(ctx, &dbInsertedIDs, "SELECT CONCAT(to_id, '-', state_change_order) FROM state_changes ORDER BY to_id")
			require.NoError(t, err)
			assert.Len(t, dbInsertedIDs, tc.wantCount)
		})
	}
}

func TestStateChangeModel_BatchCopy_DuplicateFails(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()
	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	ctx := context.Background()
	now := time.Now()

	// Create test account
	kp1 := keypair.MustRandom()
	const q = "INSERT INTO accounts (stellar_address) VALUES ($1)"
	_, err = dbConnectionPool.ExecContext(ctx, q, kp1.Address())
	require.NoError(t, err)

	// Create parent transaction
	_, err = dbConnectionPool.ExecContext(ctx, `
		INSERT INTO transactions (hash, to_id, envelope_xdr, fee_charged, result_code, meta_xdr, ledger_number, ledger_created_at, is_fee_bump)
		VALUES ('tx_for_sc_dup_test', 1, 'env', 100, 'TransactionResultCodeTxSuccess', 'meta', 1, $1, false)
	`, now)
	require.NoError(t, err)

	reason := types.StateChangeReasonCredit
	sc1 := types.StateChange{
		ToID:                999,
		StateChangeOrder:    1,
		StateChangeCategory: types.StateChangeCategoryBalance,
		StateChangeReason:   &reason,
		LedgerCreatedAt:     now,
		LedgerNumber:        1,
		AccountID:           kp1.Address(),
		OperationID:         123,
		TxHash:              "tx_for_sc_dup_test",
	}

	// Pre-insert the state change using BatchInsert (which uses ON CONFLICT DO NOTHING)
	sqlxDB, err := dbConnectionPool.SqlxDB(ctx)
	require.NoError(t, err)
	scModel := &StateChangeModel{DB: dbConnectionPool, MetricsService: metrics.NewMetricsService(sqlxDB)}
	_, err = scModel.BatchInsert(ctx, nil, []types.StateChange{sc1})
	require.NoError(t, err)

	// Verify the state change was inserted
	var count int
	err = dbConnectionPool.GetContext(ctx, &count, "SELECT COUNT(*) FROM state_changes WHERE to_id = $1 AND state_change_order = $2", sc1.ToID, sc1.StateChangeOrder)
	require.NoError(t, err)
	require.Equal(t, 1, count)

	// Now try to insert the same state change using BatchCopy - this should FAIL
	// because COPY does not support ON CONFLICT handling
	mockMetricsService := metrics.NewMockMetricsService()
	mockMetricsService.On("IncDBQueryError", "BatchCopy", "state_changes", mock.Anything).Return().Once()
	defer mockMetricsService.AssertExpectations(t)

	m := &StateChangeModel{
		DB:             dbConnectionPool,
		MetricsService: mockMetricsService,
	}

	conn, err := pgx.Connect(ctx, dbt.DSN)
	require.NoError(t, err)
	defer conn.Close(ctx)

	pgxTx, err := conn.Begin(ctx)
	require.NoError(t, err)

	_, err = m.BatchCopy(ctx, pgxTx, []types.StateChange{sc1})

	// BatchCopy should fail with a unique constraint violation
	require.Error(t, err)
	assert.Contains(t, err.Error(), "pgx CopyFrom state_changes: ERROR: duplicate key value violates unique constraint \"state_changes_pkey\"")

	// Rollback the failed transaction
	require.NoError(t, pgxTx.Rollback(ctx))
}

func TestStateChangeModel_BatchGetByAccountAddress(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()
	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

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
			('tx1', 1, 'env1', 100, 'TransactionResultCodeTxSuccess', 'meta1', 1, $1, false),
			('tx2', 2, 'env2', 200, 'TransactionResultCodeTxSuccess', 'meta2', 2, $1, true),
			('tx3', 3, 'env3', 300, 'TransactionResultCodeTxSuccess', 'meta3', 3, $1, false)
	`, now)
	require.NoError(t, err)

	// Create test state changes
	_, err = dbConnectionPool.ExecContext(ctx, `
		INSERT INTO state_changes (to_id, state_change_order, state_change_category, ledger_created_at, ledger_number, account_id, operation_id, tx_hash)
		VALUES 
			(1, 1, 'credit', $1, 1, $2, 123, 'tx1'),
			(2, 1, 'debit', $1, 2, $2, 456, 'tx2'),
			(3, 1, 'credit', $1, 3, $3, 789, 'tx3')
	`, now, address1, address2)
	require.NoError(t, err)

	mockMetricsService := metrics.NewMockMetricsService()
	mockMetricsService.On("ObserveDBQueryDuration", "BatchGetByAccountAddress", "state_changes", mock.Anything).Return().Times(2)
	mockMetricsService.On("IncDBQuery", "BatchGetByAccountAddress", "state_changes").Return().Times(2)
	defer mockMetricsService.AssertExpectations(t)

	m := &StateChangeModel{
		DB:             dbConnectionPool,
		MetricsService: mockMetricsService,
	}

	// Test BatchGetByAccount for address1
	stateChanges, err := m.BatchGetByAccountAddress(ctx, address1, nil, nil, nil, nil, "", nil, nil, ASC)
	require.NoError(t, err)
	assert.Len(t, stateChanges, 2)
	for _, sc := range stateChanges {
		assert.Equal(t, address1, sc.AccountID)
	}

	// Test BatchGetByAccount for address2
	stateChanges, err = m.BatchGetByAccountAddress(ctx, address2, nil, nil, nil, nil, "", nil, nil, ASC)
	require.NoError(t, err)
	assert.Len(t, stateChanges, 1)
	for _, sc := range stateChanges {
		assert.Equal(t, address2, sc.AccountID)
	}
}

func TestStateChangeModel_BatchGetByAccountAddress_WithFilters(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()
	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	ctx := context.Background()
	now := time.Now()

	// Create test account
	address := keypair.MustRandom().Address()
	_, err = dbConnectionPool.ExecContext(ctx, "INSERT INTO accounts (stellar_address) VALUES ($1)", address)
	require.NoError(t, err)

	// Create test transactions
	_, err = dbConnectionPool.ExecContext(ctx, `
		INSERT INTO transactions (hash, to_id, envelope_xdr, fee_charged, result_code, meta_xdr, ledger_number, ledger_created_at)
		VALUES
			('tx1', 1, 'env1', 100, 'TransactionResultCodeTxSuccess', 'meta1', 1, $1),
			('tx2', 2, 'env2', 200, 'TransactionResultCodeTxSuccess', 'meta2', 2, $1),
			('tx3', 3, 'env3', 300, 'TransactionResultCodeTxSuccess', 'meta3', 3, $1)
	`, now)
	require.NoError(t, err)

	// Create test state changes with different operation IDs, transaction hashes, categories, and reasons
	_, err = dbConnectionPool.ExecContext(ctx, `
		INSERT INTO state_changes (to_id, state_change_order, state_change_category, state_change_reason, ledger_created_at, ledger_number, account_id, operation_id, tx_hash)
		VALUES
			(1, 1, 'BALANCE', 'CREDIT', $1, 1, $2, 123, 'tx1'),
			(2, 1, 'BALANCE', 'DEBIT', $1, 2, $2, 456, 'tx2'),
			(3, 1, 'SIGNER', 'ADD', $1, 3, $2, 789, 'tx3'),
			(4, 1, 'BALANCE', 'DEBIT', $1, 4, $2, 123, 'tx1'),
			(5, 1, 'SIGNER', 'ADD', $1, 5, $2, 999, 'tx2')
	`, now, address)
	require.NoError(t, err)

	t.Run("filter by transaction hash only", func(t *testing.T) {
		mockMetricsService := metrics.NewMockMetricsService()
		mockMetricsService.On("ObserveDBQueryDuration", "BatchGetByAccountAddress", "state_changes", mock.Anything).Return().Once()
		mockMetricsService.On("IncDBQuery", "BatchGetByAccountAddress", "state_changes").Return().Once()
		defer mockMetricsService.AssertExpectations(t)

		m := &StateChangeModel{
			DB:             dbConnectionPool,
			MetricsService: mockMetricsService,
		}

		txHash := "tx1"
		stateChanges, err := m.BatchGetByAccountAddress(ctx, address, &txHash, nil, nil, nil, "", nil, nil, ASC)
		require.NoError(t, err)
		assert.Len(t, stateChanges, 2)
		for _, sc := range stateChanges {
			assert.Equal(t, "tx1", sc.TxHash)
			assert.Equal(t, address, sc.AccountID)
		}
	})

	t.Run("filter by operation ID only", func(t *testing.T) {
		mockMetricsService := metrics.NewMockMetricsService()
		mockMetricsService.On("ObserveDBQueryDuration", "BatchGetByAccountAddress", "state_changes", mock.Anything).Return().Once()
		mockMetricsService.On("IncDBQuery", "BatchGetByAccountAddress", "state_changes").Return().Once()
		defer mockMetricsService.AssertExpectations(t)

		m := &StateChangeModel{
			DB:             dbConnectionPool,
			MetricsService: mockMetricsService,
		}

		operationID := int64(123)
		stateChanges, err := m.BatchGetByAccountAddress(ctx, address, nil, &operationID, nil, nil, "", nil, nil, ASC)
		require.NoError(t, err)
		assert.Len(t, stateChanges, 2)
		for _, sc := range stateChanges {
			assert.Equal(t, int64(123), sc.OperationID)
			assert.Equal(t, address, sc.AccountID)
		}
	})

	t.Run("filter by both transaction hash and operation ID", func(t *testing.T) {
		mockMetricsService := metrics.NewMockMetricsService()
		mockMetricsService.On("ObserveDBQueryDuration", "BatchGetByAccountAddress", "state_changes", mock.Anything).Return().Once()
		mockMetricsService.On("IncDBQuery", "BatchGetByAccountAddress", "state_changes").Return().Once()
		defer mockMetricsService.AssertExpectations(t)

		m := &StateChangeModel{
			DB:             dbConnectionPool,
			MetricsService: mockMetricsService,
		}

		txHash := "tx1"
		operationID := int64(123)
		stateChanges, err := m.BatchGetByAccountAddress(ctx, address, &txHash, &operationID, nil, nil, "", nil, nil, ASC)
		require.NoError(t, err)
		// Should get only state changes that match BOTH filters
		assert.Len(t, stateChanges, 2)
		for _, sc := range stateChanges {
			assert.Equal(t, "tx1", sc.TxHash)
			assert.Equal(t, int64(123), sc.OperationID)
			assert.Equal(t, address, sc.AccountID)
		}
	})

	t.Run("filter by category only", func(t *testing.T) {
		mockMetricsService := metrics.NewMockMetricsService()
		mockMetricsService.On("ObserveDBQueryDuration", "BatchGetByAccountAddress", "state_changes", mock.Anything).Return().Once()
		mockMetricsService.On("IncDBQuery", "BatchGetByAccountAddress", "state_changes").Return().Once()
		defer mockMetricsService.AssertExpectations(t)

		m := &StateChangeModel{
			DB:             dbConnectionPool,
			MetricsService: mockMetricsService,
		}

		category := "BALANCE"
		stateChanges, err := m.BatchGetByAccountAddress(ctx, address, nil, nil, &category, nil, "", nil, nil, ASC)
		require.NoError(t, err)
		assert.Len(t, stateChanges, 3)
		for _, sc := range stateChanges {
			assert.Equal(t, types.StateChangeCategoryBalance, sc.StateChangeCategory)
			assert.Equal(t, address, sc.AccountID)
		}
	})

	t.Run("filter by reason only", func(t *testing.T) {
		mockMetricsService := metrics.NewMockMetricsService()
		mockMetricsService.On("ObserveDBQueryDuration", "BatchGetByAccountAddress", "state_changes", mock.Anything).Return().Once()
		mockMetricsService.On("IncDBQuery", "BatchGetByAccountAddress", "state_changes").Return().Once()
		defer mockMetricsService.AssertExpectations(t)

		m := &StateChangeModel{
			DB:             dbConnectionPool,
			MetricsService: mockMetricsService,
		}

		reason := "ADD"
		stateChanges, err := m.BatchGetByAccountAddress(ctx, address, nil, nil, nil, &reason, "", nil, nil, ASC)
		require.NoError(t, err)
		assert.Len(t, stateChanges, 2)
		for _, sc := range stateChanges {
			assert.Equal(t, types.StateChangeReasonAdd, *sc.StateChangeReason)
			assert.Equal(t, address, sc.AccountID)
		}
	})

	t.Run("filter by both category and reason", func(t *testing.T) {
		mockMetricsService := metrics.NewMockMetricsService()
		mockMetricsService.On("ObserveDBQueryDuration", "BatchGetByAccountAddress", "state_changes", mock.Anything).Return().Once()
		mockMetricsService.On("IncDBQuery", "BatchGetByAccountAddress", "state_changes").Return().Once()
		defer mockMetricsService.AssertExpectations(t)

		m := &StateChangeModel{
			DB:             dbConnectionPool,
			MetricsService: mockMetricsService,
		}

		category := "SIGNER"
		reason := "ADD"
		stateChanges, err := m.BatchGetByAccountAddress(ctx, address, nil, nil, &category, &reason, "", nil, nil, ASC)
		require.NoError(t, err)
		assert.Len(t, stateChanges, 2)
		for _, sc := range stateChanges {
			assert.Equal(t, types.StateChangeCategorySigner, sc.StateChangeCategory)
			assert.Equal(t, types.StateChangeReasonAdd, *sc.StateChangeReason)
			assert.Equal(t, address, sc.AccountID)
		}
	})

	t.Run("filter with all filters - txHash, operationID, category, reason", func(t *testing.T) {
		mockMetricsService := metrics.NewMockMetricsService()
		mockMetricsService.On("ObserveDBQueryDuration", "BatchGetByAccountAddress", "state_changes", mock.Anything).Return().Once()
		mockMetricsService.On("IncDBQuery", "BatchGetByAccountAddress", "state_changes").Return().Once()
		defer mockMetricsService.AssertExpectations(t)

		m := &StateChangeModel{
			DB:             dbConnectionPool,
			MetricsService: mockMetricsService,
		}

		txHash := "tx1"
		operationID := int64(123)
		category := "BALANCE"
		reason := "CREDIT"
		stateChanges, err := m.BatchGetByAccountAddress(ctx, address, &txHash, &operationID, &category, &reason, "", nil, nil, ASC)
		require.NoError(t, err)
		assert.Len(t, stateChanges, 1)
		for _, sc := range stateChanges {
			assert.Equal(t, "tx1", sc.TxHash)
			assert.Equal(t, int64(123), sc.OperationID)
			assert.Equal(t, types.StateChangeCategoryBalance, sc.StateChangeCategory)
			assert.Equal(t, types.StateChangeReasonCredit, *sc.StateChangeReason)
			assert.Equal(t, address, sc.AccountID)
		}
	})

	t.Run("filter with no matching results", func(t *testing.T) {
		mockMetricsService := metrics.NewMockMetricsService()
		mockMetricsService.On("ObserveDBQueryDuration", "BatchGetByAccountAddress", "state_changes", mock.Anything).Return().Once()
		mockMetricsService.On("IncDBQuery", "BatchGetByAccountAddress", "state_changes").Return().Once()
		defer mockMetricsService.AssertExpectations(t)

		m := &StateChangeModel{
			DB:             dbConnectionPool,
			MetricsService: mockMetricsService,
		}

		txHash := "nonexistent"
		stateChanges, err := m.BatchGetByAccountAddress(ctx, address, &txHash, nil, nil, nil, "", nil, nil, ASC)
		require.NoError(t, err)
		assert.Empty(t, stateChanges)
	})

	t.Run("filter with pagination", func(t *testing.T) {
		mockMetricsService := metrics.NewMockMetricsService()
		mockMetricsService.On("ObserveDBQueryDuration", "BatchGetByAccountAddress", "state_changes", mock.Anything).Return().Once()
		mockMetricsService.On("IncDBQuery", "BatchGetByAccountAddress", "state_changes").Return().Once()
		defer mockMetricsService.AssertExpectations(t)

		m := &StateChangeModel{
			DB:             dbConnectionPool,
			MetricsService: mockMetricsService,
		}

		txHash := "tx1"
		limit := int32(1)
		stateChanges, err := m.BatchGetByAccountAddress(ctx, address, &txHash, nil, nil, nil, "", &limit, nil, ASC)
		require.NoError(t, err)
		assert.Len(t, stateChanges, 1)
		assert.Equal(t, "tx1", stateChanges[0].TxHash)
	})
}

func TestStateChangeModel_GetAll(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()
	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	mockMetricsService := metrics.NewMockMetricsService()
	mockMetricsService.On("ObserveDBQueryDuration", "GetAll", "state_changes", mock.Anything).Return()
	mockMetricsService.On("IncDBQuery", "GetAll", "state_changes").Return()
	defer mockMetricsService.AssertExpectations(t)

	m := &StateChangeModel{
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
			('tx1', 1, 'env1', 100, 'TransactionResultCodeTxSuccess', 'meta1', 1, $1, false),
			('tx2', 2, 'env2', 200, 'TransactionResultCodeTxSuccess', 'meta2', 2, $1, true),
			('tx3', 3, 'env3', 300, 'TransactionResultCodeTxSuccess', 'meta3', 3, $1, false)
	`, now)
	require.NoError(t, err)

	// Create test state changes
	_, err = dbConnectionPool.ExecContext(ctx, `
		INSERT INTO state_changes (to_id, state_change_order, state_change_category, ledger_created_at, ledger_number, account_id, operation_id, tx_hash)
		VALUES 
			(1, 1, 'credit', $1, 1, $2, 123, 'tx1'),
			(2, 1, 'debit', $1, 2, $2, 456, 'tx2'),
			(3, 1, 'credit', $1, 3, $2, 789, 'tx3')
	`, now, address)
	require.NoError(t, err)

	// Test GetAll without limit
	stateChanges, err := m.GetAll(ctx, "", nil, nil, DESC)
	require.NoError(t, err)
	assert.Len(t, stateChanges, 3)

	// Test GetAll with limit
	limit := int32(2)
	stateChanges, err = m.GetAll(ctx, "", &limit, nil, DESC)
	require.NoError(t, err)
	assert.Len(t, stateChanges, 2)
}

func TestStateChangeModel_BatchGetByTxHashes(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()
	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

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
			('tx1', 1, 'env1', 100, 'TransactionResultCodeTxSuccess', 'meta1', 1, $1, false),
			('tx2', 2, 'env2', 200, 'TransactionResultCodeTxSuccess', 'meta2', 2, $1, true),
			('tx3', 3, 'env3', 300, 'TransactionResultCodeTxSuccess', 'meta3', 3, $1, false)
	`, now)
	require.NoError(t, err)

	// Create test state changes - multiple state changes per transaction to test ranking
	_, err = dbConnectionPool.ExecContext(ctx, `
		INSERT INTO state_changes (to_id, state_change_order, state_change_category, ledger_created_at, ledger_number, account_id, operation_id, tx_hash)
		VALUES 
			(1, 1, 'credit', $1, 1, $2, 123, 'tx1'),
			(2, 1, 'debit', $1, 2, $2, 456, 'tx2'),
			(3, 1, 'credit', $1, 3, $2, 789, 'tx1'),
			(4, 1, 'debit', $1, 4, $2, 101, 'tx1'),
			(5, 1, 'credit', $1, 5, $2, 102, 'tx2'),
			(6, 1, 'debit', $1, 6, $2, 103, 'tx3'),
			(7, 1, 'credit', $1, 7, $2, 104, 'tx2')
	`, now, address)
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
			expectedCount:     6, // 3 state changes for tx1 + 3 for tx2
			expectedTxCounts:  map[string]int{"tx1": 3, "tx2": 3},
			expectMetricCalls: 1,
		},
		{
			name:              "🟢 with limit parameter",
			txHashes:          []string{"tx1", "tx2"},
			limit:             func() *int32 { v := int32(2); return &v }(),
			sortOrder:         ASC,
			expectedCount:     4, // 2 state changes per tx hash (limited by ROW_NUMBER)
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
			expectedCount:     6,
			expectedTxCounts:  map[string]int{"tx1": 3, "tx2": 3},
			expectMetricCalls: 1,
		},
		{
			name:              "🟢 limit smaller than state changes per transaction",
			txHashes:          []string{"tx1"},
			limit:             func() *int32 { v := int32(1); return &v }(),
			sortOrder:         ASC,
			expectedCount:     1, // Only first state change due to ROW_NUMBER ranking
			expectedTxCounts:  map[string]int{"tx1": 1},
			expectMetricCalls: 1,
		},
		{
			name:              "🟢 all transactions",
			txHashes:          []string{"tx1", "tx2", "tx3"},
			limit:             nil,
			sortOrder:         ASC,
			expectedCount:     7, // All state changes
			expectedTxCounts:  map[string]int{"tx1": 3, "tx2": 3, "tx3": 1},
			expectMetricCalls: 1,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mockMetricsService := metrics.NewMockMetricsService()
			mockMetricsService.On("ObserveDBQueryDuration", "BatchGetByTxHashes", "state_changes", mock.Anything).Return().Times(tc.expectMetricCalls)
			mockMetricsService.On("ObserveDBBatchSize", "BatchGetByTxHashes", "state_changes", mock.Anything).Return().Times(tc.expectMetricCalls)
			mockMetricsService.On("IncDBQuery", "BatchGetByTxHashes", "state_changes").Return().Times(tc.expectMetricCalls)
			defer mockMetricsService.AssertExpectations(t)

			m := &StateChangeModel{
				DB:             dbConnectionPool,
				MetricsService: mockMetricsService,
			}

			stateChanges, err := m.BatchGetByTxHashes(ctx, tc.txHashes, "", tc.limit, tc.sortOrder)
			require.NoError(t, err)
			assert.Len(t, stateChanges, tc.expectedCount)

			// Verify state changes are for correct tx hashes
			txHashesFound := make(map[string]int)
			for _, sc := range stateChanges {
				txHashesFound[sc.TxHash]++
			}
			assert.Equal(t, tc.expectedTxCounts, txHashesFound)

			// Verify within-transaction ordering
			// The CTE uses ROW_NUMBER() OVER (PARTITION BY sc.tx_hash ORDER BY sc.to_id %s, sc.state_change_order %s)
			// This means state changes within each transaction should be ordered by (to_id, state_change_order)
			if len(stateChanges) > 0 {
				stateChangesByTxHash := make(map[string][]*types.StateChangeWithCursor)
				for _, sc := range stateChanges {
					stateChangesByTxHash[sc.TxHash] = append(stateChangesByTxHash[sc.TxHash], sc)
				}

				// Verify ordering within each transaction
				for txHash, txStateChanges := range stateChangesByTxHash {
					if len(txStateChanges) > 1 {
						for i := 1; i < len(txStateChanges); i++ {
							prev := txStateChanges[i-1]
							curr := txStateChanges[i]
							// After final transformation, state changes should be in ascending (to_id, state_change_order) order within each tx
							if prev.Cursor.ToID == curr.Cursor.ToID {
								assert.True(t, prev.Cursor.StateChangeOrder <= curr.Cursor.StateChangeOrder,
									"state changes within tx %s with same to_id should be ordered by state_change_order: prev=(%d,%d), curr=(%d,%d)",
									txHash, prev.Cursor.ToID, prev.Cursor.StateChangeOrder, curr.Cursor.ToID, curr.Cursor.StateChangeOrder)
							} else {
								assert.True(t, prev.Cursor.ToID <= curr.Cursor.ToID,
									"state changes within tx %s should be ordered by to_id: prev=(%d,%d), curr=(%d,%d)",
									txHash, prev.Cursor.ToID, prev.Cursor.StateChangeOrder, curr.Cursor.ToID, curr.Cursor.StateChangeOrder)
							}
						}
					}
				}
			}

			// Verify limit behavior when specified
			if tc.limit != nil && len(tc.expectedTxCounts) > 0 {
				for txHash, count := range tc.expectedTxCounts {
					assert.True(t, count <= int(*tc.limit), "number of state changes for %s should not exceed limit %d", txHash, *tc.limit)
				}
			}

			// Verify cursor structure for returned state changes
			for _, sc := range stateChanges {
				assert.NotZero(t, sc.Cursor.ToID, "cursor ToID should be set")
				assert.NotZero(t, sc.Cursor.StateChangeOrder, "cursor StateChangeOrder should be set")
			}
		})
	}
}

func TestStateChangeModel_BatchGetByOperationIDs(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()
	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	mockMetricsService := metrics.NewMockMetricsService()
	mockMetricsService.On("ObserveDBQueryDuration", "BatchGetByOperationIDs", "state_changes", mock.Anything).Return()
	mockMetricsService.On("ObserveDBBatchSize", "BatchGetByOperationIDs", "state_changes", mock.Anything).Return()
	mockMetricsService.On("IncDBQuery", "BatchGetByOperationIDs", "state_changes").Return()
	defer mockMetricsService.AssertExpectations(t)

	m := &StateChangeModel{
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
			('tx1', 1, 'env1', 100, 'TransactionResultCodeTxSuccess', 'meta1', 1, $1, false),
			('tx2', 2, 'env2', 200, 'TransactionResultCodeTxSuccess', 'meta2', 2, $1, true),
			('tx3', 3, 'env3', 300, 'TransactionResultCodeTxSuccess', 'meta3', 3, $1, false)
	`, now)
	require.NoError(t, err)

	// Create test state changes
	_, err = dbConnectionPool.ExecContext(ctx, `
		INSERT INTO state_changes (to_id, state_change_order, state_change_category, ledger_created_at, ledger_number, account_id, operation_id, tx_hash)
		VALUES
			(1, 1, 'credit', $1, 1, $2, 123, 'tx1'),
			(2, 1, 'debit', $1, 2, $2, 456, 'tx2'),
			(3, 1, 'credit', $1, 3, $2, 123, 'tx3')
	`, now, address)
	require.NoError(t, err)

	// Test BatchGetByOperationID
	limit := int32(10)
	stateChanges, err := m.BatchGetByOperationIDs(ctx, []int64{123, 456}, "", &limit, ASC)
	require.NoError(t, err)
	assert.Len(t, stateChanges, 3)

	// Verify state changes are for correct operation IDs
	operationIDsFound := make(map[int64]int)
	for _, sc := range stateChanges {
		operationIDsFound[sc.OperationID]++
	}
	assert.Equal(t, 2, operationIDsFound[123])
	assert.Equal(t, 1, operationIDsFound[456])
}

func TestStateChangeModel_BatchGetByTxHash(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()
	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	mockMetricsService := metrics.NewMockMetricsService()
	mockMetricsService.On("ObserveDBQueryDuration", "BatchGetByTxHash", "state_changes", mock.Anything).Return()
	mockMetricsService.On("IncDBQuery", "BatchGetByTxHash", "state_changes").Return()
	defer mockMetricsService.AssertExpectations(t)

	m := &StateChangeModel{
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
			('tx1', 1, 'env1', 100, 'TransactionResultCodeTxSuccess', 'meta1', 1, $1, false),
			('tx2', 2, 'env2', 200, 'TransactionResultCodeTxSuccess', 'meta2', 2, $1, true)
	`, now)
	require.NoError(t, err)

	// Create test state changes for tx1
	_, err = dbConnectionPool.ExecContext(ctx, `
		INSERT INTO state_changes (to_id, state_change_order, state_change_category, ledger_created_at, ledger_number, account_id, operation_id, tx_hash)
		VALUES
			(1, 1, 'credit', $1, 1, $2, 123, 'tx1'),
			(2, 1, 'debit', $1, 2, $2, 124, 'tx1'),
			(3, 1, 'credit', $1, 3, $2, 125, 'tx1'),
			(4, 1, 'debit', $1, 4, $2, 456, 'tx2')
	`, now, address)
	require.NoError(t, err)

	t.Run("get all state changes for single transaction", func(t *testing.T) {
		stateChanges, err := m.BatchGetByTxHash(ctx, "tx1", "", nil, nil, ASC)
		require.NoError(t, err)
		assert.Len(t, stateChanges, 3)

		// Verify all state changes are for tx1
		for _, sc := range stateChanges {
			assert.Equal(t, "tx1", sc.TxHash)
		}

		// Verify ordering (ASC by to_id, state_change_order)
		assert.Equal(t, int64(1), stateChanges[0].ToID)
		assert.Equal(t, int64(2), stateChanges[1].ToID)
		assert.Equal(t, int64(3), stateChanges[2].ToID)
	})

	t.Run("get state changes with pagination - first", func(t *testing.T) {
		limit := int32(2)
		stateChanges, err := m.BatchGetByTxHash(ctx, "tx1", "", &limit, nil, ASC)
		require.NoError(t, err)
		assert.Len(t, stateChanges, 2)

		assert.Equal(t, int64(1), stateChanges[0].ToID)
		assert.Equal(t, int64(2), stateChanges[1].ToID)
	})

	t.Run("get state changes with cursor pagination", func(t *testing.T) {
		limit := int32(2)
		cursor := &types.StateChangeCursor{ToID: 1, StateChangeOrder: 1}
		stateChanges, err := m.BatchGetByTxHash(ctx, "tx1", "", &limit, cursor, ASC)
		require.NoError(t, err)
		assert.Len(t, stateChanges, 2)

		// Should get results after cursor (to_id=1, state_change_order=1)
		assert.Equal(t, int64(2), stateChanges[0].ToID)
		assert.Equal(t, int64(3), stateChanges[1].ToID)
	})

	t.Run("get state changes with DESC ordering", func(t *testing.T) {
		stateChanges, err := m.BatchGetByTxHash(ctx, "tx1", "", nil, nil, DESC)
		require.NoError(t, err)
		assert.Len(t, stateChanges, 3)

		// Verify ordering (results should be in ASC order after DESC query transformation)
		assert.Equal(t, int64(1), stateChanges[0].ToID)
		assert.Equal(t, int64(2), stateChanges[1].ToID)
		assert.Equal(t, int64(3), stateChanges[2].ToID)
	})

	t.Run("no state changes for non-existent transaction", func(t *testing.T) {
		stateChanges, err := m.BatchGetByTxHash(ctx, "nonexistent", "", nil, nil, ASC)
		require.NoError(t, err)
		assert.Empty(t, stateChanges)
	})
}

func BenchmarkStateChangeModel_BatchInsert(b *testing.B) {
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

	m := &StateChangeModel{
		DB:             dbConnectionPool,
		MetricsService: metricsService,
	}

	// Create a parent transaction that state changes will reference
	const txHash = "benchmark_tx_hash"
	accountID := keypair.MustRandom().Address()
	now := time.Now()
	_, err = dbConnectionPool.ExecContext(ctx, `
		INSERT INTO transactions (hash, to_id, envelope_xdr, fee_charged, result_code, meta_xdr, ledger_number, ledger_created_at, is_fee_bump)
		VALUES ($1, 1, 'env', 100, 'TransactionResultCodeTxSuccess', 'meta', 1, $2, false)
	`, txHash, now)
	if err != nil {
		b.Fatalf("failed to create parent transaction: %v", err)
	}

	batchSizes := []int{1000, 5000, 10000, 50000, 100000}

	for _, size := range batchSizes {
		b.Run(fmt.Sprintf("size=%d", size), func(b *testing.B) {
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				b.StopTimer()
				// Clean up state changes before each iteration (keep the parent transaction)
				//nolint:errcheck // truncate is best-effort cleanup in benchmarks
				dbConnectionPool.ExecContext(ctx, "TRUNCATE state_changes CASCADE")
				// Generate fresh test data for each iteration
				scs := generateTestStateChanges(size, txHash, accountID, int64(i*size))
				b.StartTimer()

				_, err := m.BatchInsert(ctx, nil, scs)
				if err != nil {
					b.Fatalf("BatchInsert failed: %v", err)
				}
			}
		})
	}
}

// BenchmarkStateChangeModel_BatchCopy benchmarks bulk insert using pgx's binary COPY protocol.
func BenchmarkStateChangeModel_BatchCopy(b *testing.B) {
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

	m := &StateChangeModel{
		DB:             dbConnectionPool,
		MetricsService: metricsService,
	}

	// Create pgx connection for BatchCopy
	conn, err := pgx.Connect(ctx, dbt.DSN)
	if err != nil {
		b.Fatalf("failed to connect with pgx: %v", err)
	}
	defer conn.Close(ctx)

	// Create a parent transaction that state changes will reference
	const txHash = "benchmark_tx_hash"
	accountID := keypair.MustRandom().Address()
	now := time.Now()
	_, err = conn.Exec(ctx, `
		INSERT INTO transactions (hash, to_id, envelope_xdr, fee_charged, result_code, meta_xdr, ledger_number, ledger_created_at, is_fee_bump)
		VALUES ($1, 1, 'env', 100, 'TransactionResultCodeTxSuccess', 'meta', 1, $2, false)
	`, txHash, now)
	if err != nil {
		b.Fatalf("failed to create parent transaction: %v", err)
	}

	batchSizes := []int{1000, 5000, 10000, 50000, 100000}

	for _, size := range batchSizes {
		b.Run(fmt.Sprintf("size=%d", size), func(b *testing.B) {
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				b.StopTimer()
				// Clean up state changes before each iteration (keep the parent transaction)
				_, err = conn.Exec(ctx, "TRUNCATE state_changes CASCADE")
				if err != nil {
					b.Fatalf("failed to truncate: %v", err)
				}

				// Generate fresh test data for each iteration
				scs := generateTestStateChanges(size, txHash, accountID, int64(i*size))

				// Start a pgx transaction
				pgxTx, err := conn.Begin(ctx)
				if err != nil {
					b.Fatalf("failed to begin transaction: %v", err)
				}
				b.StartTimer()

				_, err = m.BatchCopy(ctx, pgxTx, scs)
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
