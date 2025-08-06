package data

import (
	"context"
	"database/sql"
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
	sqlxDB, err := dbConnectionPool.SqlxDB(ctx)
	require.NoError(t, err)
	txModel := &TransactionModel{DB: dbConnectionPool, MetricsService: metrics.NewMetricsService(sqlxDB)}
	_, err = txModel.BatchInsert(ctx, nil, []types.Transaction{tx1, tx2}, map[string]set.Set[string]{
		tx1.Hash: set.NewSet(kp1.Address()),
		tx2.Hash: set.NewSet(kp2.Address()),
	})
	require.NoError(t, err)

	reason := types.StateChangeReasonAdd
	sc1 := types.StateChange{
		ID:                  "sc1",
		StateChangeCategory: types.StateChangeCategoryCredit,
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
		ID:                  "sc2",
		StateChangeCategory: types.StateChangeCategoryDebit,
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
			wantIDs:      []string{sc1.ID, sc2.ID},
		},
		{
			name:         "🟢successful_insert_with_dbTx",
			useDBTx:      true,
			stateChanges: []types.StateChange{sc1},
			wantIDs:      []string{sc1.ID},
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
			wantIDs:      []string{sc1.ID},
		},
		{
			name: "🟡state_change_with_non_existing_account_is_ignored",
			stateChanges: []types.StateChange{
				{
					ID:                  "sc3",
					StateChangeCategory: types.StateChangeCategoryCredit,
					LedgerCreatedAt:     now,
					LedgerNumber:        3,
					AccountID:           nonExistingAccount.Address(),
					OperationID:         789,
					TxHash:              tx1.Hash,
				},
			},
			wantIDs: nil,
		},
		{
			name: "🟡mixture_of_existing_and_non_existing_accounts",
			stateChanges: []types.StateChange{
				sc1,
				{
					ID:                  "sc4",
					StateChangeCategory: types.StateChangeCategoryCredit,
					LedgerCreatedAt:     now,
					LedgerNumber:        4,
					AccountID:           nonExistingAccount.Address(),
					OperationID:         101,
					TxHash:              tx2.Hash,
				},
			},
			wantIDs: []string{sc1.ID},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			_, err = dbConnectionPool.ExecContext(ctx, "TRUNCATE state_changes CASCADE")
			require.NoError(t, err)

			mockMetricsService := metrics.NewMockMetricsService()
			mockMetricsService.
				On("ObserveDBQueryDuration", "INSERT", "state_changes", mock.Anything).Return().Once().
				On("IncDBQuery", "INSERT", "state_changes").Return().Once()

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
			err = sqlExecuter.SelectContext(ctx, &dbInsertedIDs, "SELECT id FROM state_changes")
			require.NoError(t, err)
			assert.ElementsMatch(t, tc.wantIDs, dbInsertedIDs)

			mockMetricsService.AssertExpectations(t)
		})
	}
}

func TestStateChangeModel_BatchGetByAccountAddress(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()
	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	ctx := context.Background()
	now := time.Now()

	mockMetricsService := metrics.NewMockMetricsService()
	mockMetricsService.
		On("ObserveDBQueryDuration", "SELECT", "state_changes", mock.Anything).Return().Once().
		On("IncDBQuery", "SELECT", "state_changes").Return().Once()
	defer mockMetricsService.AssertExpectations(t)

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

	// Create test state changes
	_, err = dbConnectionPool.ExecContext(ctx, `
		INSERT INTO state_changes (id, state_change_category, ledger_created_at, ledger_number, account_id, operation_id, tx_hash)
		VALUES 
			('sc1', 'credit', $1, 1, $2, 123, 'tx1'),
			('sc2', 'debit', $1, 2, $2, 456, 'tx2'),
			('sc3', 'credit', $1, 3, $3, 789, 'tx3')
	`, now, address1, address2)
	require.NoError(t, err)

	m := &StateChangeModel{
		DB:             dbConnectionPool,
		MetricsService: mockMetricsService,
	}

	// Test BatchGetByAccount
	stateChanges, err := m.BatchGetByAccountAddress(ctx, address1, "", nil, nil)
	require.NoError(t, err)
	assert.Len(t, stateChanges, 2)
	assert.Equal(t, "sc2", stateChanges[0].StateChange.ID)
	assert.Equal(t, "sc1", stateChanges[1].StateChange.ID)
}

func TestStateChangeModel_GetAll(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()
	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	mockMetricsService := metrics.NewMockMetricsService()
	mockMetricsService.On("ObserveDBQueryDuration", "SELECT", "state_changes", mock.Anything).Return()
	mockMetricsService.On("IncDBQuery", "SELECT", "state_changes").Return()
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
		INSERT INTO transactions (hash, to_id, envelope_xdr, result_xdr, meta_xdr, ledger_number, ledger_created_at)
		VALUES 
			('tx1', 1, 'env1', 'res1', 'meta1', 1, $1),
			('tx2', 2, 'env2', 'res2', 'meta2', 2, $1),
			('tx3', 3, 'env3', 'res3', 'meta3', 3, $1)
	`, now)
	require.NoError(t, err)

	// Create test state changes
	_, err = dbConnectionPool.ExecContext(ctx, `
		INSERT INTO state_changes (id, state_change_category, ledger_created_at, ledger_number, account_id, operation_id, tx_hash)
		VALUES 
			('sc1', 'credit', $1, 1, $2, 123, 'tx1'),
			('sc2', 'debit', $1, 2, $2, 456, 'tx2'),
			('sc3', 'credit', $1, 3, $2, 789, 'tx3')
	`, now, address)
	require.NoError(t, err)

	// Test GetAll without limit
	stateChanges, err := m.GetAll(ctx, "", nil, nil)
	require.NoError(t, err)
	assert.Len(t, stateChanges, 3)
	assert.Equal(t, "sc3", stateChanges[0].StateChange.ID)
	assert.Equal(t, "sc2", stateChanges[1].StateChange.ID)
	assert.Equal(t, "sc1", stateChanges[2].StateChange.ID)

	// Test GetAll with limit
	limit := int32(2)
	stateChanges, err = m.GetAll(ctx, "", &limit, nil)
	require.NoError(t, err)
	assert.Len(t, stateChanges, 2)
	assert.Equal(t, "sc3", stateChanges[0].StateChange.ID)
	assert.Equal(t, "sc2", stateChanges[1].StateChange.ID)
}

func TestStateChangeModel_BatchGetByTxHashes(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()
	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	mockMetricsService := metrics.NewMockMetricsService()
	mockMetricsService.On("ObserveDBQueryDuration", "SELECT", "state_changes", mock.Anything).Return()
	mockMetricsService.On("IncDBQuery", "SELECT", "state_changes").Return()
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
		INSERT INTO transactions (hash, to_id, envelope_xdr, result_xdr, meta_xdr, ledger_number, ledger_created_at)
		VALUES 
			('tx1', 1, 'env1', 'res1', 'meta1', 1, $1),
			('tx2', 2, 'env2', 'res2', 'meta2', 2, $1)
	`, now)
	require.NoError(t, err)

	// Create test state changes
	_, err = dbConnectionPool.ExecContext(ctx, `
		INSERT INTO state_changes (id, state_change_category, ledger_created_at, ledger_number, account_id, operation_id, tx_hash)
		VALUES 
			('sc1', 'credit', $1, 1, $2, 123, 'tx1'),
			('sc2', 'debit', $1, 2, $2, 456, 'tx2'),
			('sc3', 'credit', $1, 3, $2, 789, 'tx1')
	`, now, address)
	require.NoError(t, err)

	// Test BatchGetByTxHash
	stateChanges, err := m.BatchGetByTxHashes(ctx, []string{"tx1", "tx2"}, "")
	require.NoError(t, err)
	assert.Len(t, stateChanges, 3)

	// Verify state changes are for correct tx hashes
	txHashesFound := make(map[string]int)
	for _, sc := range stateChanges {
		txHashesFound[sc.TxHash]++
	}
	assert.Equal(t, 2, txHashesFound["tx1"])
	assert.Equal(t, 1, txHashesFound["tx2"])
}

func TestStateChangeModel_BatchGetByOperationIDs(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()
	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	mockMetricsService := metrics.NewMockMetricsService()
	mockMetricsService.On("ObserveDBQueryDuration", "SELECT", "state_changes", mock.Anything).Return()
	mockMetricsService.On("IncDBQuery", "SELECT", "state_changes").Return()
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
		INSERT INTO transactions (hash, to_id, envelope_xdr, result_xdr, meta_xdr, ledger_number, ledger_created_at)
		VALUES 
			('tx1', 1, 'env1', 'res1', 'meta1', 1, $1),
			('tx2', 2, 'env2', 'res2', 'meta2', 2, $1),
			('tx3', 3, 'env3', 'res3', 'meta3', 3, $1)
	`, now)
	require.NoError(t, err)

	// Create test state changes
	_, err = dbConnectionPool.ExecContext(ctx, `
		INSERT INTO state_changes (id, state_change_category, ledger_created_at, ledger_number, account_id, operation_id, tx_hash)
		VALUES 
			('sc1', 'credit', $1, 1, $2, 123, 'tx1'),
			('sc2', 'debit', $1, 2, $2, 456, 'tx2'),
			('sc3', 'credit', $1, 3, $2, 123, 'tx3')
	`, now, address)
	require.NoError(t, err)

	// Test BatchGetByOperationID
	stateChanges, err := m.BatchGetByOperationIDs(ctx, []int64{123, 456}, "")
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
