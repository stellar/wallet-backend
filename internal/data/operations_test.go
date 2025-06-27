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
	_, err = txModel.BatchInsert(ctx, nil, []types.Transaction{tx1, tx2}, map[string][]string{
		tx1.Hash: {kp1.Address()},
		tx2.Hash: {kp2.Address()},
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
		stellarAddressesByOpID map[int64][]string
		wantAccountLinks       map[int64]set.Set[string]
		wantErrContains        string
		wantIDs                []int64
	}{
		{
			name:                   "🟢successful_insert_without_dbTx",
			useDBTx:                false,
			operations:             []types.Operation{op1, op2},
			stellarAddressesByOpID: map[int64][]string{op1.ID: {kp1.Address()}, op2.ID: {kp2.Address()}},
			wantAccountLinks:       map[int64]set.Set[string]{op1.ID: set.NewSet(kp1.Address()), op2.ID: set.NewSet(kp2.Address())},
			wantErrContains:        "",
			wantIDs:                []int64{op1.ID, op2.ID},
		},
		{
			name:                   "🟢successful_insert_with_dbTx",
			useDBTx:                true,
			operations:             []types.Operation{op1},
			stellarAddressesByOpID: map[int64][]string{op1.ID: {kp1.Address()}},
			wantAccountLinks:       map[int64]set.Set[string]{op1.ID: set.NewSet(kp1.Address())},
			wantErrContains:        "",
			wantIDs:                []int64{op1.ID},
		},
		{
			name:                   "🟢empty_input",
			useDBTx:                false,
			operations:             []types.Operation{},
			stellarAddressesByOpID: map[int64][]string{},
			wantAccountLinks:       map[int64]set.Set[string]{},
			wantErrContains:        "",
			wantIDs:                nil,
		},
		{
			name:                   "🟡duplicate_operation",
			useDBTx:                false,
			operations:             []types.Operation{op1, op1},
			stellarAddressesByOpID: map[int64][]string{op1.ID: {kp1.Address()}},
			wantAccountLinks:       map[int64]set.Set[string]{op1.ID: set.NewSet(kp1.Address())},
			wantErrContains:        "",
			wantIDs:                []int64{op1.ID},
		},
		{
			name:                   "🟡op_with_all_non_existing_accounts_is_ignored",
			useDBTx:                false,
			operations:             []types.Operation{op1},
			stellarAddressesByOpID: map[int64][]string{op1.ID: {nonExistingAccount.Address()}},
			wantAccountLinks:       map[int64]set.Set[string]{},
			wantErrContains:        "",
			wantIDs:                nil,
		},
		{
			name:                   "🟡non_existing_account_is_ignored_but_op_and_other_accounts_links_are_inserted",
			useDBTx:                false,
			operations:             []types.Operation{op1},
			stellarAddressesByOpID: map[int64][]string{op1.ID: {kp1.Address(), kp2.Address(), nonExistingAccount.Address()}},
			wantAccountLinks:       map[int64]set.Set[string]{op1.ID: set.NewSet(kp1.Address(), kp2.Address())},
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
				On("ObserveDBQueryDuration", "INSERT", "operations,operations_accounts", mock.Anything).Return().Once().
				On("IncDBQuery", "INSERT", "operations,operations_accounts").Return().Once()
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
				accountLinksMap := make(map[int64]set.Set[string])
				for _, link := range accountLinks {
					if _, exists := accountLinksMap[link.OperationID]; !exists {
						accountLinksMap[link.OperationID] = set.NewSet[string]()
					}
					accountLinksMap[link.OperationID].Add(link.AccountID)
				}

				// Verify each operation has its expected account links
				require.Equal(t, tc.wantAccountLinks, accountLinksMap)
			}
		})
	}
}
