package data

import (
	"context"
	"database/sql"
	"testing"
	"time"

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

	reason := types.StateChangeReasonAdd
	sc1 := types.StateChange{
		ID:                  "sc1",
		StateChangeCategory: types.StateChangeCategoryCredit,
		StateChangeReason:   &reason,
		LedgerCreatedAt:     now,
		LedgerNumber:        1,
		AccountID:           kp1.Address(),
		OperationID:         123,
		TxHash:              "tx_hash_1",
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
		TxHash:              "tx_hash_2",
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
					TxHash:              "tx_hash_3",
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
					TxHash:              "tx_hash_4",
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
