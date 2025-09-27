package data

import (
	"context"
	"database/sql"
	"fmt"
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
			err = sqlExecuter.SelectContext(ctx, &dbInsertedIDs, "SELECT CONCAT(to_id, '-', state_change_order) FROM state_changes")
			require.NoError(t, err)
			assert.ElementsMatch(t, tc.wantIDs, dbInsertedIDs)

			mockMetricsService.AssertExpectations(t)
		})
	}
}
