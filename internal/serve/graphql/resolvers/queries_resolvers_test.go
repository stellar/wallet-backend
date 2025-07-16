package resolvers

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/stellar/wallet-backend/internal/data"
	"github.com/stellar/wallet-backend/internal/db"
	"github.com/stellar/wallet-backend/internal/db/dbtest"
	"github.com/stellar/wallet-backend/internal/indexer/types"
	"github.com/stellar/wallet-backend/internal/metrics"
)

func TestQueryResolver_TransactionByHash(t *testing.T) {
	ctx := context.Background()

	dbt := dbtest.Open(t)
	defer dbt.Close()
	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	cleanUpDB := func() {
		_, err = dbConnectionPool.ExecContext(ctx, `DELETE FROM transactions`)
		require.NoError(t, err)
	}

	t.Run("success", func(t *testing.T) {
		mockMetricsService := metrics.NewMockMetricsService()
		mockMetricsService.On("ObserveDBQueryDuration", "SELECT", "transactions", mock.Anything).Return()
		mockMetricsService.On("IncDBQuery", "SELECT", "transactions").Return()
		defer mockMetricsService.AssertExpectations(t)

		resolver := &queryResolver{
			&Resolver{
				models: &data.Models{
					Transactions: &data.TransactionModel{
						DB:             dbConnectionPool,
						MetricsService: mockMetricsService,
					},
				},
			},
		}

		expectedTx := &types.Transaction{
			Hash:            "tx1",
			ToID:            1,
			EnvelopeXDR:     "envelope1",
			ResultXDR:       "result1",
			MetaXDR:         "meta1",
			LedgerNumber:    1,
			LedgerCreatedAt: time.Now(),
		}

		dbErr := db.RunInTransaction(context.Background(), dbConnectionPool, nil, func(tx db.Transaction) error {
			_, err := tx.ExecContext(ctx,
				`INSERT INTO transactions (hash, to_id, envelope_xdr, result_xdr, meta_xdr, ledger_number, ledger_created_at) VALUES ($1, $2, $3, $4, $5, $6, $7)`,
				expectedTx.Hash, expectedTx.ToID, expectedTx.EnvelopeXDR, expectedTx.ResultXDR, expectedTx.MetaXDR, expectedTx.LedgerNumber, expectedTx.LedgerCreatedAt)
			require.NoError(t, err)
			return nil
		})
		require.NoError(t, dbErr)

		tx, err := resolver.TransactionByHash(ctx, "tx1")

		require.NoError(t, err)
		assert.Equal(t, expectedTx.Hash, tx.Hash)
		assert.Equal(t, expectedTx.ToID, tx.ToID)
		assert.Equal(t, expectedTx.EnvelopeXDR, tx.EnvelopeXDR)
		assert.Equal(t, expectedTx.ResultXDR, tx.ResultXDR)
		assert.Equal(t, expectedTx.MetaXDR, tx.MetaXDR)
		assert.Equal(t, expectedTx.LedgerNumber, tx.LedgerNumber)
		cleanUpDB()
	})
}

func TestQueryResolver_Transactions(t *testing.T) {
	ctx := context.Background()

	dbt := dbtest.Open(t)
	defer dbt.Close()
	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	cleanUpDB := func() {
		_, err = dbConnectionPool.ExecContext(ctx, `DELETE FROM transactions`)
		require.NoError(t, err)
	}

	mockMetricsService := metrics.NewMockMetricsService()
	mockMetricsService.On("ObserveDBQueryDuration", "SELECT", "transactions", mock.Anything).Return()
	mockMetricsService.On("IncDBQuery", "SELECT", "transactions").Return()
	defer mockMetricsService.AssertExpectations(t)

	resolver := &queryResolver{
		&Resolver{
			models: &data.Models{
				Transactions: &data.TransactionModel{
					DB:             dbConnectionPool,
					MetricsService: mockMetricsService,
				},
			},
		},
	}

	tx1 := &types.Transaction{
		Hash:            "tx1",
		ToID:            1,
		EnvelopeXDR:     "envelope1",
		ResultXDR:       "result1",
		MetaXDR:         "meta1",
		LedgerNumber:    1,
		LedgerCreatedAt: time.Now(),
	}
	tx2 := &types.Transaction{
		Hash:            "tx2",
		ToID:            2,
		EnvelopeXDR:     "envelope2",
		ResultXDR:       "result2",
		MetaXDR:         "meta2",
		LedgerNumber:    2,
		LedgerCreatedAt: time.Now(),
	}

	dbErr := db.RunInTransaction(context.Background(), dbConnectionPool, nil, func(tx db.Transaction) error {
		_, err := tx.ExecContext(ctx,
			`INSERT INTO transactions (hash, to_id, envelope_xdr, result_xdr, meta_xdr, ledger_number, ledger_created_at) VALUES ($1, $2, $3, $4, $5, $6, $7), ($8, $9, $10, $11, $12, $13, $14)`,
			tx1.Hash, tx1.ToID, tx1.EnvelopeXDR, tx1.ResultXDR, tx1.MetaXDR, tx1.LedgerNumber, tx1.LedgerCreatedAt,
			tx2.Hash, tx2.ToID, tx2.EnvelopeXDR, tx2.ResultXDR, tx2.MetaXDR, tx2.LedgerNumber, tx2.LedgerCreatedAt)
		require.NoError(t, err)
		return nil
	})
	require.NoError(t, dbErr)

	t.Run("get all", func(t *testing.T) {
		txs, err := resolver.Transactions(ctx, nil)
		require.NoError(t, err)
		assert.Len(t, txs, 2)
	})

	t.Run("get with limit", func(t *testing.T) {
		limit := int32(1)
		txs, err := resolver.Transactions(ctx, &limit)
		require.NoError(t, err)
		assert.Len(t, txs, 1)
	})

	cleanUpDB()
}

func TestQueryResolver_Account(t *testing.T) {
	ctx := context.Background()

	dbt := dbtest.Open(t)
	defer dbt.Close()
	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	cleanUpDB := func() {
		_, err = dbConnectionPool.ExecContext(ctx, `DELETE FROM accounts`)
		require.NoError(t, err)
	}

	mockMetricsService := metrics.NewMockMetricsService()
	mockMetricsService.On("ObserveDBQueryDuration", "SELECT", "accounts", mock.Anything).Return()
	mockMetricsService.On("IncDBQuery", "SELECT", "accounts").Return()
	defer mockMetricsService.AssertExpectations(t)

	resolver := &queryResolver{
		&Resolver{
			models: &data.Models{
				Account: &data.AccountModel{
					DB:             dbConnectionPool,
					MetricsService: mockMetricsService,
				},
			},
		},
	}

	expectedAccount := &types.Account{
		StellarAddress: "GC...",
	}

	dbErr := db.RunInTransaction(context.Background(), dbConnectionPool, nil, func(tx db.Transaction) error {
		_, err := tx.ExecContext(ctx,
			`INSERT INTO accounts (stellar_address) VALUES ($1)`,
			expectedAccount.StellarAddress)
		require.NoError(t, err)
		return nil
	})
	require.NoError(t, dbErr)

	t.Run("success", func(t *testing.T) {
		acc, err := resolver.Account(ctx, expectedAccount.StellarAddress)
		require.NoError(t, err)
		assert.Equal(t, expectedAccount.StellarAddress, acc.StellarAddress)
	})

	cleanUpDB()
}

func TestQueryResolver_Operations(t *testing.T) {
	ctx := context.Background()

	dbt := dbtest.Open(t)
	defer dbt.Close()
	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	cleanUpDB := func() {
		_, err = dbConnectionPool.ExecContext(ctx, `DELETE FROM operations`)
		require.NoError(t, err)
		_, err = dbConnectionPool.ExecContext(ctx, `DELETE FROM transactions`)
		require.NoError(t, err)
	}

	mockMetricsService := metrics.NewMockMetricsService()
	mockMetricsService.On("ObserveDBQueryDuration", "SELECT", "operations", mock.Anything).Return()
	mockMetricsService.On("IncDBQuery", "SELECT", "operations").Return()
	defer mockMetricsService.AssertExpectations(t)

	resolver := &queryResolver{
		&Resolver{
			models: &data.Models{
				Operations: &data.OperationModel{
					DB:             dbConnectionPool,
					MetricsService: mockMetricsService,
				},
			},
		},
	}

	// Insert a transaction to satisfy the foreign key constraint
	expectedTx := &types.Transaction{
		Hash:            "tx1",
		ToID:            1,
		EnvelopeXDR:     "envelope1",
		ResultXDR:       "result1",
		MetaXDR:         "meta1",
		LedgerNumber:    1,
		LedgerCreatedAt: time.Now(),
	}
	dbErr := db.RunInTransaction(context.Background(), dbConnectionPool, nil, func(tx db.Transaction) error {
		_, err := tx.ExecContext(ctx,
			`INSERT INTO transactions (hash, to_id, envelope_xdr, result_xdr, meta_xdr, ledger_number, ledger_created_at) VALUES ($1, $2, $3, $4, $5, $6, $7)`,
			expectedTx.Hash, expectedTx.ToID, expectedTx.EnvelopeXDR, expectedTx.ResultXDR, expectedTx.MetaXDR, expectedTx.LedgerNumber, expectedTx.LedgerCreatedAt)
		require.NoError(t, err)
		return nil
	})
	require.NoError(t, dbErr)

	op1 := &types.Operation{
		ID:              1,
		OperationType:   types.OperationTypePayment,
		OperationXDR:    "op1_xdr",
		TxHash:          "tx1",
		LedgerCreatedAt: time.Now(),
	}
	op2 := &types.Operation{
		ID:              2,
		OperationType:   types.OperationTypeCreateAccount,
		OperationXDR:    "op2_xdr",
		TxHash:          "tx1",
		LedgerCreatedAt: time.Now(),
	}

	dbErr = db.RunInTransaction(context.Background(), dbConnectionPool, nil, func(tx db.Transaction) error {
		_, err := tx.ExecContext(ctx,
			`INSERT INTO operations (id, operation_type, operation_xdr, tx_hash, ledger_created_at) VALUES ($1, $2, $3, $4, $5), ($6, $7, $8, $9, $10)`,
			op1.ID, op1.OperationType, op1.OperationXDR, op1.TxHash, op1.LedgerCreatedAt,
			op2.ID, op2.OperationType, op2.OperationXDR, op2.TxHash, op2.LedgerCreatedAt)
		require.NoError(t, err)
		return nil
	})
	require.NoError(t, dbErr)

	t.Run("get all", func(t *testing.T) {
		ops, err := resolver.Operations(ctx, nil)
		require.NoError(t, err)
		assert.Len(t, ops, 2)
	})

	t.Run("get with limit", func(t *testing.T) {
		limit := int32(1)
		ops, err := resolver.Operations(ctx, &limit)
		require.NoError(t, err)
		assert.Len(t, ops, 1)
	})

	cleanUpDB()
}

func TestQueryResolver_StateChanges(t *testing.T) {
	ctx := context.Background()

	dbt := dbtest.Open(t)
	defer dbt.Close()
	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	cleanUpDB := func() {
		_, err = dbConnectionPool.ExecContext(ctx, `DELETE FROM state_changes`)
		require.NoError(t, err)
		_, err = dbConnectionPool.ExecContext(ctx, `DELETE FROM operations`)
		require.NoError(t, err)
		_, err = dbConnectionPool.ExecContext(ctx, `DELETE FROM transactions`)
		require.NoError(t, err)
	}

	resolver := &queryResolver{
		&Resolver{
			models: &data.Models{
				StateChanges: &data.StateChangeModel{
					DB: dbConnectionPool,
				},
			},
		},
	}

	// Insert a transaction to satisfy the foreign key constraint
	expectedTx := &types.Transaction{
		Hash:            "tx1",
		ToID:            1,
		EnvelopeXDR:     "envelope1",
		ResultXDR:       "result1",
		MetaXDR:         "meta1",
		LedgerNumber:    1,
		LedgerCreatedAt: time.Now(),
	}
	dbErr := db.RunInTransaction(context.Background(), dbConnectionPool, nil, func(tx db.Transaction) error {
		_, err := tx.ExecContext(ctx,
			`INSERT INTO transactions (hash, to_id, envelope_xdr, result_xdr, meta_xdr, ledger_number, ledger_created_at) VALUES ($1, $2, $3, $4, $5, $6, $7)`,
			expectedTx.Hash, expectedTx.ToID, expectedTx.EnvelopeXDR, expectedTx.ResultXDR, expectedTx.MetaXDR, expectedTx.LedgerNumber, expectedTx.LedgerCreatedAt)
		require.NoError(t, err)
		return nil
	})
	require.NoError(t, dbErr)

	// Insert accounts to satisfy the foreign key constraint
	dbErr = db.RunInTransaction(context.Background(), dbConnectionPool, nil, func(tx db.Transaction) error {
		_, err := tx.ExecContext(ctx,
			`INSERT INTO accounts (stellar_address) VALUES ($1), ($2)`,
			"account1", "account2")
		require.NoError(t, err)
		return nil
	})

	sc1 := &types.StateChange{
		ID:                  "sc1",
		StateChangeCategory: types.StateChangeCategoryCredit,
		TxHash:              "tx1",
		OperationID:         1,
		AccountID:           "account1",
		LedgerCreatedAt:     time.Now(),
		LedgerNumber:        1,
	}
	sc2 := &types.StateChange{
		ID:                  "sc2",
		StateChangeCategory: types.StateChangeCategoryDebit,
		TxHash:              "tx1",
		OperationID:         1,
		AccountID:           "account2",
		LedgerCreatedAt:     time.Now(),
		LedgerNumber:        1,
	}

	dbErr = db.RunInTransaction(context.Background(), dbConnectionPool, nil, func(tx db.Transaction) error {
		_, err := tx.ExecContext(ctx,
			`INSERT INTO state_changes (id, state_change_category, tx_hash, operation_id, account_id, ledger_created_at, ledger_number) VALUES ($1, $2, $3, $4, $5, $6, $7), ($8, $9, $10, $11, $12, $13, $14)`,
			sc1.ID, sc1.StateChangeCategory, sc1.TxHash, sc1.OperationID, sc1.AccountID, sc1.LedgerCreatedAt, sc1.LedgerNumber,
			sc2.ID, sc2.StateChangeCategory, sc2.TxHash, sc2.OperationID, sc2.AccountID, sc2.LedgerCreatedAt, sc2.LedgerNumber)
		require.NoError(t, err)
		return nil
	})
	require.NoError(t, dbErr)

	t.Run("get all", func(t *testing.T) {
		mockMetricsService := metrics.NewMockMetricsService()
		resolver.models.StateChanges.MetricsService = mockMetricsService
		mockMetricsService.On("ObserveDBQueryDuration", "SELECT", "state_changes", mock.Anything).Return()
		mockMetricsService.On("IncDBQuery", "SELECT", "state_changes").Return()
		scs, err := resolver.StateChanges(ctx, nil)
		require.NoError(t, err)
		assert.Len(t, scs, 2)
		assert.Equal(t, []string{"sc1", "sc2"}, []string{scs[0].ID, scs[1].ID})
		mockMetricsService.AssertExpectations(t)
	})

	t.Run("get with limit", func(t *testing.T) {
		mockMetricsService := metrics.NewMockMetricsService()
		resolver.models.StateChanges.MetricsService = mockMetricsService
		mockMetricsService.On("ObserveDBQueryDuration", "SELECT", "state_changes", mock.Anything).Return()
		mockMetricsService.On("IncDBQuery", "SELECT", "state_changes").Return()
		limit := int32(1)
		scs, err := resolver.StateChanges(ctx, &limit)
		require.NoError(t, err)
		assert.Len(t, scs, 1)
		mockMetricsService.AssertExpectations(t)
	})

	cleanUpDB()
}
