package resolvers

import (
	"context"
	"fmt"
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

		ctx := getTestCtx("transactions", []string{"hash", "toId", "envelopeXdr", "resultXdr", "metaXdr", "ledgerNumber", "ledgerCreatedAt"})
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

	txns := make([]*types.Transaction, 0, 4)
	for i := 0; i < 4; i++ {
		txns = append(txns, &types.Transaction{
			Hash:            fmt.Sprintf("tx%d", i+1),
			ToID:            int64(i + 1),
			EnvelopeXDR:     fmt.Sprintf("envelope%d", i+1),
			ResultXDR:       fmt.Sprintf("result%d", i+1),
			MetaXDR:         fmt.Sprintf("meta%d", i+1),
			LedgerNumber:    1,
			LedgerCreatedAt: time.Now(),
		})
	}

	dbErr := db.RunInTransaction(context.Background(), dbConnectionPool, nil, func(tx db.Transaction) error {
		for _, txn := range txns {
			_, err := tx.ExecContext(ctx,
				`INSERT INTO transactions (hash, to_id, envelope_xdr, result_xdr, meta_xdr, ledger_number, ledger_created_at) VALUES ($1, $2, $3, $4, $5, $6, $7)`,
				txn.Hash, txn.ToID, txn.EnvelopeXDR, txn.ResultXDR, txn.MetaXDR, txn.LedgerNumber, txn.LedgerCreatedAt)
			require.NoError(t, err)
		}
		return nil
	})
	require.NoError(t, dbErr)

	t.Run("get all", func(t *testing.T) {
		ctx := getTestCtx("transactions", []string{"hash", "toId", "envelopeXdr", "resultXdr", "metaXdr", "ledgerNumber", "ledgerCreatedAt"})
		txs, err := resolver.Transactions(ctx, nil, nil)
		require.NoError(t, err)
		assert.Len(t, txs.Edges, 4)
		assert.Equal(t, txns[3].ToID, txs.Edges[0].Node.ToID)
		assert.Equal(t, txns[2].ToID, txs.Edges[1].Node.ToID)
		assert.Equal(t, txns[1].ToID, txs.Edges[2].Node.ToID)
		assert.Equal(t, txns[0].ToID, txs.Edges[3].Node.ToID)
	})

	t.Run("get with limit", func(t *testing.T) {
		ctx := getTestCtx("transactions", []string{"hash", "toId", "envelopeXdr", "resultXdr", "metaXdr", "ledgerNumber", "ledgerCreatedAt"})
		limit := int32(1)
		txs, err := resolver.Transactions(ctx, &limit, nil)
		require.NoError(t, err)
		assert.Len(t, txs.Edges, 1)
		assert.Equal(t, txns[3].ToID, txs.Edges[0].Node.ToID)
	})

	t.Run("get with cursor", func(t *testing.T) {
		ctx := getTestCtx("transactions", []string{"hash", "toId", "envelopeXdr", "resultXdr", "metaXdr", "ledgerNumber", "ledgerCreatedAt"})
		limit := int32(2)
		txs, err := resolver.Transactions(ctx, &limit, nil)
		require.NoError(t, err)
		assert.Len(t, txs.Edges, 2)
		assert.Equal(t, txns[3].ToID, txs.Edges[0].Node.ToID)
		assert.Equal(t, txns[2].ToID, txs.Edges[1].Node.ToID)

		// Get the next cursor
		nextCursor := txs.PageInfo.EndCursor
		assert.NotNil(t, nextCursor)
		txs, err = resolver.Transactions(ctx, &limit, nextCursor)
		require.NoError(t, err)
		assert.Len(t, txs.Edges, 2)
		assert.Equal(t, txns[1].ToID, txs.Edges[0].Node.ToID)
		assert.Equal(t, txns[0].ToID, txs.Edges[1].Node.ToID)

		hasNextPage := txs.PageInfo.HasNextPage
		assert.False(t, hasNextPage)

		hasPreviousPage := txs.PageInfo.HasPreviousPage
		assert.True(t, hasPreviousPage)
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

	operations := make([]*types.Operation, 0, 4)
	for i := 0; i < 4; i++ {
		operations = append(operations, &types.Operation{
			ID:              int64(i + 1),
			OperationType:   types.OperationTypePayment,
			OperationXDR:    fmt.Sprintf("op%d_xdr", i+1),
			TxHash:          "tx1",
			LedgerNumber:    1,
			LedgerCreatedAt: time.Now(),
		})
	}

	dbErr = db.RunInTransaction(context.Background(), dbConnectionPool, nil, func(tx db.Transaction) error {
		for _, op := range operations {
			_, err := tx.ExecContext(ctx,
				`INSERT INTO operations (id, operation_type, operation_xdr, tx_hash, ledger_number, ledger_created_at) VALUES ($1, $2, $3, $4, $5, $6)`,
				op.ID, op.OperationType, op.OperationXDR, op.TxHash, op.LedgerNumber, op.LedgerCreatedAt)
			require.NoError(t, err)
		}
		return nil
	})
	require.NoError(t, dbErr)

	t.Run("get all", func(t *testing.T) {
		ctx := getTestCtx("operations", []string{"id", "operationType", "operationXdr", "txHash", "ledgerNumber", "ledgerCreatedAt"})
		ops, err := resolver.Operations(ctx, nil, nil)
		require.NoError(t, err)
		assert.Len(t, ops.Edges, 4)
		assert.Equal(t, operations[3].ID, ops.Edges[0].Node.ID)
		assert.Equal(t, operations[2].ID, ops.Edges[1].Node.ID)
		assert.Equal(t, operations[1].ID, ops.Edges[2].Node.ID)
		assert.Equal(t, operations[0].ID, ops.Edges[3].Node.ID)
	})

	t.Run("get with limit", func(t *testing.T) {
		ctx := getTestCtx("operations", []string{"id", "operationType", "operationXdr", "txHash", "ledgerNumber", "ledgerCreatedAt"})
		limit := int32(1)
		ops, err := resolver.Operations(ctx, &limit, nil)
		require.NoError(t, err)
		assert.Len(t, ops.Edges, 1)
		assert.Equal(t, operations[3].ID, ops.Edges[0].Node.ID)
	})

	t.Run("get with cursor", func(t *testing.T) {
		ctx := getTestCtx("operations", []string{"id", "operationType", "operationXdr", "txHash", "ledgerNumber", "ledgerCreatedAt"})
		limit := int32(2)
		ops, err := resolver.Operations(ctx, &limit, nil)
		require.NoError(t, err)
		assert.Len(t, ops.Edges, 2)
		assert.Equal(t, operations[3].ID, ops.Edges[0].Node.ID)
		assert.Equal(t, operations[2].ID, ops.Edges[1].Node.ID)

		// Get the next cursor
		nextCursor := ops.PageInfo.EndCursor
		assert.NotNil(t, nextCursor)
		ops, err = resolver.Operations(ctx, &limit, nextCursor)
		require.NoError(t, err)
		assert.Len(t, ops.Edges, 2)
		assert.Equal(t, operations[1].ID, ops.Edges[0].Node.ID)
		assert.Equal(t, operations[0].ID, ops.Edges[1].Node.ID)
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
	require.NoError(t, dbErr)

	stateChanges := make([]*types.StateChange, 0, 4)
	for i := 0; i < 4; i++ {
		stateChanges = append(stateChanges, &types.StateChange{
			ToID:                int64(i + 1),
			StateChangeOrder:    int64(i + 1),
			StateChangeCategory: types.StateChangeCategoryCredit,
			TxHash:              "tx1",
			OperationID:         int64(i + 1),
			AccountID:           "account1",
			LedgerCreatedAt:     time.Now(),
			LedgerNumber:        1,
		})
	}

	dbErr = db.RunInTransaction(context.Background(), dbConnectionPool, nil, func(tx db.Transaction) error {
		for _, sc := range stateChanges {
			_, err := tx.ExecContext(ctx,
				`INSERT INTO state_changes (to_id, state_change_order, state_change_category, tx_hash, operation_id, account_id, ledger_created_at, ledger_number) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
				sc.ToID, sc.StateChangeOrder, sc.StateChangeCategory, sc.TxHash, sc.OperationID, sc.AccountID, sc.LedgerCreatedAt, sc.LedgerNumber)
			require.NoError(t, err)
		}
		return nil
	})
	require.NoError(t, dbErr)

	t.Run("get all", func(t *testing.T) {
		mockMetricsService := metrics.NewMockMetricsService()
		resolver.models.StateChanges.MetricsService = mockMetricsService
		mockMetricsService.On("ObserveDBQueryDuration", "SELECT", "state_changes", mock.Anything).Return()
		mockMetricsService.On("IncDBQuery", "SELECT", "state_changes").Return()
		ctx := getTestCtx("state_changes", []string{"id", "stateChangeCategory", "txHash", "operationId", "accountId", "ledgerCreatedAt", "ledgerNumber"})
		scs, err := resolver.StateChanges(ctx, nil, nil)
		require.NoError(t, err)
		assert.Len(t, scs.Edges, 4)
		assert.Equal(t, "sc4", fmt.Sprintf("%d:%d", scs.Edges[0].Node.ToID, scs.Edges[0].Node.StateChangeOrder))
		assert.Equal(t, "sc3", fmt.Sprintf("%d:%d", scs.Edges[1].Node.ToID, scs.Edges[1].Node.StateChangeOrder))
		assert.Equal(t, "sc2", fmt.Sprintf("%d:%d", scs.Edges[2].Node.ToID, scs.Edges[2].Node.StateChangeOrder))
		assert.Equal(t, "sc1", fmt.Sprintf("%d:%d", scs.Edges[3].Node.ToID, scs.Edges[3].Node.StateChangeOrder))
		mockMetricsService.AssertExpectations(t)
	})

	t.Run("get with limit", func(t *testing.T) {
		mockMetricsService := metrics.NewMockMetricsService()
		resolver.models.StateChanges.MetricsService = mockMetricsService
		mockMetricsService.On("ObserveDBQueryDuration", "SELECT", "state_changes", mock.Anything).Return()
		mockMetricsService.On("IncDBQuery", "SELECT", "state_changes").Return()
		limit := int32(2)
		ctx := getTestCtx("state_changes", []string{"id", "stateChangeCategory", "txHash", "operationId", "accountId", "ledgerCreatedAt", "ledgerNumber"})
		scs, err := resolver.StateChanges(ctx, &limit, nil)
		require.NoError(t, err)
		assert.Len(t, scs.Edges, 2)
		assert.Equal(t, "sc4", fmt.Sprintf("%d:%d", scs.Edges[0].Node.ToID, scs.Edges[0].Node.StateChangeOrder))
		assert.Equal(t, "sc3", fmt.Sprintf("%d:%d", scs.Edges[1].Node.ToID, scs.Edges[1].Node.StateChangeOrder))

		nextCursor := scs.PageInfo.EndCursor
		assert.NotNil(t, nextCursor)
		scs, err = resolver.StateChanges(ctx, &limit, nextCursor)
		require.NoError(t, err)
		assert.Len(t, scs.Edges, 2)
		assert.Equal(t, "sc2", fmt.Sprintf("%d:%d", scs.Edges[0].Node.ToID, scs.Edges[0].Node.StateChangeOrder))
		assert.Equal(t, "sc1", fmt.Sprintf("%d:%d", scs.Edges[1].Node.ToID, scs.Edges[1].Node.StateChangeOrder))

		hasNextPage := scs.PageInfo.HasNextPage
		assert.False(t, hasNextPage)
		mockMetricsService.AssertExpectations(t)
	})

	cleanUpDB()
}
