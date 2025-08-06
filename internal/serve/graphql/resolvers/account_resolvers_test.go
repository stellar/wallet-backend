package resolvers

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"

	"github.com/stellar/wallet-backend/internal/data"
	"github.com/stellar/wallet-backend/internal/db"
	"github.com/stellar/wallet-backend/internal/db/dbtest"
	"github.com/stellar/wallet-backend/internal/metrics"

	"github.com/99designs/gqlgen/graphql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vektah/gqlparser/v2/ast"

	"github.com/stellar/wallet-backend/internal/indexer/types"
)

func GetTestCtx(table string, columns []string) context.Context {
	opCtx := &graphql.OperationContext{
		Operation: &ast.OperationDefinition{
			SelectionSet: ast.SelectionSet{
				&ast.Field{
					Name:         table,
					SelectionSet: ast.SelectionSet{},
				},
			},
		},
	}
	ctx := graphql.WithOperationContext(context.Background(), opCtx)
	var selections ast.SelectionSet
	for _, fieldName := range columns {
		selections = append(selections, &ast.Field{Name: fieldName})
	}
	fieldCtx := &graphql.FieldContext{
		Field: graphql.CollectedField{
			Selections: selections,
		},
	}
	ctx = graphql.WithFieldContext(ctx, fieldCtx)
	return ctx
}

func TestAccountResolver_Transactions(t *testing.T) {
	parentAccount := &types.Account{StellarAddress: "test-account"}
	ctx := context.Background()

	dbt := dbtest.Open(t)
	defer dbt.Close()
	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	mockMetricsService := &metrics.MockMetricsService{}
	mockMetricsService.On("IncDBQuery", "SELECT", "transactions").Return()
	mockMetricsService.On("ObserveDBQueryDuration", "SELECT", "transactions", mock.Anything).Return()
	defer mockMetricsService.AssertExpectations(t)

	resolver := &accountResolver{&Resolver{
		models: &data.Models{
			Transactions: &data.TransactionModel{
				DB:             dbConnectionPool,
				MetricsService: mockMetricsService,
			},
		},
	}}

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
		_, err := tx.ExecContext(ctx,
			`INSERT INTO accounts (stellar_address) VALUES ($1)`,
			parentAccount.StellarAddress)
		require.NoError(t, err)

		for _, txn := range txns {
			_, err = tx.ExecContext(ctx,
				`INSERT INTO transactions (hash, to_id, envelope_xdr, result_xdr, meta_xdr, ledger_number, ledger_created_at) VALUES ($1, $2, $3, $4, $5, $6, $7)`,
				txn.Hash, txn.ToID, txn.EnvelopeXDR, txn.ResultXDR, txn.MetaXDR, txn.LedgerNumber, txn.LedgerCreatedAt)
			require.NoError(t, err)

			_, err = tx.ExecContext(ctx,
				`INSERT INTO transactions_accounts (tx_hash, account_id) VALUES ($1, $2)`,
				txn.Hash, parentAccount.StellarAddress)
			require.NoError(t, err)
		}
		return nil
	})
	require.NoError(t, dbErr)

	t.Run("success", func(t *testing.T) {
		ctx = GetTestCtx("transactions", []string{"hash"})
		transactions, err := resolver.Transactions(ctx, parentAccount, nil, nil)

		require.NoError(t, err)
		require.Len(t, transactions.Edges, 4)
		assert.Equal(t, "tx4", transactions.Edges[0].Node.Hash)
		assert.Equal(t, "tx3", transactions.Edges[1].Node.Hash)
		assert.Equal(t, "tx2", transactions.Edges[2].Node.Hash)
		assert.Equal(t, "tx1", transactions.Edges[3].Node.Hash)
	})

	t.Run("get with cursor", func(t *testing.T) {
		ctx = GetTestCtx("transactions", []string{"hash"})
		limit := int32(2)
		txs, err := resolver.Transactions(ctx, parentAccount, &limit, nil)
		require.NoError(t, err)
		assert.Len(t, txs.Edges, 2)
		assert.Equal(t, "tx4", txs.Edges[0].Node.Hash)
		assert.Equal(t, "tx3", txs.Edges[1].Node.Hash)

		// Get the next cursor
		nextCursor := txs.PageInfo.EndCursor
		assert.NotNil(t, nextCursor)
		txs, err = resolver.Transactions(ctx, parentAccount, &limit, nextCursor)
		require.NoError(t, err)
		assert.Len(t, txs.Edges, 2)
		assert.Equal(t, "tx2", txs.Edges[0].Node.Hash)
		assert.Equal(t, "tx1", txs.Edges[1].Node.Hash)

		hasNextPage := txs.PageInfo.HasNextPage
		assert.False(t, hasNextPage)
	})

	cleanUpDB(ctx, t, dbConnectionPool)
}

func TestAccountResolver_Operations(t *testing.T) {
	parentAccount := &types.Account{StellarAddress: "test-account"}
	ctx := context.Background()

	dbt := dbtest.Open(t)
	defer dbt.Close()
	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	mockMetricsService := &metrics.MockMetricsService{}
	mockMetricsService.On("IncDBQuery", "SELECT", "operations").Return()
	mockMetricsService.On("ObserveDBQueryDuration", "SELECT", "operations", mock.Anything).Return()
	defer mockMetricsService.AssertExpectations(t)

	resolver := &accountResolver{&Resolver{
		models: &data.Models{
			Operations: &data.OperationModel{
				DB:             dbConnectionPool,
				MetricsService: mockMetricsService,
			},
		},
	}}

	setupDB(ctx, t, dbConnectionPool)

	t.Run("success", func(t *testing.T) {
		ctx = GetTestCtx("operations", []string{"tx_hash"})
		operations, err := resolver.Operations(ctx, parentAccount, nil, nil)

		require.NoError(t, err)
		require.Len(t, operations.Edges, 4)
		assert.Equal(t, "tx4", operations.Edges[0].Node.TxHash)
		assert.Equal(t, "tx3", operations.Edges[1].Node.TxHash)
		assert.Equal(t, "tx2", operations.Edges[2].Node.TxHash)
		assert.Equal(t, "tx1", operations.Edges[3].Node.TxHash)
	})

	t.Run("get with cursor", func(t *testing.T) {
		ctx = GetTestCtx("operations", []string{"tx_hash"})
		limit := int32(2)
		ops, err := resolver.Operations(ctx, parentAccount, &limit, nil)
		require.NoError(t, err)
		assert.Len(t, ops.Edges, 2)
		assert.Equal(t, "tx4", ops.Edges[0].Node.TxHash)
		assert.Equal(t, "tx3", ops.Edges[1].Node.TxHash)

		// Get the next cursor
		nextCursor := ops.PageInfo.EndCursor
		assert.NotNil(t, nextCursor)
		ops, err = resolver.Operations(ctx, parentAccount, &limit, nextCursor)
		require.NoError(t, err)
		assert.Len(t, ops.Edges, 2)
		assert.Equal(t, "tx2", ops.Edges[0].Node.TxHash)
		assert.Equal(t, "tx1", ops.Edges[1].Node.TxHash)

		hasNextPage := ops.PageInfo.HasNextPage
		assert.False(t, hasNextPage)
	})

	cleanUpDB(ctx, t, dbConnectionPool)
}

func TestAccountResolver_StateChanges(t *testing.T) {
	parentAccount := &types.Account{StellarAddress: "test-account"}
	ctx := context.Background()

	dbt := dbtest.Open(t)
	defer dbt.Close()
	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	mockMetricsService := &metrics.MockMetricsService{}
	mockMetricsService.On("IncDBQuery", "SELECT", "state_changes").Return()
	mockMetricsService.On("ObserveDBQueryDuration", "SELECT", "state_changes", mock.Anything).Return()
	defer mockMetricsService.AssertExpectations(t)

	resolver := &accountResolver{&Resolver{
		models: &data.Models{
			StateChanges: &data.StateChangeModel{
				DB:             dbConnectionPool,
				MetricsService: mockMetricsService,
			},
		},
	}}

	setupDB(ctx, t, dbConnectionPool)

	t.Run("success", func(t *testing.T) {
		ctx := GetTestCtx("state_changes", []string{"id"})
		stateChanges, err := resolver.StateChanges(ctx, parentAccount, nil, nil)

		require.NoError(t, err)
		require.Len(t, stateChanges.Edges, 4)
		assert.Equal(t, "sc4", stateChanges.Edges[0].Node.ID)
		assert.Equal(t, "sc3", stateChanges.Edges[1].Node.ID)
		assert.Equal(t, "sc2", stateChanges.Edges[2].Node.ID)
		assert.Equal(t, "sc1", stateChanges.Edges[3].Node.ID)
	})

	t.Run("get with cursor", func(t *testing.T) {
		ctx := GetTestCtx("state_changes", []string{"id"})
		limit := int32(2)
		stateChanges, err := resolver.StateChanges(ctx, parentAccount, &limit, nil)

		require.NoError(t, err)
		assert.Len(t, stateChanges.Edges, 2)
		assert.Equal(t, "sc4", stateChanges.Edges[0].Node.ID)
		assert.Equal(t, "sc3", stateChanges.Edges[1].Node.ID)

		nextCursor := stateChanges.PageInfo.EndCursor
		assert.NotNil(t, nextCursor)
		stateChanges, err = resolver.StateChanges(ctx, parentAccount, &limit, nextCursor)
		require.NoError(t, err)
		assert.Len(t, stateChanges.Edges, 2)
		assert.Equal(t, "sc2", stateChanges.Edges[0].Node.ID)
		assert.Equal(t, "sc1", stateChanges.Edges[1].Node.ID)

		hasNextPage := stateChanges.PageInfo.HasNextPage
		assert.False(t, hasNextPage)
	})

	cleanUpDB(ctx, t, dbConnectionPool)
}
