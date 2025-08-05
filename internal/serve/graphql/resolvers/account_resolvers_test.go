package resolvers

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stellar/wallet-backend/internal/db"
	"github.com/stellar/wallet-backend/internal/db/dbtest"
	"github.com/stellar/wallet-backend/internal/data"
	"github.com/stellar/wallet-backend/internal/metrics"

	"github.com/99designs/gqlgen/graphql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vektah/gqlparser/v2/ast"
	"github.com/vikstrous/dataloadgen"

	"github.com/stellar/wallet-backend/internal/indexer/types"
	"github.com/stellar/wallet-backend/internal/serve/graphql/dataloaders"
	"github.com/stellar/wallet-backend/internal/serve/middleware"
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

	cleanUpDB := func() {
		_, err = dbConnectionPool.ExecContext(ctx, `DELETE FROM operations`)
		require.NoError(t, err)
		_, err = dbConnectionPool.ExecContext(ctx, `DELETE FROM transactions`)
		require.NoError(t, err)
	}

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

	cleanUpDB()
}

// func TestAccountResolver_Operations(t *testing.T) {
// 	resolver := &accountResolver{&Resolver{}}
// 	parentAccount := &types.Account{StellarAddress: "test-account"}

// 	t.Run("success", func(t *testing.T) {
// 		mockFetch := func(ctx context.Context, keys []dataloaders.OperationColumnsKey) ([][]*types.Operation, []error) {
// 			assert.Equal(t, []dataloaders.OperationColumnsKey{
// 				{AccountID: "test-account", Columns: "operations.id"},
// 			}, keys)
// 			results := [][]*types.Operation{
// 				{
// 					{ID: 1, TxHash: "tx1"},
// 					{ID: 2, TxHash: "tx2"},
// 				},
// 			}
// 			return results, nil
// 		}

// 		loader := dataloadgen.NewLoader(mockFetch)
// 		loaders := &dataloaders.Dataloaders{
// 			OperationsByAccountLoader: loader,
// 		}
// 		ctx := context.WithValue(GetTestCtx("operations", []string{"id"}), middleware.LoadersKey, loaders)

// 		operations, err := resolver.Operations(ctx, parentAccount)

// 		require.NoError(t, err)
// 		require.Len(t, operations, 2)
// 		assert.Equal(t, "tx1", operations[0].TxHash)
// 		assert.Equal(t, "tx2", operations[1].TxHash)
// 	})
// 	t.Run("dataloader error", func(t *testing.T) {
// 		mockFetch := func(ctx context.Context, keys []dataloaders.OperationColumnsKey) ([][]*types.Operation, []error) {
// 			return nil, []error{errors.New("something went wrong")}
// 		}

// 		loader := dataloadgen.NewLoader(mockFetch)
// 		loaders := &dataloaders.Dataloaders{
// 			OperationsByAccountLoader: loader,
// 		}
// 		ctx := context.WithValue(GetTestCtx("operations", []string{"id"}), middleware.LoadersKey, loaders)

// 		_, err := resolver.Operations(ctx, parentAccount)

// 		require.Error(t, err)
// 		assert.EqualError(t, err, "something went wrong")
// 	})
// }

func TestAccountResolver_StateChanges(t *testing.T) {
	resolver := &accountResolver{&Resolver{}}
	parentAccount := &types.Account{StellarAddress: "test-account"}

	t.Run("success", func(t *testing.T) {
		mockFetch := func(ctx context.Context, keys []dataloaders.StateChangeColumnsKey) ([][]*types.StateChange, []error) {
			assert.Equal(t, []dataloaders.StateChangeColumnsKey{
				{AccountID: "test-account", Columns: "state_changes.id"},
			}, keys)
			results := [][]*types.StateChange{
				{
					{ID: "sc1", TxHash: "tx1"},
					{ID: "sc2", TxHash: "tx1"},
				},
			}
			return results, nil
		}
		loader := dataloadgen.NewLoader(mockFetch)
		loaders := &dataloaders.Dataloaders{
			StateChangesByAccountLoader: loader,
		}
		ctx := context.WithValue(GetTestCtx("state_changes", []string{"id"}), middleware.LoadersKey, loaders)

		stateChanges, err := resolver.StateChanges(ctx, parentAccount)

		require.NoError(t, err)
		require.Len(t, stateChanges, 2)
		assert.Equal(t, "sc1", stateChanges[0].ID)
		assert.Equal(t, "sc2", stateChanges[1].ID)
	})

	t.Run("dataloader error", func(t *testing.T) {
		mockFetch := func(ctx context.Context, keys []dataloaders.StateChangeColumnsKey) ([][]*types.StateChange, []error) {
			return nil, []error{errors.New("sc fetch error")}
		}
		loader := dataloadgen.NewLoader(mockFetch)
		loaders := &dataloaders.Dataloaders{
			StateChangesByAccountLoader: loader,
		}
		ctx := context.WithValue(GetTestCtx("state_changes", []string{"id"}), middleware.LoadersKey, loaders)

		_, err := resolver.StateChanges(ctx, parentAccount)

		require.Error(t, err)
		assert.EqualError(t, err, "sc fetch error")
	})
}
