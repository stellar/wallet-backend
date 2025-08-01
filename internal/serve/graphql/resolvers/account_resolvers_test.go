package resolvers

import (
	"context"
	"errors"
	"testing"

	"github.com/99designs/gqlgen/graphql"
	"github.com/vektah/gqlparser/v2/ast"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
					Name: table,
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
	resolver := &accountResolver{&Resolver{}}
	parentAccount := &types.Account{StellarAddress: "test-account"}

	t.Run("success", func(t *testing.T) {
		mockFetch := func(ctx context.Context, keys []dataloaders.TransactionColumnsKey) ([][]*types.Transaction, []error) {
			assert.Equal(t, []dataloaders.TransactionColumnsKey{
				{AccountID: "test-account", Columns: "transactions.hash"},
			}, keys)
			results := [][]*types.Transaction{
				{
					{Hash: "tx1"},
					{Hash: "tx2"},
				},
			}
			return results, nil
		}

		loader := dataloadgen.NewLoader(mockFetch)
		loaders := &dataloaders.Dataloaders{
			TransactionsByAccountLoader: loader,
		}
		ctx := context.WithValue(GetTestCtx("transactions", []string{"hash"}), middleware.LoadersKey, loaders)

		transactions, err := resolver.Transactions(ctx, parentAccount)

		require.NoError(t, err)
		require.Len(t, transactions, 2)
		assert.Equal(t, "tx1", transactions[0].Hash)
		assert.Equal(t, "tx2", transactions[1].Hash)
	})

	t.Run("dataloader error", func(t *testing.T) {
		mockFetch := func(ctx context.Context, keys []dataloaders.TransactionColumnsKey) ([][]*types.Transaction, []error) {
			return nil, []error{errors.New("something went wrong")}
		}

		loader := dataloadgen.NewLoader(mockFetch)
		loaders := &dataloaders.Dataloaders{
			TransactionsByAccountLoader: loader,
		}
		ctx := context.WithValue(GetTestCtx("transactions", []string{"hash"}), middleware.LoadersKey, loaders)

		_, err := resolver.Transactions(ctx, parentAccount)

		require.Error(t, err)
		assert.EqualError(t, err, "something went wrong")
	})
}

func TestAccountResolver_Operations(t *testing.T) {
	resolver := &accountResolver{&Resolver{}}
	parentAccount := &types.Account{StellarAddress: "test-account"}

	t.Run("success", func(t *testing.T) {
		mockFetch := func(ctx context.Context, keys []dataloaders.OperationColumnsKey) ([][]*types.Operation, []error) {
			assert.Equal(t, []dataloaders.OperationColumnsKey{
				{AccountID: "test-account", Columns: "operations.id"},
			}, keys)
			results := [][]*types.Operation{
				{
					{ID: 1, TxHash: "tx1"},
					{ID: 2, TxHash: "tx2"},
				},
			}
			return results, nil
		}

		loader := dataloadgen.NewLoader(mockFetch)
		loaders := &dataloaders.Dataloaders{
			OperationsByAccountLoader: loader,
		}
		ctx := context.WithValue(GetTestCtx("operations", []string{"id"}), middleware.LoadersKey, loaders)

		operations, err := resolver.Operations(ctx, parentAccount)

		require.NoError(t, err)
		require.Len(t, operations, 2)
		assert.Equal(t, "tx1", operations[0].TxHash)
		assert.Equal(t, "tx2", operations[1].TxHash)
	})
	t.Run("dataloader error", func(t *testing.T) {
		mockFetch := func(ctx context.Context, keys []dataloaders.OperationColumnsKey) ([][]*types.Operation, []error) {
			return nil, []error{errors.New("something went wrong")}
		}

		loader := dataloadgen.NewLoader(mockFetch)
		loaders := &dataloaders.Dataloaders{
			OperationsByAccountLoader: loader,
		}
		ctx := context.WithValue(GetTestCtx("operations", []string{"id"}), middleware.LoadersKey, loaders)

		_, err := resolver.Operations(ctx, parentAccount)

		require.Error(t, err)
		assert.EqualError(t, err, "something went wrong")
	})
}

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
