package resolvers

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vikstrous/dataloadgen"

	"github.com/stellar/wallet-backend/internal/indexer/types"
	"github.com/stellar/wallet-backend/internal/serve/graphql/dataloaders"
	"github.com/stellar/wallet-backend/internal/serve/middleware"
)

func TestOperationResolver_Transaction(t *testing.T) {
	resolver := &operationResolver{&Resolver{}}
	parentOperation := &types.Operation{ID: 123}

	t.Run("success", func(t *testing.T) {
		mockFetch := func(ctx context.Context, keys []dataloaders.TransactionColumnsKey) ([]*types.Transaction, []error) {
			assert.Equal(t, []dataloaders.TransactionColumnsKey{{OperationID: 123, Columns: "transactions.hash"}}, keys)
			results := []*types.Transaction{
				{Hash: "tx1"},
			}
			return results, nil
		}

		loader := dataloadgen.NewLoader(mockFetch)
		loaders := &dataloaders.Dataloaders{
			TransactionsByOperationIDLoader: loader,
		}
		ctx := context.WithValue(getTestCtx("transactions", []string{"hash"}), middleware.LoadersKey, loaders)

		transaction, err := resolver.Transaction(ctx, parentOperation)

		require.NoError(t, err)
		require.NotNil(t, transaction)
		assert.Equal(t, "tx1", transaction.Hash)
	})

	t.Run("dataloader error", func(t *testing.T) {
		mockFetch := func(ctx context.Context, keys []dataloaders.TransactionColumnsKey) ([]*types.Transaction, []error) {
			return nil, []error{errors.New("something went wrong")}
		}

		loader := dataloadgen.NewLoader(mockFetch)
		loaders := &dataloaders.Dataloaders{
			TransactionsByOperationIDLoader: loader,
		}
		ctx := context.WithValue(getTestCtx("transactions", []string{"hash"}), middleware.LoadersKey, loaders)

		_, err := resolver.Transaction(ctx, parentOperation)

		require.Error(t, err)
		assert.EqualError(t, err, "something went wrong")
	})
}

func TestOperationResolver_Accounts(t *testing.T) {
	resolver := &operationResolver{&Resolver{}}
	parentOperation := &types.Operation{ID: 123}

	t.Run("success", func(t *testing.T) {
		mockFetch := func(ctx context.Context, keys []dataloaders.AccountColumnsKey) ([][]*types.Account, []error) {
			assert.Equal(t, []dataloaders.AccountColumnsKey{{OperationID: 123, Columns: "accounts.stellar_address"}}, keys)
			results := [][]*types.Account{
				{
					{StellarAddress: "G-ACCOUNT1"},
					{StellarAddress: "G-ACCOUNT2"},
				},
			}
			return results, nil
		}
		loader := dataloadgen.NewLoader(mockFetch)
		loaders := &dataloaders.Dataloaders{
			AccountsByOperationIDLoader: loader,
		}
		ctx := context.WithValue(getTestCtx("accounts", []string{"address"}), middleware.LoadersKey, loaders)

		accounts, err := resolver.Accounts(ctx, parentOperation)

		require.NoError(t, err)
		require.Len(t, accounts, 2)
		assert.Equal(t, "G-ACCOUNT1", accounts[0].StellarAddress)
		assert.Equal(t, "G-ACCOUNT2", accounts[1].StellarAddress)
	})

	t.Run("dataloader error", func(t *testing.T) {
		mockFetch := func(ctx context.Context, keys []dataloaders.AccountColumnsKey) ([][]*types.Account, []error) {
			return nil, []error{errors.New("account fetch error")}
		}
		loader := dataloadgen.NewLoader(mockFetch)
		loaders := &dataloaders.Dataloaders{
			AccountsByOperationIDLoader: loader,
		}
		ctx := context.WithValue(getTestCtx("accounts", []string{"address"}), middleware.LoadersKey, loaders)

		_, err := resolver.Accounts(ctx, parentOperation)

		require.Error(t, err)
		assert.EqualError(t, err, "account fetch error")
	})
}

func TestOperationResolver_StateChanges(t *testing.T) {
	resolver := &operationResolver{&Resolver{}}
	parentOperation := &types.Operation{ID: 123}

	t.Run("success", func(t *testing.T) {
		mockFetch := func(ctx context.Context, keys []dataloaders.StateChangeColumnsKey) ([][]*types.StateChange, []error) {
			assert.Equal(t, []dataloaders.StateChangeColumnsKey{{OperationID: 123, Columns: ""}}, keys)
			results := [][]*types.StateChange{
				{
					{ToID: 1, StateChangeOrder: 1},
					{ToID: 2, StateChangeOrder: 1},
				},
			}
			return results, nil
		}
		loader := dataloadgen.NewLoader(mockFetch)
		loaders := &dataloaders.Dataloaders{
			StateChangesByOperationIDLoader: loader,
		}
		ctx := context.WithValue(getTestCtx("state_changes", []string{"id"}), middleware.LoadersKey, loaders)

		stateChanges, err := resolver.StateChanges(ctx, parentOperation)

		require.NoError(t, err)
		require.Len(t, stateChanges, 2)
		assert.Equal(t, "1:1", fmt.Sprintf("%d:%d", stateChanges[0].ToID, stateChanges[0].StateChangeOrder))
		assert.Equal(t, "2:1", fmt.Sprintf("%d:%d", stateChanges[1].ToID, stateChanges[1].StateChangeOrder))
	})

	t.Run("dataloader error", func(t *testing.T) {
		mockFetch := func(ctx context.Context, keys []dataloaders.StateChangeColumnsKey) ([][]*types.StateChange, []error) {
			return nil, []error{errors.New("sc fetch error")}
		}
		loader := dataloadgen.NewLoader(mockFetch)
		loaders := &dataloaders.Dataloaders{
			StateChangesByOperationIDLoader: loader,
		}
		ctx := context.WithValue(getTestCtx("state_changes", []string{"id"}), middleware.LoadersKey, loaders)

		_, err := resolver.StateChanges(ctx, parentOperation)

		require.Error(t, err)
		assert.EqualError(t, err, "sc fetch error")
	})
}
