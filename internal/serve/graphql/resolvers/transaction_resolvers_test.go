package resolvers

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vikstrous/dataloadgen"

	"github.com/stellar/wallet-backend/internal/indexer/types"
	"github.com/stellar/wallet-backend/internal/serve/graphql/dataloaders"
	"github.com/stellar/wallet-backend/internal/serve/middleware"
)

func TestTransactionResolver_Operations(t *testing.T) {
	resolver := &transactionResolver{&Resolver{}}
	parentTx := &types.Transaction{Hash: "test-tx-hash"}

	t.Run("success", func(t *testing.T) {
		mockFetch := func(ctx context.Context, keys []string) ([][]*types.Operation, []error) {
			assert.Equal(t, []string{"test-tx-hash"}, keys)
			results := [][]*types.Operation{
				{
					{ID: 1},
					{ID: 2},
				},
			}
			return results, nil
		}

		loader := dataloadgen.NewLoader(mockFetch)
		loaders := &dataloaders.Dataloaders{
			OperationsByTxHashLoader: loader,
		}
		ctx := context.WithValue(context.Background(), middleware.LoadersKey, loaders)

		operations, err := resolver.Operations(ctx, parentTx)

		require.NoError(t, err)
		require.Len(t, operations, 2)
		assert.Equal(t, int64(1), operations[0].ID)
		assert.Equal(t, int64(2), operations[1].ID)
	})

	t.Run("dataloader error", func(t *testing.T) {
		mockFetch := func(ctx context.Context, keys []string) ([][]*types.Operation, []error) {
			return nil, []error{errors.New("op fetch error")}
		}

		loader := dataloadgen.NewLoader(mockFetch)
		loaders := &dataloaders.Dataloaders{
			OperationsByTxHashLoader: loader,
		}
		ctx := context.WithValue(context.Background(), middleware.LoadersKey, loaders)

		_, err := resolver.Operations(ctx, parentTx)

		require.Error(t, err)
		assert.EqualError(t, err, "op fetch error")
	})
}

func TestTransactionResolver_Accounts(t *testing.T) {
	resolver := &transactionResolver{&Resolver{}}
	parentTx := &types.Transaction{Hash: "test-tx-hash"}

	t.Run("success", func(t *testing.T) {
		mockFetch := func(ctx context.Context, keys []string) ([][]*types.Account, []error) {
			assert.Equal(t, []string{"test-tx-hash"}, keys)
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
			AccountsByTxHashLoader: loader,
		}
		ctx := context.WithValue(context.Background(), middleware.LoadersKey, loaders)

		accounts, err := resolver.Accounts(ctx, parentTx)

		require.NoError(t, err)
		require.Len(t, accounts, 2)
		assert.Equal(t, "G-ACCOUNT1", accounts[0].StellarAddress)
		assert.Equal(t, "G-ACCOUNT2", accounts[1].StellarAddress)
	})

	t.Run("dataloader error", func(t *testing.T) {
		mockFetch := func(ctx context.Context, keys []string) ([][]*types.Account, []error) {
			return nil, []error{errors.New("account fetch error")}
		}
		loader := dataloadgen.NewLoader(mockFetch)
		loaders := &dataloaders.Dataloaders{
			AccountsByTxHashLoader: loader,
		}
		ctx := context.WithValue(context.Background(), middleware.LoadersKey, loaders)

		_, err := resolver.Accounts(ctx, parentTx)

		require.Error(t, err)
		assert.EqualError(t, err, "account fetch error")
	})
}

func TestTransactionResolver_StateChanges(t *testing.T) {
	resolver := &transactionResolver{&Resolver{}}
	parentTx := &types.Transaction{Hash: "test-tx-hash"}

	t.Run("success", func(t *testing.T) {
		mockFetch := func(ctx context.Context, keys []string) ([][]*types.StateChange, []error) {
			assert.Equal(t, []string{"test-tx-hash"}, keys)
			results := [][]*types.StateChange{
				{
					{ID: "sc1"},
					{ID: "sc2"},
				},
			}
			return results, nil
		}
		loader := dataloadgen.NewLoader(mockFetch)
		loaders := &dataloaders.Dataloaders{
			StateChangesByTxHashLoader: loader,
		}
		ctx := context.WithValue(context.Background(), middleware.LoadersKey, loaders)

		stateChanges, err := resolver.StateChanges(ctx, parentTx)

		require.NoError(t, err)
		require.Len(t, stateChanges, 2)
		assert.Equal(t, "sc1", stateChanges[0].ID)
		assert.Equal(t, "sc2", stateChanges[1].ID)
	})

	t.Run("dataloader error", func(t *testing.T) {
		mockFetch := func(ctx context.Context, keys []string) ([][]*types.StateChange, []error) {
			return nil, []error{errors.New("sc fetch error")}
		}
		loader := dataloadgen.NewLoader(mockFetch)
		loaders := &dataloaders.Dataloaders{
			StateChangesByTxHashLoader: loader,
		}
		ctx := context.WithValue(context.Background(), middleware.LoadersKey, loaders)

		_, err := resolver.StateChanges(ctx, parentTx)

		require.Error(t, err)
		assert.EqualError(t, err, "sc fetch error")
	})
}
