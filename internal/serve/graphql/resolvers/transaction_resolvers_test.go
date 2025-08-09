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
		mockFetch := func(ctx context.Context, keys []dataloaders.OperationColumnsKey) ([][]*types.Operation, []error) {
			assert.Equal(t, []dataloaders.OperationColumnsKey{
				{TxHash: "test-tx-hash", Columns: "id"},
			}, keys)
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
		ctx := context.WithValue(GetTestCtx("operations", []string{"id"}), middleware.LoadersKey, loaders)

		operations, err := resolver.Operations(ctx, parentTx)

		require.NoError(t, err)
		require.Len(t, operations, 2)
		assert.Equal(t, int64(1), operations[0].ID)
		assert.Equal(t, int64(2), operations[1].ID)
	})

	t.Run("dataloader error", func(t *testing.T) {
		mockFetch := func(ctx context.Context, keys []dataloaders.OperationColumnsKey) ([][]*types.Operation, []error) {
			return nil, []error{errors.New("op fetch error")}
		}

		loader := dataloadgen.NewLoader(mockFetch)
		loaders := &dataloaders.Dataloaders{
			OperationsByTxHashLoader: loader,
		}
		ctx := context.WithValue(GetTestCtx("operations", []string{"id"}), middleware.LoadersKey, loaders)

		_, err := resolver.Operations(ctx, parentTx)

		require.Error(t, err)
		assert.EqualError(t, err, "op fetch error")
	})
}

func TestTransactionResolver_Accounts(t *testing.T) {
	resolver := &transactionResolver{&Resolver{}}
	parentTx := &types.Transaction{Hash: "test-tx-hash"}

	t.Run("success", func(t *testing.T) {
		mockFetch := func(ctx context.Context, keys []dataloaders.AccountColumnsKey) ([][]*types.Account, []error) {
			assert.Equal(t, []dataloaders.AccountColumnsKey{
				{TxHash: "test-tx-hash", Columns: "accounts.stellar_address"},
			}, keys)
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
		ctx := context.WithValue(GetTestCtx("accounts", []string{"address"}), middleware.LoadersKey, loaders)

		accounts, err := resolver.Accounts(ctx, parentTx)

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
			AccountsByTxHashLoader: loader,
		}
		ctx := context.WithValue(GetTestCtx("accounts", []string{"address"}), middleware.LoadersKey, loaders)

		_, err := resolver.Accounts(ctx, parentTx)

		require.Error(t, err)
		assert.EqualError(t, err, "account fetch error")
	})
}

func TestTransactionResolver_StateChanges(t *testing.T) {
	resolver := &transactionResolver{&Resolver{}}
	parentTx := &types.Transaction{Hash: "test-tx-hash"}

	t.Run("success", func(t *testing.T) {
		mockFetch := func(ctx context.Context, keys []dataloaders.StateChangeColumnsKey) ([][]*types.StateChange, []error) {
			assert.Equal(t, []dataloaders.StateChangeColumnsKey{
				{TxHash: "test-tx-hash", Columns: "account_id, state_change_category"},
			}, keys)
			results := [][]*types.StateChange{
				{
					{ToID: 1, StateChangeOrder: 1},
					{ToID: 1, StateChangeOrder: 2},
				},
			}
			return results, nil
		}
		loader := dataloadgen.NewLoader(mockFetch)
		loaders := &dataloaders.Dataloaders{
			StateChangesByTxHashLoader: loader,
		}
		ctx := context.WithValue(GetTestCtx("state_changes", []string{"accountId", "stateChangeCategory"}), middleware.LoadersKey, loaders)

		stateChanges, err := resolver.StateChanges(ctx, parentTx)

		require.NoError(t, err)
		require.Len(t, stateChanges, 2)
		assert.Equal(t, int64(1), stateChanges[0].ToID)
		assert.Equal(t, int64(1), stateChanges[0].StateChangeOrder)
		assert.Equal(t, int64(1), stateChanges[1].ToID)
		assert.Equal(t, int64(2), stateChanges[1].StateChangeOrder)
	})

	t.Run("dataloader error", func(t *testing.T) {
		mockFetch := func(ctx context.Context, keys []dataloaders.StateChangeColumnsKey) ([][]*types.StateChange, []error) {
			return nil, []error{errors.New("sc fetch error")}
		}
		loader := dataloadgen.NewLoader(mockFetch)
		loaders := &dataloaders.Dataloaders{
			StateChangesByTxHashLoader: loader,
		}
		ctx := context.WithValue(GetTestCtx("state_changes", []string{"id"}), middleware.LoadersKey, loaders)

		_, err := resolver.StateChanges(ctx, parentTx)

		require.Error(t, err)
		assert.EqualError(t, err, "sc fetch error")
	})
}
