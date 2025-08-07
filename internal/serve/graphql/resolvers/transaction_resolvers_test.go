package resolvers

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/mock"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vikstrous/dataloadgen"

	"github.com/stellar/wallet-backend/internal/db"
	"github.com/stellar/wallet-backend/internal/db/dbtest"

	"github.com/stellar/wallet-backend/internal/data"
	"github.com/stellar/wallet-backend/internal/indexer/types"
	"github.com/stellar/wallet-backend/internal/metrics"
	"github.com/stellar/wallet-backend/internal/serve/graphql/dataloaders"
	"github.com/stellar/wallet-backend/internal/serve/middleware"
)

func TestTransactionResolver_Operations(t *testing.T) {
	parentTx := &types.Transaction{Hash: "tx1"}
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

	resolver := &transactionResolver{&Resolver{
		models: &data.Models{
			Operations: &data.OperationModel{
				DB:             dbConnectionPool,
				MetricsService: mockMetricsService,
			},
		},
	}}

	setupDB(ctx, t, dbConnectionPool)

	t.Run("get without pagination", func(t *testing.T) {
		loaders := &dataloaders.Dataloaders{
			OperationsByTxHashLoader: dataloaders.OperationsByTxHashLoader(resolver.models),
		}
		ctx := context.WithValue(getTestCtx("operations", []string{"id"}), middleware.LoadersKey, loaders)

		limit := int32(2)
		operations, err := resolver.Operations(ctx, parentTx, &limit, nil)

		require.NoError(t, err)
		require.Len(t, operations.Edges, 2)
		assert.Equal(t, int64(2), operations.Edges[0].Node.ID)
		assert.Equal(t, int64(1), operations.Edges[1].Node.ID)
	})

	t.Run("get with pagination", func(t *testing.T) {
		loaders := &dataloaders.Dataloaders{
			OperationsByTxHashLoader: dataloaders.OperationsByTxHashLoader(resolver.models),
		}
		ctx := context.WithValue(getTestCtx("operations", []string{"id"}), middleware.LoadersKey, loaders)

		limit := int32(1)
		operations, err := resolver.Operations(ctx, parentTx, &limit, nil)

		require.NoError(t, err)
		require.Len(t, operations.Edges, 1)
		assert.Equal(t, int64(2), operations.Edges[0].Node.ID)

		nextCursor := operations.PageInfo.EndCursor
		assert.NotNil(t, nextCursor)
		operations, err = resolver.Operations(ctx, parentTx, &limit, nextCursor)
		require.NoError(t, err)
		require.Len(t, operations.Edges, 1)
		assert.Equal(t, int64(1), operations.Edges[0].Node.ID)

		hasNextPage := operations.PageInfo.HasNextPage
		assert.False(t, hasNextPage)
	})

	cleanUpDB(ctx, t, dbConnectionPool)
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
		ctx := context.WithValue(getTestCtx("accounts", []string{"address"}), middleware.LoadersKey, loaders)

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
		ctx := context.WithValue(getTestCtx("accounts", []string{"address"}), middleware.LoadersKey, loaders)

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
				{TxHash: "test-tx-hash", Columns: "id"},
			}, keys)
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
		ctx := context.WithValue(getTestCtx("state_changes", []string{"id"}), middleware.LoadersKey, loaders)

		stateChanges, err := resolver.StateChanges(ctx, parentTx)

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
			StateChangesByTxHashLoader: loader,
		}
		ctx := context.WithValue(getTestCtx("state_changes", []string{"id"}), middleware.LoadersKey, loaders)

		_, err := resolver.StateChanges(ctx, parentTx)

		require.Error(t, err)
		assert.EqualError(t, err, "sc fetch error")
	})
}
