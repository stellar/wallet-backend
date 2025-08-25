package resolvers

import (
	"context"
	"fmt"
	"testing"

	"github.com/stellar/go/toid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/stellar/wallet-backend/internal/data"
	"github.com/stellar/wallet-backend/internal/indexer/types"
	"github.com/stellar/wallet-backend/internal/metrics"
	"github.com/stellar/wallet-backend/internal/serve/graphql/dataloaders"
	"github.com/stellar/wallet-backend/internal/serve/middleware"
)

func TestOperationResolver_Transaction(t *testing.T) {
	mockMetricsService := &metrics.MockMetricsService{}
	mockMetricsService.On("IncDBQuery", "SELECT", "transactions").Return()
	mockMetricsService.On("ObserveDBQueryDuration", "SELECT", "transactions", mock.Anything).Return()
	defer mockMetricsService.AssertExpectations(t)

	resolver := &operationResolver{&Resolver{
		models: &data.Models{
			Transactions: &data.TransactionModel{
				DB:             testDBConnectionPool,
				MetricsService: mockMetricsService,
			},
		},
	}}
	parentOperation := &types.Operation{ID: toid.New(1000, 1, 1).ToInt64()}

	t.Run("success", func(t *testing.T) {
		loaders := dataloaders.NewDataloaders(resolver.models)
		ctx := context.WithValue(getTestCtx("transactions", []string{"hash"}), middleware.LoadersKey, loaders)

		transaction, err := resolver.Transaction(ctx, parentOperation)

		require.NoError(t, err)
		require.NotNil(t, transaction)
		assert.Equal(t, "tx1", transaction.Hash)
	})

	t.Run("nil operation panics", func(t *testing.T) {
		loaders := dataloaders.NewDataloaders(resolver.models)
		ctx := context.WithValue(getTestCtx("transactions", []string{"hash"}), middleware.LoadersKey, loaders)

		assert.Panics(t, func() {
			_, _ = resolver.Transaction(ctx, nil) //nolint:errcheck
		})
	})

	t.Run("operation with non-existent transaction", func(t *testing.T) {
		nonExistentOperation := &types.Operation{ID: 9999, TxHash: "non-existent-tx"}
		loaders := dataloaders.NewDataloaders(resolver.models)
		ctx := context.WithValue(getTestCtx("transactions", []string{"hash"}), middleware.LoadersKey, loaders)

		transaction, err := resolver.Transaction(ctx, nonExistentOperation)

		require.NoError(t, err) // Dataloader returns nil, not error for missing data
		assert.Nil(t, transaction)
	})
}

func TestOperationResolver_Accounts(t *testing.T) {
	mockMetricsService := &metrics.MockMetricsService{}
	mockMetricsService.On("IncDBQuery", "SELECT", "accounts").Return()
	mockMetricsService.On("ObserveDBQueryDuration", "SELECT", "accounts", mock.Anything).Return()
	defer mockMetricsService.AssertExpectations(t)

	resolver := &operationResolver{&Resolver{
		models: &data.Models{
			Account: &data.AccountModel{
				DB:             testDBConnectionPool,
				MetricsService: mockMetricsService,
			},
		},
	}}
	parentOperation := &types.Operation{ID: toid.New(1000, 1, 1).ToInt64()}

	t.Run("success", func(t *testing.T) {
		loaders := dataloaders.NewDataloaders(resolver.models)
		ctx := context.WithValue(getTestCtx("accounts", []string{"address"}), middleware.LoadersKey, loaders)

		accounts, err := resolver.Accounts(ctx, parentOperation)

		require.NoError(t, err)
		require.Len(t, accounts, 1)
		assert.Equal(t, "test-account", accounts[0].StellarAddress)
	})

	t.Run("nil operation panics", func(t *testing.T) {
		loaders := dataloaders.NewDataloaders(resolver.models)
		ctx := context.WithValue(getTestCtx("accounts", []string{"address"}), middleware.LoadersKey, loaders)

		assert.Panics(t, func() {
			_, _ = resolver.Accounts(ctx, nil) //nolint:errcheck
		})
	})

	t.Run("operation with no associated accounts", func(t *testing.T) {
		nonExistentOperation := &types.Operation{ID: 9999}
		loaders := dataloaders.NewDataloaders(resolver.models)
		ctx := context.WithValue(getTestCtx("accounts", []string{"address"}), middleware.LoadersKey, loaders)

		accounts, err := resolver.Accounts(ctx, nonExistentOperation)

		require.NoError(t, err)
		assert.Empty(t, accounts)
	})
}

func TestOperationResolver_StateChanges(t *testing.T) {
	mockMetricsService := &metrics.MockMetricsService{}
	mockMetricsService.On("IncDBQuery", "SELECT", "state_changes").Return()
	mockMetricsService.On("ObserveDBQueryDuration", "SELECT", "state_changes", mock.Anything).Return()
	defer mockMetricsService.AssertExpectations(t)

	resolver := &operationResolver{&Resolver{
		models: &data.Models{
			StateChanges: &data.StateChangeModel{
				DB:             testDBConnectionPool,
				MetricsService: mockMetricsService,
			},
			Operations: &data.OperationModel{
				DB:             testDBConnectionPool,
				MetricsService: mockMetricsService,
			},
		},
	}}
	parentOperation := &types.Operation{ID: toid.New(1000, 1, 1).ToInt64()}
	nonExistentOperation := &types.Operation{ID: 9999}

	t.Run("success without pagination", func(t *testing.T) {
		loaders := dataloaders.NewDataloaders(resolver.models)
		ctx := context.WithValue(getTestCtx("state_changes", []string{}), middleware.LoadersKey, loaders)

		stateChanges, err := resolver.StateChanges(ctx, parentOperation, nil, nil, nil, nil)

		require.NoError(t, err)
		require.Len(t, stateChanges.Edges, 2)
		// operation 1000,1,1 has 2 state changes
		assert.Equal(t, fmt.Sprintf("%d:1", toid.New(1000, 1, 1).ToInt64()), fmt.Sprintf("%d:%d", stateChanges.Edges[0].Node.ToID, stateChanges.Edges[0].Node.StateChangeOrder))
		assert.Equal(t, fmt.Sprintf("%d:2", toid.New(1000, 1, 1).ToInt64()), fmt.Sprintf("%d:%d", stateChanges.Edges[1].Node.ToID, stateChanges.Edges[1].Node.StateChangeOrder))
		assert.False(t, stateChanges.PageInfo.HasNextPage)
		assert.False(t, stateChanges.PageInfo.HasPreviousPage)
	})

	t.Run("get state changes with first/after pagination", func(t *testing.T) {
		loaders := dataloaders.NewDataloaders(resolver.models)
		ctx := context.WithValue(getTestCtx("state_changes", []string{""}), middleware.LoadersKey, loaders)
		first := int32(1)
		stateChanges, err := resolver.StateChanges(ctx, parentOperation, &first, nil, nil, nil)
		require.NoError(t, err)
		assert.Len(t, stateChanges.Edges, 1)
		assert.Equal(t, fmt.Sprintf("%d:1", toid.New(1000, 1, 1).ToInt64()), fmt.Sprintf("%d:%d", stateChanges.Edges[0].Node.ToID, stateChanges.Edges[0].Node.StateChangeOrder))
		assert.True(t, stateChanges.PageInfo.HasNextPage)
		assert.False(t, stateChanges.PageInfo.HasPreviousPage)

		// Get the next page using cursor
		nextCursor := stateChanges.PageInfo.EndCursor
		assert.NotNil(t, nextCursor)
		stateChanges, err = resolver.StateChanges(ctx, parentOperation, &first, nextCursor, nil, nil)
		require.NoError(t, err)
		assert.Len(t, stateChanges.Edges, 1)
		assert.Equal(t, fmt.Sprintf("%d:2", toid.New(1000, 1, 1).ToInt64()), fmt.Sprintf("%d:%d", stateChanges.Edges[0].Node.ToID, stateChanges.Edges[0].Node.StateChangeOrder))
		assert.False(t, stateChanges.PageInfo.HasNextPage)
		assert.True(t, stateChanges.PageInfo.HasPreviousPage)
	})

	t.Run("get state changes with last/before pagination", func(t *testing.T) {
		loaders := dataloaders.NewDataloaders(resolver.models)
		ctx := context.WithValue(getTestCtx("state_changes", []string{""}), middleware.LoadersKey, loaders)
		last := int32(1)
		stateChanges, err := resolver.StateChanges(ctx, parentOperation, nil, nil, &last, nil)
		require.NoError(t, err)
		assert.Len(t, stateChanges.Edges, 1)
		assert.Equal(t, fmt.Sprintf("%d:2", toid.New(1000, 1, 1).ToInt64()), fmt.Sprintf("%d:%d", stateChanges.Edges[0].Node.ToID, stateChanges.Edges[0].Node.StateChangeOrder))
		assert.False(t, stateChanges.PageInfo.HasNextPage)
		assert.True(t, stateChanges.PageInfo.HasPreviousPage)

		// Get the previous page using cursor
		last = int32(10)
		prevCursor := stateChanges.PageInfo.StartCursor
		assert.NotNil(t, prevCursor)
		stateChanges, err = resolver.StateChanges(ctx, parentOperation, nil, nil, &last, prevCursor)
		require.NoError(t, err)
		assert.Len(t, stateChanges.Edges, 1)
		assert.Equal(t, fmt.Sprintf("%d:1", toid.New(1000, 1, 1).ToInt64()), fmt.Sprintf("%d:%d", stateChanges.Edges[0].Node.ToID, stateChanges.Edges[0].Node.StateChangeOrder))
		assert.True(t, stateChanges.PageInfo.HasNextPage)
		assert.False(t, stateChanges.PageInfo.HasPreviousPage)
	})

	t.Run("invalid pagination params", func(t *testing.T) {
		loaders := dataloaders.NewDataloaders(resolver.models)
		ctx := context.WithValue(getTestCtx("state_changes", []string{""}), middleware.LoadersKey, loaders)
		first := int32(0)
		last := int32(1)
		after := encodeCursor(int64(1))
		before := encodeCursor(int64(2))

		_, err := resolver.StateChanges(ctx, parentOperation, &first, nil, nil, nil)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "validating pagination params: first must be greater than 0")

		first = int32(1)
		_, err = resolver.StateChanges(ctx, parentOperation, &first, nil, &last, nil)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "validating pagination params: first and last cannot be used together")

		_, err = resolver.StateChanges(ctx, parentOperation, nil, &after, nil, &before)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "validating pagination params: after and before cannot be used together")

		_, err = resolver.StateChanges(ctx, parentOperation, &first, nil, nil, &before)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "validating pagination params: first and before cannot be used together")

		_, err = resolver.StateChanges(ctx, parentOperation, nil, &after, &last, nil)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "validating pagination params: last and after cannot be used together")
	})

	t.Run("pagination with larger limit than available data", func(t *testing.T) {
		loaders := dataloaders.NewDataloaders(resolver.models)
		ctx := context.WithValue(getTestCtx("state_changes", []string{""}), middleware.LoadersKey, loaders)
		first := int32(100)
		stateChanges, err := resolver.StateChanges(ctx, parentOperation, &first, nil, nil, nil)
		require.NoError(t, err)
		assert.Len(t, stateChanges.Edges, 2) // operation has 2 state changes
		assert.False(t, stateChanges.PageInfo.HasNextPage)
		assert.False(t, stateChanges.PageInfo.HasPreviousPage)
	})

	t.Run("nil operation panics", func(t *testing.T) {
		loaders := dataloaders.NewDataloaders(resolver.models)
		ctx := context.WithValue(getTestCtx("state_changes", []string{""}), middleware.LoadersKey, loaders)

		assert.Panics(t, func() {
			_, _ = resolver.StateChanges(ctx, nil, nil, nil, nil, nil) //nolint:errcheck
		})
	})

	t.Run("operation with no state changes", func(t *testing.T) {
		loaders := dataloaders.NewDataloaders(resolver.models)
		ctx := context.WithValue(getTestCtx("state_changes", []string{""}), middleware.LoadersKey, loaders)

		stateChanges, err := resolver.StateChanges(ctx, nonExistentOperation, nil, nil, nil, nil)

		require.NoError(t, err)
		assert.Empty(t, stateChanges.Edges)
		assert.False(t, stateChanges.PageInfo.HasNextPage)
		assert.False(t, stateChanges.PageInfo.HasPreviousPage)
	})
}
