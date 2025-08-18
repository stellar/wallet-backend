package resolvers

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/stellar/wallet-backend/internal/data"
	"github.com/stellar/wallet-backend/internal/indexer/types"
	"github.com/stellar/wallet-backend/internal/metrics"
	"github.com/stellar/wallet-backend/internal/serve/graphql/dataloaders"
	"github.com/stellar/wallet-backend/internal/serve/middleware"
)

func TestAccountResolver_Transactions(t *testing.T) {
	parentAccount := &types.Account{StellarAddress: "test-account"}

	mockMetricsService := &metrics.MockMetricsService{}
	mockMetricsService.On("IncDBQuery", "SELECT", "transactions").Return()
	mockMetricsService.On("ObserveDBQueryDuration", "SELECT", "transactions", mock.Anything).Return()

	resolver := &accountResolver{
		&Resolver{
			models: &data.Models{
				Transactions: &data.TransactionModel{
					DB:             testDBConnectionPool,
					MetricsService: mockMetricsService,
				},
			},
		},
	}

	t.Run("get all transactions", func(t *testing.T) {
		ctx := getTestCtx("transactions", []string{"hash"})
		transactions, err := resolver.Transactions(ctx, parentAccount, nil, nil, nil, nil)

		require.NoError(t, err)
		require.Len(t, transactions.Edges, 4)
		assert.Equal(t, "tx1", transactions.Edges[0].Node.Hash)
		assert.Equal(t, "tx2", transactions.Edges[1].Node.Hash)
		assert.Equal(t, "tx3", transactions.Edges[2].Node.Hash)
		assert.Equal(t, "tx4", transactions.Edges[3].Node.Hash)
		mockMetricsService.AssertExpectations(t)
	})

	t.Run("get transactions with first/after limit and cursor", func(t *testing.T) {
		ctx := getTestCtx("transactions", []string{"hash"})
		first := int32(2)
		txs, err := resolver.Transactions(ctx, parentAccount, &first, nil, nil, nil)
		require.NoError(t, err)
		assert.Len(t, txs.Edges, 2)
		assert.Equal(t, "tx1", txs.Edges[0].Node.Hash)
		assert.Equal(t, "tx2", txs.Edges[1].Node.Hash)
		assert.True(t, txs.PageInfo.HasNextPage)
		assert.False(t, txs.PageInfo.HasPreviousPage)

		// Get the next cursor
		nextCursor := txs.PageInfo.EndCursor
		assert.NotNil(t, nextCursor)
		txs, err = resolver.Transactions(ctx, parentAccount, &first, nextCursor, nil, nil)
		require.NoError(t, err)
		assert.Len(t, txs.Edges, 2)
		assert.Equal(t, "tx3", txs.Edges[0].Node.Hash)
		assert.Equal(t, "tx4", txs.Edges[1].Node.Hash)
		assert.False(t, txs.PageInfo.HasNextPage)
		assert.True(t, txs.PageInfo.HasPreviousPage)
		mockMetricsService.AssertExpectations(t)
	})

	t.Run("get transactions with last/before limit and cursor", func(t *testing.T) {
		ctx := getTestCtx("transactions", []string{"hash"})
		last := int32(2)
		txs, err := resolver.Transactions(ctx, parentAccount, nil, nil, &last, nil)
		require.NoError(t, err)
		assert.Len(t, txs.Edges, 2)
		assert.Equal(t, "tx3", txs.Edges[0].Node.Hash)
		assert.Equal(t, "tx4", txs.Edges[1].Node.Hash)
		assert.False(t, txs.PageInfo.HasNextPage)
		assert.True(t, txs.PageInfo.HasPreviousPage)

		// Get the next cursor
		last = int32(1)
		nextCursor := txs.PageInfo.EndCursor
		assert.NotNil(t, nextCursor)
		txs, err = resolver.Transactions(ctx, parentAccount, nil, nil, &last, nextCursor)
		require.NoError(t, err)
		assert.Len(t, txs.Edges, 1)
		assert.Equal(t, "tx2", txs.Edges[0].Node.Hash)
		assert.True(t, txs.PageInfo.HasNextPage)
		assert.True(t, txs.PageInfo.HasPreviousPage)

		nextCursor = txs.PageInfo.EndCursor
		assert.NotNil(t, nextCursor)
		last = int32(10)
		txs, err = resolver.Transactions(ctx, parentAccount, nil, nil, &last, nextCursor)
		require.NoError(t, err)
		assert.Len(t, txs.Edges, 1)
		assert.Equal(t, "tx1", txs.Edges[0].Node.Hash)
		assert.True(t, txs.PageInfo.HasNextPage)
		assert.False(t, txs.PageInfo.HasPreviousPage)
		mockMetricsService.AssertExpectations(t)
	})

	t.Run("account with no transactions", func(t *testing.T) {
		nonExistentAccount := &types.Account{StellarAddress: "non-existent-account"}
		ctx := getTestCtx("transactions", []string{"hash"})
		transactions, err := resolver.Transactions(ctx, nonExistentAccount, nil, nil, nil, nil)

		require.NoError(t, err)
		assert.Empty(t, transactions.Edges)
		mockMetricsService.AssertExpectations(t)
	})

	t.Run("invalid pagination params", func(t *testing.T) {
		ctx := getTestCtx("transactions", []string{"hash"})
		first := int32(0)
		last := int32(1)
		after := encodeCursor(int64(4))
		before := encodeCursor(int64(1))
		_, err := resolver.Transactions(ctx, parentAccount, &first, &after, nil, nil)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "validating pagination params: first must be greater than 0")

		first = int32(1)
		_, err = resolver.Transactions(ctx, parentAccount, &first, nil, &last, nil)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "validating pagination params: first and last cannot be used together")

		_, err = resolver.Transactions(ctx, parentAccount, nil, &after, nil, &before)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "validating pagination params: after and before cannot be used together")

		_, err = resolver.Transactions(ctx, parentAccount, &first, nil, nil, &before)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "validating pagination params: first and before cannot be used together")

		_, err = resolver.Transactions(ctx, parentAccount, nil, &after, &last, nil)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "validating pagination params: last and after cannot be used together")
	})
}

func TestAccountResolver_Operations(t *testing.T) {
	parentAccount := &types.Account{StellarAddress: "test-account"}

	mockMetricsService := &metrics.MockMetricsService{}
	mockMetricsService.On("IncDBQuery", "SELECT", "operations").Return()
	mockMetricsService.On("ObserveDBQueryDuration", "SELECT", "operations", mock.Anything).Return()
	defer mockMetricsService.AssertExpectations(t)

	resolver := &accountResolver{&Resolver{
		models: &data.Models{
			Operations: &data.OperationModel{
				DB:             testDBConnectionPool,
				MetricsService: mockMetricsService,
			},
		},
	}}

	t.Run("get all operations", func(t *testing.T) {
		ctx := getTestCtx("operations", []string{"operation_type"})
		operations, err := resolver.Operations(ctx, parentAccount, nil, nil, nil, nil)

		require.NoError(t, err)
		require.Len(t, operations.Edges, 8)
		assert.Equal(t, int64(1001), operations.Edges[0].Node.ID)
		assert.Equal(t, int64(1002), operations.Edges[1].Node.ID)
		assert.Equal(t, int64(1003), operations.Edges[2].Node.ID)
		assert.Equal(t, int64(1004), operations.Edges[3].Node.ID)
	})

	t.Run("get operations with first/after limit and cursor", func(t *testing.T) {
		ctx := getTestCtx("operations", []string{"operation_type"})
		first := int32(2)
		ops, err := resolver.Operations(ctx, parentAccount, &first, nil, nil, nil)
		require.NoError(t, err)
		assert.Len(t, ops.Edges, 2)
		assert.Equal(t, int64(1001), ops.Edges[0].Node.ID)
		assert.Equal(t, int64(1002), ops.Edges[1].Node.ID)
		assert.True(t, ops.PageInfo.HasNextPage)
		assert.False(t, ops.PageInfo.HasPreviousPage)

		// Get the next cursor
		nextCursor := ops.PageInfo.EndCursor
		assert.NotNil(t, nextCursor)
		ops, err = resolver.Operations(ctx, parentAccount, &first, nextCursor, nil, nil)
		require.NoError(t, err)
		assert.Len(t, ops.Edges, 2)
		assert.Equal(t, int64(1003), ops.Edges[0].Node.ID)
		assert.Equal(t, int64(1004), ops.Edges[1].Node.ID)
		assert.True(t, ops.PageInfo.HasNextPage)
		assert.True(t, ops.PageInfo.HasPreviousPage)

		first = int32(10)
		nextCursor = ops.PageInfo.EndCursor
		assert.NotNil(t, nextCursor)
		ops, err = resolver.Operations(ctx, parentAccount, &first, nextCursor, nil, nil)
		require.NoError(t, err)
		assert.Len(t, ops.Edges, 4)
		assert.Equal(t, int64(1005), ops.Edges[0].Node.ID)
		assert.Equal(t, int64(1006), ops.Edges[1].Node.ID)
		assert.Equal(t, int64(1007), ops.Edges[2].Node.ID)
		assert.Equal(t, int64(1008), ops.Edges[3].Node.ID)
		assert.False(t, ops.PageInfo.HasNextPage)
		assert.True(t, ops.PageInfo.HasPreviousPage)
	})

	t.Run("get operations with last/before limit and cursor", func(t *testing.T) {
		ctx := getTestCtx("operations", []string{"operation_type"})
		last := int32(2)
		ops, err := resolver.Operations(ctx, parentAccount, nil, nil, &last, nil)
		require.NoError(t, err)
		assert.Len(t, ops.Edges, 2)
		assert.Equal(t, int64(1007), ops.Edges[0].Node.ID)
		assert.Equal(t, int64(1008), ops.Edges[1].Node.ID)
		assert.True(t, ops.PageInfo.HasPreviousPage)
		assert.False(t, ops.PageInfo.HasNextPage)

		// Get the next cursor
		nextCursor := ops.PageInfo.EndCursor
		assert.NotNil(t, nextCursor)
		ops, err = resolver.Operations(ctx, parentAccount, nil, nil, &last, nextCursor)
		require.NoError(t, err)
		assert.Len(t, ops.Edges, 2)
		assert.Equal(t, int64(1005), ops.Edges[0].Node.ID)
		assert.Equal(t, int64(1006), ops.Edges[1].Node.ID)
		assert.True(t, ops.PageInfo.HasNextPage)
		assert.True(t, ops.PageInfo.HasPreviousPage)

		nextCursor = ops.PageInfo.EndCursor
		assert.NotNil(t, nextCursor)
		last = int32(10)
		ops, err = resolver.Operations(ctx, parentAccount, nil, nil, &last, nextCursor)
		require.NoError(t, err)
		assert.Len(t, ops.Edges, 4)
		assert.Equal(t, int64(1001), ops.Edges[0].Node.ID)
		assert.Equal(t, int64(1002), ops.Edges[1].Node.ID)
		assert.Equal(t, int64(1003), ops.Edges[2].Node.ID)
		assert.Equal(t, int64(1004), ops.Edges[3].Node.ID)
		assert.True(t, ops.PageInfo.HasNextPage)
		assert.False(t, ops.PageInfo.HasPreviousPage)
	})

	t.Run("account with no operations", func(t *testing.T) {
		nonExistentAccount := &types.Account{StellarAddress: "non-existent-account"}
		ctx := getTestCtx("operations", []string{"id"})
		operations, err := resolver.Operations(ctx, nonExistentAccount, nil, nil, nil, nil)

		require.NoError(t, err)
		assert.Empty(t, operations.Edges)
	})
}

func TestAccountResolver_StateChanges(t *testing.T) {
	parentAccount := &types.Account{StellarAddress: "test-account"}

	mockMetricsService := &metrics.MockMetricsService{}
	mockMetricsService.On("IncDBQuery", "SELECT", "state_changes").Return()
	mockMetricsService.On("ObserveDBQueryDuration", "SELECT", "state_changes", mock.Anything).Return()
	defer mockMetricsService.AssertExpectations(t)

	resolver := &accountResolver{&Resolver{
		models: &data.Models{
			StateChanges: &data.StateChangeModel{
				DB:             testDBConnectionPool,
				MetricsService: mockMetricsService,
			},
		},
	}}

	t.Run("success", func(t *testing.T) {
		loaders := &dataloaders.Dataloaders{
			StateChangesByAccountLoader: dataloaders.StateChangesByAccountLoader(resolver.models),
		}
		ctx := context.WithValue(getTestCtx("state_changes", []string{"to_id", "state_change_order"}), middleware.LoadersKey, loaders)
		stateChanges, err := resolver.StateChanges(ctx, parentAccount)

		require.NoError(t, err)
		require.Len(t, stateChanges, 20)
		// With 16 state changes ordered by ToID descending, check first few
		assert.Equal(t, "1008:2", fmt.Sprintf("%d:%d", stateChanges[0].ToID, stateChanges[0].StateChangeOrder))
		assert.Equal(t, "1008:1", fmt.Sprintf("%d:%d", stateChanges[1].ToID, stateChanges[1].StateChangeOrder))
		assert.Equal(t, "1007:2", fmt.Sprintf("%d:%d", stateChanges[2].ToID, stateChanges[2].StateChangeOrder))
		assert.Equal(t, "1007:1", fmt.Sprintf("%d:%d", stateChanges[3].ToID, stateChanges[3].StateChangeOrder))
	})

	t.Run("nil account panics", func(t *testing.T) {
		loaders := &dataloaders.Dataloaders{
			StateChangesByAccountLoader: dataloaders.StateChangesByAccountLoader(resolver.models),
		}
		ctx := context.WithValue(getTestCtx("state_changes", []string{"to_id", "state_change_order"}), middleware.LoadersKey, loaders)

		assert.Panics(t, func() {
			_, _ = resolver.StateChanges(ctx, nil) //nolint:errcheck
		})
	})

	t.Run("account with no state changes", func(t *testing.T) {
		nonExistentAccount := &types.Account{StellarAddress: "non-existent-account"}
		loaders := &dataloaders.Dataloaders{
			StateChangesByAccountLoader: dataloaders.StateChangesByAccountLoader(resolver.models),
		}
		ctx := context.WithValue(getTestCtx("state_changes", []string{"to_id", "state_change_order"}), middleware.LoadersKey, loaders)
		stateChanges, err := resolver.StateChanges(ctx, nonExistentAccount)

		require.NoError(t, err)
		assert.Empty(t, stateChanges)
	})
}
