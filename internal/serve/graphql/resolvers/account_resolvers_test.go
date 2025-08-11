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
	defer mockMetricsService.AssertExpectations(t)

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
		transactions, err := resolver.Transactions(ctx, parentAccount, nil, nil)

		require.NoError(t, err)
		require.Len(t, transactions.Edges, 4)
		assert.Equal(t, "tx4", transactions.Edges[0].Node.Hash)
		assert.Equal(t, "tx3", transactions.Edges[1].Node.Hash)
		assert.Equal(t, "tx2", transactions.Edges[2].Node.Hash)
		assert.Equal(t, "tx1", transactions.Edges[3].Node.Hash)
	})

	t.Run("get transactions with limit and cursor", func(t *testing.T) {
		ctx := getTestCtx("transactions", []string{"hash"})
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

	t.Run("account with no transactions", func(t *testing.T) {
		nonExistentAccount := &types.Account{StellarAddress: "non-existent-account"}
		ctx := getTestCtx("transactions", []string{"hash"})
		transactions, err := resolver.Transactions(ctx, nonExistentAccount, nil, nil)

		require.NoError(t, err)
		assert.Empty(t, transactions.Edges)
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

	t.Run("success", func(t *testing.T) {
		loaders := &dataloaders.Dataloaders{
			OperationsByAccountLoader: dataloaders.OperationsByAccountLoader(resolver.models),
		}
		ctx := context.WithValue(getTestCtx("operations", []string{"id"}), middleware.LoadersKey, loaders)
		operations, err := resolver.Operations(ctx, parentAccount)

		require.NoError(t, err)
		require.Len(t, operations, 8)
		assert.Equal(t, int64(1008), operations[0].ID)
		assert.Equal(t, int64(1007), operations[1].ID)
		assert.Equal(t, int64(1006), operations[2].ID)
		assert.Equal(t, int64(1005), operations[3].ID)
	})

	t.Run("nil account panics", func(t *testing.T) {
		loaders := &dataloaders.Dataloaders{
			OperationsByAccountLoader: dataloaders.OperationsByAccountLoader(resolver.models),
		}
		ctx := context.WithValue(getTestCtx("operations", []string{"id"}), middleware.LoadersKey, loaders)

		assert.Panics(t, func() {
			_, _ = resolver.Operations(ctx, nil) //nolint:errcheck
		})
	})

	t.Run("account with no operations", func(t *testing.T) {
		nonExistentAccount := &types.Account{StellarAddress: "non-existent-account"}
		loaders := &dataloaders.Dataloaders{
			OperationsByAccountLoader: dataloaders.OperationsByAccountLoader(resolver.models),
		}
		ctx := context.WithValue(getTestCtx("operations", []string{"id"}), middleware.LoadersKey, loaders)
		operations, err := resolver.Operations(ctx, nonExistentAccount)

		require.NoError(t, err)
		assert.Empty(t, operations)
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
