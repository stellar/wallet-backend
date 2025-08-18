package resolvers

import (
	"fmt"
	"testing"

	"github.com/stellar/go/toid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/stellar/wallet-backend/internal/data"
	"github.com/stellar/wallet-backend/internal/metrics"
)

func TestQueryResolver_TransactionByHash(t *testing.T) {
	mockMetricsService := &metrics.MockMetricsService{}
	mockMetricsService.On("ObserveDBQueryDuration", "SELECT", "transactions", mock.Anything).Return()
	mockMetricsService.On("IncDBQuery", "SELECT", "transactions").Return()
	defer mockMetricsService.AssertExpectations(t)

	resolver := &queryResolver{
		&Resolver{
			models: &data.Models{
				Transactions: &data.TransactionModel{
					DB:             testDBConnectionPool,
					MetricsService: mockMetricsService,
				},
			},
		},
	}

	t.Run("success", func(t *testing.T) {
		ctx := getTestCtx("transactions", []string{"hash", "toId", "envelopeXdr", "resultXdr", "metaXdr", "ledgerNumber", "ledgerCreatedAt"})
		tx, err := resolver.TransactionByHash(ctx, "tx1")

		require.NoError(t, err)
		assert.Equal(t, "tx1", tx.Hash)
		assert.Equal(t, toid.New(1000, 1, 0).ToInt64(), tx.ToID)
		assert.Equal(t, "envelope1", tx.EnvelopeXDR)
		assert.Equal(t, "result1", tx.ResultXDR)
		assert.Equal(t, "meta1", tx.MetaXDR)
		assert.Equal(t, uint32(1), tx.LedgerNumber)
	})

	t.Run("non-existent hash", func(t *testing.T) {
		ctx := getTestCtx("transactions", []string{"hash"})
		tx, err := resolver.TransactionByHash(ctx, "non-existent-hash")

		require.Error(t, err)
		assert.Nil(t, tx)
	})

	t.Run("empty hash", func(t *testing.T) {
		ctx := getTestCtx("transactions", []string{"hash"})
		tx, err := resolver.TransactionByHash(ctx, "")

		require.Error(t, err)
		assert.Nil(t, tx)
	})
}

func TestQueryResolver_Transactions(t *testing.T) {
	mockMetricsService := &metrics.MockMetricsService{}
	mockMetricsService.On("ObserveDBQueryDuration", "SELECT", "transactions", mock.Anything).Return()
	mockMetricsService.On("IncDBQuery", "SELECT", "transactions").Return()
	defer mockMetricsService.AssertExpectations(t)

	resolver := &queryResolver{
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
		transactions, err := resolver.Transactions(ctx, nil, nil, nil, nil)

		require.NoError(t, err)
		require.Len(t, transactions.Edges, 4)
		assert.Equal(t, "tx4", transactions.Edges[0].Node.Hash)
		assert.Equal(t, "tx3", transactions.Edges[1].Node.Hash)
		assert.Equal(t, "tx2", transactions.Edges[2].Node.Hash)
		assert.Equal(t, "tx1", transactions.Edges[3].Node.Hash)
	})

	t.Run("get transactions with first/after limit and cursor", func(t *testing.T) {
		ctx := getTestCtx("transactions", []string{"hash"})
		first := int32(2)
		txs, err := resolver.Transactions(ctx, &first, nil, nil, nil)
		require.NoError(t, err)
		assert.Len(t, txs.Edges, 2)
		assert.Equal(t, "tx4", txs.Edges[0].Node.Hash)
		assert.Equal(t, "tx3", txs.Edges[1].Node.Hash)
		assert.True(t, txs.PageInfo.HasNextPage)
		assert.False(t, txs.PageInfo.HasPreviousPage)

		// Get the next cursor
		first = int32(1)
		nextCursor := txs.PageInfo.EndCursor
		assert.NotNil(t, nextCursor)
		txs, err = resolver.Transactions(ctx, &first, nextCursor, nil, nil)
		require.NoError(t, err)
		assert.Len(t, txs.Edges, 1)
		assert.Equal(t, "tx2", txs.Edges[0].Node.Hash)
		assert.True(t, txs.PageInfo.HasNextPage)
		assert.True(t, txs.PageInfo.HasPreviousPage)

		// Get the previous cursor
		first = int32(10)
		nextCursor = txs.PageInfo.EndCursor
		assert.NotNil(t, nextCursor)
		txs, err = resolver.Transactions(ctx, &first, nextCursor, nil, nil)
		require.NoError(t, err)
		assert.Len(t, txs.Edges, 1)
		assert.Equal(t, "tx1", txs.Edges[0].Node.Hash)
		assert.False(t, txs.PageInfo.HasNextPage)
		assert.True(t, txs.PageInfo.HasPreviousPage)
	})

	t.Run("get transactions with last/before limit and cursor", func(t *testing.T) {
		ctx := getTestCtx("transactions", []string{"hash"})
		last := int32(2)
		txs, err := resolver.Transactions(ctx, nil, nil, &last, nil)
		require.NoError(t, err)
		assert.Len(t, txs.Edges, 2)
		assert.Equal(t, "tx2", txs.Edges[0].Node.Hash)
		assert.Equal(t, "tx1", txs.Edges[1].Node.Hash)
		assert.False(t, txs.PageInfo.HasNextPage)
		assert.True(t, txs.PageInfo.HasPreviousPage)

		// Get the next cursor
		last = int32(1)
		nextCursor := txs.PageInfo.EndCursor
		assert.NotNil(t, nextCursor)
		txs, err = resolver.Transactions(ctx, nil, nil, &last, nextCursor)
		require.NoError(t, err)
		assert.Len(t, txs.Edges, 1)
		assert.Equal(t, "tx3", txs.Edges[0].Node.Hash)
		assert.True(t, txs.PageInfo.HasNextPage)
		assert.True(t, txs.PageInfo.HasPreviousPage)

		nextCursor = txs.PageInfo.EndCursor
		assert.NotNil(t, nextCursor)
		last = int32(10)
		txs, err = resolver.Transactions(ctx, nil, nil, &last, nextCursor)
		require.NoError(t, err)
		assert.Len(t, txs.Edges, 1)
		assert.Equal(t, "tx4", txs.Edges[0].Node.Hash)
		assert.True(t, txs.PageInfo.HasNextPage)
		assert.False(t, txs.PageInfo.HasPreviousPage)
	})

	t.Run("returns error when first is negative", func(t *testing.T) {
		ctx := getTestCtx("transactions", []string{"hash"})
		first := int32(-1)
		txs, err := resolver.Transactions(ctx, &first, nil, nil, nil)
		require.Error(t, err)
		assert.Nil(t, txs)
		assert.Contains(t, err.Error(), "first must be greater than 0")
	})

	t.Run("returns error when last is negative", func(t *testing.T) {
		ctx := getTestCtx("transactions", []string{"hash"})
		last := int32(-1)
		txs, err := resolver.Transactions(ctx, nil, nil, &last, nil)
		require.Error(t, err)
		assert.Nil(t, txs)
		assert.Contains(t, err.Error(), "last must be greater than 0")
	})

	t.Run("returns error when first is zero", func(t *testing.T) {
		ctx := getTestCtx("transactions", []string{"hash"})
		first := int32(0)
		txs, err := resolver.Transactions(ctx, &first, nil, nil, nil)
		require.Error(t, err)
		assert.Nil(t, txs)
		assert.Contains(t, err.Error(), "first must be greater than 0")
	})

	t.Run("returns error when last is zero", func(t *testing.T) {
		ctx := getTestCtx("transactions", []string{"hash"})
		last := int32(0)
		txs, err := resolver.Transactions(ctx, nil, nil, &last, nil)
		require.Error(t, err)
		assert.Nil(t, txs)
		assert.Contains(t, err.Error(), "last must be greater than 0")
	})

	t.Run("first parameter's value larger than available data", func(t *testing.T) {
		ctx := getTestCtx("transactions", []string{"hash"})
		first := int32(100)
		txs, err := resolver.Transactions(ctx, &first, nil, nil, nil)
		require.NoError(t, err)
		assert.Len(t, txs.Edges, 4)
		assert.False(t, txs.PageInfo.HasNextPage)
		assert.False(t, txs.PageInfo.HasPreviousPage)
	})

	t.Run("last parameter's value larger than available data", func(t *testing.T) {
		ctx := getTestCtx("transactions", []string{"hash"})
		last := int32(100)
		txs, err := resolver.Transactions(ctx, nil, nil, &last, nil)
		require.NoError(t, err)
		assert.Len(t, txs.Edges, 4)
		assert.False(t, txs.PageInfo.HasNextPage)
		assert.False(t, txs.PageInfo.HasPreviousPage)
	})
}

func TestQueryResolver_Account(t *testing.T) {
	mockMetricsService := &metrics.MockMetricsService{}
	mockMetricsService.On("ObserveDBQueryDuration", "SELECT", "accounts", mock.Anything).Return()
	mockMetricsService.On("IncDBQuery", "SELECT", "accounts").Return()
	defer mockMetricsService.AssertExpectations(t)

	resolver := &queryResolver{
		&Resolver{
			models: &data.Models{
				Account: &data.AccountModel{
					DB:             testDBConnectionPool,
					MetricsService: mockMetricsService,
				},
			},
		},
	}

	t.Run("success", func(t *testing.T) {
		acc, err := resolver.Account(testCtx, "test-account")
		require.NoError(t, err)
		assert.Equal(t, "test-account", acc.StellarAddress)
	})

	t.Run("non-existent account", func(t *testing.T) {
		acc, err := resolver.Account(testCtx, "non-existent-account")
		require.Error(t, err)
		assert.Nil(t, acc)
	})

	t.Run("empty address", func(t *testing.T) {
		acc, err := resolver.Account(testCtx, "")
		require.Error(t, err)
		assert.Nil(t, acc)
	})
}

func TestQueryResolver_Operations(t *testing.T) {
	mockMetricsService := &metrics.MockMetricsService{}
	mockMetricsService.On("ObserveDBQueryDuration", "SELECT", "operations", mock.Anything).Return()
	mockMetricsService.On("IncDBQuery", "SELECT", "operations").Return()
	defer mockMetricsService.AssertExpectations(t)

	resolver := &queryResolver{
		&Resolver{
			models: &data.Models{
				Operations: &data.OperationModel{
					DB:             testDBConnectionPool,
					MetricsService: mockMetricsService,
				},
			},
		},
	}

	t.Run("get all operations", func(t *testing.T) {
		ctx := getTestCtx("operations", []string{"id"})
		operations, err := resolver.Operations(ctx, nil, nil, nil, nil)

		require.NoError(t, err)
		require.Len(t, operations.Edges, 8)
		// Operations are ordered by ID descending
		assert.Equal(t, toid.New(1000, 4, 2).ToInt64(), operations.Edges[0].Node.ID)
		assert.Equal(t, toid.New(1000, 4, 1).ToInt64(), operations.Edges[1].Node.ID)
		assert.Equal(t, toid.New(1000, 3, 2).ToInt64(), operations.Edges[2].Node.ID)
		assert.Equal(t, toid.New(1000, 3, 1).ToInt64(), operations.Edges[3].Node.ID)
	})

	t.Run("get operations with first/after limit and cursor", func(t *testing.T) {
		ctx := getTestCtx("operations", []string{"id"})
		first := int32(2)
		ops, err := resolver.Operations(ctx, &first, nil, nil, nil)
		require.NoError(t, err)
		assert.Len(t, ops.Edges, 2)
		assert.Equal(t, toid.New(1000, 4, 2).ToInt64(), ops.Edges[0].Node.ID)
		assert.Equal(t, toid.New(1000, 4, 1).ToInt64(), ops.Edges[1].Node.ID)
		assert.True(t, ops.PageInfo.HasNextPage)
		assert.False(t, ops.PageInfo.HasPreviousPage)

		// Get the next cursor
		first = int32(1)
		nextCursor := ops.PageInfo.EndCursor
		assert.NotNil(t, nextCursor)
		ops, err = resolver.Operations(ctx, &first, nextCursor, nil, nil)
		require.NoError(t, err)
		assert.Len(t, ops.Edges, 1)
		assert.Equal(t, toid.New(1000, 3, 2).ToInt64(), ops.Edges[0].Node.ID)
		assert.True(t, ops.PageInfo.HasNextPage)
		assert.True(t, ops.PageInfo.HasPreviousPage)

		// Get the next page
		first = int32(10)
		nextCursor = ops.PageInfo.EndCursor
		assert.NotNil(t, nextCursor)
		ops, err = resolver.Operations(ctx, &first, nextCursor, nil, nil)
		require.NoError(t, err)
		assert.Len(t, ops.Edges, 5)
		assert.Equal(t, toid.New(1000, 3, 1).ToInt64(), ops.Edges[0].Node.ID)
		assert.Equal(t, toid.New(1000, 2, 2).ToInt64(), ops.Edges[1].Node.ID)
		assert.Equal(t, toid.New(1000, 2, 1).ToInt64(), ops.Edges[2].Node.ID)
		assert.Equal(t, toid.New(1000, 1, 2).ToInt64(), ops.Edges[3].Node.ID)
		assert.Equal(t, toid.New(1000, 1, 1).ToInt64(), ops.Edges[4].Node.ID)
		assert.False(t, ops.PageInfo.HasNextPage)
		assert.True(t, ops.PageInfo.HasPreviousPage)
	})

	t.Run("get operations with last/before limit and cursor", func(t *testing.T) {
		ctx := getTestCtx("operations", []string{"id"})
		last := int32(2)
		ops, err := resolver.Operations(ctx, nil, nil, &last, nil)
		require.NoError(t, err)
		assert.Len(t, ops.Edges, 2)
		// With backward pagination, we get the last 2 items
		assert.Equal(t, toid.New(1000, 1, 2).ToInt64(), ops.Edges[0].Node.ID)
		assert.Equal(t, toid.New(1000, 1, 1).ToInt64(), ops.Edges[1].Node.ID)
		assert.False(t, ops.PageInfo.HasNextPage)
		assert.True(t, ops.PageInfo.HasPreviousPage)

		// Get the previous page
		last = int32(1)
		prevCursor := ops.PageInfo.EndCursor
		assert.NotNil(t, prevCursor)
		ops, err = resolver.Operations(ctx, nil, nil, &last, prevCursor)
		require.NoError(t, err)
		assert.Len(t, ops.Edges, 1)
		assert.Equal(t, toid.New(1000, 2, 1).ToInt64(), ops.Edges[0].Node.ID)
		assert.True(t, ops.PageInfo.HasNextPage)
		assert.True(t, ops.PageInfo.HasPreviousPage)

		prevCursor = ops.PageInfo.EndCursor
		assert.NotNil(t, prevCursor)
		last = int32(10)
		ops, err = resolver.Operations(ctx, nil, nil, &last, prevCursor)
		require.NoError(t, err)
		// There are 5 operations before (2,1): (2,2), (3,1), (3,2), (4,1), (4,2)
		assert.Len(t, ops.Edges, 5)
		assert.Equal(t, toid.New(1000, 4, 2).ToInt64(), ops.Edges[0].Node.ID)
		assert.Equal(t, toid.New(1000, 4, 1).ToInt64(), ops.Edges[1].Node.ID)
		assert.Equal(t, toid.New(1000, 3, 2).ToInt64(), ops.Edges[2].Node.ID)
		assert.Equal(t, toid.New(1000, 3, 1).ToInt64(), ops.Edges[3].Node.ID)
		assert.Equal(t, toid.New(1000, 2, 2).ToInt64(), ops.Edges[4].Node.ID)
		assert.True(t, ops.PageInfo.HasNextPage)
		assert.False(t, ops.PageInfo.HasPreviousPage)
	})

	t.Run("returns error when first is negative", func(t *testing.T) {
		ctx := getTestCtx("operations", []string{"id"})
		first := int32(-1)
		ops, err := resolver.Operations(ctx, &first, nil, nil, nil)
		require.Error(t, err)
		assert.Nil(t, ops)
		assert.Contains(t, err.Error(), "first must be greater than 0")
	})

	t.Run("returns error when last is negative", func(t *testing.T) {
		ctx := getTestCtx("operations", []string{"id"})
		last := int32(-1)
		ops, err := resolver.Operations(ctx, nil, nil, &last, nil)
		require.Error(t, err)
		assert.Nil(t, ops)
		assert.Contains(t, err.Error(), "last must be greater than 0")
	})

	t.Run("returns error when first is zero", func(t *testing.T) {
		ctx := getTestCtx("operations", []string{"id"})
		first := int32(0)
		ops, err := resolver.Operations(ctx, &first, nil, nil, nil)
		require.Error(t, err)
		assert.Nil(t, ops)
		assert.Contains(t, err.Error(), "first must be greater than 0")
	})

	t.Run("returns error when last is zero", func(t *testing.T) {
		ctx := getTestCtx("operations", []string{"id"})
		last := int32(0)
		ops, err := resolver.Operations(ctx, nil, nil, &last, nil)
		require.Error(t, err)
		assert.Nil(t, ops)
		assert.Contains(t, err.Error(), "last must be greater than 0")
	})

	t.Run("first parameter's value larger than available data", func(t *testing.T) {
		ctx := getTestCtx("operations", []string{"id"})
		first := int32(100)
		ops, err := resolver.Operations(ctx, &first, nil, nil, nil)
		require.NoError(t, err)
		assert.Len(t, ops.Edges, 8)
		assert.False(t, ops.PageInfo.HasNextPage)
		assert.False(t, ops.PageInfo.HasPreviousPage)
	})

	t.Run("last parameter's value larger than available data", func(t *testing.T) {
		ctx := getTestCtx("operations", []string{"id"})
		last := int32(100)
		ops, err := resolver.Operations(ctx, nil, nil, &last, nil)
		require.NoError(t, err)
		assert.Len(t, ops.Edges, 8)
		assert.False(t, ops.PageInfo.HasNextPage)
		assert.False(t, ops.PageInfo.HasPreviousPage)
	})
}

func TestQueryResolver_StateChanges(t *testing.T) {
	mockMetricsService := &metrics.MockMetricsService{}
	mockMetricsService.On("ObserveDBQueryDuration", "SELECT", "state_changes", mock.Anything).Return()
	mockMetricsService.On("IncDBQuery", "SELECT", "state_changes").Return()
	defer mockMetricsService.AssertExpectations(t)

	resolver := &queryResolver{
		&Resolver{
			models: &data.Models{
				StateChanges: &data.StateChangeModel{
					DB:             testDBConnectionPool,
					MetricsService: mockMetricsService,
				},
			},
		},
	}

	t.Run("get all", func(t *testing.T) {
		ctx := getTestCtx("state_changes", []string{"stateChangeCategory", "txHash", "operationId", "accountId", "ledgerCreatedAt", "ledgerNumber"})
		scs, err := resolver.StateChanges(ctx, nil)
		require.NoError(t, err)
		assert.Len(t, scs, 20)
		// Verify the state changes have the expected account ID
		assert.Equal(t, "test-account", scs[0].AccountID)
	})

	t.Run("get with limit", func(t *testing.T) {
		limit := int32(3)
		ctx := getTestCtx("state_changes", []string{"stateChangeCategory", "txHash", "operationId", "accountId", "ledgerCreatedAt", "ledgerNumber"})
		stateChanges, err := resolver.StateChanges(ctx, &limit)
		require.NoError(t, err)
		assert.Len(t, stateChanges, 3)
		assert.Equal(t, fmt.Sprintf("%d:2", toid.New(1000, 4, 2).ToInt64()), fmt.Sprintf("%d:%d", stateChanges[0].ToID, stateChanges[0].StateChangeOrder))
		assert.Equal(t, fmt.Sprintf("%d:1", toid.New(1000, 4, 2).ToInt64()), fmt.Sprintf("%d:%d", stateChanges[1].ToID, stateChanges[1].StateChangeOrder))
		assert.Equal(t, fmt.Sprintf("%d:2", toid.New(1000, 4, 1).ToInt64()), fmt.Sprintf("%d:%d", stateChanges[2].ToID, stateChanges[2].StateChangeOrder))
	})

	t.Run("negative limit error", func(t *testing.T) {
		ctx := getTestCtx("state_changes", []string{"accountId"})
		limit := int32(-10)
		scs, err := resolver.StateChanges(ctx, &limit)
		require.Error(t, err)
		assert.Nil(t, scs)
		assert.Contains(t, err.Error(), "limit must be non-negative")
	})

	t.Run("zero limit", func(t *testing.T) {
		ctx := getTestCtx("state_changes", []string{"accountId"})
		limit := int32(0)
		scs, err := resolver.StateChanges(ctx, &limit)
		require.NoError(t, err)
		assert.Len(t, scs, 0)
	})

	t.Run("limit larger than available data", func(t *testing.T) {
		ctx := getTestCtx("state_changes", []string{"accountId"})
		limit := int32(50)
		scs, err := resolver.StateChanges(ctx, &limit)
		require.NoError(t, err)
		assert.Len(t, scs, 20)
	})
}
