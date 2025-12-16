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
	mockMetricsService.On("ObserveDBQueryDuration", "GetByHash", "transactions", mock.Anything).Return()
	mockMetricsService.On("IncDBQuery", "GetByHash", "transactions").Return()
	mockMetricsService.On("IncDBQueryError", "GetByHash", "transactions", mock.Anything).Return().Maybe()
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
		require.NotNil(t, tx.EnvelopeXDR)
		assert.Equal(t, "envelope1", *tx.EnvelopeXDR)
		assert.Equal(t, "result1", tx.ResultXDR)
		require.NotNil(t, tx.MetaXDR)
		assert.Equal(t, "meta1", *tx.MetaXDR)
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
	mockMetricsService.On("ObserveDBQueryDuration", "GetAll", "transactions", mock.Anything).Return()
	mockMetricsService.On("IncDBQuery", "GetAll", "transactions").Return()
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
		assert.Equal(t, "tx1", transactions.Edges[0].Node.Hash)
		assert.Equal(t, "tx2", transactions.Edges[1].Node.Hash)
		assert.Equal(t, "tx3", transactions.Edges[2].Node.Hash)
		assert.Equal(t, "tx4", transactions.Edges[3].Node.Hash)
	})

	t.Run("get transactions with first/after limit and cursor", func(t *testing.T) {
		ctx := getTestCtx("transactions", []string{"hash"})
		first := int32(2)
		txs, err := resolver.Transactions(ctx, &first, nil, nil, nil)
		require.NoError(t, err)
		assert.Len(t, txs.Edges, 2)
		assert.Equal(t, "tx1", txs.Edges[0].Node.Hash)
		assert.Equal(t, "tx2", txs.Edges[1].Node.Hash)
		assert.True(t, txs.PageInfo.HasNextPage)
		assert.False(t, txs.PageInfo.HasPreviousPage)

		// Get the next cursor
		first = int32(1)
		nextCursor := txs.PageInfo.EndCursor
		assert.NotNil(t, nextCursor)
		txs, err = resolver.Transactions(ctx, &first, nextCursor, nil, nil)
		require.NoError(t, err)
		assert.Len(t, txs.Edges, 1)
		assert.Equal(t, "tx3", txs.Edges[0].Node.Hash)
		assert.True(t, txs.PageInfo.HasNextPage)
		assert.True(t, txs.PageInfo.HasPreviousPage)

		// Get the previous cursor
		first = int32(10)
		nextCursor = txs.PageInfo.EndCursor
		assert.NotNil(t, nextCursor)
		txs, err = resolver.Transactions(ctx, &first, nextCursor, nil, nil)
		require.NoError(t, err)
		assert.Len(t, txs.Edges, 1)
		assert.Equal(t, "tx4", txs.Edges[0].Node.Hash)
		assert.False(t, txs.PageInfo.HasNextPage)
		assert.True(t, txs.PageInfo.HasPreviousPage)
	})

	t.Run("get transactions with last/before limit and cursor", func(t *testing.T) {
		ctx := getTestCtx("transactions", []string{"hash"})
		last := int32(2)
		txs, err := resolver.Transactions(ctx, nil, nil, &last, nil)
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
		txs, err = resolver.Transactions(ctx, nil, nil, &last, nextCursor)
		require.NoError(t, err)
		assert.Len(t, txs.Edges, 1)
		assert.Equal(t, "tx2", txs.Edges[0].Node.Hash)
		assert.True(t, txs.PageInfo.HasNextPage)
		assert.True(t, txs.PageInfo.HasPreviousPage)

		nextCursor = txs.PageInfo.EndCursor
		assert.NotNil(t, nextCursor)
		last = int32(10)
		txs, err = resolver.Transactions(ctx, nil, nil, &last, nextCursor)
		require.NoError(t, err)
		assert.Len(t, txs.Edges, 1)
		assert.Equal(t, "tx1", txs.Edges[0].Node.Hash)
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
	mockMetricsService.On("ObserveDBQueryDuration", "Get", "accounts", mock.Anything).Return()
	mockMetricsService.On("IncDBQuery", "Get", "accounts").Return()
	mockMetricsService.On("IncDBQueryError", "Get", "accounts", mock.Anything).Return().Maybe()
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
		acc, err := resolver.AccountByAddress(testCtx, "test-account")
		require.NoError(t, err)
		assert.Equal(t, "test-account", acc.StellarAddress)
	})

	t.Run("non-existent account", func(t *testing.T) {
		acc, err := resolver.AccountByAddress(testCtx, "non-existent-account")
		require.Error(t, err)
		assert.Nil(t, acc)
	})

	t.Run("empty address", func(t *testing.T) {
		acc, err := resolver.AccountByAddress(testCtx, "")
		require.Error(t, err)
		assert.Nil(t, acc)
	})
}

func TestQueryResolver_Operations(t *testing.T) {
	mockMetricsService := &metrics.MockMetricsService{}
	mockMetricsService.On("ObserveDBQueryDuration", "GetAll", "operations", mock.Anything).Return()
	mockMetricsService.On("IncDBQuery", "GetAll", "operations").Return()
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
		ctx := getTestCtx("operations", []string{"operation_xdr"})
		operations, err := resolver.Operations(ctx, nil, nil, nil, nil)

		require.NoError(t, err)
		require.Len(t, operations.Edges, 8)
		// Operations are ordered by ID ascending
		assert.Equal(t, "opxdr1", operations.Edges[0].Node.OperationXDR)
		assert.Equal(t, "opxdr2", operations.Edges[1].Node.OperationXDR)
		assert.Equal(t, "opxdr3", operations.Edges[2].Node.OperationXDR)
		assert.Equal(t, "opxdr4", operations.Edges[3].Node.OperationXDR)
	})

	t.Run("get operations with first/after limit and cursor", func(t *testing.T) {
		ctx := getTestCtx("operations", []string{"operation_xdr"})
		first := int32(2)
		ops, err := resolver.Operations(ctx, &first, nil, nil, nil)
		require.NoError(t, err)
		assert.Len(t, ops.Edges, 2)
		assert.Equal(t, "opxdr1", ops.Edges[0].Node.OperationXDR)
		assert.Equal(t, "opxdr2", ops.Edges[1].Node.OperationXDR)
		assert.True(t, ops.PageInfo.HasNextPage)
		assert.False(t, ops.PageInfo.HasPreviousPage)

		// Get the next cursor
		first = int32(1)
		nextCursor := ops.PageInfo.EndCursor
		assert.NotNil(t, nextCursor)
		ops, err = resolver.Operations(ctx, &first, nextCursor, nil, nil)
		require.NoError(t, err)
		assert.Len(t, ops.Edges, 1)
		assert.Equal(t, "opxdr3", ops.Edges[0].Node.OperationXDR)
		assert.True(t, ops.PageInfo.HasNextPage)
		assert.True(t, ops.PageInfo.HasPreviousPage)

		// Get the next page
		first = int32(10)
		nextCursor = ops.PageInfo.EndCursor
		assert.NotNil(t, nextCursor)
		ops, err = resolver.Operations(ctx, &first, nextCursor, nil, nil)
		require.NoError(t, err)
		assert.Len(t, ops.Edges, 5)
		assert.Equal(t, "opxdr4", ops.Edges[0].Node.OperationXDR)
		assert.Equal(t, "opxdr5", ops.Edges[1].Node.OperationXDR)
		assert.Equal(t, "opxdr6", ops.Edges[2].Node.OperationXDR)
		assert.Equal(t, "opxdr7", ops.Edges[3].Node.OperationXDR)
		assert.Equal(t, "opxdr8", ops.Edges[4].Node.OperationXDR)
		assert.False(t, ops.PageInfo.HasNextPage)
		assert.True(t, ops.PageInfo.HasPreviousPage)
	})

	t.Run("get operations with last/before limit and cursor", func(t *testing.T) {
		ctx := getTestCtx("operations", []string{"operation_xdr"})
		last := int32(2)
		ops, err := resolver.Operations(ctx, nil, nil, &last, nil)
		require.NoError(t, err)
		assert.Len(t, ops.Edges, 2)
		// With backward pagination, we get the last 2 items
		assert.Equal(t, "opxdr7", ops.Edges[0].Node.OperationXDR)
		assert.Equal(t, "opxdr8", ops.Edges[1].Node.OperationXDR)
		assert.False(t, ops.PageInfo.HasNextPage)
		assert.True(t, ops.PageInfo.HasPreviousPage)

		// Get the previous page
		last = int32(1)
		prevCursor := ops.PageInfo.EndCursor
		assert.NotNil(t, prevCursor)
		ops, err = resolver.Operations(ctx, nil, nil, &last, prevCursor)
		require.NoError(t, err)
		assert.Len(t, ops.Edges, 1)
		assert.Equal(t, "opxdr6", ops.Edges[0].Node.OperationXDR)
		assert.True(t, ops.PageInfo.HasNextPage)
		assert.True(t, ops.PageInfo.HasPreviousPage)

		prevCursor = ops.PageInfo.EndCursor
		assert.NotNil(t, prevCursor)
		last = int32(10)
		ops, err = resolver.Operations(ctx, nil, nil, &last, prevCursor)
		require.NoError(t, err)
		// There are 5 operations before (2,1): (2,2), (3,1), (3,2), (4,1), (4,2)
		assert.Len(t, ops.Edges, 5)
		assert.Equal(t, "opxdr1", ops.Edges[0].Node.OperationXDR)
		assert.Equal(t, "opxdr2", ops.Edges[1].Node.OperationXDR)
		assert.Equal(t, "opxdr3", ops.Edges[2].Node.OperationXDR)
		assert.Equal(t, "opxdr4", ops.Edges[3].Node.OperationXDR)
		assert.Equal(t, "opxdr5", ops.Edges[4].Node.OperationXDR)
		assert.True(t, ops.PageInfo.HasNextPage)
		assert.False(t, ops.PageInfo.HasPreviousPage)
	})

	t.Run("returns error when first is negative", func(t *testing.T) {
		ctx := getTestCtx("operations", []string{"operation_xdr"})
		first := int32(-1)
		ops, err := resolver.Operations(ctx, &first, nil, nil, nil)
		require.Error(t, err)
		assert.Nil(t, ops)
		assert.Contains(t, err.Error(), "first must be greater than 0")
	})

	t.Run("returns error when last is negative", func(t *testing.T) {
		ctx := getTestCtx("operations", []string{"operation_xdr"})
		last := int32(-1)
		ops, err := resolver.Operations(ctx, nil, nil, &last, nil)
		require.Error(t, err)
		assert.Nil(t, ops)
		assert.Contains(t, err.Error(), "last must be greater than 0")
	})

	t.Run("returns error when first is zero", func(t *testing.T) {
		ctx := getTestCtx("operations", []string{"operation_xdr"})
		first := int32(0)
		ops, err := resolver.Operations(ctx, &first, nil, nil, nil)
		require.Error(t, err)
		assert.Nil(t, ops)
		assert.Contains(t, err.Error(), "first must be greater than 0")
	})

	t.Run("returns error when last is zero", func(t *testing.T) {
		ctx := getTestCtx("operations", []string{"operation_xdr"})
		last := int32(0)
		ops, err := resolver.Operations(ctx, nil, nil, &last, nil)
		require.Error(t, err)
		assert.Nil(t, ops)
		assert.Contains(t, err.Error(), "last must be greater than 0")
	})

	t.Run("first parameter's value larger than available data", func(t *testing.T) {
		ctx := getTestCtx("operations", []string{"operation_xdr"})
		first := int32(100)
		ops, err := resolver.Operations(ctx, &first, nil, nil, nil)
		require.NoError(t, err)
		assert.Len(t, ops.Edges, 8)
		assert.False(t, ops.PageInfo.HasNextPage)
		assert.False(t, ops.PageInfo.HasPreviousPage)
	})

	t.Run("last parameter's value larger than available data", func(t *testing.T) {
		ctx := getTestCtx("operations", []string{"operation_xdr"})
		last := int32(100)
		ops, err := resolver.Operations(ctx, nil, nil, &last, nil)
		require.NoError(t, err)
		assert.Len(t, ops.Edges, 8)
		assert.False(t, ops.PageInfo.HasNextPage)
		assert.False(t, ops.PageInfo.HasPreviousPage)
	})
}

func TestQueryResolver_OperationByID(t *testing.T) {
	mockMetricsService := &metrics.MockMetricsService{}
	mockMetricsService.On("ObserveDBQueryDuration", "GetByID", "operations", mock.Anything).Return()
	mockMetricsService.On("IncDBQuery", "GetByID", "operations").Return()
	mockMetricsService.On("IncDBQueryError", "GetByID", "operations", mock.Anything).Return().Maybe()
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

	t.Run("success", func(t *testing.T) {
		ctx := getTestCtx("operations", []string{""})
		op, err := resolver.OperationByID(ctx, toid.New(1000, 1, 1).ToInt64())

		require.NoError(t, err)
		assert.Equal(t, toid.New(1000, 1, 1).ToInt64(), op.ID)
		assert.Equal(t, "opxdr1", op.OperationXDR)
		assert.Equal(t, "tx1", op.TxHash)
		assert.Equal(t, uint32(1), op.LedgerNumber)
	})

	t.Run("non-existent ID", func(t *testing.T) {
		ctx := getTestCtx("operations", []string{"id"})
		op, err := resolver.OperationByID(ctx, 999)

		require.Error(t, err)
		assert.Nil(t, op)
	})

	t.Run("zero ID", func(t *testing.T) {
		ctx := getTestCtx("operations", []string{"id"})
		op, err := resolver.OperationByID(ctx, 0)

		require.Error(t, err)
		assert.Nil(t, op)
	})

	t.Run("negative ID", func(t *testing.T) {
		ctx := getTestCtx("operations", []string{"id"})
		op, err := resolver.OperationByID(ctx, -1)

		require.Error(t, err)
		assert.Nil(t, op)
	})
}

func TestQueryResolver_StateChanges(t *testing.T) {
	mockMetricsService := &metrics.MockMetricsService{}
	mockMetricsService.On("ObserveDBQueryDuration", "GetAll", "state_changes", mock.Anything).Return()
	mockMetricsService.On("IncDBQuery", "GetAll", "state_changes").Return()
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
		ctx := getTestCtx("state_changes", []string{})
		stateChanges, err := resolver.StateChanges(ctx, nil, nil, nil, nil)
		require.NoError(t, err)
		assert.Len(t, stateChanges.Edges, 20)
		assert.Equal(t, fmt.Sprintf("%d:1", toid.New(1000, 1, 0).ToInt64()), fmt.Sprintf("%d:%d", extractStateChangeIDs(stateChanges.Edges[0].Node).ToID, extractStateChangeIDs(stateChanges.Edges[0].Node).StateChangeOrder))
		assert.Equal(t, fmt.Sprintf("%d:1", toid.New(1000, 1, 1).ToInt64()), fmt.Sprintf("%d:%d", extractStateChangeIDs(stateChanges.Edges[1].Node).ToID, extractStateChangeIDs(stateChanges.Edges[1].Node).StateChangeOrder))
		assert.Equal(t, fmt.Sprintf("%d:2", toid.New(1000, 1, 1).ToInt64()), fmt.Sprintf("%d:%d", extractStateChangeIDs(stateChanges.Edges[2].Node).ToID, extractStateChangeIDs(stateChanges.Edges[2].Node).StateChangeOrder))
	})

	t.Run("get state changes with first/after limit and cursor", func(t *testing.T) {
		ctx := getTestCtx("state_changes", []string{})
		first := int32(2)
		scs, err := resolver.StateChanges(ctx, &first, nil, nil, nil)
		require.NoError(t, err)
		assert.Len(t, scs.Edges, 2)
		assert.Equal(t, fmt.Sprintf("%d:1", toid.New(1000, 1, 0).ToInt64()), fmt.Sprintf("%d:%d", extractStateChangeIDs(scs.Edges[0].Node).ToID, extractStateChangeIDs(scs.Edges[0].Node).StateChangeOrder))
		assert.Equal(t, fmt.Sprintf("%d:1", toid.New(1000, 1, 1).ToInt64()), fmt.Sprintf("%d:%d", extractStateChangeIDs(scs.Edges[1].Node).ToID, extractStateChangeIDs(scs.Edges[1].Node).StateChangeOrder))
		assert.True(t, scs.PageInfo.HasNextPage)
		assert.False(t, scs.PageInfo.HasPreviousPage)

		// Get the next cursor
		nextCursor := scs.PageInfo.EndCursor
		assert.NotNil(t, nextCursor)
		scs, err = resolver.StateChanges(ctx, &first, nextCursor, nil, nil)
		require.NoError(t, err)
		assert.Len(t, scs.Edges, 2)
		assert.Equal(t, fmt.Sprintf("%d:2", toid.New(1000, 1, 1).ToInt64()), fmt.Sprintf("%d:%d", extractStateChangeIDs(scs.Edges[0].Node).ToID, extractStateChangeIDs(scs.Edges[0].Node).StateChangeOrder))
		assert.Equal(t, fmt.Sprintf("%d:1", toid.New(1000, 1, 2).ToInt64()), fmt.Sprintf("%d:%d", extractStateChangeIDs(scs.Edges[1].Node).ToID, extractStateChangeIDs(scs.Edges[1].Node).StateChangeOrder))
		assert.True(t, scs.PageInfo.HasNextPage)
		assert.True(t, scs.PageInfo.HasPreviousPage)

		// Get the next page with larger limit
		first = int32(20)
		nextCursor = scs.PageInfo.EndCursor
		assert.NotNil(t, nextCursor)
		scs, err = resolver.StateChanges(ctx, &first, nextCursor, nil, nil)
		require.NoError(t, err)
		assert.Len(t, scs.Edges, 16) // Should return next 10 items
		assert.Equal(t, fmt.Sprintf("%d:2", toid.New(1000, 1, 2).ToInt64()), fmt.Sprintf("%d:%d", extractStateChangeIDs(scs.Edges[0].Node).ToID, extractStateChangeIDs(scs.Edges[0].Node).StateChangeOrder))
		assert.Equal(t, fmt.Sprintf("%d:1", toid.New(1000, 2, 0).ToInt64()), fmt.Sprintf("%d:%d", extractStateChangeIDs(scs.Edges[1].Node).ToID, extractStateChangeIDs(scs.Edges[1].Node).StateChangeOrder))
		assert.False(t, scs.PageInfo.HasNextPage)
		assert.True(t, scs.PageInfo.HasPreviousPage)
	})

	t.Run("get state changes with last/before limit and cursor", func(t *testing.T) {
		ctx := getTestCtx("state_changes", []string{})
		last := int32(2)
		stateChanges, err := resolver.StateChanges(ctx, nil, nil, &last, nil)
		require.NoError(t, err)
		assert.Len(t, stateChanges.Edges, 2)
		assert.Equal(t, fmt.Sprintf("%d:1", toid.New(1000, 4, 2).ToInt64()), fmt.Sprintf("%d:%d", extractStateChangeIDs(stateChanges.Edges[0].Node).ToID, extractStateChangeIDs(stateChanges.Edges[0].Node).StateChangeOrder))
		assert.Equal(t, fmt.Sprintf("%d:2", toid.New(1000, 4, 2).ToInt64()), fmt.Sprintf("%d:%d", extractStateChangeIDs(stateChanges.Edges[1].Node).ToID, extractStateChangeIDs(stateChanges.Edges[1].Node).StateChangeOrder))
		assert.False(t, stateChanges.PageInfo.HasNextPage)
		assert.True(t, stateChanges.PageInfo.HasPreviousPage)

		// Get the previous page
		prevCursor := stateChanges.PageInfo.EndCursor
		assert.NotNil(t, prevCursor)
		stateChanges, err = resolver.StateChanges(ctx, nil, nil, &last, prevCursor)
		require.NoError(t, err)
		assert.Len(t, stateChanges.Edges, 2)
		assert.Equal(t, fmt.Sprintf("%d:1", toid.New(1000, 4, 1).ToInt64()), fmt.Sprintf("%d:%d", extractStateChangeIDs(stateChanges.Edges[0].Node).ToID, extractStateChangeIDs(stateChanges.Edges[0].Node).StateChangeOrder))
		assert.Equal(t, fmt.Sprintf("%d:2", toid.New(1000, 4, 1).ToInt64()), fmt.Sprintf("%d:%d", extractStateChangeIDs(stateChanges.Edges[1].Node).ToID, extractStateChangeIDs(stateChanges.Edges[1].Node).StateChangeOrder))
		assert.True(t, stateChanges.PageInfo.HasNextPage)
		assert.True(t, stateChanges.PageInfo.HasPreviousPage)

		// Get more previous items
		prevCursor = stateChanges.PageInfo.EndCursor
		assert.NotNil(t, prevCursor)
		last = int32(20)
		stateChanges, err = resolver.StateChanges(ctx, nil, nil, &last, prevCursor)
		require.NoError(t, err)
		assert.Len(t, stateChanges.Edges, 16)
		assert.Equal(t, fmt.Sprintf("%d:1", toid.New(1000, 1, 0).ToInt64()), fmt.Sprintf("%d:%d", extractStateChangeIDs(stateChanges.Edges[0].Node).ToID, extractStateChangeIDs(stateChanges.Edges[0].Node).StateChangeOrder))
		assert.Equal(t, fmt.Sprintf("%d:1", toid.New(1000, 1, 1).ToInt64()), fmt.Sprintf("%d:%d", extractStateChangeIDs(stateChanges.Edges[1].Node).ToID, extractStateChangeIDs(stateChanges.Edges[1].Node).StateChangeOrder))
		assert.True(t, stateChanges.PageInfo.HasNextPage)
		assert.False(t, stateChanges.PageInfo.HasPreviousPage) // We're at the beginning
	})

	t.Run("returns error when first is negative", func(t *testing.T) {
		ctx := getTestCtx("state_changes", []string{})
		first := int32(-1)
		stateChanges, err := resolver.StateChanges(ctx, &first, nil, nil, nil)
		require.Error(t, err)
		assert.Nil(t, stateChanges)
		assert.Contains(t, err.Error(), "first must be greater than 0")
	})

	t.Run("returns error when last is negative", func(t *testing.T) {
		ctx := getTestCtx("state_changes", []string{})
		last := int32(-1)
		stateChanges, err := resolver.StateChanges(ctx, nil, nil, &last, nil)
		require.Error(t, err)
		assert.Nil(t, stateChanges)
		assert.Contains(t, err.Error(), "last must be greater than 0")
	})

	t.Run("returns error when first is zero", func(t *testing.T) {
		ctx := getTestCtx("state_changes", []string{})
		first := int32(0)
		stateChanges, err := resolver.StateChanges(ctx, &first, nil, nil, nil)
		require.Error(t, err)
		assert.Nil(t, stateChanges)
		assert.Contains(t, err.Error(), "first must be greater than 0")
	})

	t.Run("returns error when last is zero", func(t *testing.T) {
		ctx := getTestCtx("state_changes", []string{})
		last := int32(0)
		stateChanges, err := resolver.StateChanges(ctx, nil, nil, &last, nil)
		require.Error(t, err)
		assert.Nil(t, stateChanges)
		assert.Contains(t, err.Error(), "last must be greater than 0")
	})

	t.Run("first parameter's value larger than available data", func(t *testing.T) {
		ctx := getTestCtx("state_changes", []string{})
		first := int32(100)
		stateChanges, err := resolver.StateChanges(ctx, &first, nil, nil, nil)
		require.NoError(t, err)
		assert.Len(t, stateChanges.Edges, 20) // Total available state changes
		assert.False(t, stateChanges.PageInfo.HasNextPage)
		assert.False(t, stateChanges.PageInfo.HasPreviousPage)
	})

	t.Run("last parameter's value larger than available data", func(t *testing.T) {
		ctx := getTestCtx("state_changes", []string{})
		last := int32(100)
		stateChanges, err := resolver.StateChanges(ctx, nil, nil, &last, nil)
		require.NoError(t, err)
		assert.Len(t, stateChanges.Edges, 20) // Total available state changes
		assert.False(t, stateChanges.PageInfo.HasNextPage)
		assert.False(t, stateChanges.PageInfo.HasPreviousPage)
	})
}
