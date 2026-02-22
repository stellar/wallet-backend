package resolvers

import (
	"encoding/base64"
	"fmt"
	"testing"

	"github.com/stellar/go-stellar-sdk/toid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/stellar/wallet-backend/internal/data"
	"github.com/stellar/wallet-backend/internal/metrics"
)

// testOpXDR returns the expected base64-encoded XDR for test operation N
func testOpXDR(n int) string {
	return base64.StdEncoding.EncodeToString([]byte(fmt.Sprintf("opxdr%d", n)))
}

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
		ctx := getTestCtx("transactions", []string{"hash", "toId", "envelopeXdr", "feeCharged", "resultCode", "metaXdr", "ledgerNumber", "ledgerCreatedAt", "isFeeBump"})
		tx, err := resolver.TransactionByHash(ctx, testTxHash1)

		require.NoError(t, err)
		assert.Equal(t, testTxHash1, tx.Hash.String())
		assert.Equal(t, toid.New(1000, 1, 0).ToInt64(), tx.ToID)
		require.NotNil(t, tx.EnvelopeXDR)
		assert.Equal(t, "envelope1", *tx.EnvelopeXDR)
		assert.Equal(t, int64(100), tx.FeeCharged)
		assert.Equal(t, "TransactionResultCodeTxSuccess", tx.ResultCode)
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
		assert.Equal(t, testTxHash1, transactions.Edges[0].Node.Hash.String())
		assert.Equal(t, testTxHash2, transactions.Edges[1].Node.Hash.String())
		assert.Equal(t, testTxHash3, transactions.Edges[2].Node.Hash.String())
		assert.Equal(t, testTxHash4, transactions.Edges[3].Node.Hash.String())
	})

	t.Run("get transactions with first/after limit and cursor", func(t *testing.T) {
		ctx := getTestCtx("transactions", []string{"hash"})
		first := int32(2)
		txs, err := resolver.Transactions(ctx, &first, nil, nil, nil)
		require.NoError(t, err)
		assert.Len(t, txs.Edges, 2)
		assert.Equal(t, testTxHash1, txs.Edges[0].Node.Hash.String())
		assert.Equal(t, testTxHash2, txs.Edges[1].Node.Hash.String())
		assert.True(t, txs.PageInfo.HasNextPage)
		assert.False(t, txs.PageInfo.HasPreviousPage)

		// Get the next cursor
		first = int32(1)
		nextCursor := txs.PageInfo.EndCursor
		assert.NotNil(t, nextCursor)
		txs, err = resolver.Transactions(ctx, &first, nextCursor, nil, nil)
		require.NoError(t, err)
		assert.Len(t, txs.Edges, 1)
		assert.Equal(t, testTxHash3, txs.Edges[0].Node.Hash.String())
		assert.True(t, txs.PageInfo.HasNextPage)
		assert.True(t, txs.PageInfo.HasPreviousPage)

		// Get the previous cursor
		first = int32(10)
		nextCursor = txs.PageInfo.EndCursor
		assert.NotNil(t, nextCursor)
		txs, err = resolver.Transactions(ctx, &first, nextCursor, nil, nil)
		require.NoError(t, err)
		assert.Len(t, txs.Edges, 1)
		assert.Equal(t, testTxHash4, txs.Edges[0].Node.Hash.String())
		assert.False(t, txs.PageInfo.HasNextPage)
		assert.True(t, txs.PageInfo.HasPreviousPage)
	})

	t.Run("get transactions with last/before limit and cursor", func(t *testing.T) {
		ctx := getTestCtx("transactions", []string{"hash"})
		last := int32(2)
		txs, err := resolver.Transactions(ctx, nil, nil, &last, nil)
		require.NoError(t, err)
		assert.Len(t, txs.Edges, 2)
		assert.Equal(t, testTxHash3, txs.Edges[0].Node.Hash.String())
		assert.Equal(t, testTxHash4, txs.Edges[1].Node.Hash.String())
		assert.False(t, txs.PageInfo.HasNextPage)
		assert.True(t, txs.PageInfo.HasPreviousPage)

		// Get the next cursor (going backward, use StartCursor per Relay spec)
		last = int32(1)
		nextCursor := txs.PageInfo.StartCursor
		assert.NotNil(t, nextCursor)
		txs, err = resolver.Transactions(ctx, nil, nil, &last, nextCursor)
		require.NoError(t, err)
		assert.Len(t, txs.Edges, 1)
		assert.Equal(t, testTxHash2, txs.Edges[0].Node.Hash.String())
		assert.True(t, txs.PageInfo.HasNextPage)
		assert.True(t, txs.PageInfo.HasPreviousPage)

		nextCursor = txs.PageInfo.StartCursor
		assert.NotNil(t, nextCursor)
		last = int32(10)
		txs, err = resolver.Transactions(ctx, nil, nil, &last, nextCursor)
		require.NoError(t, err)
		assert.Len(t, txs.Edges, 1)
		assert.Equal(t, testTxHash1, txs.Edges[0].Node.Hash.String())
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
	resolver := &queryResolver{&Resolver{}}

	t.Run("success", func(t *testing.T) {
		acc, err := resolver.AccountByAddress(testCtx, sharedTestAccountAddress)
		require.NoError(t, err)
		assert.Equal(t, sharedTestAccountAddress, string(acc.StellarAddress))
	})

	t.Run("any valid address returns account", func(t *testing.T) {
		acc, err := resolver.AccountByAddress(testCtx, sharedNonExistentAccountAddress)
		require.NoError(t, err)
		assert.NotNil(t, acc)
		assert.Equal(t, sharedNonExistentAccountAddress, string(acc.StellarAddress))
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
		assert.Equal(t, testOpXDR(1), operations.Edges[0].Node.OperationXDR.String())
		assert.Equal(t, testOpXDR(2), operations.Edges[1].Node.OperationXDR.String())
		assert.Equal(t, testOpXDR(3), operations.Edges[2].Node.OperationXDR.String())
		assert.Equal(t, testOpXDR(4), operations.Edges[3].Node.OperationXDR.String())
	})

	t.Run("get operations with first/after limit and cursor", func(t *testing.T) {
		ctx := getTestCtx("operations", []string{"operation_xdr"})
		first := int32(2)
		ops, err := resolver.Operations(ctx, &first, nil, nil, nil)
		require.NoError(t, err)
		assert.Len(t, ops.Edges, 2)
		assert.Equal(t, testOpXDR(1), ops.Edges[0].Node.OperationXDR.String())
		assert.Equal(t, testOpXDR(2), ops.Edges[1].Node.OperationXDR.String())
		assert.True(t, ops.PageInfo.HasNextPage)
		assert.False(t, ops.PageInfo.HasPreviousPage)

		// Get the next cursor
		first = int32(1)
		nextCursor := ops.PageInfo.EndCursor
		assert.NotNil(t, nextCursor)
		ops, err = resolver.Operations(ctx, &first, nextCursor, nil, nil)
		require.NoError(t, err)
		assert.Len(t, ops.Edges, 1)
		assert.Equal(t, testOpXDR(3), ops.Edges[0].Node.OperationXDR.String())
		assert.True(t, ops.PageInfo.HasNextPage)
		assert.True(t, ops.PageInfo.HasPreviousPage)

		// Get the next page
		first = int32(10)
		nextCursor = ops.PageInfo.EndCursor
		assert.NotNil(t, nextCursor)
		ops, err = resolver.Operations(ctx, &first, nextCursor, nil, nil)
		require.NoError(t, err)
		assert.Len(t, ops.Edges, 5)
		assert.Equal(t, testOpXDR(4), ops.Edges[0].Node.OperationXDR.String())
		assert.Equal(t, testOpXDR(5), ops.Edges[1].Node.OperationXDR.String())
		assert.Equal(t, testOpXDR(6), ops.Edges[2].Node.OperationXDR.String())
		assert.Equal(t, testOpXDR(7), ops.Edges[3].Node.OperationXDR.String())
		assert.Equal(t, testOpXDR(8), ops.Edges[4].Node.OperationXDR.String())
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
		assert.Equal(t, testOpXDR(7), ops.Edges[0].Node.OperationXDR.String())
		assert.Equal(t, testOpXDR(8), ops.Edges[1].Node.OperationXDR.String())
		assert.False(t, ops.PageInfo.HasNextPage)
		assert.True(t, ops.PageInfo.HasPreviousPage)

		// Get the previous page (use StartCursor per Relay spec)
		last = int32(1)
		prevCursor := ops.PageInfo.StartCursor
		assert.NotNil(t, prevCursor)
		ops, err = resolver.Operations(ctx, nil, nil, &last, prevCursor)
		require.NoError(t, err)
		assert.Len(t, ops.Edges, 1)
		assert.Equal(t, testOpXDR(6), ops.Edges[0].Node.OperationXDR.String())
		assert.True(t, ops.PageInfo.HasNextPage)
		assert.True(t, ops.PageInfo.HasPreviousPage)

		prevCursor = ops.PageInfo.StartCursor
		assert.NotNil(t, prevCursor)
		last = int32(10)
		ops, err = resolver.Operations(ctx, nil, nil, &last, prevCursor)
		require.NoError(t, err)
		// There are 5 operations before (2,1): (2,2), (3,1), (3,2), (4,1), (4,2)
		assert.Len(t, ops.Edges, 5)
		assert.Equal(t, testOpXDR(1), ops.Edges[0].Node.OperationXDR.String())
		assert.Equal(t, testOpXDR(2), ops.Edges[1].Node.OperationXDR.String())
		assert.Equal(t, testOpXDR(3), ops.Edges[2].Node.OperationXDR.String())
		assert.Equal(t, testOpXDR(4), ops.Edges[3].Node.OperationXDR.String())
		assert.Equal(t, testOpXDR(5), ops.Edges[4].Node.OperationXDR.String())
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
		assert.Equal(t, testOpXDR(1), op.OperationXDR.String())
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

		tx1ToID := toid.New(1000, 1, 0).ToInt64()
		op1ID := toid.New(1000, 1, 1).ToInt64()

		// Edge 0: tx1 fee state change
		cursor := extractStateChangeIDs(stateChanges.Edges[0].Node)
		assert.Equal(t, tx1ToID, cursor.ToID)
		assert.Equal(t, int64(0), cursor.OperationID)
		assert.Equal(t, int64(1), cursor.StateChangeOrder)

		// Edge 1: tx1 op1 first state change
		cursor = extractStateChangeIDs(stateChanges.Edges[1].Node)
		assert.Equal(t, tx1ToID, cursor.ToID)
		assert.Equal(t, op1ID, cursor.OperationID)
		assert.Equal(t, int64(1), cursor.StateChangeOrder)

		// Edge 2: tx1 op1 second state change
		cursor = extractStateChangeIDs(stateChanges.Edges[2].Node)
		assert.Equal(t, tx1ToID, cursor.ToID)
		assert.Equal(t, op1ID, cursor.OperationID)
		assert.Equal(t, int64(2), cursor.StateChangeOrder)
	})

	t.Run("get state changes with first/after limit and cursor", func(t *testing.T) {
		ctx := getTestCtx("state_changes", []string{})

		tx1ToID := toid.New(1000, 1, 0).ToInt64()
		tx1Op1ID := toid.New(1000, 1, 1).ToInt64()
		tx1Op2ID := toid.New(1000, 1, 2).ToInt64()
		tx2ToID := toid.New(1000, 2, 0).ToInt64()

		first := int32(2)
		scs, err := resolver.StateChanges(ctx, &first, nil, nil, nil)
		require.NoError(t, err)
		assert.Len(t, scs.Edges, 2)

		// Page 1: tx1 fee and tx1 op1 sc1
		cursor := extractStateChangeIDs(scs.Edges[0].Node)
		assert.Equal(t, tx1ToID, cursor.ToID)
		assert.Equal(t, int64(0), cursor.OperationID)
		assert.Equal(t, int64(1), cursor.StateChangeOrder)

		cursor = extractStateChangeIDs(scs.Edges[1].Node)
		assert.Equal(t, tx1ToID, cursor.ToID)
		assert.Equal(t, tx1Op1ID, cursor.OperationID)
		assert.Equal(t, int64(1), cursor.StateChangeOrder)

		assert.True(t, scs.PageInfo.HasNextPage)
		assert.False(t, scs.PageInfo.HasPreviousPage)

		// Get the next cursor
		nextCursor := scs.PageInfo.EndCursor
		assert.NotNil(t, nextCursor)
		scs, err = resolver.StateChanges(ctx, &first, nextCursor, nil, nil)
		require.NoError(t, err)
		assert.Len(t, scs.Edges, 2)

		// Page 2: tx1 op1 sc2 and tx1 op2 sc1
		cursor = extractStateChangeIDs(scs.Edges[0].Node)
		assert.Equal(t, tx1ToID, cursor.ToID)
		assert.Equal(t, tx1Op1ID, cursor.OperationID)
		assert.Equal(t, int64(2), cursor.StateChangeOrder)

		cursor = extractStateChangeIDs(scs.Edges[1].Node)
		assert.Equal(t, tx1ToID, cursor.ToID)
		assert.Equal(t, tx1Op2ID, cursor.OperationID)
		assert.Equal(t, int64(1), cursor.StateChangeOrder)

		assert.True(t, scs.PageInfo.HasNextPage)
		assert.True(t, scs.PageInfo.HasPreviousPage)

		// Get the next page with larger limit
		first = int32(20)
		nextCursor = scs.PageInfo.EndCursor
		assert.NotNil(t, nextCursor)
		scs, err = resolver.StateChanges(ctx, &first, nextCursor, nil, nil)
		require.NoError(t, err)
		assert.Len(t, scs.Edges, 16) // Should return next 16 items

		// Page 3: tx1 op2 sc2 and tx2 fee
		cursor = extractStateChangeIDs(scs.Edges[0].Node)
		assert.Equal(t, tx1ToID, cursor.ToID)
		assert.Equal(t, tx1Op2ID, cursor.OperationID)
		assert.Equal(t, int64(2), cursor.StateChangeOrder)

		cursor = extractStateChangeIDs(scs.Edges[1].Node)
		assert.Equal(t, tx2ToID, cursor.ToID)
		assert.Equal(t, int64(0), cursor.OperationID) // Fee state change
		assert.Equal(t, int64(1), cursor.StateChangeOrder)

		assert.False(t, scs.PageInfo.HasNextPage)
		assert.True(t, scs.PageInfo.HasPreviousPage)
	})

	t.Run("get state changes with last/before limit and cursor", func(t *testing.T) {
		ctx := getTestCtx("state_changes", []string{})

		tx1ToID := toid.New(1000, 1, 0).ToInt64()
		tx1Op1ID := toid.New(1000, 1, 1).ToInt64()
		tx4ToID := toid.New(1000, 4, 0).ToInt64()
		tx4Op1ID := toid.New(1000, 4, 1).ToInt64()
		tx4Op2ID := toid.New(1000, 4, 2).ToInt64()

		last := int32(2)
		stateChanges, err := resolver.StateChanges(ctx, nil, nil, &last, nil)
		require.NoError(t, err)
		assert.Len(t, stateChanges.Edges, 2)

		// Last 2: tx4 op2 sc1 and tx4 op2 sc2
		cursor := extractStateChangeIDs(stateChanges.Edges[0].Node)
		assert.Equal(t, tx4ToID, cursor.ToID)
		assert.Equal(t, tx4Op2ID, cursor.OperationID)
		assert.Equal(t, int64(1), cursor.StateChangeOrder)

		cursor = extractStateChangeIDs(stateChanges.Edges[1].Node)
		assert.Equal(t, tx4ToID, cursor.ToID)
		assert.Equal(t, tx4Op2ID, cursor.OperationID)
		assert.Equal(t, int64(2), cursor.StateChangeOrder)

		assert.False(t, stateChanges.PageInfo.HasNextPage)
		assert.True(t, stateChanges.PageInfo.HasPreviousPage)

		// Get the previous page (use StartCursor per Relay spec)
		prevCursor := stateChanges.PageInfo.StartCursor
		assert.NotNil(t, prevCursor)
		stateChanges, err = resolver.StateChanges(ctx, nil, nil, &last, prevCursor)
		require.NoError(t, err)
		assert.Len(t, stateChanges.Edges, 2)

		// Previous 2: tx4 op1 sc1 and tx4 op1 sc2
		cursor = extractStateChangeIDs(stateChanges.Edges[0].Node)
		assert.Equal(t, tx4ToID, cursor.ToID)
		assert.Equal(t, tx4Op1ID, cursor.OperationID)
		assert.Equal(t, int64(1), cursor.StateChangeOrder)

		cursor = extractStateChangeIDs(stateChanges.Edges[1].Node)
		assert.Equal(t, tx4ToID, cursor.ToID)
		assert.Equal(t, tx4Op1ID, cursor.OperationID)
		assert.Equal(t, int64(2), cursor.StateChangeOrder)

		assert.True(t, stateChanges.PageInfo.HasNextPage)
		assert.True(t, stateChanges.PageInfo.HasPreviousPage)

		// Get more previous items (use StartCursor per Relay spec)
		prevCursor = stateChanges.PageInfo.StartCursor
		assert.NotNil(t, prevCursor)
		last = int32(20)
		stateChanges, err = resolver.StateChanges(ctx, nil, nil, &last, prevCursor)
		require.NoError(t, err)
		assert.Len(t, stateChanges.Edges, 16)

		// First page: tx1 fee, tx1 op1 sc1, ...
		cursor = extractStateChangeIDs(stateChanges.Edges[0].Node)
		assert.Equal(t, tx1ToID, cursor.ToID)
		assert.Equal(t, int64(0), cursor.OperationID)
		assert.Equal(t, int64(1), cursor.StateChangeOrder)

		cursor = extractStateChangeIDs(stateChanges.Edges[1].Node)
		assert.Equal(t, tx1ToID, cursor.ToID)
		assert.Equal(t, tx1Op1ID, cursor.OperationID)
		assert.Equal(t, int64(1), cursor.StateChangeOrder)

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
