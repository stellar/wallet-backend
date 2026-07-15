package resolvers

import (
	"context"
	"encoding/base64"
	"fmt"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stellar/go-stellar-sdk/toid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vektah/gqlparser/v2/gqlerror"

	"github.com/stellar/wallet-backend/internal/data"
	"github.com/stellar/wallet-backend/internal/indexer/types"
	"github.com/stellar/wallet-backend/internal/metrics"
)

// testOpXDR returns the expected base64-encoded XDR for test operation N
func testOpXDR(n int) string {
	return base64.StdEncoding.EncodeToString([]byte(fmt.Sprintf("opxdr%d", n)))
}

func TestQueryResolver_TransactionByHash(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := metrics.NewMetrics(reg)

	resolver := &queryResolver{
		&Resolver{
			models: &data.Models{
				Transactions: &data.TransactionModel{
					DB:      testDBConnectionPool,
					Metrics: m.DB,
				},
			},
		},
	}

	t.Run("success", func(t *testing.T) {
		ctx := getTestCtx("transactions", []string{"hash", "toId", "feeCharged", "resultCode", "ledgerNumber", "ledgerCreatedAt", "isFeeBump"})
		tx, err := resolver.TransactionByHash(ctx, testTxHash1)

		require.NoError(t, err)
		assert.Equal(t, testTxHash1, tx.Hash.String())
		assert.Equal(t, toid.New(1000, 1, 0).ToInt64(), tx.ToID)
		assert.Equal(t, int64(100), tx.FeeCharged)
		assert.Equal(t, "TransactionResultCodeTxSuccess", tx.ResultCode)
		assert.Equal(t, uint32(1), tx.LedgerNumber)
	})

	t.Run("non-existent hash", func(t *testing.T) {
		ctx := getTestCtx("transactions", []string{"hash"})
		tx, err := resolver.TransactionByHash(ctx, "0000000000000000000000000000000000000000000000000000000000000000")

		require.Error(t, err)
		assert.Nil(t, tx)
	})

	t.Run("invalid hash format", func(t *testing.T) {
		ctx := getTestCtx("transactions", []string{"hash"})
		tx, err := resolver.TransactionByHash(ctx, "not-a-valid-hash")

		require.Error(t, err)
		assert.Nil(t, tx)
		var gqlErr *gqlerror.Error
		require.ErrorAs(t, err, &gqlErr)
		assert.Equal(t, ErrMsgInvalidTransactionHash, gqlErr.Message)
	})

	t.Run("empty hash", func(t *testing.T) {
		ctx := getTestCtx("transactions", []string{"hash"})
		tx, err := resolver.TransactionByHash(ctx, "")

		require.Error(t, err)
		assert.Nil(t, tx)
	})
}

func TestQueryResolver_Transactions(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := metrics.NewMetrics(reg)

	resolver := &queryResolver{
		&Resolver{
			models: &data.Models{
				Transactions: &data.TransactionModel{
					DB:      testDBConnectionPool,
					Metrics: m.DB,
				},
			},
		},
	}

	t.Run("get all transactions", func(t *testing.T) {
		ctx := getTestCtx("transactions", []string{"hash"})
		transactions, err := resolver.Transactions(ctx, nil, nil, nil, nil, nil, nil)

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
		txs, err := resolver.Transactions(ctx, nil, nil, &first, nil, nil, nil)
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
		txs, err = resolver.Transactions(ctx, nil, nil, &first, nextCursor, nil, nil)
		require.NoError(t, err)
		assert.Len(t, txs.Edges, 1)
		assert.Equal(t, testTxHash3, txs.Edges[0].Node.Hash.String())
		assert.True(t, txs.PageInfo.HasNextPage)
		assert.True(t, txs.PageInfo.HasPreviousPage)

		// Get the previous cursor
		first = int32(10)
		nextCursor = txs.PageInfo.EndCursor
		assert.NotNil(t, nextCursor)
		txs, err = resolver.Transactions(ctx, nil, nil, &first, nextCursor, nil, nil)
		require.NoError(t, err)
		assert.Len(t, txs.Edges, 1)
		assert.Equal(t, testTxHash4, txs.Edges[0].Node.Hash.String())
		assert.False(t, txs.PageInfo.HasNextPage)
		assert.True(t, txs.PageInfo.HasPreviousPage)
	})

	t.Run("get transactions with last/before limit and cursor", func(t *testing.T) {
		ctx := getTestCtx("transactions", []string{"hash"})
		last := int32(2)
		txs, err := resolver.Transactions(ctx, nil, nil, nil, nil, &last, nil)
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
		txs, err = resolver.Transactions(ctx, nil, nil, nil, nil, &last, nextCursor)
		require.NoError(t, err)
		assert.Len(t, txs.Edges, 1)
		assert.Equal(t, testTxHash2, txs.Edges[0].Node.Hash.String())
		assert.True(t, txs.PageInfo.HasNextPage)
		assert.True(t, txs.PageInfo.HasPreviousPage)

		nextCursor = txs.PageInfo.StartCursor
		assert.NotNil(t, nextCursor)
		last = int32(10)
		txs, err = resolver.Transactions(ctx, nil, nil, nil, nil, &last, nextCursor)
		require.NoError(t, err)
		assert.Len(t, txs.Edges, 1)
		assert.Equal(t, testTxHash1, txs.Edges[0].Node.Hash.String())
		assert.True(t, txs.PageInfo.HasNextPage)
		assert.False(t, txs.PageInfo.HasPreviousPage)
	})

	t.Run("returns error when first is negative", func(t *testing.T) {
		ctx := getTestCtx("transactions", []string{"hash"})
		first := int32(-1)
		txs, err := resolver.Transactions(ctx, nil, nil, &first, nil, nil, nil)
		require.Error(t, err)
		assert.Nil(t, txs)
		assert.Contains(t, err.Error(), "first must be greater than 0")
	})

	t.Run("returns error when last is negative", func(t *testing.T) {
		ctx := getTestCtx("transactions", []string{"hash"})
		last := int32(-1)
		txs, err := resolver.Transactions(ctx, nil, nil, nil, nil, &last, nil)
		require.Error(t, err)
		assert.Nil(t, txs)
		assert.Contains(t, err.Error(), "last must be greater than 0")
	})

	t.Run("returns error when first is zero", func(t *testing.T) {
		ctx := getTestCtx("transactions", []string{"hash"})
		first := int32(0)
		txs, err := resolver.Transactions(ctx, nil, nil, &first, nil, nil, nil)
		require.Error(t, err)
		assert.Nil(t, txs)
		assert.Contains(t, err.Error(), "first must be greater than 0")
	})

	t.Run("returns error when last is zero", func(t *testing.T) {
		ctx := getTestCtx("transactions", []string{"hash"})
		last := int32(0)
		txs, err := resolver.Transactions(ctx, nil, nil, nil, nil, &last, nil)
		require.Error(t, err)
		assert.Nil(t, txs)
		assert.Contains(t, err.Error(), "last must be greater than 0")
	})

	t.Run("first parameter's value larger than available data", func(t *testing.T) {
		ctx := getTestCtx("transactions", []string{"hash"})
		first := int32(100)
		txs, err := resolver.Transactions(ctx, nil, nil, &first, nil, nil, nil)
		require.NoError(t, err)
		assert.Len(t, txs.Edges, 4)
		assert.False(t, txs.PageInfo.HasNextPage)
		assert.False(t, txs.PageInfo.HasPreviousPage)
	})

	t.Run("last parameter's value larger than available data", func(t *testing.T) {
		ctx := getTestCtx("transactions", []string{"hash"})
		last := int32(100)
		txs, err := resolver.Transactions(ctx, nil, nil, nil, nil, &last, nil)
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
	reg := prometheus.NewRegistry()
	m := metrics.NewMetrics(reg)

	resolver := &queryResolver{
		&Resolver{
			models: &data.Models{
				Operations: &data.OperationModel{
					DB:      testDBConnectionPool,
					Metrics: m.DB,
				},
			},
		},
	}

	t.Run("get all operations", func(t *testing.T) {
		ctx := getTestCtx("operations", []string{"operation_xdr"})
		operations, err := resolver.Operations(ctx, nil, nil, nil, nil, nil, nil)

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
		ops, err := resolver.Operations(ctx, nil, nil, &first, nil, nil, nil)
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
		ops, err = resolver.Operations(ctx, nil, nil, &first, nextCursor, nil, nil)
		require.NoError(t, err)
		assert.Len(t, ops.Edges, 1)
		assert.Equal(t, testOpXDR(3), ops.Edges[0].Node.OperationXDR.String())
		assert.True(t, ops.PageInfo.HasNextPage)
		assert.True(t, ops.PageInfo.HasPreviousPage)

		// Get the next page
		first = int32(10)
		nextCursor = ops.PageInfo.EndCursor
		assert.NotNil(t, nextCursor)
		ops, err = resolver.Operations(ctx, nil, nil, &first, nextCursor, nil, nil)
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
		ops, err := resolver.Operations(ctx, nil, nil, nil, nil, &last, nil)
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
		ops, err = resolver.Operations(ctx, nil, nil, nil, nil, &last, prevCursor)
		require.NoError(t, err)
		assert.Len(t, ops.Edges, 1)
		assert.Equal(t, testOpXDR(6), ops.Edges[0].Node.OperationXDR.String())
		assert.True(t, ops.PageInfo.HasNextPage)
		assert.True(t, ops.PageInfo.HasPreviousPage)

		prevCursor = ops.PageInfo.StartCursor
		assert.NotNil(t, prevCursor)
		last = int32(10)
		ops, err = resolver.Operations(ctx, nil, nil, nil, nil, &last, prevCursor)
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
		ops, err := resolver.Operations(ctx, nil, nil, &first, nil, nil, nil)
		require.Error(t, err)
		assert.Nil(t, ops)
		assert.Contains(t, err.Error(), "first must be greater than 0")
	})

	t.Run("returns error when last is negative", func(t *testing.T) {
		ctx := getTestCtx("operations", []string{"operation_xdr"})
		last := int32(-1)
		ops, err := resolver.Operations(ctx, nil, nil, nil, nil, &last, nil)
		require.Error(t, err)
		assert.Nil(t, ops)
		assert.Contains(t, err.Error(), "last must be greater than 0")
	})

	t.Run("returns error when first is zero", func(t *testing.T) {
		ctx := getTestCtx("operations", []string{"operation_xdr"})
		first := int32(0)
		ops, err := resolver.Operations(ctx, nil, nil, &first, nil, nil, nil)
		require.Error(t, err)
		assert.Nil(t, ops)
		assert.Contains(t, err.Error(), "first must be greater than 0")
	})

	t.Run("returns error when last is zero", func(t *testing.T) {
		ctx := getTestCtx("operations", []string{"operation_xdr"})
		last := int32(0)
		ops, err := resolver.Operations(ctx, nil, nil, nil, nil, &last, nil)
		require.Error(t, err)
		assert.Nil(t, ops)
		assert.Contains(t, err.Error(), "last must be greater than 0")
	})

	t.Run("first parameter's value larger than available data", func(t *testing.T) {
		ctx := getTestCtx("operations", []string{"operation_xdr"})
		first := int32(100)
		ops, err := resolver.Operations(ctx, nil, nil, &first, nil, nil, nil)
		require.NoError(t, err)
		assert.Len(t, ops.Edges, 8)
		assert.False(t, ops.PageInfo.HasNextPage)
		assert.False(t, ops.PageInfo.HasPreviousPage)
	})

	t.Run("last parameter's value larger than available data", func(t *testing.T) {
		ctx := getTestCtx("operations", []string{"operation_xdr"})
		last := int32(100)
		ops, err := resolver.Operations(ctx, nil, nil, nil, nil, &last, nil)
		require.NoError(t, err)
		assert.Len(t, ops.Edges, 8)
		assert.False(t, ops.PageInfo.HasNextPage)
		assert.False(t, ops.PageInfo.HasPreviousPage)
	})
}

func TestQueryResolver_OperationByID(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := metrics.NewMetrics(reg)

	resolver := &queryResolver{
		&Resolver{
			models: &data.Models{
				Operations: &data.OperationModel{
					DB:      testDBConnectionPool,
					Metrics: m.DB,
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
	reg := prometheus.NewRegistry()
	m := metrics.NewMetrics(reg)

	resolver := &queryResolver{
		&Resolver{
			models: &data.Models{
				StateChanges: &data.StateChangeModel{
					DB:      testDBConnectionPool,
					Metrics: m.DB,
				},
			},
		},
	}

	t.Run("get all", func(t *testing.T) {
		ctx := getTestCtx("state_changes", []string{})
		stateChanges, err := resolver.StateChanges(ctx, nil, nil, nil, nil, nil, nil)
		require.NoError(t, err)
		assert.Len(t, stateChanges.Edges, 20)

		tx1ToID := toid.New(1000, 1, 0).ToInt64()
		op1ID := toid.New(1000, 1, 1).ToInt64()

		// Edge 0: tx1 fee state change
		cursor := extractStateChangeIDs(stateChanges.Edges[0].Node)
		assert.Equal(t, tx1ToID, cursor.ToID)
		assert.Equal(t, int64(0), cursor.OperationID)
		assert.Equal(t, int64(1), cursor.StateChangeID)

		// Edge 1: tx1 op1 first state change
		cursor = extractStateChangeIDs(stateChanges.Edges[1].Node)
		assert.Equal(t, tx1ToID, cursor.ToID)
		assert.Equal(t, op1ID, cursor.OperationID)
		assert.Equal(t, int64(1), cursor.StateChangeID)

		// Edge 2: tx1 op1 second state change
		cursor = extractStateChangeIDs(stateChanges.Edges[2].Node)
		assert.Equal(t, tx1ToID, cursor.ToID)
		assert.Equal(t, op1ID, cursor.OperationID)
		assert.Equal(t, int64(2), cursor.StateChangeID)
	})

	t.Run("get state changes with first/after limit and cursor", func(t *testing.T) {
		ctx := getTestCtx("state_changes", []string{})

		tx1ToID := toid.New(1000, 1, 0).ToInt64()
		tx1Op1ID := toid.New(1000, 1, 1).ToInt64()
		tx1Op2ID := toid.New(1000, 1, 2).ToInt64()
		tx2ToID := toid.New(1000, 2, 0).ToInt64()

		first := int32(2)
		scs, err := resolver.StateChanges(ctx, nil, nil, &first, nil, nil, nil)
		require.NoError(t, err)
		assert.Len(t, scs.Edges, 2)

		// Page 1: tx1 fee and tx1 op1 sc1
		cursor := extractStateChangeIDs(scs.Edges[0].Node)
		assert.Equal(t, tx1ToID, cursor.ToID)
		assert.Equal(t, int64(0), cursor.OperationID)
		assert.Equal(t, int64(1), cursor.StateChangeID)

		cursor = extractStateChangeIDs(scs.Edges[1].Node)
		assert.Equal(t, tx1ToID, cursor.ToID)
		assert.Equal(t, tx1Op1ID, cursor.OperationID)
		assert.Equal(t, int64(1), cursor.StateChangeID)

		assert.True(t, scs.PageInfo.HasNextPage)
		assert.False(t, scs.PageInfo.HasPreviousPage)

		// Get the next cursor
		nextCursor := scs.PageInfo.EndCursor
		assert.NotNil(t, nextCursor)
		scs, err = resolver.StateChanges(ctx, nil, nil, &first, nextCursor, nil, nil)
		require.NoError(t, err)
		assert.Len(t, scs.Edges, 2)

		// Page 2: tx1 op1 sc2 and tx1 op2 sc1
		cursor = extractStateChangeIDs(scs.Edges[0].Node)
		assert.Equal(t, tx1ToID, cursor.ToID)
		assert.Equal(t, tx1Op1ID, cursor.OperationID)
		assert.Equal(t, int64(2), cursor.StateChangeID)

		cursor = extractStateChangeIDs(scs.Edges[1].Node)
		assert.Equal(t, tx1ToID, cursor.ToID)
		assert.Equal(t, tx1Op2ID, cursor.OperationID)
		assert.Equal(t, int64(1), cursor.StateChangeID)

		assert.True(t, scs.PageInfo.HasNextPage)
		assert.True(t, scs.PageInfo.HasPreviousPage)

		// Get the next page with larger limit
		first = int32(20)
		nextCursor = scs.PageInfo.EndCursor
		assert.NotNil(t, nextCursor)
		scs, err = resolver.StateChanges(ctx, nil, nil, &first, nextCursor, nil, nil)
		require.NoError(t, err)
		assert.Len(t, scs.Edges, 16) // Should return next 16 items

		// Page 3: tx1 op2 sc2 and tx2 fee
		cursor = extractStateChangeIDs(scs.Edges[0].Node)
		assert.Equal(t, tx1ToID, cursor.ToID)
		assert.Equal(t, tx1Op2ID, cursor.OperationID)
		assert.Equal(t, int64(2), cursor.StateChangeID)

		cursor = extractStateChangeIDs(scs.Edges[1].Node)
		assert.Equal(t, tx2ToID, cursor.ToID)
		assert.Equal(t, int64(0), cursor.OperationID) // Fee state change
		assert.Equal(t, int64(1), cursor.StateChangeID)

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
		stateChanges, err := resolver.StateChanges(ctx, nil, nil, nil, nil, &last, nil)
		require.NoError(t, err)
		assert.Len(t, stateChanges.Edges, 2)

		// Last 2: tx4 op2 sc1 and tx4 op2 sc2
		cursor := extractStateChangeIDs(stateChanges.Edges[0].Node)
		assert.Equal(t, tx4ToID, cursor.ToID)
		assert.Equal(t, tx4Op2ID, cursor.OperationID)
		assert.Equal(t, int64(1), cursor.StateChangeID)

		cursor = extractStateChangeIDs(stateChanges.Edges[1].Node)
		assert.Equal(t, tx4ToID, cursor.ToID)
		assert.Equal(t, tx4Op2ID, cursor.OperationID)
		assert.Equal(t, int64(2), cursor.StateChangeID)

		assert.False(t, stateChanges.PageInfo.HasNextPage)
		assert.True(t, stateChanges.PageInfo.HasPreviousPage)

		// Get the previous page (use StartCursor per Relay spec)
		prevCursor := stateChanges.PageInfo.StartCursor
		assert.NotNil(t, prevCursor)
		stateChanges, err = resolver.StateChanges(ctx, nil, nil, nil, nil, &last, prevCursor)
		require.NoError(t, err)
		assert.Len(t, stateChanges.Edges, 2)

		// Previous 2: tx4 op1 sc1 and tx4 op1 sc2
		cursor = extractStateChangeIDs(stateChanges.Edges[0].Node)
		assert.Equal(t, tx4ToID, cursor.ToID)
		assert.Equal(t, tx4Op1ID, cursor.OperationID)
		assert.Equal(t, int64(1), cursor.StateChangeID)

		cursor = extractStateChangeIDs(stateChanges.Edges[1].Node)
		assert.Equal(t, tx4ToID, cursor.ToID)
		assert.Equal(t, tx4Op1ID, cursor.OperationID)
		assert.Equal(t, int64(2), cursor.StateChangeID)

		assert.True(t, stateChanges.PageInfo.HasNextPage)
		assert.True(t, stateChanges.PageInfo.HasPreviousPage)

		// Get more previous items (use StartCursor per Relay spec)
		prevCursor = stateChanges.PageInfo.StartCursor
		assert.NotNil(t, prevCursor)
		last = int32(20)
		stateChanges, err = resolver.StateChanges(ctx, nil, nil, nil, nil, &last, prevCursor)
		require.NoError(t, err)
		assert.Len(t, stateChanges.Edges, 16)

		// First page: tx1 fee, tx1 op1 sc1, ...
		cursor = extractStateChangeIDs(stateChanges.Edges[0].Node)
		assert.Equal(t, tx1ToID, cursor.ToID)
		assert.Equal(t, int64(0), cursor.OperationID)
		assert.Equal(t, int64(1), cursor.StateChangeID)

		cursor = extractStateChangeIDs(stateChanges.Edges[1].Node)
		assert.Equal(t, tx1ToID, cursor.ToID)
		assert.Equal(t, tx1Op1ID, cursor.OperationID)
		assert.Equal(t, int64(1), cursor.StateChangeID)

		assert.True(t, stateChanges.PageInfo.HasNextPage)
		assert.False(t, stateChanges.PageInfo.HasPreviousPage) // We're at the beginning
	})

	t.Run("returns error when first is negative", func(t *testing.T) {
		ctx := getTestCtx("state_changes", []string{})
		first := int32(-1)
		stateChanges, err := resolver.StateChanges(ctx, nil, nil, &first, nil, nil, nil)
		require.Error(t, err)
		assert.Nil(t, stateChanges)
		assert.Contains(t, err.Error(), "first must be greater than 0")
	})

	t.Run("returns error when last is negative", func(t *testing.T) {
		ctx := getTestCtx("state_changes", []string{})
		last := int32(-1)
		stateChanges, err := resolver.StateChanges(ctx, nil, nil, nil, nil, &last, nil)
		require.Error(t, err)
		assert.Nil(t, stateChanges)
		assert.Contains(t, err.Error(), "last must be greater than 0")
	})

	t.Run("returns error when first is zero", func(t *testing.T) {
		ctx := getTestCtx("state_changes", []string{})
		first := int32(0)
		stateChanges, err := resolver.StateChanges(ctx, nil, nil, &first, nil, nil, nil)
		require.Error(t, err)
		assert.Nil(t, stateChanges)
		assert.Contains(t, err.Error(), "first must be greater than 0")
	})

	t.Run("returns error when last is zero", func(t *testing.T) {
		ctx := getTestCtx("state_changes", []string{})
		last := int32(0)
		stateChanges, err := resolver.StateChanges(ctx, nil, nil, nil, nil, &last, nil)
		require.Error(t, err)
		assert.Nil(t, stateChanges)
		assert.Contains(t, err.Error(), "last must be greater than 0")
	})

	t.Run("first parameter's value larger than available data", func(t *testing.T) {
		ctx := getTestCtx("state_changes", []string{})
		first := int32(100)
		stateChanges, err := resolver.StateChanges(ctx, nil, nil, &first, nil, nil, nil)
		require.NoError(t, err)
		assert.Len(t, stateChanges.Edges, 20) // Total available state changes
		assert.False(t, stateChanges.PageInfo.HasNextPage)
		assert.False(t, stateChanges.PageInfo.HasPreviousPage)
	})

	t.Run("last parameter's value larger than available data", func(t *testing.T) {
		ctx := getTestCtx("state_changes", []string{})
		last := int32(100)
		stateChanges, err := resolver.StateChanges(ctx, nil, nil, nil, nil, &last, nil)
		require.NoError(t, err)
		assert.Len(t, stateChanges.Edges, 20) // Total available state changes
		assert.False(t, stateChanges.PageInfo.HasNextPage)
		assert.False(t, stateChanges.PageInfo.HasPreviousPage)
	})
}

// TestQueryResolver_RootConnectionsCapPageSize covers SQL-04: the root transactions/operations/
// stateChanges connections must reject first/last above maxRootPageLimit (100), including the
// pathological first: math.MaxInt32 case that would otherwise overflow queryLimit := *params.Limit
// + 1. Since parseRootPaginationParams rejects any first/last over the cap before that arithmetic
// runs, math.MaxInt32 is rejected the same way as 101 — never reaches the +1.
func TestQueryResolver_RootConnectionsCapPageSize(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := metrics.NewMetrics(reg)
	resolver := &queryResolver{
		&Resolver{
			models: &data.Models{
				Transactions: &data.TransactionModel{DB: testDBConnectionPool, Metrics: m.DB},
				Operations:   &data.OperationModel{DB: testDBConnectionPool, Metrics: m.DB},
				StateChanges: &data.StateChangeModel{DB: testDBConnectionPool, Metrics: m.DB},
			},
		},
	}

	testCases := []struct {
		name  string
		first int32
	}{
		{name: "first just above the cap (101)", first: 101},
		{name: "first at math.MaxInt32 (not an overflow, rejected by the cap)", first: 2147483647},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			first := tc.first

			_, err := resolver.Transactions(getTestCtx("transactions", []string{"hash"}), nil, nil, &first, nil, nil, nil)
			requireBadUserInput(t, err)

			_, err = resolver.Operations(getTestCtx("operations", []string{"id"}), nil, nil, &first, nil, nil, nil)
			requireBadUserInput(t, err)

			_, err = resolver.StateChanges(getTestCtx("state_changes", []string{}), nil, nil, &first, nil, nil, nil)
			requireBadUserInput(t, err)
		})
	}
}

// TestQueryResolver_Transactions_WithTimeRange covers D3 for the root transactions connection:
// the default 7-day window, explicit since/until, the until-before-since error, and cursor
// pagination staying within an explicit window across pages.
func TestQueryResolver_Transactions_WithTimeRange(t *testing.T) {
	ctx := context.Background()
	reg := prometheus.NewRegistry()
	m := metrics.NewMetrics(reg)
	resolver := &queryResolver{
		&Resolver{models: &data.Models{Transactions: &data.TransactionModel{DB: testDBConnectionPool, Metrics: m.DB}}},
	}

	// A unique, high to_id keeps this row from colliding with setupDB's seed transactions
	// (toid.New(1000, 1..4, 0)) or other tests' fixtures.
	const oldToID int64 = 1 << 41
	oldTime := time.Now().Add(-10 * 24 * time.Hour)

	t.Cleanup(func() {
		_, _ = testDBConnectionPool.Exec(ctx, `DELETE FROM transactions WHERE to_id = $1`, oldToID) //nolint:errcheck
	})

	_, err := testDBConnectionPool.Exec(ctx, `
		INSERT INTO transactions (hash, to_id, fee_charged, result_code, ledger_number, ledger_created_at, is_fee_bump)
		VALUES ($1, $2, 100, 'TransactionResultCodeTxSuccess', 1, $3, false)
	`, types.HashBytea("00000000000000000000000000000000000000000000000000000000000000bb"), oldToID, oldTime)
	require.NoError(t, err)

	t.Run("default window excludes a transaction older than 7 days", func(t *testing.T) {
		txs, err := resolver.Transactions(getTestCtx("transactions", []string{"hash"}), nil, nil, nil, nil, nil, nil)
		require.NoError(t, err)
		for _, edge := range txs.Edges {
			assert.NotEqual(t, oldToID, edge.Node.ToID, "the default 7-day window must exclude history older than the window")
		}
	})

	t.Run("explicit since covering the old row includes it", func(t *testing.T) {
		since := oldTime.Add(-1 * time.Hour)
		txs, err := resolver.Transactions(getTestCtx("transactions", []string{"hash"}), &since, nil, nil, nil, nil, nil)
		require.NoError(t, err)

		var found bool
		for _, edge := range txs.Edges {
			if edge.Node.ToID == oldToID {
				found = true
			}
		}
		assert.True(t, found, "an explicit since covering the old row's ledger_created_at must include it")
	})

	t.Run("until before since is rejected as BAD_USER_INPUT", func(t *testing.T) {
		since := time.Now()
		until := since.Add(-1 * time.Hour)
		_, err := resolver.Transactions(getTestCtx("transactions", []string{"hash"}), &since, &until, nil, nil, nil, nil)
		requireBadUserInput(t, err)
	})

	t.Run("cursor pagination stays within the same explicit window across pages", func(t *testing.T) {
		since := oldTime.Add(-1 * time.Hour)
		first := int32(1)

		// Page 1, sorted ascending by ledger_created_at: the synthetic old row is the oldest in
		// the window, so it comes first.
		page1, err := resolver.Transactions(getTestCtx("transactions", []string{"hash"}), &since, nil, &first, nil, nil, nil)
		require.NoError(t, err)
		require.Len(t, page1.Edges, 1)
		assert.Equal(t, oldToID, page1.Edges[0].Node.ToID)
		require.True(t, page1.PageInfo.HasNextPage)

		// Page 2: repeating the same since alongside the cursor stays within the same window
		// (the cursor itself carries no window information — see the transactions field's schema
		// description).
		cursor := page1.PageInfo.EndCursor
		require.NotNil(t, cursor)
		page2, err := resolver.Transactions(getTestCtx("transactions", []string{"hash"}), &since, nil, &first, cursor, nil, nil)
		require.NoError(t, err)
		require.Len(t, page2.Edges, 1)
		assert.NotEqual(t, oldToID, page2.Edges[0].Node.ToID)
	})
}

// TestQueryResolver_Operations_WithTimeRange covers D3's default window and explicit since for
// the root operations connection.
func TestQueryResolver_Operations_WithTimeRange(t *testing.T) {
	ctx := context.Background()
	reg := prometheus.NewRegistry()
	m := metrics.NewMetrics(reg)
	resolver := &queryResolver{
		&Resolver{models: &data.Models{Operations: &data.OperationModel{DB: testDBConnectionPool, Metrics: m.DB}}},
	}

	const oldOpID int64 = 1 << 42
	oldTime := time.Now().Add(-10 * 24 * time.Hour)

	t.Cleanup(func() {
		_, _ = testDBConnectionPool.Exec(ctx, `DELETE FROM operations WHERE id = $1`, oldOpID) //nolint:errcheck
	})

	_, err := testDBConnectionPool.Exec(ctx, `
		INSERT INTO operations (id, operation_type, operation_xdr, result_code, successful, ledger_number, ledger_created_at)
		VALUES ($1, 'PAYMENT', $2, 'op_success', true, 1, $3)
	`, oldOpID, types.XDRBytea([]byte("old-op-xdr")), oldTime)
	require.NoError(t, err)

	t.Run("default window excludes an operation older than 7 days", func(t *testing.T) {
		ops, err := resolver.Operations(getTestCtx("operations", []string{"id"}), nil, nil, nil, nil, nil, nil)
		require.NoError(t, err)
		for _, edge := range ops.Edges {
			assert.NotEqual(t, oldOpID, edge.Node.ID, "the default 7-day window must exclude history older than the window")
		}
	})

	t.Run("explicit since covering the old row includes it", func(t *testing.T) {
		since := oldTime.Add(-1 * time.Hour)
		ops, err := resolver.Operations(getTestCtx("operations", []string{"id"}), &since, nil, nil, nil, nil, nil)
		require.NoError(t, err)

		var found bool
		for _, edge := range ops.Edges {
			if edge.Node.ID == oldOpID {
				found = true
			}
		}
		assert.True(t, found, "an explicit since covering the old row's ledger_created_at must include it")
	})
}

// TestQueryResolver_StateChanges_WithTimeRange covers D3's default window and explicit since for
// the root stateChanges connection.
func TestQueryResolver_StateChanges_WithTimeRange(t *testing.T) {
	ctx := context.Background()
	reg := prometheus.NewRegistry()
	m := metrics.NewMetrics(reg)
	resolver := &queryResolver{
		&Resolver{models: &data.Models{StateChanges: &data.StateChangeModel{DB: testDBConnectionPool, Metrics: m.DB}}},
	}

	const oldToID int64 = 1 << 43
	oldTime := time.Now().Add(-10 * 24 * time.Hour)

	t.Cleanup(func() {
		_, _ = testDBConnectionPool.Exec(ctx, `DELETE FROM state_changes WHERE to_id = $1`, oldToID) //nolint:errcheck
	})

	_, err := testDBConnectionPool.Exec(ctx, `
		INSERT INTO state_changes (to_id, state_change_id, state_change_category, state_change_reason, ledger_created_at, ledger_number, account_id, operation_id)
		VALUES ($1, 1, 'BALANCE', 'CREDIT', $2, 1, $3, 0)
	`, oldToID, oldTime, types.AddressBytea(sharedTestAccountAddress))
	require.NoError(t, err)

	t.Run("default window excludes a state change older than 7 days", func(t *testing.T) {
		scs, err := resolver.StateChanges(getTestCtx("state_changes", []string{}), nil, nil, nil, nil, nil, nil)
		require.NoError(t, err)
		for _, edge := range scs.Edges {
			assert.NotEqual(t, oldToID, extractStateChangeIDs(edge.Node).ToID, "the default 7-day window must exclude history older than the window")
		}
	})

	t.Run("explicit since covering the old row includes it", func(t *testing.T) {
		since := oldTime.Add(-1 * time.Hour)
		scs, err := resolver.StateChanges(getTestCtx("state_changes", []string{}), &since, nil, nil, nil, nil, nil)
		require.NoError(t, err)

		var found bool
		for _, edge := range scs.Edges {
			if extractStateChangeIDs(edge.Node).ToID == oldToID {
				found = true
			}
		}
		assert.True(t, found, "an explicit since covering the old row's ledger_created_at must include it")
	})
}
