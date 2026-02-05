package resolvers

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/stellar/go-stellar-sdk/toid"

	"github.com/stellar/wallet-backend/internal/data"
	"github.com/stellar/wallet-backend/internal/indexer/types"
	"github.com/stellar/wallet-backend/internal/metrics"
	graphql1 "github.com/stellar/wallet-backend/internal/serve/graphql/generated"
)

func TestAccountResolver_Transactions(t *testing.T) {
	parentAccount := &types.Account{StellarAddress: types.AddressBytea(sharedTestAccountAddress)}

	mockMetricsService := &metrics.MockMetricsService{}
	mockMetricsService.On("IncDBQuery", "BatchGetByAccountAddress", "transactions").Return()
	mockMetricsService.On("ObserveDBQueryDuration", "BatchGetByAccountAddress", "transactions", mock.Anything).Return()

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
		assert.Equal(t, testTxHash1, transactions.Edges[0].Node.Hash.String())
		assert.Equal(t, testTxHash2, transactions.Edges[1].Node.Hash.String())
		assert.Equal(t, testTxHash3, transactions.Edges[2].Node.Hash.String())
		assert.Equal(t, testTxHash4, transactions.Edges[3].Node.Hash.String())
		mockMetricsService.AssertExpectations(t)
	})

	t.Run("get transactions with first/after limit and cursor", func(t *testing.T) {
		ctx := getTestCtx("transactions", []string{"hash"})
		first := int32(2)
		txs, err := resolver.Transactions(ctx, parentAccount, &first, nil, nil, nil)
		require.NoError(t, err)
		assert.Len(t, txs.Edges, 2)
		assert.Equal(t, testTxHash1, txs.Edges[0].Node.Hash.String())
		assert.Equal(t, testTxHash2, txs.Edges[1].Node.Hash.String())
		assert.True(t, txs.PageInfo.HasNextPage)
		assert.False(t, txs.PageInfo.HasPreviousPage)

		// Get the next cursor
		nextCursor := txs.PageInfo.EndCursor
		assert.NotNil(t, nextCursor)
		txs, err = resolver.Transactions(ctx, parentAccount, &first, nextCursor, nil, nil)
		require.NoError(t, err)
		assert.Len(t, txs.Edges, 2)
		assert.Equal(t, testTxHash3, txs.Edges[0].Node.Hash.String())
		assert.Equal(t, testTxHash4, txs.Edges[1].Node.Hash.String())
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
		assert.Equal(t, testTxHash3, txs.Edges[0].Node.Hash.String())
		assert.Equal(t, testTxHash4, txs.Edges[1].Node.Hash.String())
		assert.False(t, txs.PageInfo.HasNextPage)
		assert.True(t, txs.PageInfo.HasPreviousPage)

		// Get the next cursor
		last = int32(1)
		nextCursor := txs.PageInfo.EndCursor
		assert.NotNil(t, nextCursor)
		txs, err = resolver.Transactions(ctx, parentAccount, nil, nil, &last, nextCursor)
		require.NoError(t, err)
		assert.Len(t, txs.Edges, 1)
		assert.Equal(t, testTxHash2, txs.Edges[0].Node.Hash.String())
		assert.True(t, txs.PageInfo.HasNextPage)
		assert.True(t, txs.PageInfo.HasPreviousPage)

		nextCursor = txs.PageInfo.EndCursor
		assert.NotNil(t, nextCursor)
		last = int32(10)
		txs, err = resolver.Transactions(ctx, parentAccount, nil, nil, &last, nextCursor)
		require.NoError(t, err)
		assert.Len(t, txs.Edges, 1)
		assert.Equal(t, testTxHash1, txs.Edges[0].Node.Hash.String())
		assert.True(t, txs.PageInfo.HasNextPage)
		assert.False(t, txs.PageInfo.HasPreviousPage)
		mockMetricsService.AssertExpectations(t)
	})

	t.Run("account with no transactions", func(t *testing.T) {
		nonExistentAccount := &types.Account{StellarAddress: types.AddressBytea(sharedNonExistentAccountAddress)}
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
	parentAccount := &types.Account{StellarAddress: types.AddressBytea(sharedTestAccountAddress)}

	mockMetricsService := &metrics.MockMetricsService{}
	mockMetricsService.On("IncDBQuery", "BatchGetByAccountAddress", "operations").Return()
	mockMetricsService.On("ObserveDBQueryDuration", "BatchGetByAccountAddress", "operations", mock.Anything).Return()
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
		ctx := getTestCtx("operations", []string{"operation_xdr"})
		operations, err := resolver.Operations(ctx, parentAccount, nil, nil, nil, nil)

		require.NoError(t, err)
		require.Len(t, operations.Edges, 8)
		assert.Equal(t, "opxdr1", operations.Edges[0].Node.OperationXDR)
		assert.Equal(t, "opxdr2", operations.Edges[1].Node.OperationXDR)
		assert.Equal(t, "opxdr3", operations.Edges[2].Node.OperationXDR)
		assert.Equal(t, "opxdr4", operations.Edges[3].Node.OperationXDR)
	})

	t.Run("get operations with first/after limit and cursor", func(t *testing.T) {
		ctx := getTestCtx("operations", []string{"operation_xdr"})
		first := int32(2)
		ops, err := resolver.Operations(ctx, parentAccount, &first, nil, nil, nil)
		require.NoError(t, err)
		assert.Len(t, ops.Edges, 2)
		assert.Equal(t, "opxdr1", ops.Edges[0].Node.OperationXDR)
		assert.Equal(t, "opxdr2", ops.Edges[1].Node.OperationXDR)
		assert.True(t, ops.PageInfo.HasNextPage)
		assert.False(t, ops.PageInfo.HasPreviousPage)

		// Get the next cursor
		nextCursor := ops.PageInfo.EndCursor
		assert.NotNil(t, nextCursor)
		ops, err = resolver.Operations(ctx, parentAccount, &first, nextCursor, nil, nil)
		require.NoError(t, err)
		assert.Len(t, ops.Edges, 2)
		assert.Equal(t, "opxdr3", ops.Edges[0].Node.OperationXDR)
		assert.Equal(t, "opxdr4", ops.Edges[1].Node.OperationXDR)
		assert.True(t, ops.PageInfo.HasNextPage)
		assert.True(t, ops.PageInfo.HasPreviousPage)

		first = int32(10)
		nextCursor = ops.PageInfo.EndCursor
		assert.NotNil(t, nextCursor)
		ops, err = resolver.Operations(ctx, parentAccount, &first, nextCursor, nil, nil)
		require.NoError(t, err)
		assert.Len(t, ops.Edges, 4)
		assert.Equal(t, "opxdr5", ops.Edges[0].Node.OperationXDR)
		assert.Equal(t, "opxdr6", ops.Edges[1].Node.OperationXDR)
		assert.Equal(t, "opxdr7", ops.Edges[2].Node.OperationXDR)
		assert.Equal(t, "opxdr8", ops.Edges[3].Node.OperationXDR)
		assert.False(t, ops.PageInfo.HasNextPage)
		assert.True(t, ops.PageInfo.HasPreviousPage)
	})

	t.Run("get operations with last/before limit and cursor", func(t *testing.T) {
		ctx := getTestCtx("operations", []string{"operation_xdr"})
		last := int32(2)
		ops, err := resolver.Operations(ctx, parentAccount, nil, nil, &last, nil)
		require.NoError(t, err)
		assert.Len(t, ops.Edges, 2)
		assert.Equal(t, "opxdr7", ops.Edges[0].Node.OperationXDR)
		assert.Equal(t, "opxdr8", ops.Edges[1].Node.OperationXDR)
		assert.True(t, ops.PageInfo.HasPreviousPage)
		assert.False(t, ops.PageInfo.HasNextPage)

		// Get the next cursor
		nextCursor := ops.PageInfo.EndCursor
		assert.NotNil(t, nextCursor)
		ops, err = resolver.Operations(ctx, parentAccount, nil, nil, &last, nextCursor)
		require.NoError(t, err)
		assert.Len(t, ops.Edges, 2)
		assert.Equal(t, "opxdr5", ops.Edges[0].Node.OperationXDR)
		assert.Equal(t, "opxdr6", ops.Edges[1].Node.OperationXDR)
		assert.True(t, ops.PageInfo.HasNextPage)
		assert.True(t, ops.PageInfo.HasPreviousPage)

		nextCursor = ops.PageInfo.EndCursor
		assert.NotNil(t, nextCursor)
		last = int32(10)
		ops, err = resolver.Operations(ctx, parentAccount, nil, nil, &last, nextCursor)
		require.NoError(t, err)
		assert.Len(t, ops.Edges, 4)
		assert.Equal(t, "opxdr1", ops.Edges[0].Node.OperationXDR)
		assert.Equal(t, "opxdr2", ops.Edges[1].Node.OperationXDR)
		assert.Equal(t, "opxdr3", ops.Edges[2].Node.OperationXDR)
		assert.Equal(t, "opxdr4", ops.Edges[3].Node.OperationXDR)
		assert.True(t, ops.PageInfo.HasNextPage)
		assert.False(t, ops.PageInfo.HasPreviousPage)
	})

	t.Run("account with no operations", func(t *testing.T) {
		nonExistentAccount := &types.Account{StellarAddress: types.AddressBytea(sharedNonExistentAccountAddress)}
		ctx := getTestCtx("operations", []string{"id"})
		operations, err := resolver.Operations(ctx, nonExistentAccount, nil, nil, nil, nil)

		require.NoError(t, err)
		assert.Empty(t, operations.Edges)
	})
}

func TestAccountResolver_StateChanges(t *testing.T) {
	parentAccount := &types.Account{StellarAddress: types.AddressBytea(sharedTestAccountAddress)}

	mockMetricsService := &metrics.MockMetricsService{}
	mockMetricsService.On("IncDBQuery", "BatchGetByAccountAddress", "state_changes").Return()
	mockMetricsService.On("ObserveDBQueryDuration", "BatchGetByAccountAddress", "state_changes", mock.Anything).Return()
	defer mockMetricsService.AssertExpectations(t)

	resolver := &accountResolver{&Resolver{
		models: &data.Models{
			StateChanges: &data.StateChangeModel{
				DB:             testDBConnectionPool,
				MetricsService: mockMetricsService,
			},
		},
	}}

	t.Run("get all state changes", func(t *testing.T) {
		ctx := getTestCtx("state_changes", []string{""})
		stateChanges, err := resolver.StateChanges(ctx, parentAccount, nil, nil, nil, nil, nil)

		require.NoError(t, err)
		require.Len(t, stateChanges.Edges, 20)

		// State changes ordered by (to_id, operation_id, state_change_order)
		tx1ToID := toid.New(1000, 1, 0).ToInt64()
		op1ID := toid.New(1000, 1, 1).ToInt64()
		op2ID := toid.New(1000, 1, 2).ToInt64()

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

		// Edge 3: tx1 op2 first state change
		cursor = extractStateChangeIDs(stateChanges.Edges[3].Node)
		assert.Equal(t, tx1ToID, cursor.ToID)
		assert.Equal(t, op2ID, cursor.OperationID)
		assert.Equal(t, int64(1), cursor.StateChangeOrder)
	})

	t.Run("get state changes with first/after limit and cursor", func(t *testing.T) {
		ctx := getTestCtx("state_changes", []string{""})

		tx1ToID := toid.New(1000, 1, 0).ToInt64()
		tx1Op1ID := toid.New(1000, 1, 1).ToInt64()
		tx1Op2ID := toid.New(1000, 1, 2).ToInt64()
		tx2ToID := toid.New(1000, 2, 0).ToInt64()
		tx2Op1ID := toid.New(1000, 2, 1).ToInt64()
		tx2Op2ID := toid.New(1000, 2, 2).ToInt64()

		first := int32(3)
		stateChanges, err := resolver.StateChanges(ctx, parentAccount, nil, &first, nil, nil, nil)
		require.NoError(t, err)
		assert.Len(t, stateChanges.Edges, 3)

		// Page 1: tx1 fee, tx1 op1 sc1, tx1 op1 sc2
		cursor := extractStateChangeIDs(stateChanges.Edges[0].Node)
		assert.Equal(t, tx1ToID, cursor.ToID)
		assert.Equal(t, int64(0), cursor.OperationID)
		assert.Equal(t, int64(1), cursor.StateChangeOrder)

		cursor = extractStateChangeIDs(stateChanges.Edges[1].Node)
		assert.Equal(t, tx1ToID, cursor.ToID)
		assert.Equal(t, tx1Op1ID, cursor.OperationID)
		assert.Equal(t, int64(1), cursor.StateChangeOrder)

		cursor = extractStateChangeIDs(stateChanges.Edges[2].Node)
		assert.Equal(t, tx1ToID, cursor.ToID)
		assert.Equal(t, tx1Op1ID, cursor.OperationID)
		assert.Equal(t, int64(2), cursor.StateChangeOrder)

		assert.True(t, stateChanges.PageInfo.HasNextPage)
		assert.False(t, stateChanges.PageInfo.HasPreviousPage)

		// Get the next cursor
		nextCursor := stateChanges.PageInfo.EndCursor
		assert.NotNil(t, nextCursor)
		stateChanges, err = resolver.StateChanges(ctx, parentAccount, nil, &first, nextCursor, nil, nil)
		require.NoError(t, err)
		assert.Len(t, stateChanges.Edges, 3)

		// Page 2: tx1 op2 sc1, tx1 op2 sc2, tx2 fee
		cursor = extractStateChangeIDs(stateChanges.Edges[0].Node)
		assert.Equal(t, tx1ToID, cursor.ToID)
		assert.Equal(t, tx1Op2ID, cursor.OperationID)
		assert.Equal(t, int64(1), cursor.StateChangeOrder)

		cursor = extractStateChangeIDs(stateChanges.Edges[1].Node)
		assert.Equal(t, tx1ToID, cursor.ToID)
		assert.Equal(t, tx1Op2ID, cursor.OperationID)
		assert.Equal(t, int64(2), cursor.StateChangeOrder)

		cursor = extractStateChangeIDs(stateChanges.Edges[2].Node)
		assert.Equal(t, tx2ToID, cursor.ToID)
		assert.Equal(t, int64(0), cursor.OperationID) // Fee state change
		assert.Equal(t, int64(1), cursor.StateChangeOrder)

		assert.True(t, stateChanges.PageInfo.HasNextPage)
		assert.True(t, stateChanges.PageInfo.HasPreviousPage)

		// Get the next cursor
		first = int32(100)
		nextCursor = stateChanges.PageInfo.EndCursor
		assert.NotNil(t, nextCursor)
		stateChanges, err = resolver.StateChanges(ctx, parentAccount, nil, &first, nextCursor, nil, nil)
		require.NoError(t, err)
		assert.Len(t, stateChanges.Edges, 14)

		// Page 3: tx2 op1 sc1, tx2 op1 sc2, tx2 op2 sc1, tx2 op2 sc2, ...
		cursor = extractStateChangeIDs(stateChanges.Edges[0].Node)
		assert.Equal(t, tx2ToID, cursor.ToID)
		assert.Equal(t, tx2Op1ID, cursor.OperationID)
		assert.Equal(t, int64(1), cursor.StateChangeOrder)

		cursor = extractStateChangeIDs(stateChanges.Edges[1].Node)
		assert.Equal(t, tx2ToID, cursor.ToID)
		assert.Equal(t, tx2Op1ID, cursor.OperationID)
		assert.Equal(t, int64(2), cursor.StateChangeOrder)

		cursor = extractStateChangeIDs(stateChanges.Edges[2].Node)
		assert.Equal(t, tx2ToID, cursor.ToID)
		assert.Equal(t, tx2Op2ID, cursor.OperationID)
		assert.Equal(t, int64(1), cursor.StateChangeOrder)

		cursor = extractStateChangeIDs(stateChanges.Edges[3].Node)
		assert.Equal(t, tx2ToID, cursor.ToID)
		assert.Equal(t, tx2Op2ID, cursor.OperationID)
		assert.Equal(t, int64(2), cursor.StateChangeOrder)

		assert.False(t, stateChanges.PageInfo.HasNextPage)
		assert.True(t, stateChanges.PageInfo.HasPreviousPage)
	})

	t.Run("get state changes with last/before limit and cursor", func(t *testing.T) {
		ctx := getTestCtx("state_changes", []string{""})

		tx1ToID := toid.New(1000, 1, 0).ToInt64()
		tx1Op1ID := toid.New(1000, 1, 1).ToInt64()
		tx3Op2ID := toid.New(1000, 3, 2).ToInt64()
		tx3ToID := toid.New(1000, 3, 0).ToInt64()
		tx4ToID := toid.New(1000, 4, 0).ToInt64()
		tx4Op1ID := toid.New(1000, 4, 1).ToInt64()
		tx4Op2ID := toid.New(1000, 4, 2).ToInt64()

		last := int32(3)
		stateChanges, err := resolver.StateChanges(ctx, parentAccount, nil, nil, nil, &last, nil)
		require.NoError(t, err)
		assert.Len(t, stateChanges.Edges, 3)

		// Last 3: tx4 op1 sc2, tx4 op2 sc1, tx4 op2 sc2
		cursor := extractStateChangeIDs(stateChanges.Edges[0].Node)
		assert.Equal(t, tx4ToID, cursor.ToID)
		assert.Equal(t, tx4Op1ID, cursor.OperationID)
		assert.Equal(t, int64(2), cursor.StateChangeOrder)

		cursor = extractStateChangeIDs(stateChanges.Edges[1].Node)
		assert.Equal(t, tx4ToID, cursor.ToID)
		assert.Equal(t, tx4Op2ID, cursor.OperationID)
		assert.Equal(t, int64(1), cursor.StateChangeOrder)

		cursor = extractStateChangeIDs(stateChanges.Edges[2].Node)
		assert.Equal(t, tx4ToID, cursor.ToID)
		assert.Equal(t, tx4Op2ID, cursor.OperationID)
		assert.Equal(t, int64(2), cursor.StateChangeOrder)

		assert.True(t, stateChanges.PageInfo.HasPreviousPage)
		assert.False(t, stateChanges.PageInfo.HasNextPage)

		// Get the next cursor (going backward)
		nextCursor := stateChanges.PageInfo.EndCursor
		assert.NotNil(t, nextCursor)
		stateChanges, err = resolver.StateChanges(ctx, parentAccount, nil, nil, nil, &last, nextCursor)
		require.NoError(t, err)
		assert.Len(t, stateChanges.Edges, 3)

		// Previous 3: tx3 op2 sc2, tx4 fee, tx4 op1 sc1
		cursor = extractStateChangeIDs(stateChanges.Edges[0].Node)
		assert.Equal(t, tx3ToID, cursor.ToID)
		assert.Equal(t, tx3Op2ID, cursor.OperationID)
		assert.Equal(t, int64(2), cursor.StateChangeOrder)

		cursor = extractStateChangeIDs(stateChanges.Edges[1].Node)
		assert.Equal(t, tx4ToID, cursor.ToID)
		assert.Equal(t, int64(0), cursor.OperationID) // Fee state change
		assert.Equal(t, int64(1), cursor.StateChangeOrder)

		cursor = extractStateChangeIDs(stateChanges.Edges[2].Node)
		assert.Equal(t, tx4ToID, cursor.ToID)
		assert.Equal(t, tx4Op1ID, cursor.OperationID)
		assert.Equal(t, int64(1), cursor.StateChangeOrder)

		assert.True(t, stateChanges.PageInfo.HasNextPage)
		assert.True(t, stateChanges.PageInfo.HasPreviousPage)

		nextCursor = stateChanges.PageInfo.EndCursor
		assert.NotNil(t, nextCursor)
		last = int32(100)
		stateChanges, err = resolver.StateChanges(ctx, parentAccount, nil, nil, nil, &last, nextCursor)
		require.NoError(t, err)
		assert.Len(t, stateChanges.Edges, 14)

		// First page: tx1 fee, tx1 op1 sc1, tx1 op1 sc2, ...
		cursor = extractStateChangeIDs(stateChanges.Edges[0].Node)
		assert.Equal(t, tx1ToID, cursor.ToID)
		assert.Equal(t, int64(0), cursor.OperationID)
		assert.Equal(t, int64(1), cursor.StateChangeOrder)

		cursor = extractStateChangeIDs(stateChanges.Edges[1].Node)
		assert.Equal(t, tx1ToID, cursor.ToID)
		assert.Equal(t, tx1Op1ID, cursor.OperationID)
		assert.Equal(t, int64(1), cursor.StateChangeOrder)

		cursor = extractStateChangeIDs(stateChanges.Edges[2].Node)
		assert.Equal(t, tx1ToID, cursor.ToID)
		assert.Equal(t, tx1Op1ID, cursor.OperationID)
		assert.Equal(t, int64(2), cursor.StateChangeOrder)

		assert.True(t, stateChanges.PageInfo.HasNextPage)
		assert.False(t, stateChanges.PageInfo.HasPreviousPage)
	})

	t.Run("account with no state changes", func(t *testing.T) {
		nonExistentAccount := &types.Account{StellarAddress: types.AddressBytea(sharedNonExistentAccountAddress)}
		ctx := getTestCtx("state_changes", []string{"to_id", "state_change_order"})
		stateChanges, err := resolver.StateChanges(ctx, nonExistentAccount, nil, nil, nil, nil, nil)

		require.NoError(t, err)
		assert.Empty(t, stateChanges.Edges)
	})
}

func TestAccountResolver_StateChanges_WithFilters(t *testing.T) {
	parentAccount := &types.Account{StellarAddress: types.AddressBytea(sharedTestAccountAddress)}

	mockMetricsService := &metrics.MockMetricsService{}
	mockMetricsService.On("IncDBQuery", "BatchGetByAccountAddress", "state_changes").Return()
	mockMetricsService.On("ObserveDBQueryDuration", "BatchGetByAccountAddress", "state_changes", mock.Anything).Return()
	defer mockMetricsService.AssertExpectations(t)

	resolver := &accountResolver{&Resolver{
		models: &data.Models{
			StateChanges: &data.StateChangeModel{
				DB:             testDBConnectionPool,
				MetricsService: mockMetricsService,
			},
		},
	}}

	t.Run("filter by transaction hash only", func(t *testing.T) {
		ctx := getTestCtx("state_changes", []string{""})
		txHash := testTxHash1
		filter := &graphql1.AccountStateChangeFilterInput{
			TransactionHash: &txHash,
		}
		stateChanges, err := resolver.StateChanges(ctx, parentAccount, filter, nil, nil, nil, nil)

		require.NoError(t, err)
		// tx1 has 3 operations (0, 1, 2), each operation has 2 state changes except op 0 (1 state change)
		// Total: 1 + 2 + 2 = 5 state changes
		require.Len(t, stateChanges.Edges, 5)

		tx1ToID := toid.New(1000, 1, 0).ToInt64()
		op1ID := toid.New(1000, 1, 1).ToInt64()
		op2ID := toid.New(1000, 1, 2).ToInt64()

		// Edge 0: Fee state change (no operation)
		cursor := extractStateChangeIDs(stateChanges.Edges[0].Node)
		assert.Equal(t, tx1ToID, cursor.ToID)
		assert.Equal(t, int64(0), cursor.OperationID)
		assert.Equal(t, int64(1), cursor.StateChangeOrder)

		// Edge 1: Operation 1's first state change
		cursor = extractStateChangeIDs(stateChanges.Edges[1].Node)
		assert.Equal(t, tx1ToID, cursor.ToID)
		assert.Equal(t, op1ID, cursor.OperationID)
		assert.Equal(t, int64(1), cursor.StateChangeOrder)

		// Edge 2: Operation 1's second state change
		cursor = extractStateChangeIDs(stateChanges.Edges[2].Node)
		assert.Equal(t, tx1ToID, cursor.ToID)
		assert.Equal(t, op1ID, cursor.OperationID)
		assert.Equal(t, int64(2), cursor.StateChangeOrder)

		// Edge 3: Operation 2's first state change
		cursor = extractStateChangeIDs(stateChanges.Edges[3].Node)
		assert.Equal(t, tx1ToID, cursor.ToID)
		assert.Equal(t, op2ID, cursor.OperationID)
		assert.Equal(t, int64(1), cursor.StateChangeOrder)

		// Edge 4: Operation 2's second state change
		cursor = extractStateChangeIDs(stateChanges.Edges[4].Node)
		assert.Equal(t, tx1ToID, cursor.ToID)
		assert.Equal(t, op2ID, cursor.OperationID)
		assert.Equal(t, int64(2), cursor.StateChangeOrder)
	})

	t.Run("filter by operation ID only", func(t *testing.T) {
		ctx := getTestCtx("state_changes", []string{""})
		opID := toid.New(1000, 1, 1).ToInt64()
		txToID := opID &^ 0xFFF // Derive transaction to_id from operation_id
		filter := &graphql1.AccountStateChangeFilterInput{
			OperationID: &opID,
		}
		stateChanges, err := resolver.StateChanges(ctx, parentAccount, filter, nil, nil, nil, nil)

		require.NoError(t, err)
		// Operation 1 has 2 state changes
		require.Len(t, stateChanges.Edges, 2)

		// Verify both state changes are from the specified operation
		cursor := extractStateChangeIDs(stateChanges.Edges[0].Node)
		assert.Equal(t, txToID, cursor.ToID)
		assert.Equal(t, opID, cursor.OperationID)
		assert.Equal(t, int64(1), cursor.StateChangeOrder)

		cursor = extractStateChangeIDs(stateChanges.Edges[1].Node)
		assert.Equal(t, txToID, cursor.ToID)
		assert.Equal(t, opID, cursor.OperationID)
		assert.Equal(t, int64(2), cursor.StateChangeOrder)
	})

	t.Run("filter by both transaction hash and operation ID", func(t *testing.T) {
		ctx := getTestCtx("state_changes", []string{""})
		txHash := testTxHash2
		opID := toid.New(1000, 2, 1).ToInt64()
		txToID := opID &^ 0xFFF // Derive transaction to_id from operation_id
		filter := &graphql1.AccountStateChangeFilterInput{
			TransactionHash: &txHash,
			OperationID:     &opID,
		}
		stateChanges, err := resolver.StateChanges(ctx, parentAccount, filter, nil, nil, nil, nil)

		require.NoError(t, err)
		// Only state changes that match both filters
		require.Len(t, stateChanges.Edges, 2)

		// Verify both state changes match both filters
		cursor := extractStateChangeIDs(stateChanges.Edges[0].Node)
		assert.Equal(t, txToID, cursor.ToID)
		assert.Equal(t, opID, cursor.OperationID)
		assert.Equal(t, int64(1), cursor.StateChangeOrder)

		cursor = extractStateChangeIDs(stateChanges.Edges[1].Node)
		assert.Equal(t, txToID, cursor.ToID)
		assert.Equal(t, opID, cursor.OperationID)
		assert.Equal(t, int64(2), cursor.StateChangeOrder)
	})

	t.Run("filter with no matching results", func(t *testing.T) {
		ctx := getTestCtx("state_changes", []string{""})
		// Use a valid 64-char hex hash that doesn't exist in the test DB
		txHash := "0000000000000000000000000000000000000000000000000000000000000000"
		filter := &graphql1.AccountStateChangeFilterInput{
			TransactionHash: &txHash,
		}
		stateChanges, err := resolver.StateChanges(ctx, parentAccount, filter, nil, nil, nil, nil)

		require.NoError(t, err)
		require.Empty(t, stateChanges.Edges)
		assert.False(t, stateChanges.PageInfo.HasNextPage)
		assert.False(t, stateChanges.PageInfo.HasPreviousPage)
	})

	t.Run("filter with pagination", func(t *testing.T) {
		ctx := getTestCtx("state_changes", []string{""})
		txHash := testTxHash1
		filter := &graphql1.AccountStateChangeFilterInput{
			TransactionHash: &txHash,
		}

		tx1ToID := toid.New(1000, 1, 0).ToInt64()
		op1ID := toid.New(1000, 1, 1).ToInt64()
		op2ID := toid.New(1000, 1, 2).ToInt64()

		// Get first 2 state changes
		first := int32(2)
		stateChanges, err := resolver.StateChanges(ctx, parentAccount, filter, &first, nil, nil, nil)
		require.NoError(t, err)
		require.Len(t, stateChanges.Edges, 2)

		// Page 1: Fee change and Op 1 first state change
		cursor := extractStateChangeIDs(stateChanges.Edges[0].Node)
		assert.Equal(t, tx1ToID, cursor.ToID)
		assert.Equal(t, int64(0), cursor.OperationID)
		assert.Equal(t, int64(1), cursor.StateChangeOrder)

		cursor = extractStateChangeIDs(stateChanges.Edges[1].Node)
		assert.Equal(t, tx1ToID, cursor.ToID)
		assert.Equal(t, op1ID, cursor.OperationID)
		assert.Equal(t, int64(1), cursor.StateChangeOrder)

		assert.True(t, stateChanges.PageInfo.HasNextPage)
		assert.False(t, stateChanges.PageInfo.HasPreviousPage)

		// Get next page
		nextCursor := stateChanges.PageInfo.EndCursor
		assert.NotNil(t, nextCursor)
		stateChanges, err = resolver.StateChanges(ctx, parentAccount, filter, &first, nextCursor, nil, nil)
		require.NoError(t, err)
		require.Len(t, stateChanges.Edges, 2)

		// Page 2: Op 1 second state change and Op 2 first state change
		cursor = extractStateChangeIDs(stateChanges.Edges[0].Node)
		assert.Equal(t, tx1ToID, cursor.ToID)
		assert.Equal(t, op1ID, cursor.OperationID)
		assert.Equal(t, int64(2), cursor.StateChangeOrder)

		cursor = extractStateChangeIDs(stateChanges.Edges[1].Node)
		assert.Equal(t, tx1ToID, cursor.ToID)
		assert.Equal(t, op2ID, cursor.OperationID)
		assert.Equal(t, int64(1), cursor.StateChangeOrder)

		assert.True(t, stateChanges.PageInfo.HasNextPage)
		assert.True(t, stateChanges.PageInfo.HasPreviousPage)

		// Get final page
		nextCursor = stateChanges.PageInfo.EndCursor
		assert.NotNil(t, nextCursor)
		stateChanges, err = resolver.StateChanges(ctx, parentAccount, filter, &first, nextCursor, nil, nil)
		require.NoError(t, err)
		require.Len(t, stateChanges.Edges, 1)

		// Page 3: Op 2 second state change
		cursor = extractStateChangeIDs(stateChanges.Edges[0].Node)
		assert.Equal(t, tx1ToID, cursor.ToID)
		assert.Equal(t, op2ID, cursor.OperationID)
		assert.Equal(t, int64(2), cursor.StateChangeOrder)

		assert.False(t, stateChanges.PageInfo.HasNextPage)
		assert.True(t, stateChanges.PageInfo.HasPreviousPage)
	})
}

func TestAccountResolver_StateChanges_WithCategoryReasonFilters(t *testing.T) {
	parentAccount := &types.Account{StellarAddress: types.AddressBytea(sharedTestAccountAddress)}

	mockMetricsService := &metrics.MockMetricsService{}
	mockMetricsService.On("IncDBQuery", "BatchGetByAccountAddress", "state_changes").Return()
	mockMetricsService.On("ObserveDBQueryDuration", "BatchGetByAccountAddress", "state_changes", mock.Anything).Return()
	defer mockMetricsService.AssertExpectations(t)

	resolver := &accountResolver{&Resolver{
		models: &data.Models{
			StateChanges: &data.StateChangeModel{
				DB:             testDBConnectionPool,
				MetricsService: mockMetricsService,
			},
		},
	}}

	t.Run("filter by category only", func(t *testing.T) {
		ctx := getTestCtx("state_changes", []string{""})
		category := "BALANCE"
		filter := &graphql1.AccountStateChangeFilterInput{
			Category: &category,
		}
		stateChanges, err := resolver.StateChanges(ctx, parentAccount, filter, nil, nil, nil, nil)
		require.NoError(t, err)
		// Verify all returned state changes are BALANCE category
		for _, sc := range stateChanges.Edges {
			assert.Equal(t, types.StateChangeCategoryBalance, sc.Node.GetType())
		}
	})

	t.Run("filter by reason only", func(t *testing.T) {
		ctx := getTestCtx("state_changes", []string{""})
		reason := "CREDIT"
		filter := &graphql1.AccountStateChangeFilterInput{
			Reason: &reason,
		}
		stateChanges, err := resolver.StateChanges(ctx, parentAccount, filter, nil, nil, nil, nil)
		require.NoError(t, err)

		// Verify all returned state changes are CREDIT reason
		for _, sc := range stateChanges.Edges {
			assert.Equal(t, types.StateChangeReasonCredit, sc.Node.GetReason())
		}
	})

	t.Run("filter by both category and reason", func(t *testing.T) {
		ctx := getTestCtx("state_changes", []string{""})
		category := "SIGNER"
		reason := "ADD"
		filter := &graphql1.AccountStateChangeFilterInput{
			Category: &category,
			Reason:   &reason,
		}
		stateChanges, err := resolver.StateChanges(ctx, parentAccount, filter, nil, nil, nil, nil)
		require.NoError(t, err)

		// Verify all returned state changes are SIGNER category and ADD reason
		for _, sc := range stateChanges.Edges {
			assert.Equal(t, types.StateChangeCategorySigner, sc.Node.GetType())
			assert.Equal(t, types.StateChangeReasonAdd, sc.Node.GetReason())
		}
	})

	t.Run("filter with all filters - txHash, operationID, category, reason", func(t *testing.T) {
		ctx := getTestCtx("state_changes", []string{""})
		txHash := testTxHash1
		opID := toid.New(1000, 1, 1).ToInt64()
		txToID := opID &^ 0xFFF // Derive transaction to_id from operation_id
		category := "BALANCE"
		reason := "CREDIT"
		filter := &graphql1.AccountStateChangeFilterInput{
			TransactionHash: &txHash,
			OperationID:     &opID,
			Category:        &category,
			Reason:          &reason,
		}
		stateChanges, err := resolver.StateChanges(ctx, parentAccount, filter, nil, nil, nil, nil)
		require.NoError(t, err)

		// Verify all returned state changes have correct IDs, category and reason
		for _, sc := range stateChanges.Edges {
			cursor := extractStateChangeIDs(sc.Node)
			assert.Equal(t, txToID, cursor.ToID)
			assert.Equal(t, opID, cursor.OperationID)
			assert.Equal(t, types.StateChangeCategoryBalance, sc.Node.GetType())
			assert.Equal(t, types.StateChangeReasonCredit, sc.Node.GetReason())
		}
	})

	t.Run("filter by category with pagination", func(t *testing.T) {
		ctx := getTestCtx("state_changes", []string{""})
		category := "BALANCE"
		filter := &graphql1.AccountStateChangeFilterInput{
			Category: &category,
		}

		// Get first 2 state changes
		first := int32(2)
		stateChanges, err := resolver.StateChanges(ctx, parentAccount, filter, &first, nil, nil, nil)
		require.NoError(t, err)
		require.Len(t, stateChanges.Edges, 2)
		assert.True(t, stateChanges.PageInfo.HasNextPage)
		assert.False(t, stateChanges.PageInfo.HasPreviousPage)
		for _, sc := range stateChanges.Edges {
			assert.Equal(t, types.StateChangeCategoryBalance, sc.Node.GetType())
		}

		// Get next page
		nextCursor := stateChanges.PageInfo.EndCursor
		assert.NotNil(t, nextCursor)
		stateChanges, err = resolver.StateChanges(ctx, parentAccount, filter, &first, nextCursor, nil, nil)
		require.NoError(t, err)
		assert.LessOrEqual(t, len(stateChanges.Edges), 2)
		for _, sc := range stateChanges.Edges {
			assert.Equal(t, types.StateChangeCategoryBalance, sc.Node.GetType())
		}
	})

	t.Run("filter with no matching results", func(t *testing.T) {
		ctx := getTestCtx("state_changes", []string{""})
		category := "NON_EXISTENT_CATEGORY"
		filter := &graphql1.AccountStateChangeFilterInput{
			Category: &category,
		}
		stateChanges, err := resolver.StateChanges(ctx, parentAccount, filter, nil, nil, nil, nil)

		require.NoError(t, err)
		require.Empty(t, stateChanges.Edges)
		assert.False(t, stateChanges.PageInfo.HasNextPage)
		assert.False(t, stateChanges.PageInfo.HasPreviousPage)
	})
}
