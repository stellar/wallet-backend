package resolvers

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/stellar/go/toid"

	"github.com/stellar/wallet-backend/internal/data"
	"github.com/stellar/wallet-backend/internal/indexer/types"
	"github.com/stellar/wallet-backend/internal/metrics"
	graphql1 "github.com/stellar/wallet-backend/internal/serve/graphql/generated"
)

func TestAccountResolver_Transactions(t *testing.T) {
	parentAccount := &types.Account{StellarAddress: "test-account"}

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
		assert.Equal(t, []byte("opxdr1"), operations.Edges[0].Node.OperationXDR)
		assert.Equal(t, []byte("opxdr2"), operations.Edges[1].Node.OperationXDR)
		assert.Equal(t, []byte("opxdr3"), operations.Edges[2].Node.OperationXDR)
		assert.Equal(t, []byte("opxdr4"), operations.Edges[3].Node.OperationXDR)
	})

	t.Run("get operations with first/after limit and cursor", func(t *testing.T) {
		ctx := getTestCtx("operations", []string{"operation_xdr"})
		first := int32(2)
		ops, err := resolver.Operations(ctx, parentAccount, &first, nil, nil, nil)
		require.NoError(t, err)
		assert.Len(t, ops.Edges, 2)
		assert.Equal(t, []byte("opxdr1"), ops.Edges[0].Node.OperationXDR)
		assert.Equal(t, []byte("opxdr2"), ops.Edges[1].Node.OperationXDR)
		assert.True(t, ops.PageInfo.HasNextPage)
		assert.False(t, ops.PageInfo.HasPreviousPage)

		// Get the next cursor
		nextCursor := ops.PageInfo.EndCursor
		assert.NotNil(t, nextCursor)
		ops, err = resolver.Operations(ctx, parentAccount, &first, nextCursor, nil, nil)
		require.NoError(t, err)
		assert.Len(t, ops.Edges, 2)
		assert.Equal(t, []byte("opxdr3"), ops.Edges[0].Node.OperationXDR)
		assert.Equal(t, []byte("opxdr4"), ops.Edges[1].Node.OperationXDR)
		assert.True(t, ops.PageInfo.HasNextPage)
		assert.True(t, ops.PageInfo.HasPreviousPage)

		first = int32(10)
		nextCursor = ops.PageInfo.EndCursor
		assert.NotNil(t, nextCursor)
		ops, err = resolver.Operations(ctx, parentAccount, &first, nextCursor, nil, nil)
		require.NoError(t, err)
		assert.Len(t, ops.Edges, 4)
		assert.Equal(t, []byte("opxdr5"), ops.Edges[0].Node.OperationXDR)
		assert.Equal(t, []byte("opxdr6"), ops.Edges[1].Node.OperationXDR)
		assert.Equal(t, []byte("opxdr7"), ops.Edges[2].Node.OperationXDR)
		assert.Equal(t, []byte("opxdr8"), ops.Edges[3].Node.OperationXDR)
		assert.False(t, ops.PageInfo.HasNextPage)
		assert.True(t, ops.PageInfo.HasPreviousPage)
	})

	t.Run("get operations with last/before limit and cursor", func(t *testing.T) {
		ctx := getTestCtx("operations", []string{"operation_xdr"})
		last := int32(2)
		ops, err := resolver.Operations(ctx, parentAccount, nil, nil, &last, nil)
		require.NoError(t, err)
		assert.Len(t, ops.Edges, 2)
		assert.Equal(t, []byte("opxdr7"), ops.Edges[0].Node.OperationXDR)
		assert.Equal(t, []byte("opxdr8"), ops.Edges[1].Node.OperationXDR)
		assert.True(t, ops.PageInfo.HasPreviousPage)
		assert.False(t, ops.PageInfo.HasNextPage)

		// Get the next cursor
		nextCursor := ops.PageInfo.EndCursor
		assert.NotNil(t, nextCursor)
		ops, err = resolver.Operations(ctx, parentAccount, nil, nil, &last, nextCursor)
		require.NoError(t, err)
		assert.Len(t, ops.Edges, 2)
		assert.Equal(t, []byte("opxdr5"), ops.Edges[0].Node.OperationXDR)
		assert.Equal(t, []byte("opxdr6"), ops.Edges[1].Node.OperationXDR)
		assert.True(t, ops.PageInfo.HasNextPage)
		assert.True(t, ops.PageInfo.HasPreviousPage)

		nextCursor = ops.PageInfo.EndCursor
		assert.NotNil(t, nextCursor)
		last = int32(10)
		ops, err = resolver.Operations(ctx, parentAccount, nil, nil, &last, nextCursor)
		require.NoError(t, err)
		assert.Len(t, ops.Edges, 4)
		assert.Equal(t, []byte("opxdr1"), ops.Edges[0].Node.OperationXDR)
		assert.Equal(t, []byte("opxdr2"), ops.Edges[1].Node.OperationXDR)
		assert.Equal(t, []byte("opxdr3"), ops.Edges[2].Node.OperationXDR)
		assert.Equal(t, []byte("opxdr4"), ops.Edges[3].Node.OperationXDR)
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
		// With 16 state changes ordered by ToID descending, check first few
		assert.Equal(t, fmt.Sprintf("%d:1", toid.New(1000, 1, 0).ToInt64()), fmt.Sprintf("%d:%d", extractStateChangeIDs(stateChanges.Edges[0].Node).ToID, extractStateChangeIDs(stateChanges.Edges[0].Node).StateChangeOrder))
		assert.Equal(t, fmt.Sprintf("%d:1", toid.New(1000, 1, 1).ToInt64()), fmt.Sprintf("%d:%d", extractStateChangeIDs(stateChanges.Edges[1].Node).ToID, extractStateChangeIDs(stateChanges.Edges[1].Node).StateChangeOrder))
		assert.Equal(t, fmt.Sprintf("%d:2", toid.New(1000, 1, 1).ToInt64()), fmt.Sprintf("%d:%d", extractStateChangeIDs(stateChanges.Edges[2].Node).ToID, extractStateChangeIDs(stateChanges.Edges[2].Node).StateChangeOrder))
		assert.Equal(t, fmt.Sprintf("%d:1", toid.New(1000, 1, 2).ToInt64()), fmt.Sprintf("%d:%d", extractStateChangeIDs(stateChanges.Edges[3].Node).ToID, extractStateChangeIDs(stateChanges.Edges[3].Node).StateChangeOrder))
	})

	t.Run("get state changes with first/after limit and cursor", func(t *testing.T) {
		ctx := getTestCtx("state_changes", []string{""})
		first := int32(3)
		stateChanges, err := resolver.StateChanges(ctx, parentAccount, nil, &first, nil, nil, nil)
		require.NoError(t, err)
		assert.Len(t, stateChanges.Edges, 3)
		assert.Equal(t, fmt.Sprintf("%d:1", toid.New(1000, 1, 0).ToInt64()), fmt.Sprintf("%d:%d", extractStateChangeIDs(stateChanges.Edges[0].Node).ToID, extractStateChangeIDs(stateChanges.Edges[0].Node).StateChangeOrder))
		assert.Equal(t, fmt.Sprintf("%d:1", toid.New(1000, 1, 1).ToInt64()), fmt.Sprintf("%d:%d", extractStateChangeIDs(stateChanges.Edges[1].Node).ToID, extractStateChangeIDs(stateChanges.Edges[1].Node).StateChangeOrder))
		assert.Equal(t, fmt.Sprintf("%d:2", toid.New(1000, 1, 1).ToInt64()), fmt.Sprintf("%d:%d", extractStateChangeIDs(stateChanges.Edges[2].Node).ToID, extractStateChangeIDs(stateChanges.Edges[2].Node).StateChangeOrder))
		assert.True(t, stateChanges.PageInfo.HasNextPage)
		assert.False(t, stateChanges.PageInfo.HasPreviousPage)

		// Get the next cursor
		nextCursor := stateChanges.PageInfo.EndCursor
		assert.NotNil(t, nextCursor)
		stateChanges, err = resolver.StateChanges(ctx, parentAccount, nil, &first, nextCursor, nil, nil)
		require.NoError(t, err)
		assert.Len(t, stateChanges.Edges, 3)
		// Fee state change with no operation
		assert.Equal(t, fmt.Sprintf("%d:1", toid.New(1000, 1, 2).ToInt64()), fmt.Sprintf("%d:%d", extractStateChangeIDs(stateChanges.Edges[0].Node).ToID, extractStateChangeIDs(stateChanges.Edges[0].Node).StateChangeOrder))
		assert.Equal(t, fmt.Sprintf("%d:2", toid.New(1000, 1, 2).ToInt64()), fmt.Sprintf("%d:%d", extractStateChangeIDs(stateChanges.Edges[1].Node).ToID, extractStateChangeIDs(stateChanges.Edges[1].Node).StateChangeOrder))
		assert.Equal(t, fmt.Sprintf("%d:1", toid.New(1000, 2, 0).ToInt64()), fmt.Sprintf("%d:%d", extractStateChangeIDs(stateChanges.Edges[2].Node).ToID, extractStateChangeIDs(stateChanges.Edges[2].Node).StateChangeOrder))
		assert.True(t, stateChanges.PageInfo.HasNextPage)
		assert.True(t, stateChanges.PageInfo.HasPreviousPage)

		// Get the next cursor
		first = int32(100)
		nextCursor = stateChanges.PageInfo.EndCursor
		assert.NotNil(t, nextCursor)
		stateChanges, err = resolver.StateChanges(ctx, parentAccount, nil, &first, nextCursor, nil, nil)
		require.NoError(t, err)
		assert.Len(t, stateChanges.Edges, 14)
		assert.Equal(t, fmt.Sprintf("%d:1", toid.New(1000, 2, 1).ToInt64()), fmt.Sprintf("%d:%d", extractStateChangeIDs(stateChanges.Edges[0].Node).ToID, extractStateChangeIDs(stateChanges.Edges[0].Node).StateChangeOrder))
		assert.Equal(t, fmt.Sprintf("%d:2", toid.New(1000, 2, 1).ToInt64()), fmt.Sprintf("%d:%d", extractStateChangeIDs(stateChanges.Edges[1].Node).ToID, extractStateChangeIDs(stateChanges.Edges[1].Node).StateChangeOrder))
		assert.Equal(t, fmt.Sprintf("%d:1", toid.New(1000, 2, 2).ToInt64()), fmt.Sprintf("%d:%d", extractStateChangeIDs(stateChanges.Edges[2].Node).ToID, extractStateChangeIDs(stateChanges.Edges[2].Node).StateChangeOrder))
		assert.Equal(t, fmt.Sprintf("%d:2", toid.New(1000, 2, 2).ToInt64()), fmt.Sprintf("%d:%d", extractStateChangeIDs(stateChanges.Edges[3].Node).ToID, extractStateChangeIDs(stateChanges.Edges[3].Node).StateChangeOrder))
		assert.False(t, stateChanges.PageInfo.HasNextPage)
		assert.True(t, stateChanges.PageInfo.HasPreviousPage)
	})

	t.Run("get state changes with last/before limit and cursor", func(t *testing.T) {
		ctx := getTestCtx("state_changes", []string{""})
		last := int32(3)
		stateChanges, err := resolver.StateChanges(ctx, parentAccount, nil, nil, nil, &last, nil)
		require.NoError(t, err)
		assert.Len(t, stateChanges.Edges, 3)
		assert.Equal(t, fmt.Sprintf("%d:2", toid.New(1000, 4, 1).ToInt64()), fmt.Sprintf("%d:%d", extractStateChangeIDs(stateChanges.Edges[0].Node).ToID, extractStateChangeIDs(stateChanges.Edges[0].Node).StateChangeOrder))
		assert.Equal(t, fmt.Sprintf("%d:1", toid.New(1000, 4, 2).ToInt64()), fmt.Sprintf("%d:%d", extractStateChangeIDs(stateChanges.Edges[1].Node).ToID, extractStateChangeIDs(stateChanges.Edges[1].Node).StateChangeOrder))
		assert.Equal(t, fmt.Sprintf("%d:2", toid.New(1000, 4, 2).ToInt64()), fmt.Sprintf("%d:%d", extractStateChangeIDs(stateChanges.Edges[2].Node).ToID, extractStateChangeIDs(stateChanges.Edges[2].Node).StateChangeOrder))
		assert.True(t, stateChanges.PageInfo.HasPreviousPage)
		assert.False(t, stateChanges.PageInfo.HasNextPage)

		// Get the next cursor
		nextCursor := stateChanges.PageInfo.EndCursor
		assert.NotNil(t, nextCursor)
		stateChanges, err = resolver.StateChanges(ctx, parentAccount, nil, nil, nil, &last, nextCursor)
		require.NoError(t, err)
		assert.Len(t, stateChanges.Edges, 3)
		assert.Equal(t, fmt.Sprintf("%d:2", toid.New(1000, 3, 2).ToInt64()), fmt.Sprintf("%d:%d", extractStateChangeIDs(stateChanges.Edges[0].Node).ToID, extractStateChangeIDs(stateChanges.Edges[0].Node).StateChangeOrder))
		assert.Equal(t, fmt.Sprintf("%d:1", toid.New(1000, 4, 0).ToInt64()), fmt.Sprintf("%d:%d", extractStateChangeIDs(stateChanges.Edges[1].Node).ToID, extractStateChangeIDs(stateChanges.Edges[1].Node).StateChangeOrder))
		assert.Equal(t, fmt.Sprintf("%d:1", toid.New(1000, 4, 1).ToInt64()), fmt.Sprintf("%d:%d", extractStateChangeIDs(stateChanges.Edges[2].Node).ToID, extractStateChangeIDs(stateChanges.Edges[2].Node).StateChangeOrder))
		assert.True(t, stateChanges.PageInfo.HasNextPage)
		assert.True(t, stateChanges.PageInfo.HasPreviousPage)

		nextCursor = stateChanges.PageInfo.EndCursor
		assert.NotNil(t, nextCursor)
		last = int32(100)
		stateChanges, err = resolver.StateChanges(ctx, parentAccount, nil, nil, nil, &last, nextCursor)
		require.NoError(t, err)
		assert.Len(t, stateChanges.Edges, 14)
		assert.Equal(t, fmt.Sprintf("%d:1", toid.New(1000, 1, 0).ToInt64()), fmt.Sprintf("%d:%d", extractStateChangeIDs(stateChanges.Edges[0].Node).ToID, extractStateChangeIDs(stateChanges.Edges[0].Node).StateChangeOrder))
		assert.Equal(t, fmt.Sprintf("%d:1", toid.New(1000, 1, 1).ToInt64()), fmt.Sprintf("%d:%d", extractStateChangeIDs(stateChanges.Edges[1].Node).ToID, extractStateChangeIDs(stateChanges.Edges[1].Node).StateChangeOrder))
		assert.Equal(t, fmt.Sprintf("%d:2", toid.New(1000, 1, 1).ToInt64()), fmt.Sprintf("%d:%d", extractStateChangeIDs(stateChanges.Edges[2].Node).ToID, extractStateChangeIDs(stateChanges.Edges[2].Node).StateChangeOrder))
		assert.True(t, stateChanges.PageInfo.HasNextPage)
		assert.False(t, stateChanges.PageInfo.HasPreviousPage)
	})

	t.Run("account with no state changes", func(t *testing.T) {
		nonExistentAccount := &types.Account{StellarAddress: "non-existent-account"}
		ctx := getTestCtx("state_changes", []string{"to_id", "state_change_order"})
		stateChanges, err := resolver.StateChanges(ctx, nonExistentAccount, nil, nil, nil, nil, nil)

		require.NoError(t, err)
		assert.Empty(t, stateChanges.Edges)
	})
}

func TestAccountResolver_StateChanges_WithFilters(t *testing.T) {
	parentAccount := &types.Account{StellarAddress: "test-account"}

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
		txHash := "tx1"
		filter := &graphql1.AccountStateChangeFilterInput{
			TransactionHash: &txHash,
		}
		stateChanges, err := resolver.StateChanges(ctx, parentAccount, filter, nil, nil, nil, nil)

		require.NoError(t, err)
		// tx1 has 3 operations (0, 1, 2), each operation has 2 state changes except op 0 (1 state change)
		// Total: 1 + 2 + 2 = 5 state changes
		require.Len(t, stateChanges.Edges, 5)

		// Verify all returned state changes are from tx1
		// First state change is from operation 0
		assert.Equal(t, fmt.Sprintf("%d:1", toid.New(1000, 1, 0).ToInt64()),
			fmt.Sprintf("%d:%d", extractStateChangeIDs(stateChanges.Edges[0].Node).ToID, extractStateChangeIDs(stateChanges.Edges[0].Node).StateChangeOrder))
		// Next state changes are from operation 1
		assert.Equal(t, fmt.Sprintf("%d:1", toid.New(1000, 1, 1).ToInt64()),
			fmt.Sprintf("%d:%d", extractStateChangeIDs(stateChanges.Edges[1].Node).ToID, extractStateChangeIDs(stateChanges.Edges[1].Node).StateChangeOrder))
		assert.Equal(t, fmt.Sprintf("%d:2", toid.New(1000, 1, 1).ToInt64()),
			fmt.Sprintf("%d:%d", extractStateChangeIDs(stateChanges.Edges[2].Node).ToID, extractStateChangeIDs(stateChanges.Edges[2].Node).StateChangeOrder))
		// Last state changes are from operation 2
		assert.Equal(t, fmt.Sprintf("%d:1", toid.New(1000, 1, 2).ToInt64()),
			fmt.Sprintf("%d:%d", extractStateChangeIDs(stateChanges.Edges[3].Node).ToID, extractStateChangeIDs(stateChanges.Edges[3].Node).StateChangeOrder))
		assert.Equal(t, fmt.Sprintf("%d:2", toid.New(1000, 1, 2).ToInt64()),
			fmt.Sprintf("%d:%d", extractStateChangeIDs(stateChanges.Edges[4].Node).ToID, extractStateChangeIDs(stateChanges.Edges[4].Node).StateChangeOrder))
	})

	t.Run("filter by operation ID only", func(t *testing.T) {
		ctx := getTestCtx("state_changes", []string{""})
		opID := toid.New(1000, 1, 1).ToInt64()
		filter := &graphql1.AccountStateChangeFilterInput{
			OperationID: &opID,
		}
		stateChanges, err := resolver.StateChanges(ctx, parentAccount, filter, nil, nil, nil, nil)

		require.NoError(t, err)
		// Operation 1 has 2 state changes
		require.Len(t, stateChanges.Edges, 2)

		// Verify both state changes are from the specified operation
		assert.Equal(t, fmt.Sprintf("%d:1", opID),
			fmt.Sprintf("%d:%d", extractStateChangeIDs(stateChanges.Edges[0].Node).ToID, extractStateChangeIDs(stateChanges.Edges[0].Node).StateChangeOrder))
		assert.Equal(t, fmt.Sprintf("%d:2", opID),
			fmt.Sprintf("%d:%d", extractStateChangeIDs(stateChanges.Edges[1].Node).ToID, extractStateChangeIDs(stateChanges.Edges[1].Node).StateChangeOrder))
	})

	t.Run("filter by both transaction hash and operation ID", func(t *testing.T) {
		ctx := getTestCtx("state_changes", []string{""})
		txHash := "tx2"
		opID := toid.New(1000, 2, 1).ToInt64()
		filter := &graphql1.AccountStateChangeFilterInput{
			TransactionHash: &txHash,
			OperationID:     &opID,
		}
		stateChanges, err := resolver.StateChanges(ctx, parentAccount, filter, nil, nil, nil, nil)

		require.NoError(t, err)
		// Only state changes that match both filters
		require.Len(t, stateChanges.Edges, 2)

		// Verify both state changes match both filters
		assert.Equal(t, fmt.Sprintf("%d:1", opID),
			fmt.Sprintf("%d:%d", extractStateChangeIDs(stateChanges.Edges[0].Node).ToID, extractStateChangeIDs(stateChanges.Edges[0].Node).StateChangeOrder))
		assert.Equal(t, fmt.Sprintf("%d:2", opID),
			fmt.Sprintf("%d:%d", extractStateChangeIDs(stateChanges.Edges[1].Node).ToID, extractStateChangeIDs(stateChanges.Edges[1].Node).StateChangeOrder))
	})

	t.Run("filter with no matching results", func(t *testing.T) {
		ctx := getTestCtx("state_changes", []string{""})
		txHash := "non-existent-tx"
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
		txHash := "tx1"
		filter := &graphql1.AccountStateChangeFilterInput{
			TransactionHash: &txHash,
		}

		// Get first 2 state changes
		first := int32(2)
		stateChanges, err := resolver.StateChanges(ctx, parentAccount, filter, &first, nil, nil, nil)
		require.NoError(t, err)
		require.Len(t, stateChanges.Edges, 2)
		assert.Equal(t, fmt.Sprintf("%d:1", toid.New(1000, 1, 0).ToInt64()),
			fmt.Sprintf("%d:%d", extractStateChangeIDs(stateChanges.Edges[0].Node).ToID, extractStateChangeIDs(stateChanges.Edges[0].Node).StateChangeOrder))
		assert.Equal(t, fmt.Sprintf("%d:1", toid.New(1000, 1, 1).ToInt64()),
			fmt.Sprintf("%d:%d", extractStateChangeIDs(stateChanges.Edges[1].Node).ToID, extractStateChangeIDs(stateChanges.Edges[1].Node).StateChangeOrder))
		assert.True(t, stateChanges.PageInfo.HasNextPage)
		assert.False(t, stateChanges.PageInfo.HasPreviousPage)

		// Get next page
		nextCursor := stateChanges.PageInfo.EndCursor
		assert.NotNil(t, nextCursor)
		stateChanges, err = resolver.StateChanges(ctx, parentAccount, filter, &first, nextCursor, nil, nil)
		require.NoError(t, err)
		require.Len(t, stateChanges.Edges, 2)
		assert.Equal(t, fmt.Sprintf("%d:2", toid.New(1000, 1, 1).ToInt64()),
			fmt.Sprintf("%d:%d", extractStateChangeIDs(stateChanges.Edges[0].Node).ToID, extractStateChangeIDs(stateChanges.Edges[0].Node).StateChangeOrder))
		assert.Equal(t, fmt.Sprintf("%d:1", toid.New(1000, 1, 2).ToInt64()),
			fmt.Sprintf("%d:%d", extractStateChangeIDs(stateChanges.Edges[1].Node).ToID, extractStateChangeIDs(stateChanges.Edges[1].Node).StateChangeOrder))
		assert.True(t, stateChanges.PageInfo.HasNextPage)
		assert.True(t, stateChanges.PageInfo.HasPreviousPage)

		// Get final page
		nextCursor = stateChanges.PageInfo.EndCursor
		assert.NotNil(t, nextCursor)
		stateChanges, err = resolver.StateChanges(ctx, parentAccount, filter, &first, nextCursor, nil, nil)
		require.NoError(t, err)
		require.Len(t, stateChanges.Edges, 1)
		assert.Equal(t, fmt.Sprintf("%d:2", toid.New(1000, 1, 2).ToInt64()),
			fmt.Sprintf("%d:%d", extractStateChangeIDs(stateChanges.Edges[0].Node).ToID, extractStateChangeIDs(stateChanges.Edges[0].Node).StateChangeOrder))
		assert.False(t, stateChanges.PageInfo.HasNextPage)
		assert.True(t, stateChanges.PageInfo.HasPreviousPage)
	})
}

func TestAccountResolver_StateChanges_WithCategoryReasonFilters(t *testing.T) {
	parentAccount := &types.Account{StellarAddress: "test-account"}

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
		txHash := "tx1"
		opID := toid.New(1000, 1, 1).ToInt64()
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

		// Verify all returned state changes are BALANCE category and CREDIT reason
		for _, sc := range stateChanges.Edges {
			assert.Equal(t, opID, extractStateChangeIDs(sc.Node).ToID)
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
