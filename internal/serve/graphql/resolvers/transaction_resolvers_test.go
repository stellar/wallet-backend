package resolvers

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/stellar/go-stellar-sdk/toid"

	"github.com/stellar/wallet-backend/internal/data"
	"github.com/stellar/wallet-backend/internal/indexer/types"
	"github.com/stellar/wallet-backend/internal/metrics"
	"github.com/stellar/wallet-backend/internal/serve/graphql/dataloaders"
	"github.com/stellar/wallet-backend/internal/serve/middleware"
)

func TestTransactionResolver_Operations(t *testing.T) {
	mockMetricsService := &metrics.MockMetricsService{}
	mockMetricsService.On("IncDBQuery", "BatchGetByToID", "operations").Return()
	mockMetricsService.On("ObserveDBQueryDuration", "BatchGetByToID", "operations", mock.Anything).Return()
	defer mockMetricsService.AssertExpectations(t)

	resolver := &transactionResolver{&Resolver{
		models: &data.Models{
			Operations: &data.OperationModel{
				DB:             testDBConnectionPool,
				MetricsService: mockMetricsService,
			},
		},
	}}
	// ToID=toid.New(1000, 1, 0) matches the test data setup in test_utils.go (testLedger=1000, i=0)
	parentTx := &types.Transaction{Hash: "tx1", ToID: toid.New(1000, 1, 0).ToInt64()}

	t.Run("success", func(t *testing.T) {
		loaders := dataloaders.NewDataloaders(resolver.models)
		ctx := context.WithValue(getTestCtx("operations", []string{"operation_xdr"}), middleware.LoadersKey, loaders)

		operations, err := resolver.Operations(ctx, parentTx, nil, nil, nil, nil)

		require.NoError(t, err)
		require.Len(t, operations.Edges, 2)
		assert.Equal(t, "opxdr1", operations.Edges[0].Node.OperationXDR)
		assert.Equal(t, "opxdr2", operations.Edges[1].Node.OperationXDR)
	})

	t.Run("nil transaction panics", func(t *testing.T) {
		loaders := dataloaders.NewDataloaders(resolver.models)
		ctx := context.WithValue(getTestCtx("operations", []string{"id"}), middleware.LoadersKey, loaders)

		assert.Panics(t, func() {
			_, _ = resolver.Operations(ctx, nil, nil, nil, nil, nil) //nolint:errcheck
		})
	})

	t.Run("transaction with no operations", func(t *testing.T) {
		nonExistentTx := &types.Transaction{Hash: "non-existent-tx", ToID: 999999}
		loaders := dataloaders.NewDataloaders(resolver.models)
		ctx := context.WithValue(getTestCtx("operations", []string{"id"}), middleware.LoadersKey, loaders)

		operations, err := resolver.Operations(ctx, nonExistentTx, nil, nil, nil, nil)

		require.NoError(t, err)
		assert.Empty(t, operations.Edges)
	})

	t.Run("get operations with first/after pagination", func(t *testing.T) {
		loaders := dataloaders.NewDataloaders(resolver.models)
		ctx := context.WithValue(getTestCtx("operations", []string{"operation_xdr"}), middleware.LoadersKey, loaders)
		first := int32(1)
		ops, err := resolver.Operations(ctx, parentTx, &first, nil, nil, nil)
		require.NoError(t, err)
		assert.Len(t, ops.Edges, 1)
		assert.Equal(t, "opxdr1", ops.Edges[0].Node.OperationXDR)
		assert.True(t, ops.PageInfo.HasNextPage)
		assert.False(t, ops.PageInfo.HasPreviousPage)

		// Get the next page using cursor
		nextCursor := ops.PageInfo.EndCursor
		assert.NotNil(t, nextCursor)
		ops, err = resolver.Operations(ctx, parentTx, &first, nextCursor, nil, nil)
		require.NoError(t, err)
		assert.Len(t, ops.Edges, 1)
		assert.Equal(t, "opxdr2", ops.Edges[0].Node.OperationXDR)
		assert.False(t, ops.PageInfo.HasNextPage)
		assert.True(t, ops.PageInfo.HasPreviousPage)
	})

	t.Run("get operations with last/before pagination", func(t *testing.T) {
		loaders := dataloaders.NewDataloaders(resolver.models)
		ctx := context.WithValue(getTestCtx("operations", []string{"operation_xdr"}), middleware.LoadersKey, loaders)
		last := int32(1)
		ops, err := resolver.Operations(ctx, parentTx, nil, nil, &last, nil)
		require.NoError(t, err)
		assert.Len(t, ops.Edges, 1)
		assert.Equal(t, "opxdr2", ops.Edges[0].Node.OperationXDR)
		assert.False(t, ops.PageInfo.HasNextPage)
		assert.True(t, ops.PageInfo.HasPreviousPage)

		// Get the previous page using cursor
		prevCursor := ops.PageInfo.EndCursor
		assert.NotNil(t, prevCursor)
		ops, err = resolver.Operations(ctx, parentTx, nil, nil, &last, prevCursor)
		require.NoError(t, err)
		assert.Len(t, ops.Edges, 1)
		assert.Equal(t, "opxdr1", ops.Edges[0].Node.OperationXDR)
		assert.True(t, ops.PageInfo.HasNextPage)
		assert.False(t, ops.PageInfo.HasPreviousPage)
	})

	t.Run("invalid pagination params", func(t *testing.T) {
		loaders := dataloaders.NewDataloaders(resolver.models)
		ctx := context.WithValue(getTestCtx("operations", []string{"id"}), middleware.LoadersKey, loaders)
		first := int32(0)
		last := int32(1)
		after := encodeCursor(int64(1))
		before := encodeCursor(int64(2))

		_, err := resolver.Operations(ctx, parentTx, &first, nil, nil, nil)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "validating pagination params: first must be greater than 0")

		first = int32(1)
		_, err = resolver.Operations(ctx, parentTx, &first, nil, &last, nil)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "validating pagination params: first and last cannot be used together")

		_, err = resolver.Operations(ctx, parentTx, nil, &after, nil, &before)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "validating pagination params: after and before cannot be used together")

		_, err = resolver.Operations(ctx, parentTx, &first, nil, nil, &before)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "validating pagination params: first and before cannot be used together")

		_, err = resolver.Operations(ctx, parentTx, nil, &after, &last, nil)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "validating pagination params: last and after cannot be used together")
	})

	t.Run("pagination with larger limit than available data", func(t *testing.T) {
		loaders := dataloaders.NewDataloaders(resolver.models)
		ctx := context.WithValue(getTestCtx("operations", []string{"id"}), middleware.LoadersKey, loaders)
		first := int32(100)
		ops, err := resolver.Operations(ctx, parentTx, &first, nil, nil, nil)
		require.NoError(t, err)
		assert.Len(t, ops.Edges, 2) // tx1 has 2 operations
		assert.False(t, ops.PageInfo.HasNextPage)
		assert.False(t, ops.PageInfo.HasPreviousPage)
	})
}

func TestTransactionResolver_Accounts(t *testing.T) {
	mockMetricsService := &metrics.MockMetricsService{}
	mockMetricsService.On("IncDBQuery", "BatchGetByToIDs", "accounts").Return()
	mockMetricsService.On("ObserveDBQueryDuration", "BatchGetByToIDs", "accounts", mock.Anything).Return()
	mockMetricsService.On("ObserveDBBatchSize", "BatchGetByToIDs", "accounts", mock.Anything).Return()
	defer mockMetricsService.AssertExpectations(t)

	resolver := &transactionResolver{&Resolver{
		models: &data.Models{
			Account: &data.AccountModel{
				DB:             testDBConnectionPool,
				MetricsService: mockMetricsService,
			},
		},
	}}
	parentTx := &types.Transaction{ToID: toid.New(1000, 1, 0).ToInt64(), Hash: "tx1"}

	t.Run("success", func(t *testing.T) {
		loaders := dataloaders.NewDataloaders(resolver.models)
		ctx := context.WithValue(getTestCtx("accounts", []string{"address"}), middleware.LoadersKey, loaders)

		accounts, err := resolver.Accounts(ctx, parentTx)

		require.NoError(t, err)
		require.Len(t, accounts, 1)
		assert.Equal(t, "test-account", accounts[0].StellarAddress)
	})

	t.Run("nil transaction panics", func(t *testing.T) {
		loaders := dataloaders.NewDataloaders(resolver.models)
		ctx := context.WithValue(getTestCtx("accounts", []string{"address"}), middleware.LoadersKey, loaders)

		assert.Panics(t, func() {
			_, _ = resolver.Accounts(ctx, nil) //nolint:errcheck
		})
	})

	t.Run("transaction with no associated accounts", func(t *testing.T) {
		nonExistentTx := &types.Transaction{Hash: "non-existent-tx"}
		loaders := dataloaders.NewDataloaders(resolver.models)
		ctx := context.WithValue(getTestCtx("accounts", []string{"address"}), middleware.LoadersKey, loaders)

		accounts, err := resolver.Accounts(ctx, nonExistentTx)

		require.NoError(t, err)
		assert.Empty(t, accounts)
	})
}

func TestTransactionResolver_StateChanges(t *testing.T) {
	mockMetricsService := &metrics.MockMetricsService{}
	mockMetricsService.On("IncDBQuery", "BatchGetByToID", "state_changes").Return()
	mockMetricsService.On("ObserveDBQueryDuration", "BatchGetByToID", "state_changes", mock.Anything).Return()
	defer mockMetricsService.AssertExpectations(t)

	resolver := &transactionResolver{&Resolver{
		models: &data.Models{
			StateChanges: &data.StateChangeModel{
				DB:             testDBConnectionPool,
				MetricsService: mockMetricsService,
			},
			Transactions: &data.TransactionModel{
				DB:             testDBConnectionPool,
				MetricsService: mockMetricsService,
			},
		},
	}}
	parentTx := &types.Transaction{Hash: "tx1", ToID: toid.New(1000, 1, 0).ToInt64()}
	nonExistentTx := &types.Transaction{Hash: "non-existent-tx", ToID: 0}

	t.Run("success without pagination", func(t *testing.T) {
		loaders := dataloaders.NewDataloaders(resolver.models)
		ctx := context.WithValue(getTestCtx("state_changes", []string{}), middleware.LoadersKey, loaders)

		stateChanges, err := resolver.StateChanges(ctx, parentTx, nil, nil, nil, nil)

		require.NoError(t, err)
		require.Len(t, stateChanges.Edges, 5)

		// For tx1: operations 1 and 2, each with 2 state changes and 1 fee change
		txToID := toid.New(1000, 1, 0).ToInt64()
		op1ID := toid.New(1000, 1, 1).ToInt64()
		op2ID := toid.New(1000, 1, 2).ToInt64()

		// Edge 0: Fee state change (no operation)
		cursor := extractStateChangeIDs(stateChanges.Edges[0].Node)
		assert.Equal(t, txToID, cursor.ToID)
		assert.Equal(t, int64(0), cursor.OperationID) // Fee change has no operation
		assert.Equal(t, int64(1), cursor.StateChangeOrder)

		// Edge 1: Operation 1's first state change
		cursor = extractStateChangeIDs(stateChanges.Edges[1].Node)
		assert.Equal(t, txToID, cursor.ToID)
		assert.Equal(t, op1ID, cursor.OperationID)
		assert.Equal(t, int64(1), cursor.StateChangeOrder)

		// Edge 2: Operation 1's second state change
		cursor = extractStateChangeIDs(stateChanges.Edges[2].Node)
		assert.Equal(t, txToID, cursor.ToID)
		assert.Equal(t, op1ID, cursor.OperationID)
		assert.Equal(t, int64(2), cursor.StateChangeOrder)

		// Edge 3: Operation 2's first state change
		cursor = extractStateChangeIDs(stateChanges.Edges[3].Node)
		assert.Equal(t, txToID, cursor.ToID)
		assert.Equal(t, op2ID, cursor.OperationID)
		assert.Equal(t, int64(1), cursor.StateChangeOrder)

		// Edge 4: Operation 2's second state change
		cursor = extractStateChangeIDs(stateChanges.Edges[4].Node)
		assert.Equal(t, txToID, cursor.ToID)
		assert.Equal(t, op2ID, cursor.OperationID)
		assert.Equal(t, int64(2), cursor.StateChangeOrder)

		assert.False(t, stateChanges.PageInfo.HasNextPage)
		assert.False(t, stateChanges.PageInfo.HasPreviousPage)
	})

	t.Run("get state changes with first/after pagination", func(t *testing.T) {
		loaders := dataloaders.NewDataloaders(resolver.models)
		ctx := context.WithValue(getTestCtx("state_changes", []string{"accountId", "stateChangeCategory"}), middleware.LoadersKey, loaders)

		txToID := toid.New(1000, 1, 0).ToInt64()
		op1ID := toid.New(1000, 1, 1).ToInt64()
		op2ID := toid.New(1000, 1, 2).ToInt64()

		first := int32(2)
		stateChanges, err := resolver.StateChanges(ctx, parentTx, &first, nil, nil, nil)
		require.NoError(t, err)
		assert.Len(t, stateChanges.Edges, 2)

		// Page 1: Fee change and Op 1 first state change
		cursor := extractStateChangeIDs(stateChanges.Edges[0].Node)
		assert.Equal(t, txToID, cursor.ToID)
		assert.Equal(t, int64(0), cursor.OperationID)
		assert.Equal(t, int64(1), cursor.StateChangeOrder)

		cursor = extractStateChangeIDs(stateChanges.Edges[1].Node)
		assert.Equal(t, txToID, cursor.ToID)
		assert.Equal(t, op1ID, cursor.OperationID)
		assert.Equal(t, int64(1), cursor.StateChangeOrder)

		assert.True(t, stateChanges.PageInfo.HasNextPage)
		assert.False(t, stateChanges.PageInfo.HasPreviousPage)

		// Get the next page using cursor
		nextCursor := stateChanges.PageInfo.EndCursor
		assert.NotNil(t, nextCursor)
		stateChanges, err = resolver.StateChanges(ctx, parentTx, &first, nextCursor, nil, nil)
		require.NoError(t, err)
		assert.Len(t, stateChanges.Edges, 2)

		// Page 2: Op 1 second state change and Op 2 first state change
		cursor = extractStateChangeIDs(stateChanges.Edges[0].Node)
		assert.Equal(t, txToID, cursor.ToID)
		assert.Equal(t, op1ID, cursor.OperationID)
		assert.Equal(t, int64(2), cursor.StateChangeOrder)

		cursor = extractStateChangeIDs(stateChanges.Edges[1].Node)
		assert.Equal(t, txToID, cursor.ToID)
		assert.Equal(t, op2ID, cursor.OperationID)
		assert.Equal(t, int64(1), cursor.StateChangeOrder)

		assert.True(t, stateChanges.PageInfo.HasNextPage)
		assert.True(t, stateChanges.PageInfo.HasPreviousPage)
	})

	t.Run("get state changes with last/before pagination", func(t *testing.T) {
		loaders := dataloaders.NewDataloaders(resolver.models)
		ctx := context.WithValue(getTestCtx("state_changes", []string{"accountId", "stateChangeCategory"}), middleware.LoadersKey, loaders)

		txToID := toid.New(1000, 1, 0).ToInt64()
		op1ID := toid.New(1000, 1, 1).ToInt64()
		op2ID := toid.New(1000, 1, 2).ToInt64()

		last := int32(2)
		stateChanges, err := resolver.StateChanges(ctx, parentTx, nil, nil, &last, nil)
		require.NoError(t, err)
		assert.Len(t, stateChanges.Edges, 2)

		// Last page: Op 2's two state changes
		cursor := extractStateChangeIDs(stateChanges.Edges[0].Node)
		assert.Equal(t, txToID, cursor.ToID)
		assert.Equal(t, op2ID, cursor.OperationID)
		assert.Equal(t, int64(1), cursor.StateChangeOrder)

		cursor = extractStateChangeIDs(stateChanges.Edges[1].Node)
		assert.Equal(t, txToID, cursor.ToID)
		assert.Equal(t, op2ID, cursor.OperationID)
		assert.Equal(t, int64(2), cursor.StateChangeOrder)

		assert.False(t, stateChanges.PageInfo.HasNextPage)
		assert.True(t, stateChanges.PageInfo.HasPreviousPage)

		// Get the previous page using cursor
		last = int32(10)
		prevCursor := stateChanges.PageInfo.StartCursor
		assert.NotNil(t, prevCursor)
		stateChanges, err = resolver.StateChanges(ctx, parentTx, nil, nil, &last, prevCursor)
		require.NoError(t, err)
		assert.Len(t, stateChanges.Edges, 3)

		// Previous page: Fee change, Op 1's first and second state changes
		cursor = extractStateChangeIDs(stateChanges.Edges[0].Node)
		assert.Equal(t, txToID, cursor.ToID)
		assert.Equal(t, int64(0), cursor.OperationID)
		assert.Equal(t, int64(1), cursor.StateChangeOrder)

		cursor = extractStateChangeIDs(stateChanges.Edges[1].Node)
		assert.Equal(t, txToID, cursor.ToID)
		assert.Equal(t, op1ID, cursor.OperationID)
		assert.Equal(t, int64(1), cursor.StateChangeOrder)

		cursor = extractStateChangeIDs(stateChanges.Edges[2].Node)
		assert.Equal(t, txToID, cursor.ToID)
		assert.Equal(t, op1ID, cursor.OperationID)
		assert.Equal(t, int64(2), cursor.StateChangeOrder)

		assert.True(t, stateChanges.PageInfo.HasNextPage)
		assert.False(t, stateChanges.PageInfo.HasPreviousPage)
	})

	t.Run("invalid pagination params", func(t *testing.T) {
		loaders := dataloaders.NewDataloaders(resolver.models)
		ctx := context.WithValue(getTestCtx("state_changes", []string{"accountId", "stateChangeCategory"}), middleware.LoadersKey, loaders)
		first := int32(0)
		last := int32(1)
		after := encodeCursor(int64(1))
		before := encodeCursor(int64(2))

		_, err := resolver.StateChanges(ctx, parentTx, &first, nil, nil, nil)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "validating pagination params: first must be greater than 0")

		first = int32(1)
		_, err = resolver.StateChanges(ctx, parentTx, &first, nil, &last, nil)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "validating pagination params: first and last cannot be used together")

		_, err = resolver.StateChanges(ctx, parentTx, nil, &after, nil, &before)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "validating pagination params: after and before cannot be used together")

		_, err = resolver.StateChanges(ctx, parentTx, &first, nil, nil, &before)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "validating pagination params: first and before cannot be used together")

		_, err = resolver.StateChanges(ctx, parentTx, nil, &after, &last, nil)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "validating pagination params: last and after cannot be used together")
	})

	t.Run("pagination with larger limit than available data", func(t *testing.T) {
		loaders := dataloaders.NewDataloaders(resolver.models)
		ctx := context.WithValue(getTestCtx("state_changes", []string{"accountId", "stateChangeCategory"}), middleware.LoadersKey, loaders)
		first := int32(100)
		stateChanges, err := resolver.StateChanges(ctx, parentTx, &first, nil, nil, nil)
		require.NoError(t, err)
		assert.Len(t, stateChanges.Edges, 5) // tx1 has 5 state changes
		assert.False(t, stateChanges.PageInfo.HasNextPage)
		assert.False(t, stateChanges.PageInfo.HasPreviousPage)
	})

	t.Run("nil transaction panics", func(t *testing.T) {
		loaders := dataloaders.NewDataloaders(resolver.models)
		ctx := context.WithValue(getTestCtx("state_changes", []string{"accountId", "stateChangeCategory"}), middleware.LoadersKey, loaders)

		assert.Panics(t, func() {
			_, _ = resolver.StateChanges(ctx, nil, nil, nil, nil, nil) //nolint:errcheck
		})
	})

	t.Run("transaction with no state changes", func(t *testing.T) {
		loaders := dataloaders.NewDataloaders(resolver.models)
		ctx := context.WithValue(getTestCtx("state_changes", []string{"accountId", "stateChangeCategory"}), middleware.LoadersKey, loaders)

		stateChanges, err := resolver.StateChanges(ctx, nonExistentTx, nil, nil, nil, nil)

		require.NoError(t, err)
		assert.Empty(t, stateChanges.Edges)
		assert.False(t, stateChanges.PageInfo.HasNextPage)
		assert.False(t, stateChanges.PageInfo.HasPreviousPage)
	})
}
