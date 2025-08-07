package resolvers

import (
	"context"
	"testing"

	"github.com/stretchr/testify/mock"

	"github.com/stellar/wallet-backend/internal/data"
	"github.com/stellar/wallet-backend/internal/db"
	"github.com/stellar/wallet-backend/internal/db/dbtest"
	"github.com/stellar/wallet-backend/internal/metrics"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/wallet-backend/internal/indexer/types"
)

func TestAccountResolver_Transactions(t *testing.T) {
	parentAccount := &types.Account{StellarAddress: "test-account"}
	ctx := context.Background()

	dbt := dbtest.Open(t)
	defer dbt.Close()
	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	mockMetricsService := &metrics.MockMetricsService{}
	mockMetricsService.On("IncDBQuery", "SELECT", "transactions").Return()
	mockMetricsService.On("ObserveDBQueryDuration", "SELECT", "transactions", mock.Anything).Return()
	defer mockMetricsService.AssertExpectations(t)

	resolver := &accountResolver{&Resolver{
		models: &data.Models{
			Transactions: &data.TransactionModel{
				DB:             dbConnectionPool,
				MetricsService: mockMetricsService,
			},
		},
	}}

	setupDB(ctx, t, dbConnectionPool)

	t.Run("success", func(t *testing.T) {
		ctx = getTestCtx("transactions", []string{"hash"})
		transactions, err := resolver.Transactions(ctx, parentAccount, nil, nil)

		require.NoError(t, err)
		require.Len(t, transactions.Edges, 4)
		assert.Equal(t, "tx4", transactions.Edges[0].Node.Hash)
		assert.Equal(t, "tx3", transactions.Edges[1].Node.Hash)
		assert.Equal(t, "tx2", transactions.Edges[2].Node.Hash)
		assert.Equal(t, "tx1", transactions.Edges[3].Node.Hash)
	})

	t.Run("get with cursor", func(t *testing.T) {
		ctx = getTestCtx("transactions", []string{"hash"})
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

	cleanUpDB(ctx, t, dbConnectionPool)
}

func TestAccountResolver_Operations(t *testing.T) {
	parentAccount := &types.Account{StellarAddress: "test-account"}
	ctx := context.Background()

	dbt := dbtest.Open(t)
	defer dbt.Close()
	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	mockMetricsService := &metrics.MockMetricsService{}
	mockMetricsService.On("IncDBQuery", "SELECT", "operations").Return()
	mockMetricsService.On("ObserveDBQueryDuration", "SELECT", "operations", mock.Anything).Return()
	defer mockMetricsService.AssertExpectations(t)

	resolver := &accountResolver{&Resolver{
		models: &data.Models{
			Operations: &data.OperationModel{
				DB:             dbConnectionPool,
				MetricsService: mockMetricsService,
			},
		},
	}}

	setupDB(ctx, t, dbConnectionPool)

	t.Run("success", func(t *testing.T) {
		ctx = getTestCtx("operations", []string{"tx_hash"})
		operations, err := resolver.Operations(ctx, parentAccount, nil, nil)

		require.NoError(t, err)
		require.Len(t, operations.Edges, 4)
		assert.Equal(t, "tx4", operations.Edges[0].Node.TxHash)
		assert.Equal(t, "tx3", operations.Edges[1].Node.TxHash)
		assert.Equal(t, "tx2", operations.Edges[2].Node.TxHash)
		assert.Equal(t, "tx1", operations.Edges[3].Node.TxHash)
	})

	t.Run("get with cursor", func(t *testing.T) {
		ctx = getTestCtx("operations", []string{"tx_hash"})
		limit := int32(2)
		ops, err := resolver.Operations(ctx, parentAccount, &limit, nil)
		require.NoError(t, err)
		assert.Len(t, ops.Edges, 2)
		assert.Equal(t, "tx4", ops.Edges[0].Node.TxHash)
		assert.Equal(t, "tx3", ops.Edges[1].Node.TxHash)

		// Get the next cursor
		nextCursor := ops.PageInfo.EndCursor
		assert.NotNil(t, nextCursor)
		ops, err = resolver.Operations(ctx, parentAccount, &limit, nextCursor)
		require.NoError(t, err)
		assert.Len(t, ops.Edges, 2)
		assert.Equal(t, "tx2", ops.Edges[0].Node.TxHash)
		assert.Equal(t, "tx1", ops.Edges[1].Node.TxHash)

		hasNextPage := ops.PageInfo.HasNextPage
		assert.False(t, hasNextPage)
	})

	cleanUpDB(ctx, t, dbConnectionPool)
}

func TestAccountResolver_StateChanges(t *testing.T) {
	parentAccount := &types.Account{StellarAddress: "test-account"}
	ctx := context.Background()

	dbt := dbtest.Open(t)
	defer dbt.Close()
	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	mockMetricsService := &metrics.MockMetricsService{}
	mockMetricsService.On("IncDBQuery", "SELECT", "state_changes").Return()
	mockMetricsService.On("ObserveDBQueryDuration", "SELECT", "state_changes", mock.Anything).Return()
	defer mockMetricsService.AssertExpectations(t)

	resolver := &accountResolver{&Resolver{
		models: &data.Models{
			StateChanges: &data.StateChangeModel{
				DB:             dbConnectionPool,
				MetricsService: mockMetricsService,
			},
		},
	}}

	setupDB(ctx, t, dbConnectionPool)

	t.Run("success", func(t *testing.T) {
		ctx := getTestCtx("state_changes", []string{"id"})
		stateChanges, err := resolver.StateChanges(ctx, parentAccount, nil, nil)

		require.NoError(t, err)
		require.Len(t, stateChanges.Edges, 4)
		assert.Equal(t, "sc4", stateChanges.Edges[0].Node.ID)
		assert.Equal(t, "sc3", stateChanges.Edges[1].Node.ID)
		assert.Equal(t, "sc2", stateChanges.Edges[2].Node.ID)
		assert.Equal(t, "sc1", stateChanges.Edges[3].Node.ID)
	})

	t.Run("get with cursor", func(t *testing.T) {
		ctx := getTestCtx("state_changes", []string{"id"})
		limit := int32(2)
		stateChanges, err := resolver.StateChanges(ctx, parentAccount, &limit, nil)

		require.NoError(t, err)
		assert.Len(t, stateChanges.Edges, 2)
		assert.Equal(t, "sc4", stateChanges.Edges[0].Node.ID)
		assert.Equal(t, "sc3", stateChanges.Edges[1].Node.ID)

		nextCursor := stateChanges.PageInfo.EndCursor
		assert.NotNil(t, nextCursor)
		stateChanges, err = resolver.StateChanges(ctx, parentAccount, &limit, nextCursor)
		require.NoError(t, err)
		assert.Len(t, stateChanges.Edges, 2)
		assert.Equal(t, "sc2", stateChanges.Edges[0].Node.ID)
		assert.Equal(t, "sc1", stateChanges.Edges[1].Node.ID)

		hasNextPage := stateChanges.PageInfo.HasNextPage
		assert.False(t, hasNextPage)
	})

	cleanUpDB(ctx, t, dbConnectionPool)
}
