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

	t.Run("get all", func(t *testing.T) {
		ctx := getTestCtx("transactions", []string{"hash", "toId", "envelopeXdr", "resultXdr", "metaXdr", "ledgerNumber", "ledgerCreatedAt"})
		txs, err := resolver.Transactions(ctx, nil)
		require.NoError(t, err)
		assert.Len(t, txs, 4)
	})

	t.Run("get with limit", func(t *testing.T) {
		ctx := getTestCtx("transactions", []string{"hash", "toId", "envelopeXdr", "resultXdr", "metaXdr", "ledgerNumber", "ledgerCreatedAt"})
		limit := int32(1)
		txs, err := resolver.Transactions(ctx, &limit)
		require.NoError(t, err)
		assert.Len(t, txs, 1)
	})

	t.Run("negative limit error", func(t *testing.T) {
		ctx := getTestCtx("transactions", []string{"hash"})
		limit := int32(-1)
		txs, err := resolver.Transactions(ctx, &limit)
		require.Error(t, err)
		assert.Nil(t, txs)
		assert.Contains(t, err.Error(), "limit must be non-negative")
	})

	t.Run("zero limit", func(t *testing.T) {
		ctx := getTestCtx("transactions", []string{"hash"})
		limit := int32(0)
		txs, err := resolver.Transactions(ctx, &limit)
		require.NoError(t, err)
		assert.Len(t, txs, 0)
	})

	t.Run("limit larger than available data", func(t *testing.T) {
		ctx := getTestCtx("transactions", []string{"hash"})
		limit := int32(100)
		txs, err := resolver.Transactions(ctx, &limit)
		require.NoError(t, err)
		assert.Len(t, txs, 4)
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

	t.Run("get all", func(t *testing.T) {
		ctx := getTestCtx("operations", []string{"id", "operationType", "operationXdr", "txHash", "ledgerNumber", "ledgerCreatedAt"})
		ops, err := resolver.Operations(ctx, nil)
		require.NoError(t, err)
		assert.Len(t, ops, 8)
	})

	t.Run("get with limit", func(t *testing.T) {
		ctx := getTestCtx("operations", []string{"id", "operationType", "operationXdr", "txHash", "ledgerNumber", "ledgerCreatedAt"})
		limit := int32(1)
		ops, err := resolver.Operations(ctx, &limit)
		require.NoError(t, err)
		assert.Len(t, ops, 1)
	})

	t.Run("negative limit error", func(t *testing.T) {
		ctx := getTestCtx("operations", []string{"id"})
		limit := int32(-5)
		ops, err := resolver.Operations(ctx, &limit)
		require.Error(t, err)
		assert.Nil(t, ops)
		assert.Contains(t, err.Error(), "limit must be non-negative")
	})

	t.Run("zero limit", func(t *testing.T) {
		ctx := getTestCtx("operations", []string{"id"})
		limit := int32(0)
		ops, err := resolver.Operations(ctx, &limit)
		require.NoError(t, err)
		assert.Len(t, ops, 0)
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
