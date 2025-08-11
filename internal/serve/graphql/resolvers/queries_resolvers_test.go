package resolvers

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/stellar/wallet-backend/internal/data"
	"github.com/stellar/wallet-backend/internal/metrics"
)


func TestQueryResolver_TransactionByHash(t *testing.T) {
	t.Run("success", func(t *testing.T) {
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

		ctx := getTestCtx("transactions", []string{"hash", "toId", "envelopeXdr", "resultXdr", "metaXdr", "ledgerNumber", "ledgerCreatedAt"})
		tx, err := resolver.TransactionByHash(ctx, "tx1")

		require.NoError(t, err)
		assert.Equal(t, "tx1", tx.Hash)
		assert.Equal(t, int64(1), tx.ToID)
		assert.Equal(t, "envelope1", tx.EnvelopeXDR)
		assert.Equal(t, "result1", tx.ResultXDR)
		assert.Equal(t, "meta1", tx.MetaXDR)
		assert.Equal(t, uint32(1), tx.LedgerNumber)
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
}

func TestQueryResolver_StateChanges(t *testing.T) {
	resolver := &queryResolver{
		&Resolver{
			models: &data.Models{
				StateChanges: &data.StateChangeModel{
					DB: testDBConnectionPool,
				},
			},
		},
	}

	t.Run("get all", func(t *testing.T) {
		mockMetricsService := &metrics.MockMetricsService{}
		resolver.models.StateChanges.MetricsService = mockMetricsService
		mockMetricsService.On("ObserveDBQueryDuration", "SELECT", "state_changes", mock.Anything).Return()
		mockMetricsService.On("IncDBQuery", "SELECT", "state_changes").Return()
		ctx := getTestCtx("state_changes", []string{"stateChangeCategory", "txHash", "operationId", "accountId", "ledgerCreatedAt", "ledgerNumber"})
		scs, err := resolver.StateChanges(ctx, nil)
		require.NoError(t, err)
		assert.Len(t, scs, 4)
		// Verify the state changes have the expected account ID
		assert.Equal(t, "test-account", scs[0].AccountID)
		mockMetricsService.AssertExpectations(t)
	})

	t.Run("get with limit", func(t *testing.T) {
		mockMetricsService := &metrics.MockMetricsService{}
		resolver.models.StateChanges.MetricsService = mockMetricsService
		mockMetricsService.On("ObserveDBQueryDuration", "SELECT", "state_changes", mock.Anything).Return()
		mockMetricsService.On("IncDBQuery", "SELECT", "state_changes").Return()
		limit := int32(1)
		ctx := getTestCtx("state_changes", []string{"stateChangeCategory", "txHash", "operationId", "accountId", "ledgerCreatedAt", "ledgerNumber"})
		scs, err := resolver.StateChanges(ctx, &limit)
		require.NoError(t, err)
		assert.Len(t, scs, 1)
		mockMetricsService.AssertExpectations(t)
	})
}
