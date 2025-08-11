package resolvers

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/stellar/wallet-backend/internal/data"
	"github.com/stellar/wallet-backend/internal/indexer/types"
	"github.com/stellar/wallet-backend/internal/metrics"
	"github.com/stellar/wallet-backend/internal/serve/graphql/dataloaders"
	"github.com/stellar/wallet-backend/internal/serve/middleware"
)

func TestTransactionResolver_Operations(t *testing.T) {
	mockMetricsService := &metrics.MockMetricsService{}
	mockMetricsService.On("IncDBQuery", "SELECT", "operations").Return()
	mockMetricsService.On("ObserveDBQueryDuration", "SELECT", "operations", mock.Anything).Return()
	defer mockMetricsService.AssertExpectations(t)

	resolver := &transactionResolver{&Resolver{
		models: &data.Models{
			Operations: &data.OperationModel{
				DB:             testDBConnectionPool,
				MetricsService: mockMetricsService,
			},
		},
	}}
	parentTx := &types.Transaction{Hash: "tx1"}

	t.Run("success", func(t *testing.T) {
		loaders := dataloaders.NewDataloaders(resolver.models)
		ctx := context.WithValue(getTestCtx("operations", []string{"id"}), middleware.LoadersKey, loaders)

		operations, err := resolver.Operations(ctx, parentTx)

		require.NoError(t, err)
		require.Len(t, operations, 2)
		assert.Equal(t, int64(1001), operations[0].ID)
		assert.Equal(t, int64(1002), operations[1].ID)
	})

	t.Run("nil transaction panics", func(t *testing.T) {
		loaders := dataloaders.NewDataloaders(resolver.models)
		ctx := context.WithValue(getTestCtx("operations", []string{"id"}), middleware.LoadersKey, loaders)

		assert.Panics(t, func() {
			resolver.Operations(ctx, nil)
		})
	})

	t.Run("transaction with no operations", func(t *testing.T) {
		nonExistentTx := &types.Transaction{Hash: "non-existent-tx"}
		loaders := dataloaders.NewDataloaders(resolver.models)
		ctx := context.WithValue(getTestCtx("operations", []string{"id"}), middleware.LoadersKey, loaders)

		operations, err := resolver.Operations(ctx, nonExistentTx)

		require.NoError(t, err)
		assert.Empty(t, operations)
	})
}

func TestTransactionResolver_Accounts(t *testing.T) {
	mockMetricsService := &metrics.MockMetricsService{}
	mockMetricsService.On("IncDBQuery", "SELECT", "accounts").Return()
	mockMetricsService.On("ObserveDBQueryDuration", "SELECT", "accounts", mock.Anything).Return()
	defer mockMetricsService.AssertExpectations(t)

	resolver := &transactionResolver{&Resolver{
		models: &data.Models{
			Account: &data.AccountModel{
				DB:             testDBConnectionPool,
				MetricsService: mockMetricsService,
			},
		},
	}}
	parentTx := &types.Transaction{Hash: "tx1"}

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
			resolver.Accounts(ctx, nil)
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
	mockMetricsService.On("IncDBQuery", "SELECT", "state_changes").Return()
	mockMetricsService.On("ObserveDBQueryDuration", "SELECT", "state_changes", mock.Anything).Return()
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
	parentTx := &types.Transaction{Hash: "tx1"}

	t.Run("success", func(t *testing.T) {
		loaders := dataloaders.NewDataloaders(resolver.models)
		ctx := context.WithValue(getTestCtx("state_changes", []string{"accountId", "stateChangeCategory"}), middleware.LoadersKey, loaders)

		stateChanges, err := resolver.StateChanges(ctx, parentTx)

		require.NoError(t, err)
		require.Len(t, stateChanges, 5)
		// For tx1: operations 1 and 2, each with 2 state changes and 1 fee change
		assert.Equal(t, int64(1002), stateChanges[0].ToID)
		assert.Equal(t, int64(2), stateChanges[0].StateChangeOrder)
		assert.Equal(t, int64(1002), stateChanges[1].ToID)
		assert.Equal(t, int64(1), stateChanges[1].StateChangeOrder)
		assert.Equal(t, int64(1001), stateChanges[2].ToID)
		assert.Equal(t, int64(2), stateChanges[2].StateChangeOrder)
		assert.Equal(t, int64(1001), stateChanges[3].ToID)
		assert.Equal(t, int64(1), stateChanges[3].StateChangeOrder)
		assert.Equal(t, int64(1), stateChanges[4].ToID)
		assert.Equal(t, int64(1), stateChanges[4].StateChangeOrder)
	})

	t.Run("nil transaction panics", func(t *testing.T) {
		loaders := dataloaders.NewDataloaders(resolver.models)
		ctx := context.WithValue(getTestCtx("state_changes", []string{"accountId", "stateChangeCategory"}), middleware.LoadersKey, loaders)

		assert.Panics(t, func() {
			resolver.StateChanges(ctx, nil)
		})
	})

	t.Run("transaction with no state changes", func(t *testing.T) {
		nonExistentTx := &types.Transaction{Hash: "non-existent-tx"}
		loaders := dataloaders.NewDataloaders(resolver.models)
		ctx := context.WithValue(getTestCtx("state_changes", []string{"accountId", "stateChangeCategory"}), middleware.LoadersKey, loaders)

		stateChanges, err := resolver.StateChanges(ctx, nonExistentTx)

		require.NoError(t, err)
		assert.Empty(t, stateChanges)
	})
}
