package resolvers

import (
	"context"
	"fmt"
	"testing"

	"github.com/stellar/go/toid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/stellar/wallet-backend/internal/data"
	"github.com/stellar/wallet-backend/internal/indexer/types"
	"github.com/stellar/wallet-backend/internal/metrics"
	"github.com/stellar/wallet-backend/internal/serve/graphql/dataloaders"
	"github.com/stellar/wallet-backend/internal/serve/middleware"
)

func TestOperationResolver_Transaction(t *testing.T) {
	mockMetricsService := &metrics.MockMetricsService{}
	mockMetricsService.On("IncDBQuery", "SELECT", "transactions").Return()
	mockMetricsService.On("ObserveDBQueryDuration", "SELECT", "transactions", mock.Anything).Return()
	defer mockMetricsService.AssertExpectations(t)

	resolver := &operationResolver{&Resolver{
		models: &data.Models{
			Transactions: &data.TransactionModel{
				DB:             testDBConnectionPool,
				MetricsService: mockMetricsService,
			},
		},
	}}
	parentOperation := &types.Operation{ID: toid.New(1000, 1, 1).ToInt64()}

	t.Run("success", func(t *testing.T) {
		loaders := dataloaders.NewDataloaders(resolver.models)
		ctx := context.WithValue(getTestCtx("transactions", []string{"hash"}), middleware.LoadersKey, loaders)

		transaction, err := resolver.Transaction(ctx, parentOperation)

		require.NoError(t, err)
		require.NotNil(t, transaction)
		assert.Equal(t, "tx1", transaction.Hash)
	})

	t.Run("nil operation panics", func(t *testing.T) {
		loaders := dataloaders.NewDataloaders(resolver.models)
		ctx := context.WithValue(getTestCtx("transactions", []string{"hash"}), middleware.LoadersKey, loaders)

		assert.Panics(t, func() {
			_, _ = resolver.Transaction(ctx, nil) //nolint:errcheck
		})
	})

	t.Run("operation with non-existent transaction", func(t *testing.T) {
		nonExistentOperation := &types.Operation{ID: 9999, TxHash: "non-existent-tx"}
		loaders := dataloaders.NewDataloaders(resolver.models)
		ctx := context.WithValue(getTestCtx("transactions", []string{"hash"}), middleware.LoadersKey, loaders)

		transaction, err := resolver.Transaction(ctx, nonExistentOperation)

		require.NoError(t, err) // Dataloader returns nil, not error for missing data
		assert.Nil(t, transaction)
	})
}

func TestOperationResolver_Accounts(t *testing.T) {
	mockMetricsService := &metrics.MockMetricsService{}
	mockMetricsService.On("IncDBQuery", "SELECT", "accounts").Return()
	mockMetricsService.On("ObserveDBQueryDuration", "SELECT", "accounts", mock.Anything).Return()
	defer mockMetricsService.AssertExpectations(t)

	resolver := &operationResolver{&Resolver{
		models: &data.Models{
			Account: &data.AccountModel{
				DB:             testDBConnectionPool,
				MetricsService: mockMetricsService,
			},
		},
	}}
	parentOperation := &types.Operation{ID: toid.New(1000, 1, 1).ToInt64()}

	t.Run("success", func(t *testing.T) {
		loaders := dataloaders.NewDataloaders(resolver.models)
		ctx := context.WithValue(getTestCtx("accounts", []string{"address"}), middleware.LoadersKey, loaders)

		accounts, err := resolver.Accounts(ctx, parentOperation)

		require.NoError(t, err)
		require.Len(t, accounts, 1)
		assert.Equal(t, "test-account", accounts[0].StellarAddress)
	})

	t.Run("nil operation panics", func(t *testing.T) {
		loaders := dataloaders.NewDataloaders(resolver.models)
		ctx := context.WithValue(getTestCtx("accounts", []string{"address"}), middleware.LoadersKey, loaders)

		assert.Panics(t, func() {
			_, _ = resolver.Accounts(ctx, nil) //nolint:errcheck
		})
	})

	t.Run("operation with no associated accounts", func(t *testing.T) {
		nonExistentOperation := &types.Operation{ID: 9999}
		loaders := dataloaders.NewDataloaders(resolver.models)
		ctx := context.WithValue(getTestCtx("accounts", []string{"address"}), middleware.LoadersKey, loaders)

		accounts, err := resolver.Accounts(ctx, nonExistentOperation)

		require.NoError(t, err)
		assert.Empty(t, accounts)
	})
}

func TestOperationResolver_StateChanges(t *testing.T) {
	mockMetricsService := &metrics.MockMetricsService{}
	mockMetricsService.On("IncDBQuery", "SELECT", "state_changes").Return()
	mockMetricsService.On("ObserveDBQueryDuration", "SELECT", "state_changes", mock.Anything).Return()
	defer mockMetricsService.AssertExpectations(t)

	resolver := &operationResolver{&Resolver{
		models: &data.Models{
			StateChanges: &data.StateChangeModel{
				DB:             testDBConnectionPool,
				MetricsService: mockMetricsService,
			},
			Operations: &data.OperationModel{
				DB:             testDBConnectionPool,
				MetricsService: mockMetricsService,
			},
		},
	}}
	parentOperation := &types.Operation{ID: toid.New(1000, 1, 1).ToInt64()}

	t.Run("success", func(t *testing.T) {
		loaders := dataloaders.NewDataloaders(resolver.models)
		ctx := context.WithValue(getTestCtx("state_changes", []string{"accountId", "stateChangeCategory"}), middleware.LoadersKey, loaders)

		stateChanges, err := resolver.StateChanges(ctx, parentOperation)

		require.NoError(t, err)
		require.Len(t, stateChanges, 2)
		assert.Equal(t, fmt.Sprintf("%d:2", toid.New(1000, 1, 1).ToInt64()), fmt.Sprintf("%d:%d", stateChanges[0].ToID, stateChanges[0].StateChangeOrder))
		assert.Equal(t, fmt.Sprintf("%d:1", toid.New(1000, 1, 1).ToInt64()), fmt.Sprintf("%d:%d", stateChanges[1].ToID, stateChanges[1].StateChangeOrder))
	})

	t.Run("nil operation panics", func(t *testing.T) {
		loaders := dataloaders.NewDataloaders(resolver.models)
		ctx := context.WithValue(getTestCtx("state_changes", []string{"accountId", "stateChangeCategory"}), middleware.LoadersKey, loaders)

		assert.Panics(t, func() {
			_, _ = resolver.StateChanges(ctx, nil) //nolint:errcheck
		})
	})

	t.Run("operation with no state changes", func(t *testing.T) {
		nonExistentOperation := &types.Operation{ID: 9999}
		loaders := dataloaders.NewDataloaders(resolver.models)
		ctx := context.WithValue(getTestCtx("state_changes", []string{"accountId", "stateChangeCategory"}), middleware.LoadersKey, loaders)

		stateChanges, err := resolver.StateChanges(ctx, nonExistentOperation)

		require.NoError(t, err)
		assert.Empty(t, stateChanges)
	})
}
