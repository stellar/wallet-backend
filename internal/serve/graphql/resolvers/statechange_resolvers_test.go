package resolvers

import (
	"context"
	"database/sql"
	"encoding/json"
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

func TestStateChangeResolver_NullableStringFields(t *testing.T) {
	resolver := &standardBalanceChangeResolver{&Resolver{}}
	ctx := context.Background()

	t.Run("all valid", func(t *testing.T) {
		obj := &types.StandardBalanceStateChangeModel{
			StateChange: types.StateChange{
				TokenID: sql.NullString{String: "token1", Valid: true},
				Amount:  sql.NullString{String: "100.5", Valid: true},
			},
		}

		tokenID, err := resolver.TokenID(ctx, obj)
		require.NoError(t, err)
		assert.Equal(t, "token1", tokenID)

		amount, err := resolver.Amount(ctx, obj)
		require.NoError(t, err)
		assert.Equal(t, "100.5", amount)
	})

	t.Run("all null", func(t *testing.T) {
		obj := &types.StandardBalanceStateChangeModel{} // All fields are zero-valued (Valid: false)

		tokenID, err := resolver.TokenID(ctx, obj)
		require.NoError(t, err)
		assert.Equal(t, "", tokenID)

		amount, err := resolver.Amount(ctx, obj)
		require.NoError(t, err)
		assert.Equal(t, "", amount)
	})
}

func TestStateChangeResolver_JSONFields(t *testing.T) {
	ctx := context.Background()

	t.Run("signer weights", func(t *testing.T) {
		resolver := &signerChangeResolver{&Resolver{}}
		obj := &types.SignerStateChangeModel{
			StateChange: types.StateChange{
				SignerWeights: types.NullableJSONB{"weight": 1},
			},
		}
		expectedJSON, err := json.Marshal(obj.SignerWeights)
		require.NoError(t, err)

		jsonStr, err := resolver.SignerWeights(ctx, obj)
		require.NoError(t, err)
		assert.JSONEq(t, string(expectedJSON), *jsonStr)

		obj.SignerWeights = nil
		jsonStr, err = resolver.SignerWeights(ctx, obj)
		require.NoError(t, err)
		assert.Nil(t, jsonStr)
	})

	t.Run("thresholds", func(t *testing.T) {
		resolver := &signerThresholdsChangeResolver{&Resolver{}}
		obj := &types.SignerThresholdsStateChangeModel{
			StateChange: types.StateChange{
				Thresholds: types.NullableJSONB{"low": 1, "med": 2},
			},
		}
		expectedJSON, err := json.Marshal(obj.Thresholds)
		require.NoError(t, err)

		jsonStr, err := resolver.Thresholds(ctx, obj)
		require.NoError(t, err)
		assert.JSONEq(t, string(expectedJSON), jsonStr)
	})

	t.Run("flags", func(t *testing.T) {
		resolver := &flagsChangeResolver{&Resolver{}}
		obj := &types.FlagsStateChangeModel{
			StateChange: types.StateChange{
				Flags: types.NullableJSON{"auth_required", "auth_revocable"},
			},
		}
		flags, err := resolver.Flags(ctx, obj)
		require.NoError(t, err)
		assert.Equal(t, []string{"auth_required", "auth_revocable"}, flags)

		obj.Flags = nil
		flags, err = resolver.Flags(ctx, obj)
		require.NoError(t, err)
		assert.Empty(t, flags)
	})

	t.Run("key value", func(t *testing.T) {
		resolver := &metadataChangeResolver{&Resolver{}}
		obj := &types.MetadataStateChangeModel{
			StateChange: types.StateChange{
				KeyValue: types.NullableJSONB{"key": "value"},
			},
		}
		expectedJSON, err := json.Marshal(obj.KeyValue)
		require.NoError(t, err)

		jsonStr, err := resolver.KeyValue(ctx, obj)
		require.NoError(t, err)
		assert.JSONEq(t, string(expectedJSON), jsonStr)
	})
}

func TestStateChangeResolver_Account(t *testing.T) {
	mockMetricsService := &metrics.MockMetricsService{}
	mockMetricsService.On("IncDBQuery", "BatchGetByStateChangeIDs", "accounts").Return()
	mockMetricsService.On("ObserveDBQueryDuration", "BatchGetByStateChangeIDs", "accounts", mock.Anything).Return()
	mockMetricsService.On("ObserveDBBatchSize", "BatchGetByStateChangeIDs", "accounts", mock.Anything).Return()
	defer mockMetricsService.AssertExpectations(t)

	resolver := &standardBalanceChangeResolver{&Resolver{
		models: &data.Models{
			Account: &data.AccountModel{
				DB:             testDBConnectionPool,
				MetricsService: mockMetricsService,
			},
		},
	}}
	parentSC := types.StandardBalanceStateChangeModel{
		StateChange: types.StateChange{
			ToID:                toid.New(1000, 1, 1).ToInt64(),
			StateChangeOrder:    1,
			StateChangeCategory: types.StateChangeCategoryBalance,
		},
	}

	t.Run("success", func(t *testing.T) {
		loaders := dataloaders.NewDataloaders(resolver.models)
		ctx := context.WithValue(getTestCtx("accounts", []string{""}), middleware.LoadersKey, loaders)

		account, err := resolver.Account(ctx, &parentSC)
		require.NoError(t, err)
		assert.Equal(t, "test-account", account.StellarAddress)
	})

	t.Run("nil state change panics", func(t *testing.T) {
		loaders := dataloaders.NewDataloaders(resolver.models)
		ctx := context.WithValue(getTestCtx("accounts", []string{""}), middleware.LoadersKey, loaders)

		assert.Panics(t, func() {
			_, _ = resolver.Account(ctx, nil) //nolint:errcheck
		})
	})

	t.Run("state change with non-existent account", func(t *testing.T) {
		nonExistentSC := types.StandardBalanceStateChangeModel{
			StateChange: types.StateChange{
				ToID:                9999,
				StateChangeOrder:    1,
				StateChangeCategory: types.StateChangeCategoryBalance,
			},
		}
		loaders := dataloaders.NewDataloaders(resolver.models)
		ctx := context.WithValue(getTestCtx("accounts", []string{""}), middleware.LoadersKey, loaders)

		account, err := resolver.Account(ctx, &nonExistentSC)
		require.NoError(t, err) // Dataloader returns nil, not error for missing data
		assert.Nil(t, account)
	})
}

func TestStateChangeResolver_Operation(t *testing.T) {
	mockMetricsService := &metrics.MockMetricsService{}
	mockMetricsService.On("IncDBQuery", "BatchGetByStateChangeIDs", "operations").Return()
	mockMetricsService.On("ObserveDBQueryDuration", "BatchGetByStateChangeIDs", "operations", mock.Anything).Return()
	mockMetricsService.On("ObserveDBBatchSize", "BatchGetByStateChangeIDs", "operations", mock.Anything).Return()
	defer mockMetricsService.AssertExpectations(t)

	resolver := &standardBalanceChangeResolver{&Resolver{
		models: &data.Models{
			Operations: &data.OperationModel{
				DB:             testDBConnectionPool,
				MetricsService: mockMetricsService,
			},
		},
	}}
	parentSC := types.StandardBalanceStateChangeModel{
		StateChange: types.StateChange{
			ToID:                toid.New(1000, 1, 1).ToInt64(),
			StateChangeOrder:    1,
			StateChangeCategory: types.StateChangeCategoryBalance,
		},
	}

	t.Run("success", func(t *testing.T) {
		loaders := dataloaders.NewDataloaders(resolver.models)
		ctx := context.WithValue(getTestCtx("operations", []string{"id"}), middleware.LoadersKey, loaders)

		op, err := resolver.Operation(ctx, &parentSC)
		require.NoError(t, err)
		assert.Equal(t, toid.New(1000, 1, 1).ToInt64(), op.ID)
	})

	t.Run("nil state change panics", func(t *testing.T) {
		loaders := dataloaders.NewDataloaders(resolver.models)
		ctx := context.WithValue(getTestCtx("operations", []string{"id"}), middleware.LoadersKey, loaders)

		assert.Panics(t, func() {
			_, _ = resolver.Operation(ctx, nil) //nolint:errcheck
		})
	})

	t.Run("state change with non-existent operation", func(t *testing.T) {
		nonExistentSC := types.StandardBalanceStateChangeModel{
			StateChange: types.StateChange{
				ToID:                9999,
				StateChangeOrder:    1,
				StateChangeCategory: types.StateChangeCategoryBalance,
			},
		}
		loaders := dataloaders.NewDataloaders(resolver.models)
		ctx := context.WithValue(getTestCtx("operations", []string{"id"}), middleware.LoadersKey, loaders)

		op, err := resolver.Operation(ctx, &nonExistentSC)
		require.NoError(t, err) // Dataloader returns nil, not error for missing data
		assert.Nil(t, op)
	})
}

func TestStateChangeResolver_Transaction(t *testing.T) {
	mockMetricsService := &metrics.MockMetricsService{}
	mockMetricsService.On("IncDBQuery", "BatchGetByStateChangeIDs", "transactions").Return()
	mockMetricsService.On("ObserveDBQueryDuration", "BatchGetByStateChangeIDs", "transactions", mock.Anything).Return()
	mockMetricsService.On("ObserveDBBatchSize", "BatchGetByStateChangeIDs", "transactions", mock.Anything).Return()
	defer mockMetricsService.AssertExpectations(t)

	resolver := &standardBalanceChangeResolver{&Resolver{
		models: &data.Models{
			Transactions: &data.TransactionModel{
				DB:             testDBConnectionPool,
				MetricsService: mockMetricsService,
			},
		},
	}}
	parentSC := types.StandardBalanceStateChangeModel{
		StateChange: types.StateChange{
			ToID:                toid.New(1000, 1, 0).ToInt64(),
			StateChangeOrder:    1,
			StateChangeCategory: types.StateChangeCategoryBalance,
		},
	}

	t.Run("success", func(t *testing.T) {
		loaders := dataloaders.NewDataloaders(resolver.models)
		ctx := context.WithValue(getTestCtx("transactions", []string{"hash"}), middleware.LoadersKey, loaders)

		tx, err := resolver.Transaction(ctx, &parentSC)
		require.NoError(t, err)
		assert.Equal(t, "tx1", tx.Hash)
	})

	t.Run("nil state change panics", func(t *testing.T) {
		loaders := dataloaders.NewDataloaders(resolver.models)
		ctx := context.WithValue(getTestCtx("transactions", []string{"hash"}), middleware.LoadersKey, loaders)

		assert.Panics(t, func() {
			_, _ = resolver.Transaction(ctx, nil) //nolint:errcheck
		})
	})

	t.Run("state change with non-existent transaction", func(t *testing.T) {
		nonExistentSC := types.StandardBalanceStateChangeModel{
			StateChange: types.StateChange{
				ToID:                9999,
				StateChangeOrder:    1,
				TxHash:              "non-existent-tx",
				StateChangeCategory: types.StateChangeCategoryBalance,
			},
		}
		loaders := dataloaders.NewDataloaders(resolver.models)
		ctx := context.WithValue(getTestCtx("transactions", []string{"hash"}), middleware.LoadersKey, loaders)

		tx, err := resolver.Transaction(ctx, &nonExistentSC)
		require.NoError(t, err) // Dataloader returns nil, not error for missing data
		assert.Nil(t, tx)
	})
}
