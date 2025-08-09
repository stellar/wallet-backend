package resolvers

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/wallet-backend/internal/data"
	"github.com/stellar/wallet-backend/internal/db"
	"github.com/stellar/wallet-backend/internal/indexer/types"
	"github.com/stellar/wallet-backend/internal/metrics"
	"github.com/stellar/wallet-backend/internal/db/dbtest"
	"github.com/stellar/wallet-backend/internal/serve/middleware"
	"github.com/stellar/wallet-backend/internal/serve/graphql/dataloaders"
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
		},
	}

	setupDB(ctx, t, dbConnectionPool)
	defer cleanUpDB(ctx, t, dbConnectionPool)

	t.Run("success", func(t *testing.T) {
		loaders := &dataloaders.Dataloaders{
			TransactionsByAccountLoader: dataloaders.TransactionsByAccountLoader(resolver.models),
		}
		ctx = context.WithValue(getTestCtx("transactions", []string{"hash"}), middleware.LoadersKey, loaders)
		transactions, err := resolver.Transactions(ctx, parentAccount)

		require.NoError(t, err)
		require.Len(t, transactions, 4)
		assert.Equal(t, "tx4", transactions[0].Hash)
		assert.Equal(t, "tx3", transactions[1].Hash)
		assert.Equal(t, "tx2", transactions[2].Hash)
		assert.Equal(t, "tx1", transactions[3].Hash)
	})
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
	defer cleanUpDB(ctx, t, dbConnectionPool)

	t.Run("success", func(t *testing.T) {
		loaders := &dataloaders.Dataloaders{
			OperationsByAccountLoader: dataloaders.OperationsByAccountLoader(resolver.models),
		}
		ctx = context.WithValue(getTestCtx("operations", []string{"id"}), middleware.LoadersKey, loaders)
		operations, err := resolver.Operations(ctx, parentAccount)

		require.NoError(t, err)
		require.Len(t, operations, 8)
		assert.Equal(t, int64(8), operations[0].ID)
		assert.Equal(t, int64(7), operations[1].ID)
		assert.Equal(t, int64(6), operations[2].ID)
		assert.Equal(t, int64(5), operations[3].ID)
	})
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
	defer cleanUpDB(ctx, t, dbConnectionPool)

	t.Run("success", func(t *testing.T) {
		loaders := &dataloaders.Dataloaders{
			StateChangesByAccountLoader: dataloaders.StateChangesByAccountLoader(resolver.models),
		}
		ctx = context.WithValue(getTestCtx("state_changes", []string{"to_id", "state_change_order"}), middleware.LoadersKey, loaders)
		stateChanges, err := resolver.StateChanges(ctx, parentAccount)

		require.NoError(t, err)
		require.Len(t, stateChanges, 4)
		assert.Equal(t, "4:1", fmt.Sprintf("%d:%d", stateChanges[0].ToID, stateChanges[0].StateChangeOrder))
		assert.Equal(t, "3:1", fmt.Sprintf("%d:%d", stateChanges[1].ToID, stateChanges[1].StateChangeOrder))
		assert.Equal(t, "2:1", fmt.Sprintf("%d:%d", stateChanges[2].ToID, stateChanges[2].StateChangeOrder))
		assert.Equal(t, "1:1", fmt.Sprintf("%d:%d", stateChanges[3].ToID, stateChanges[3].StateChangeOrder))
	})
}
