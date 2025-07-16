package resolvers

import (
	"context"
	"testing"
	"time"

	"github.com/stellar/wallet-backend/internal/data"
	"github.com/stellar/wallet-backend/internal/db"
	"github.com/stellar/wallet-backend/internal/db/dbtest"
	"github.com/stellar/wallet-backend/internal/indexer/types"
	"github.com/stellar/wallet-backend/internal/metrics"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestQueryResolver_TransactionByHash(t *testing.T) {
	ctx := context.Background()

	dbt := dbtest.Open(t)
	defer dbt.Close()
	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	cleanUpDB := func() {
		_, err = dbConnectionPool.ExecContext(ctx, `DELETE FROM transactions`)
		require.NoError(t, err)
	}

	t.Run("success", func(t *testing.T) {
		mockMetricsService := metrics.NewMockMetricsService()
		mockMetricsService.On("ObserveDBQueryDuration", "SELECT", "transactions", mock.Anything).Return()
		mockMetricsService.On("IncDBQuery", "SELECT", "transactions").Return()
		defer mockMetricsService.AssertExpectations(t)

		resolver := &queryResolver{
			&Resolver{
				models: &data.Models{
					Transactions: &data.TransactionModel{
						DB:             dbConnectionPool,
						MetricsService: mockMetricsService,
					},
				},
			},
		}

		expectedTx := &types.Transaction{
			Hash:            "tx1",
			ToID:            1,
			EnvelopeXDR:     "envelope1",
			ResultXDR:       "result1",
			MetaXDR:         "meta1",
			LedgerNumber:    1,
			LedgerCreatedAt: time.Now(),
		}

		dbErr := db.RunInTransaction(context.Background(), dbConnectionPool, nil, func(tx db.Transaction) error {
			_, err := tx.ExecContext(ctx, 
				`INSERT INTO transactions (hash, to_id, envelope_xdr, result_xdr, meta_xdr, ledger_number, ledger_created_at) VALUES ($1, $2, $3, $4, $5, $6, $7)`, 
			expectedTx.Hash, expectedTx.ToID, expectedTx.EnvelopeXDR, expectedTx.ResultXDR, expectedTx.MetaXDR, expectedTx.LedgerNumber, expectedTx.LedgerCreatedAt)
			require.NoError(t, err)
			return nil
		})
		require.NoError(t, dbErr)

		tx, err := resolver.TransactionByHash(ctx, "tx1")

		require.NoError(t, err)
		assert.Equal(t, expectedTx.Hash, tx.Hash)
		assert.Equal(t, expectedTx.ToID, tx.ToID)
		assert.Equal(t, expectedTx.EnvelopeXDR, tx.EnvelopeXDR)
		assert.Equal(t, expectedTx.ResultXDR, tx.ResultXDR)
		assert.Equal(t, expectedTx.MetaXDR, tx.MetaXDR)
		assert.Equal(t, expectedTx.LedgerNumber, tx.LedgerNumber)
		cleanUpDB()
	})
}
