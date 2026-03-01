// Unit tests for TrustlineAssetModel.
package data

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/stellar/wallet-backend/internal/db"
	"github.com/stellar/wallet-backend/internal/db/dbtest"
	"github.com/stellar/wallet-backend/internal/metrics"
)

func TestDeterministicAssetID(t *testing.T) {
	// Test deterministic behavior - same input should always produce same output
	id1 := DeterministicAssetID("USDC", "ISSUER1")
	id2 := DeterministicAssetID("USDC", "ISSUER1")
	require.Equal(t, id1, id2, "same inputs should produce same ID")

	// Test different assets produce different IDs
	id3 := DeterministicAssetID("EURC", "ISSUER1")
	require.NotEqual(t, id1, id3, "different codes should produce different IDs")

	// Test same code with different issuers produces different IDs
	id4 := DeterministicAssetID("USDC", "ISSUER2")
	require.NotEqual(t, id1, id4, "different issuers should produce different IDs")
}

// Helper function to verify asset exists in database
func verifyAssetExists(t *testing.T, ctx context.Context, dbPool db.ConnectionPool, id uuid.UUID, expectedCode, expectedIssuer string) {
	var code, issuer string
	err := dbPool.Pool().QueryRow(ctx, `SELECT code, issuer FROM trustline_assets WHERE id = $1`, id).Scan(&code, &issuer)
	require.NoError(t, err)
	require.Equal(t, expectedCode, code)
	require.Equal(t, expectedIssuer, issuer)
}

// Helper function to count assets by IDs
func countAssets(t *testing.T, ctx context.Context, dbPool db.ConnectionPool, ids []uuid.UUID) int {
	var count int
	err := dbPool.Pool().QueryRow(ctx, `SELECT COUNT(*) FROM trustline_assets WHERE id = ANY($1)`, ids).Scan(&count)
	require.NoError(t, err)
	return count
}

func TestTrustlineAssetModel_BatchInsert(t *testing.T) {
	ctx := context.Background()

	dbt := dbtest.Open(t)
	defer dbt.Close()
	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	cleanUpDB := func() {
		_, err = dbConnectionPool.Pool().Exec(ctx, `DELETE FROM trustline_assets`)
		require.NoError(t, err)
	}

	t.Run("returns nil error for empty input", func(t *testing.T) {
		mockMetricsService := metrics.NewMockMetricsService()
		defer mockMetricsService.AssertExpectations(t)

		m := &TrustlineAssetModel{
			DB:             dbConnectionPool,
			MetricsService: mockMetricsService,
		}

		pgxTx, err := dbConnectionPool.Pool().Begin(ctx)
		require.NoError(t, err)
		defer pgxTx.Rollback(ctx) //nolint:errcheck

		err = m.BatchInsert(ctx, pgxTx, []TrustlineAsset{})
		require.NoError(t, err)
	})

	t.Run("inserts single asset with deterministic ID", func(t *testing.T) {
		cleanUpDB()
		mockMetricsService := metrics.NewMockMetricsService()
		mockMetricsService.On("ObserveDBQueryDuration", "BatchInsert", "trustline_assets", mock.Anything).Return()
		mockMetricsService.On("IncDBQuery", "BatchInsert", "trustline_assets").Return()
		defer mockMetricsService.AssertExpectations(t)

		m := &TrustlineAssetModel{
			DB:             dbConnectionPool,
			MetricsService: mockMetricsService,
		}

		pgxTx, err := dbConnectionPool.Pool().Begin(ctx)
		require.NoError(t, err)

		expectedID := DeterministicAssetID("USDC", "GBBD47IF6LWK7P7MDEVSCWR7DPUWV3NY3DTQEVFL4NAT4AQH3ZLLFLA5")
		assets := []TrustlineAsset{
			{ID: expectedID, Code: "USDC", Issuer: "GBBD47IF6LWK7P7MDEVSCWR7DPUWV3NY3DTQEVFL4NAT4AQH3ZLLFLA5"},
		}

		err = m.BatchInsert(ctx, pgxTx, assets)
		require.NoError(t, err)
		require.NoError(t, pgxTx.Commit(ctx))

		// Verify inserted asset exists in database
		verifyAssetExists(t, ctx, dbConnectionPool, expectedID, "USDC", "GBBD47IF6LWK7P7MDEVSCWR7DPUWV3NY3DTQEVFL4NAT4AQH3ZLLFLA5")

		cleanUpDB()
	})

	t.Run("inserts multiple assets with deterministic IDs", func(t *testing.T) {
		cleanUpDB()
		mockMetricsService := metrics.NewMockMetricsService()
		mockMetricsService.On("ObserveDBQueryDuration", "BatchInsert", "trustline_assets", mock.Anything).Return()
		mockMetricsService.On("IncDBQuery", "BatchInsert", "trustline_assets").Return()
		defer mockMetricsService.AssertExpectations(t)

		m := &TrustlineAssetModel{
			DB:             dbConnectionPool,
			MetricsService: mockMetricsService,
		}

		pgxTx, err := dbConnectionPool.Pool().Begin(ctx)
		require.NoError(t, err)

		id1 := DeterministicAssetID("USDC", "ISSUER1")
		id2 := DeterministicAssetID("EURC", "ISSUER2")
		id3 := DeterministicAssetID("BTC", "ISSUER3")
		assets := []TrustlineAsset{
			{ID: id1, Code: "USDC", Issuer: "ISSUER1"},
			{ID: id2, Code: "EURC", Issuer: "ISSUER2"},
			{ID: id3, Code: "BTC", Issuer: "ISSUER3"},
		}

		err = m.BatchInsert(ctx, pgxTx, assets)
		require.NoError(t, err)
		require.NoError(t, pgxTx.Commit(ctx))

		// Verify all IDs are unique
		require.NotEqual(t, id1, id2)
		require.NotEqual(t, id2, id3)

		// Verify all assets exist in database
		count := countAssets(t, ctx, dbConnectionPool, []uuid.UUID{id1, id2, id3})
		require.Equal(t, 3, count)

		cleanUpDB()
	})

	t.Run("idempotent - duplicate insert does not error", func(t *testing.T) {
		cleanUpDB()
		mockMetricsService := metrics.NewMockMetricsService()
		mockMetricsService.On("ObserveDBQueryDuration", "BatchInsert", "trustline_assets", mock.Anything).Return()
		mockMetricsService.On("IncDBQuery", "BatchInsert", "trustline_assets").Return()
		defer mockMetricsService.AssertExpectations(t)

		m := &TrustlineAssetModel{
			DB:             dbConnectionPool,
			MetricsService: mockMetricsService,
		}

		id1 := DeterministicAssetID("USDC", "ISSUER1")
		id2 := DeterministicAssetID("EURC", "ISSUER2")
		assets := []TrustlineAsset{
			{ID: id1, Code: "USDC", Issuer: "ISSUER1"},
			{ID: id2, Code: "EURC", Issuer: "ISSUER2"},
		}

		// First insert
		pgxTx1, err := dbConnectionPool.Pool().Begin(ctx)
		require.NoError(t, err)
		err = m.BatchInsert(ctx, pgxTx1, assets)
		require.NoError(t, err)
		require.NoError(t, pgxTx1.Commit(ctx))

		// Second insert - should succeed with ON CONFLICT DO NOTHING
		pgxTx2, err := dbConnectionPool.Pool().Begin(ctx)
		require.NoError(t, err)
		err = m.BatchInsert(ctx, pgxTx2, assets)
		require.NoError(t, err)
		require.NoError(t, pgxTx2.Commit(ctx))

		cleanUpDB()
	})

	t.Run("same asset code different issuers get different IDs", func(t *testing.T) {
		cleanUpDB()
		mockMetricsService := metrics.NewMockMetricsService()
		mockMetricsService.On("ObserveDBQueryDuration", "BatchInsert", "trustline_assets", mock.Anything).Return()
		mockMetricsService.On("IncDBQuery", "BatchInsert", "trustline_assets").Return()
		defer mockMetricsService.AssertExpectations(t)

		m := &TrustlineAssetModel{
			DB:             dbConnectionPool,
			MetricsService: mockMetricsService,
		}

		// Same asset code but different issuers
		id1 := DeterministicAssetID("USDC", "ISSUER1")
		id2 := DeterministicAssetID("USDC", "ISSUER2")
		require.NotEqual(t, id1, id2, "same code with different issuers should have different IDs")

		assets := []TrustlineAsset{
			{ID: id1, Code: "USDC", Issuer: "ISSUER1"},
			{ID: id2, Code: "USDC", Issuer: "ISSUER2"},
		}

		pgxTx, err := dbConnectionPool.Pool().Begin(ctx)
		require.NoError(t, err)
		err = m.BatchInsert(ctx, pgxTx, assets)
		require.NoError(t, err)
		require.NoError(t, pgxTx.Commit(ctx))

		// Verify both were inserted
		count := countAssets(t, ctx, dbConnectionPool, []uuid.UUID{id1, id2})
		require.Equal(t, 2, count)

		cleanUpDB()
	})
}

func TestTrustlineAsset_AssetKey(t *testing.T) {
	asset := &TrustlineAsset{
		ID:     DeterministicAssetID("USDC", "GBBD47IF6LWK7P7MDEVSCWR7DPUWV3NY3DTQEVFL4NAT4AQH3ZLLFLA5"),
		Code:   "USDC",
		Issuer: "GBBD47IF6LWK7P7MDEVSCWR7DPUWV3NY3DTQEVFL4NAT4AQH3ZLLFLA5",
	}

	require.Equal(t, "USDC:GBBD47IF6LWK7P7MDEVSCWR7DPUWV3NY3DTQEVFL4NAT4AQH3ZLLFLA5", asset.AssetKey())
}
