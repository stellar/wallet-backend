// Unit tests for TrustlineAssetModel.
package data

import (
	"context"
	"testing"

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

func TestTrustlineAssetModel_BatchGetByIDs(t *testing.T) {
	ctx := context.Background()

	dbt := dbtest.Open(t)
	defer dbt.Close()
	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	cleanUpDB := func() {
		_, err = dbConnectionPool.ExecContext(ctx, `DELETE FROM trustline_assets`)
		require.NoError(t, err)
	}

	t.Run("returns empty for empty input", func(t *testing.T) {
		mockMetricsService := metrics.NewMockMetricsService()
		defer mockMetricsService.AssertExpectations(t)

		m := &TrustlineAssetModel{
			DB:             dbConnectionPool,
			MetricsService: mockMetricsService,
		}

		assets, err := m.BatchGetByIDs(ctx, []int64{})
		require.NoError(t, err)
		require.Nil(t, assets)
	})

	t.Run("returns assets for valid IDs", func(t *testing.T) {
		cleanUpDB()
		mockMetricsService := metrics.NewMockMetricsService()
		mockMetricsService.On("ObserveDBQueryDuration", "BatchInsert", "trustline_assets", mock.Anything).Return()
		mockMetricsService.On("IncDBQuery", "BatchInsert", "trustline_assets").Return()
		mockMetricsService.On("ObserveDBQueryDuration", "BatchGetByIDs", "trustline_assets", mock.Anything).Return()
		mockMetricsService.On("IncDBQuery", "BatchGetByIDs", "trustline_assets").Return()
		defer mockMetricsService.AssertExpectations(t)

		m := &TrustlineAssetModel{
			DB:             dbConnectionPool,
			MetricsService: mockMetricsService,
		}

		// Create assets with deterministic IDs
		id1 := DeterministicAssetID("USDC", "ISSUER1")
		id2 := DeterministicAssetID("EURC", "ISSUER2")
		assets := []TrustlineAsset{
			{ID: id1, Code: "USDC", Issuer: "ISSUER1"},
			{ID: id2, Code: "EURC", Issuer: "ISSUER2"},
		}

		pgxTx, err := dbConnectionPool.PgxPool().Begin(ctx)
		require.NoError(t, err)
		err = m.BatchInsert(ctx, pgxTx, assets)
		require.NoError(t, err)
		require.NoError(t, pgxTx.Commit(ctx))

		// Retrieve them
		retrievedAssets, err := m.BatchGetByIDs(ctx, []int64{id1, id2})
		require.NoError(t, err)
		require.Len(t, retrievedAssets, 2)

		// Build map for easier assertion
		assetMap := make(map[int64]*TrustlineAsset)
		for _, a := range retrievedAssets {
			assetMap[a.ID] = a
		}

		require.Equal(t, "USDC", assetMap[id1].Code)
		require.Equal(t, "ISSUER1", assetMap[id1].Issuer)
		require.Equal(t, "EURC", assetMap[id2].Code)
		require.Equal(t, "ISSUER2", assetMap[id2].Issuer)

		cleanUpDB()
	})

	t.Run("returns empty for non-existent IDs", func(t *testing.T) {
		cleanUpDB()
		mockMetricsService := metrics.NewMockMetricsService()
		mockMetricsService.On("ObserveDBQueryDuration", "BatchGetByIDs", "trustline_assets", mock.Anything).Return()
		mockMetricsService.On("IncDBQuery", "BatchGetByIDs", "trustline_assets").Return()
		defer mockMetricsService.AssertExpectations(t)

		m := &TrustlineAssetModel{
			DB:             dbConnectionPool,
			MetricsService: mockMetricsService,
		}

		assets, err := m.BatchGetByIDs(ctx, []int64{999, 1000})
		require.NoError(t, err)
		require.Empty(t, assets)

		cleanUpDB()
	})
}

func TestTrustlineAssetModel_BatchInsert(t *testing.T) {
	ctx := context.Background()

	dbt := dbtest.Open(t)
	defer dbt.Close()
	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	cleanUpDB := func() {
		_, err = dbConnectionPool.ExecContext(ctx, `DELETE FROM trustline_assets`)
		require.NoError(t, err)
	}

	t.Run("returns nil error for empty input", func(t *testing.T) {
		mockMetricsService := metrics.NewMockMetricsService()
		defer mockMetricsService.AssertExpectations(t)

		m := &TrustlineAssetModel{
			DB:             dbConnectionPool,
			MetricsService: mockMetricsService,
		}

		pgxTx, err := dbConnectionPool.PgxPool().Begin(ctx)
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
		mockMetricsService.On("ObserveDBQueryDuration", "BatchGetByIDs", "trustline_assets", mock.Anything).Return()
		mockMetricsService.On("IncDBQuery", "BatchGetByIDs", "trustline_assets").Return()
		defer mockMetricsService.AssertExpectations(t)

		m := &TrustlineAssetModel{
			DB:             dbConnectionPool,
			MetricsService: mockMetricsService,
		}

		pgxTx, err := dbConnectionPool.PgxPool().Begin(ctx)
		require.NoError(t, err)

		expectedID := DeterministicAssetID("USDC", "GBBD47IF6LWK7P7MDEVSCWR7DPUWV3NY3DTQEVFL4NAT4AQH3ZLLFLA5")
		assets := []TrustlineAsset{
			{ID: expectedID, Code: "USDC", Issuer: "GBBD47IF6LWK7P7MDEVSCWR7DPUWV3NY3DTQEVFL4NAT4AQH3ZLLFLA5"},
		}

		err = m.BatchInsert(ctx, pgxTx, assets)
		require.NoError(t, err)
		require.NoError(t, pgxTx.Commit(ctx))

		// Verify inserted asset can be retrieved
		retrieved, err := m.BatchGetByIDs(ctx, []int64{expectedID})
		require.NoError(t, err)
		require.Len(t, retrieved, 1)
		require.Equal(t, expectedID, retrieved[0].ID)
		require.Equal(t, "USDC", retrieved[0].Code)
		require.Equal(t, "GBBD47IF6LWK7P7MDEVSCWR7DPUWV3NY3DTQEVFL4NAT4AQH3ZLLFLA5", retrieved[0].Issuer)

		cleanUpDB()
	})

	t.Run("inserts multiple assets with deterministic IDs", func(t *testing.T) {
		cleanUpDB()
		mockMetricsService := metrics.NewMockMetricsService()
		mockMetricsService.On("ObserveDBQueryDuration", "BatchInsert", "trustline_assets", mock.Anything).Return()
		mockMetricsService.On("IncDBQuery", "BatchInsert", "trustline_assets").Return()
		mockMetricsService.On("ObserveDBQueryDuration", "BatchGetByIDs", "trustline_assets", mock.Anything).Return()
		mockMetricsService.On("IncDBQuery", "BatchGetByIDs", "trustline_assets").Return()
		defer mockMetricsService.AssertExpectations(t)

		m := &TrustlineAssetModel{
			DB:             dbConnectionPool,
			MetricsService: mockMetricsService,
		}

		pgxTx, err := dbConnectionPool.PgxPool().Begin(ctx)
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

		// Verify all assets can be retrieved
		retrieved, err := m.BatchGetByIDs(ctx, []int64{id1, id2, id3})
		require.NoError(t, err)
		require.Len(t, retrieved, 3)

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
		pgxTx1, err := dbConnectionPool.PgxPool().Begin(ctx)
		require.NoError(t, err)
		err = m.BatchInsert(ctx, pgxTx1, assets)
		require.NoError(t, err)
		require.NoError(t, pgxTx1.Commit(ctx))

		// Second insert - should succeed with ON CONFLICT DO NOTHING
		pgxTx2, err := dbConnectionPool.PgxPool().Begin(ctx)
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
		mockMetricsService.On("ObserveDBQueryDuration", "BatchGetByIDs", "trustline_assets", mock.Anything).Return()
		mockMetricsService.On("IncDBQuery", "BatchGetByIDs", "trustline_assets").Return()
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

		pgxTx, err := dbConnectionPool.PgxPool().Begin(ctx)
		require.NoError(t, err)
		err = m.BatchInsert(ctx, pgxTx, assets)
		require.NoError(t, err)
		require.NoError(t, pgxTx.Commit(ctx))

		// Verify both were inserted
		retrieved, err := m.BatchGetByIDs(ctx, []int64{id1, id2})
		require.NoError(t, err)
		require.Len(t, retrieved, 2)

		cleanUpDB()
	})
}

func TestTrustlineAsset_AssetKey(t *testing.T) {
	asset := &TrustlineAsset{
		ID:     1,
		Code:   "USDC",
		Issuer: "GBBD47IF6LWK7P7MDEVSCWR7DPUWV3NY3DTQEVFL4NAT4AQH3ZLLFLA5",
	}

	require.Equal(t, "USDC:GBBD47IF6LWK7P7MDEVSCWR7DPUWV3NY3DTQEVFL4NAT4AQH3ZLLFLA5", asset.AssetKey())
}
