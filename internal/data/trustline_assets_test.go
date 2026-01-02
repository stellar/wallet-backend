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
		mockMetricsService.On("ObserveDBQueryDuration", "BatchGetOrInsert", "trustline_assets", mock.Anything).Return()
		mockMetricsService.On("IncDBQuery", "BatchGetOrInsert", "trustline_assets").Return()
		mockMetricsService.On("ObserveDBQueryDuration", "BatchGetByIDs", "trustline_assets", mock.Anything).Return()
		mockMetricsService.On("IncDBQuery", "BatchGetByIDs", "trustline_assets").Return()
		defer mockMetricsService.AssertExpectations(t)

		m := &TrustlineAssetModel{
			DB:             dbConnectionPool,
			MetricsService: mockMetricsService,
		}

		// Create some assets using BatchGetOrCreateIDs
		assets := []TrustlineAsset{
			{Code: "USDC", Issuer: "ISSUER1"},
			{Code: "EURC", Issuer: "ISSUER2"},
		}
		assetIDs, err := m.BatchGetOrInsert(ctx, assets)
		require.NoError(t, err)
		id1 := assetIDs["USDC:ISSUER1"]
		id2 := assetIDs["EURC:ISSUER2"]

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

func TestTrustlineAssetModel_BatchGetOrInsert(t *testing.T) {
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

	t.Run("returns empty map for empty input", func(t *testing.T) {
		mockMetricsService := metrics.NewMockMetricsService()
		defer mockMetricsService.AssertExpectations(t)

		m := &TrustlineAssetModel{
			DB:             dbConnectionPool,
			MetricsService: mockMetricsService,
		}

		result, err := m.BatchGetOrInsert(ctx, []TrustlineAsset{})
		require.NoError(t, err)
		require.Empty(t, result)
	})

	t.Run("creates single new asset and returns ID", func(t *testing.T) {
		cleanUpDB()
		mockMetricsService := metrics.NewMockMetricsService()
		mockMetricsService.On("ObserveDBQueryDuration", "BatchGetOrInsert", "trustline_assets", mock.Anything).Return()
		mockMetricsService.On("IncDBQuery", "BatchGetOrInsert", "trustline_assets").Return()
		defer mockMetricsService.AssertExpectations(t)

		m := &TrustlineAssetModel{
			DB:             dbConnectionPool,
			MetricsService: mockMetricsService,
		}

		assets := []TrustlineAsset{
			{Code: "USDC", Issuer: "GBBD47IF6LWK7P7MDEVSCWR7DPUWV3NY3DTQEVFL4NAT4AQH3ZLLFLA5"},
		}

		result, err := m.BatchGetOrInsert(ctx, assets)
		require.NoError(t, err)
		require.Len(t, result, 1)
		require.Greater(t, result["USDC:GBBD47IF6LWK7P7MDEVSCWR7DPUWV3NY3DTQEVFL4NAT4AQH3ZLLFLA5"], int64(0))

		cleanUpDB()
	})

	t.Run("creates multiple new assets and returns IDs", func(t *testing.T) {
		cleanUpDB()
		mockMetricsService := metrics.NewMockMetricsService()
		mockMetricsService.On("ObserveDBQueryDuration", "BatchGetOrInsert", "trustline_assets", mock.Anything).Return()
		mockMetricsService.On("IncDBQuery", "BatchGetOrInsert", "trustline_assets").Return()
		defer mockMetricsService.AssertExpectations(t)

		m := &TrustlineAssetModel{
			DB:             dbConnectionPool,
			MetricsService: mockMetricsService,
		}

		assets := []TrustlineAsset{
			{Code: "USDC", Issuer: "ISSUER1"},
			{Code: "EURC", Issuer: "ISSUER2"},
			{Code: "BTC", Issuer: "ISSUER3"},
		}

		result, err := m.BatchGetOrInsert(ctx, assets)
		require.NoError(t, err)
		require.Len(t, result, 3)

		require.Greater(t, result["USDC:ISSUER1"], int64(0))
		require.Greater(t, result["EURC:ISSUER2"], int64(0))
		require.Greater(t, result["BTC:ISSUER3"], int64(0))

		// Verify all IDs are unique
		require.NotEqual(t, result["USDC:ISSUER1"], result["EURC:ISSUER2"])
		require.NotEqual(t, result["EURC:ISSUER2"], result["BTC:ISSUER3"])

		cleanUpDB()
	})

	t.Run("returns existing IDs for already existing assets (fast path)", func(t *testing.T) {
		cleanUpDB()
		mockMetricsService := metrics.NewMockMetricsService()
		mockMetricsService.On("ObserveDBQueryDuration", "BatchGetOrInsert", "trustline_assets", mock.Anything).Return()
		mockMetricsService.On("IncDBQuery", "BatchGetOrInsert", "trustline_assets").Return()
		defer mockMetricsService.AssertExpectations(t)

		m := &TrustlineAssetModel{
			DB:             dbConnectionPool,
			MetricsService: mockMetricsService,
		}

		assets := []TrustlineAsset{
			{Code: "USDC", Issuer: "ISSUER1"},
			{Code: "EURC", Issuer: "ISSUER2"},
		}

		// First call - creates assets
		result1, err := m.BatchGetOrInsert(ctx, assets)
		require.NoError(t, err)
		require.Len(t, result1, 2)

		// Second call - should return same IDs (fast path - all exist)
		result2, err := m.BatchGetOrInsert(ctx, assets)
		require.NoError(t, err)
		require.Len(t, result2, 2)

		// IDs should match
		require.Equal(t, result1["USDC:ISSUER1"], result2["USDC:ISSUER1"])
		require.Equal(t, result1["EURC:ISSUER2"], result2["EURC:ISSUER2"])

		cleanUpDB()
	})

	t.Run("handles mix of existing and new assets", func(t *testing.T) {
		cleanUpDB()
		mockMetricsService := metrics.NewMockMetricsService()
		mockMetricsService.On("ObserveDBQueryDuration", "BatchGetOrCreateIDs", "trustline_assets", mock.Anything).Return()
		mockMetricsService.On("IncDBQuery", "BatchGetOrCreateIDs", "trustline_assets").Return()
		defer mockMetricsService.AssertExpectations(t)

		m := &TrustlineAssetModel{
			DB:             dbConnectionPool,
			MetricsService: mockMetricsService,
		}

		// First call - create initial assets
		initialAssets := []TrustlineAsset{
			{Code: "USDC", Issuer: "ISSUER1"},
		}
		result1, err := m.BatchGetOrInsert(ctx, initialAssets)
		require.NoError(t, err)
		require.Len(t, result1, 1)

		// Second call - mix of existing and new
		mixedAssets := []TrustlineAsset{
			{Code: "USDC", Issuer: "ISSUER1"}, // existing
			{Code: "EURC", Issuer: "ISSUER2"}, // new
			{Code: "BTC", Issuer: "ISSUER3"},  // new
		}
		result2, err := m.BatchGetOrInsert(ctx, mixedAssets)
		require.NoError(t, err)
		require.Len(t, result2, 3)

		// Existing asset should have same ID
		require.Equal(t, result1["USDC:ISSUER1"], result2["USDC:ISSUER1"])

		// New assets should have unique IDs
		require.Greater(t, result2["EURC:ISSUER2"], int64(0))
		require.Greater(t, result2["BTC:ISSUER3"], int64(0))
		require.NotEqual(t, result2["USDC:ISSUER1"], result2["EURC:ISSUER2"])
		require.NotEqual(t, result2["EURC:ISSUER2"], result2["BTC:ISSUER3"])

		cleanUpDB()
	})

	t.Run("same asset different issuers get different IDs", func(t *testing.T) {
		cleanUpDB()
		mockMetricsService := metrics.NewMockMetricsService()
		mockMetricsService.On("ObserveDBQueryDuration", "BatchGetOrInsert", "trustline_assets", mock.Anything).Return()
		mockMetricsService.On("IncDBQuery", "BatchGetOrInsert", "trustline_assets").Return()
		defer mockMetricsService.AssertExpectations(t)

		m := &TrustlineAssetModel{
			DB:             dbConnectionPool,
			MetricsService: mockMetricsService,
		}

		// Same asset code but different issuers
		assets := []TrustlineAsset{
			{Code: "USDC", Issuer: "ISSUER1"},
			{Code: "USDC", Issuer: "ISSUER2"},
		}

		result, err := m.BatchGetOrInsert(ctx, assets)
		require.NoError(t, err)
		require.Len(t, result, 2)

		require.Greater(t, result["USDC:ISSUER1"], int64(0))
		require.Greater(t, result["USDC:ISSUER2"], int64(0))
		require.NotEqual(t, result["USDC:ISSUER1"], result["USDC:ISSUER2"])

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
