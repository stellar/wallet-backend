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

func TestTrustlineAssetModel_GetOrCreateID(t *testing.T) {
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

	t.Run("creates new asset and returns ID", func(t *testing.T) {
		cleanUpDB()
		mockMetricsService := metrics.NewMockMetricsService()
		mockMetricsService.On("ObserveDBQueryDuration", "GetOrCreateID", "trustline_assets", mock.Anything).Return()
		mockMetricsService.On("IncDBQuery", "GetOrCreateID", "trustline_assets").Return()
		defer mockMetricsService.AssertExpectations(t)

		m := &TrustlineAssetModel{
			DB:             dbConnectionPool,
			MetricsService: mockMetricsService,
		}

		id, err := m.GetOrCreateID(ctx, "USDC", "GBBD47IF6LWK7P7MDEVSCWR7DPUWV3NY3DTQEVFL4NAT4AQH3ZLLFLA5")
		require.NoError(t, err)
		require.Greater(t, id, int64(0))

		cleanUpDB()
	})

	t.Run("returns existing ID for same asset", func(t *testing.T) {
		cleanUpDB()
		mockMetricsService := metrics.NewMockMetricsService()
		mockMetricsService.On("ObserveDBQueryDuration", "GetOrCreateID", "trustline_assets", mock.Anything).Return()
		mockMetricsService.On("IncDBQuery", "GetOrCreateID", "trustline_assets").Return()
		defer mockMetricsService.AssertExpectations(t)

		m := &TrustlineAssetModel{
			DB:             dbConnectionPool,
			MetricsService: mockMetricsService,
		}

		// First call - creates
		id1, err := m.GetOrCreateID(ctx, "USDC", "GBBD47IF6LWK7P7MDEVSCWR7DPUWV3NY3DTQEVFL4NAT4AQH3ZLLFLA5")
		require.NoError(t, err)

		// Second call - returns existing
		id2, err := m.GetOrCreateID(ctx, "USDC", "GBBD47IF6LWK7P7MDEVSCWR7DPUWV3NY3DTQEVFL4NAT4AQH3ZLLFLA5")
		require.NoError(t, err)

		require.Equal(t, id1, id2)

		cleanUpDB()
	})

	t.Run("different assets get different IDs", func(t *testing.T) {
		cleanUpDB()
		mockMetricsService := metrics.NewMockMetricsService()
		mockMetricsService.On("ObserveDBQueryDuration", "GetOrCreateID", "trustline_assets", mock.Anything).Return()
		mockMetricsService.On("IncDBQuery", "GetOrCreateID", "trustline_assets").Return()
		defer mockMetricsService.AssertExpectations(t)

		m := &TrustlineAssetModel{
			DB:             dbConnectionPool,
			MetricsService: mockMetricsService,
		}

		id1, err := m.GetOrCreateID(ctx, "USDC", "GBBD47IF6LWK7P7MDEVSCWR7DPUWV3NY3DTQEVFL4NAT4AQH3ZLLFLA5")
		require.NoError(t, err)

		id2, err := m.GetOrCreateID(ctx, "EURC", "GBBD47IF6LWK7P7MDEVSCWR7DPUWV3NY3DTQEVFL4NAT4AQH3ZLLFLA5")
		require.NoError(t, err)

		require.NotEqual(t, id1, id2)

		cleanUpDB()
	})
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
		mockMetricsService.On("ObserveDBQueryDuration", "GetOrCreateID", "trustline_assets", mock.Anything).Return()
		mockMetricsService.On("IncDBQuery", "GetOrCreateID", "trustline_assets").Return()
		mockMetricsService.On("ObserveDBQueryDuration", "BatchGetByIDs", "trustline_assets", mock.Anything).Return()
		mockMetricsService.On("IncDBQuery", "BatchGetByIDs", "trustline_assets").Return()
		defer mockMetricsService.AssertExpectations(t)

		m := &TrustlineAssetModel{
			DB:             dbConnectionPool,
			MetricsService: mockMetricsService,
		}

		// Create some assets
		id1, err := m.GetOrCreateID(ctx, "USDC", "ISSUER1")
		require.NoError(t, err)
		id2, err := m.GetOrCreateID(ctx, "EURC", "ISSUER2")
		require.NoError(t, err)

		// Retrieve them
		assets, err := m.BatchGetByIDs(ctx, []int64{id1, id2})
		require.NoError(t, err)
		require.Len(t, assets, 2)

		// Build map for easier assertion
		assetMap := make(map[int64]*TrustlineAsset)
		for _, a := range assets {
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

	t.Run("returns empty map for empty input", func(t *testing.T) {
		mockMetricsService := metrics.NewMockMetricsService()
		defer mockMetricsService.AssertExpectations(t)

		m := &TrustlineAssetModel{
			DB:             dbConnectionPool,
			MetricsService: mockMetricsService,
		}

		result, err := m.BatchInsert(ctx, []TrustlineAsset{})
		require.NoError(t, err)
		require.Empty(t, result)
	})

	t.Run("inserts multiple new assets", func(t *testing.T) {
		cleanUpDB()
		mockMetricsService := metrics.NewMockMetricsService()
		mockMetricsService.On("ObserveDBQueryDuration", "BatchInsert", "trustline_assets", mock.Anything).Return()
		mockMetricsService.On("ObserveDBBatchSize", "BatchInsert", "trustline_assets", 3).Return()
		mockMetricsService.On("IncDBQuery", "BatchInsert", "trustline_assets").Return()
		defer mockMetricsService.AssertExpectations(t)

		m := &TrustlineAssetModel{
			DB:             dbConnectionPool,
			MetricsService: mockMetricsService,
		}

		assets := []TrustlineAsset{
			{Code: "USDC", Issuer: "ISSUER1"},
			{Code: "EURC", Issuer: "ISSUER2"},
			{Code: "XLM", Issuer: "ISSUER3"},
		}

		result, err := m.BatchInsert(ctx, assets)
		require.NoError(t, err)
		require.Len(t, result, 3)

		require.Greater(t, result["USDC:ISSUER1"], int64(0))
		require.Greater(t, result["EURC:ISSUER2"], int64(0))
		require.Greater(t, result["XLM:ISSUER3"], int64(0))

		// Verify all IDs are unique
		require.NotEqual(t, result["USDC:ISSUER1"], result["EURC:ISSUER2"])
		require.NotEqual(t, result["EURC:ISSUER2"], result["XLM:ISSUER3"])

		cleanUpDB()
	})

	t.Run("returns existing IDs for duplicates", func(t *testing.T) {
		cleanUpDB()
		mockMetricsService := metrics.NewMockMetricsService()
		mockMetricsService.On("ObserveDBQueryDuration", "BatchInsert", "trustline_assets", mock.Anything).Return()
		mockMetricsService.On("ObserveDBBatchSize", "BatchInsert", "trustline_assets", mock.Anything).Return()
		mockMetricsService.On("IncDBQuery", "BatchInsert", "trustline_assets").Return()
		defer mockMetricsService.AssertExpectations(t)

		m := &TrustlineAssetModel{
			DB:             dbConnectionPool,
			MetricsService: mockMetricsService,
		}

		// First insert
		assets1 := []TrustlineAsset{
			{Code: "USDC", Issuer: "ISSUER1"},
		}
		result1, err := m.BatchInsert(ctx, assets1)
		require.NoError(t, err)
		require.Len(t, result1, 1)

		// Second insert with same asset plus new one
		assets2 := []TrustlineAsset{
			{Code: "USDC", Issuer: "ISSUER1"}, // duplicate
			{Code: "EURC", Issuer: "ISSUER2"}, // new
		}
		result2, err := m.BatchInsert(ctx, assets2)
		require.NoError(t, err)
		require.Len(t, result2, 2)

		// Same asset should have same ID
		require.Equal(t, result1["USDC:ISSUER1"], result2["USDC:ISSUER1"])

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
