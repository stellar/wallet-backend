// Health handler tests for wallet backend health check endpoint
package httphandler

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/stellar/wallet-backend/internal/apptracker"
	"github.com/stellar/wallet-backend/internal/data"
	"github.com/stellar/wallet-backend/internal/db"
	"github.com/stellar/wallet-backend/internal/db/dbtest"
	"github.com/stellar/wallet-backend/internal/entities"
	"github.com/stellar/wallet-backend/internal/metrics"
	"github.com/stellar/wallet-backend/internal/services"
)

func TestHealthHandler_GetHealth(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()

	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	mockMetricsService := metrics.NewMockMetricsService()
	mockMetricsService.On("ObserveDBQueryDuration", "SELECT", "ingest_store", mock.AnythingOfType("float64")).Return()
	mockMetricsService.On("IncDBQuery", "SELECT", "ingest_store").Return()
	defer mockMetricsService.AssertExpectations(t)

	models, err := data.NewModels(dbConnectionPool, mockMetricsService)
	require.NoError(t, err)

	t.Run("healthy - RPC and backend in sync", func(t *testing.T) {
		ctx := context.Background()
		_, err := dbConnectionPool.ExecContext(ctx, "DELETE FROM ingest_store WHERE key = 'live_ingest_cursor'")
		require.NoError(t, err)
		_, err = dbConnectionPool.ExecContext(ctx, "INSERT INTO ingest_store (key, value) VALUES ('live_ingest_cursor', $1)", uint32(98))
		require.NoError(t, err)

		mockRPCService := &services.RPCServiceMock{}
		mockAppTracker := apptracker.NewMockAppTracker(t)
		mockAppTracker.On("CaptureException", mock.Anything).Return().Maybe()
		defer mockAppTracker.AssertExpectations(t)

		handler := &HealthHandler{
			Models:     models,
			RPCService: mockRPCService,
			AppTracker: mockAppTracker,
		}

		rpcHealthResult := entities.RPCGetHealthResult{
			Status:       "healthy",
			LatestLedger: 100,
		}
		mockRPCService.On("GetHealth").Return(rpcHealthResult, nil)
		defer mockRPCService.AssertExpectations(t)

		req := httptest.NewRequest(http.MethodGet, "/health", nil)
		recorder := httptest.NewRecorder()
		handler.GetHealth(recorder, req)
		assert.Equal(t, http.StatusOK, recorder.Code)

		var response map[string]any
		err = json.Unmarshal(recorder.Body.Bytes(), &response)
		require.NoError(t, err)

		assert.Equal(t, "ok", response["status"])
		assert.Equal(t, float64(98), response["backend_latest_ledger"])

		_, cleanupErr := dbConnectionPool.ExecContext(ctx, "DELETE FROM ingest_store WHERE key = 'live_ingest_cursor'")
		require.NoError(t, cleanupErr)
	})

	t.Run("unhealthy - RPC service error", func(t *testing.T) {
		ctx := context.Background()
		_, err := dbConnectionPool.ExecContext(ctx, "DELETE FROM ingest_store WHERE key = 'live_ingest_cursor'")
		require.NoError(t, err)

		mockRPCService := &services.RPCServiceMock{}
		mockAppTracker := apptracker.NewMockAppTracker(t)
		mockAppTracker.On("CaptureException", mock.Anything).Return().Maybe()
		defer mockAppTracker.AssertExpectations(t)

		handler := &HealthHandler{
			Models:     models,
			RPCService: mockRPCService,
			AppTracker: mockAppTracker,
		}

		mockRPCService.On("GetHealth").Return(entities.RPCGetHealthResult{}, errors.New("RPC connection failed"))
		defer mockRPCService.AssertExpectations(t)

		req := httptest.NewRequest(http.MethodGet, "/health", nil)
		recorder := httptest.NewRecorder()
		handler.GetHealth(recorder, req)
		assert.Equal(t, http.StatusInternalServerError, recorder.Code)
		assert.Contains(t, recorder.Body.String(), "failed to get RPC health")

		_, cleanupErr := dbConnectionPool.ExecContext(ctx, "DELETE FROM ingest_store WHERE key = 'live_ingest_cursor'")
		require.NoError(t, cleanupErr)
	})

	t.Run("unhealthy - RPC status not healthy", func(t *testing.T) {
		ctx := context.Background()
		_, err := dbConnectionPool.ExecContext(ctx, "DELETE FROM ingest_store WHERE key = 'live_ingest_cursor'")
		require.NoError(t, err)
		_, err = dbConnectionPool.ExecContext(ctx, "INSERT INTO ingest_store (key, value) VALUES ('live_ingest_cursor', $1)", uint32(98))
		require.NoError(t, err)

		mockRPCService := &services.RPCServiceMock{}
		mockAppTracker := apptracker.NewMockAppTracker(t)
		mockAppTracker.On("CaptureException", mock.Anything).Return().Maybe()
		defer mockAppTracker.AssertExpectations(t)

		handler := &HealthHandler{
			Models:     models,
			RPCService: mockRPCService,
			AppTracker: mockAppTracker,
		}

		rpcHealthResult := entities.RPCGetHealthResult{
			Status:       "unhealthy",
			LatestLedger: 100,
		}
		mockRPCService.On("GetHealth").Return(rpcHealthResult, nil)
		defer mockRPCService.AssertExpectations(t)

		req := httptest.NewRequest(http.MethodGet, "/health", nil)
		recorder := httptest.NewRecorder()
		handler.GetHealth(recorder, req)
		assert.Equal(t, http.StatusServiceUnavailable, recorder.Code)

		_, cleanupErr := dbConnectionPool.ExecContext(ctx, "DELETE FROM ingest_store WHERE key = 'live_ingest_cursor'")
		require.NoError(t, cleanupErr)
	})

	t.Run("unhealthy - backend significantly behind", func(t *testing.T) {
		ctx := context.Background()
		_, err := dbConnectionPool.ExecContext(ctx, "DELETE FROM ingest_store WHERE key = 'live_ingest_cursor'")
		require.NoError(t, err)
		_, err = dbConnectionPool.ExecContext(ctx, "INSERT INTO ingest_store (key, value) VALUES ('live_ingest_cursor', $1)", uint32(900))
		require.NoError(t, err)

		mockRPCService := &services.RPCServiceMock{}
		mockAppTracker := apptracker.NewMockAppTracker(t)
		mockAppTracker.On("CaptureException", mock.Anything).Return().Maybe()
		defer mockAppTracker.AssertExpectations(t)

		handler := &HealthHandler{
			Models:     models,
			RPCService: mockRPCService,
			AppTracker: mockAppTracker,
		}

		rpcHealthResult := entities.RPCGetHealthResult{
			Status:       "healthy",
			LatestLedger: 1000,
		}
		mockRPCService.On("GetHealth").Return(rpcHealthResult, nil)
		defer mockRPCService.AssertExpectations(t)

		req := httptest.NewRequest(http.MethodGet, "/health", nil)
		recorder := httptest.NewRecorder()
		handler.GetHealth(recorder, req)
		assert.Equal(t, http.StatusServiceUnavailable, recorder.Code)

		var response map[string]any
		err = json.Unmarshal(recorder.Body.Bytes(), &response)
		require.NoError(t, err)

		assert.Contains(t, recorder.Body.String(), "wallet backend is not in sync with the RPC")
		assert.Equal(t, float64(1000), response["extras"].(map[string]any)["rpc_latest_ledger"])
		assert.Equal(t, float64(900), response["extras"].(map[string]any)["backend_latest_ledger"])

		_, cleanupErr := dbConnectionPool.ExecContext(ctx, "DELETE FROM ingest_store WHERE key = 'live_ingest_cursor'")
		require.NoError(t, cleanupErr)
	})
}
