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

	testCases := []struct {
		name                    string
		rpcHealthResult         entities.RPCGetHealthResult
		rpcHealthError          error
		backendLatestLedger     uint32
		hasCursorInDB           bool
		expectedStatusCode      int
		expectedResponseStatus  string
		shouldCheckResponseBody bool
	}{
		{
			name: "healthy - RPC and backend in sync",
			rpcHealthResult: entities.RPCGetHealthResult{
				Status:       "healthy",
				LatestLedger: 100,
			},
			rpcHealthError:          nil,
			backendLatestLedger:     98,
			hasCursorInDB:           true,
			expectedStatusCode:      http.StatusOK,
			expectedResponseStatus:  "ok",
			shouldCheckResponseBody: true,
		},
		{
			name: "healthy - at threshold boundary (exactly 5 ledgers behind)",
			rpcHealthResult: entities.RPCGetHealthResult{
				Status:       "healthy",
				LatestLedger: 105,
			},
			rpcHealthError:          nil,
			backendLatestLedger:     100,
			hasCursorInDB:           true,
			expectedStatusCode:      http.StatusOK,
			expectedResponseStatus:  "ok",
			shouldCheckResponseBody: true,
		},
		{
			name:                    "unhealthy - RPC service error",
			rpcHealthResult:         entities.RPCGetHealthResult{},
			rpcHealthError:          errors.New("RPC connection failed"),
			backendLatestLedger:     0,
			hasCursorInDB:           false,
			expectedStatusCode:      http.StatusInternalServerError,
			expectedResponseStatus:  "",
			shouldCheckResponseBody: false,
		},
		{
			name: "unhealthy - RPC status not healthy",
			rpcHealthResult: entities.RPCGetHealthResult{
				Status:       "unhealthy",
				LatestLedger: 100,
			},
			rpcHealthError:          nil,
			backendLatestLedger:     98,
			hasCursorInDB:           true,
			expectedStatusCode:      http.StatusInternalServerError,
			expectedResponseStatus:  "",
			shouldCheckResponseBody: false,
		},
		{
			name: "unhealthy - backend significantly behind",
			rpcHealthResult: entities.RPCGetHealthResult{
				Status:       "healthy",
				LatestLedger: 1000,
			},
			rpcHealthError:          nil,
			backendLatestLedger:     900,
			hasCursorInDB:           true,
			expectedStatusCode:      http.StatusInternalServerError,
			expectedResponseStatus:  "",
			shouldCheckResponseBody: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Setup database state
			ctx := context.Background()
			_, err := dbConnectionPool.ExecContext(ctx, "DELETE FROM ingest_store WHERE key = 'live_ingest_cursor'")
			require.NoError(t, err)

			if tc.hasCursorInDB {
				_, err = dbConnectionPool.ExecContext(ctx, "INSERT INTO ingest_store (key, value) VALUES ('live_ingest_cursor', $1)", tc.backendLatestLedger)
				require.NoError(t, err)
			}

			// Setup mocks
			mockRPCService := &services.RPCServiceMock{}
			mockAppTracker := apptracker.NewMockAppTracker(t)
			mockAppTracker.On("CaptureException", mock.Anything).Return().Maybe()
			defer mockAppTracker.AssertExpectations(t)

			handler := &HealthHandler{
				Models:     models,
				RPCService: mockRPCService,
				AppTracker: mockAppTracker,
			}

			mockRPCService.On("GetHealth").Return(tc.rpcHealthResult, tc.rpcHealthError)
			defer mockRPCService.AssertExpectations(t)

			req := httptest.NewRequest(http.MethodGet, "/health", nil)
			recorder := httptest.NewRecorder()
			handler.GetHealth(recorder, req)
			assert.Equal(t, tc.expectedStatusCode, recorder.Code)

			if tc.shouldCheckResponseBody {
				var response map[string]interface{}
				err := json.Unmarshal(recorder.Body.Bytes(), &response)
				require.NoError(t, err)

				assert.Equal(t, tc.expectedResponseStatus, response["status"])
				assert.Equal(t, float64(tc.rpcHealthResult.LatestLedger), response["rpc_latest_ledger"])

				expectedBackendLedger := uint32(0)
				if tc.hasCursorInDB {
					expectedBackendLedger = tc.backendLatestLedger
				}
				assert.Equal(t, float64(expectedBackendLedger), response["backend_latest_ledger"])
			}

			// Clean up
			_, cleanupErr := dbConnectionPool.ExecContext(ctx, "DELETE FROM ingest_store WHERE key = 'live_ingest_cursor'")
			require.NoError(t, cleanupErr)
		})
	}
}

