package services

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/stellar/go/support/log"
	"github.com/stellar/wallet-backend/internal/db"
	"github.com/stellar/wallet-backend/internal/db/dbtest"
	"github.com/stellar/wallet-backend/internal/entities"
	"github.com/stellar/wallet-backend/internal/metrics"
	"github.com/stellar/wallet-backend/internal/utils"
)

type errorReader struct{}

func (e *errorReader) Read(p []byte) (n int, err error) {
	return 0, fmt.Errorf("read error")
}

func (e *errorReader) Close() error {
	return nil
}

func TestSendRPCRequest(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()

	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()
	sqlxDB, err := dbConnectionPool.SqlxDB(context.Background())
	require.NoError(t, err)
	metricsService := metrics.NewMetricsService(sqlxDB)
	mockHTTPClient := utils.MockHTTPClient{}
	rpcURL := "http://api.vibrantapp.com/soroban/rpc"
	rpcService, _ := NewRPCService(rpcURL, &mockHTTPClient, metricsService)

	t.Run("successful", func(t *testing.T) {
		httpResponse := http.Response{
			StatusCode: http.StatusOK,
			Body: io.NopCloser(strings.NewReader(`{
				"jsonrpc": "2.0",
				"id": 8675309,
				"result": {
					"testValue": "theTestValue"
				}
			}`)),
		}
		mockHTTPClient.
			On("Post", rpcURL, "application/json", mock.Anything).
			Return(&httpResponse, nil).
			Once()

		resp, err := rpcService.sendRPCRequest("sendTransaction", entities.RPCParams{})

		require.NoError(t, err)

		var resultStruct struct {
			TestValue string `json:"testValue"`
		}
		err = json.Unmarshal(resp, &resultStruct)
		require.NoError(t, err)

		assert.Equal(t, "theTestValue", resultStruct.TestValue)
	})

	t.Run("rpc_post_call_fails", func(t *testing.T) {
		mockHTTPClient.
			On("Post", rpcURL, "application/json", mock.Anything).
			Return(&http.Response{}, errors.New("connection failed")).
			Once()

		resp, err := rpcService.sendRPCRequest("sendTransaction", entities.RPCParams{})

		assert.Nil(t, resp)
		assert.Equal(t, "sending POST request to RPC: connection failed", err.Error())
	})

	t.Run("unmarshaling_rpc_response_fails", func(t *testing.T) {
		httpResponse := &http.Response{
			StatusCode: http.StatusOK,
			Body:       io.NopCloser(&errorReader{}),
		}
		mockHTTPClient.
			On("Post", rpcURL, "application/json", mock.Anything).
			Return(httpResponse, nil).
			Once()

		resp, err := rpcService.sendRPCRequest("sendTransaction", entities.RPCParams{})

		assert.Nil(t, resp)
		assert.Equal(t, "unmarshaling RPC response: read error", err.Error())
	})

	t.Run("unmarshaling_json_fails", func(t *testing.T) {
		httpResponse := &http.Response{
			StatusCode: http.StatusOK,
			Body:       io.NopCloser(strings.NewReader(`{invalid-json`)),
		}
		mockHTTPClient.
			On("Post", rpcURL, "application/json", mock.Anything).
			Return(httpResponse, nil).
			Once()

		resp, err := rpcService.sendRPCRequest("sendTransaction", entities.RPCParams{})

		assert.Nil(t, resp)
		assert.Equal(t, "parsing RPC response JSON: invalid character 'i' looking for beginning of object key string", err.Error())
	})

	t.Run("response_has_no_result_field", func(t *testing.T) {
		httpResponse := &http.Response{
			StatusCode: http.StatusOK,
			Body:       io.NopCloser(strings.NewReader(`{"status": "success"}`)),
		}
		mockHTTPClient.
			On("Post", rpcURL, "application/json", mock.Anything).
			Return(httpResponse, nil).
			Once()

		result, err := rpcService.sendRPCRequest("sendTransaction", entities.RPCParams{})

		assert.Empty(t, result)
		assert.Equal(t, `response {"status": "success"} missing result field`, err.Error())
	})
}

func TestSendTransaction(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()

	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()
	sqlxDB, err := dbConnectionPool.SqlxDB(context.Background())
	require.NoError(t, err)
	metricsService := metrics.NewMetricsService(sqlxDB)
	mockHTTPClient := utils.MockHTTPClient{}
	rpcURL := "http://api.vibrantapp.com/soroban/rpc"
	rpcService, _ := NewRPCService(rpcURL, &mockHTTPClient, metricsService)

	t.Run("successful", func(t *testing.T) {
		transactionXDR := "AAAAAgAAAABYJgX6SmA2tGVDv3GXfOWbkeL869ahE0e5DG9HnXQw/QAAAGQAAjpnAAAAAQAAAAEAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAEAAAAAAAAAAQAAAACxaDFEbbssZfrbRgFxTYIygITSQxsUpDmneN2gAZBEFQAAAAAAAAAABfXhAAAAAAAAAAAA"
		params := entities.RPCParams{Transaction: transactionXDR}

		payload := map[string]interface{}{
			"jsonrpc": "2.0",
			"id":      1,
			"method":  "sendTransaction",
			"params":  params,
		}
		jsonData, _ := json.Marshal(payload)

		httpResponse := http.Response{
			StatusCode: http.StatusOK,
			Body: io.NopCloser(strings.NewReader(`{
				"jsonrpc": "2.0",
				"id": 8675309,
				"result": {
					"status": "PENDING",
					"hash": "d8ec9b68780314ffdfdfc2194b1b35dd27d7303c3bceaef6447e31631a1419dc",
					"latestLedger": 2553978,
					"latestLedgerCloseTime": "1700159337"
				}
			}`)),
		}

		mockHTTPClient.
			On("Post", rpcURL, "application/json", bytes.NewBuffer(jsonData)).
			Return(&httpResponse, nil).
			Once()

		result, err := rpcService.SendTransaction(transactionXDR)
		require.NoError(t, err)

		assert.Equal(t, entities.RPCSendTransactionResult{
			Status:                "PENDING",
			Hash:                  "d8ec9b68780314ffdfdfc2194b1b35dd27d7303c3bceaef6447e31631a1419dc",
			LatestLedger:          2553978,
			LatestLedgerCloseTime: "1700159337",
		}, result)
	})

	t.Run("rpc_request_fails", func(t *testing.T) {
		mockHTTPClient.
			On("Post", rpcURL, "application/json", mock.Anything).
			Return(&http.Response{}, errors.New("connection failed")).
			Once()

		result, err := rpcService.SendTransaction("XDR")
		require.Error(t, err)

		assert.Equal(t, entities.RPCSendTransactionResult{}, result)
		assert.Equal(t, "sending sendTransaction request: sending POST request to RPC: connection failed", err.Error())
	})
}

func TestGetTransaction(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()

	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()
	sqlxDB, err := dbConnectionPool.SqlxDB(context.Background())
	require.NoError(t, err)
	metricsService := metrics.NewMetricsService(sqlxDB)
	mockHTTPClient := utils.MockHTTPClient{}
	rpcURL := "http://api.vibrantapp.com/soroban/rpc"
	rpcService, _ := NewRPCService(rpcURL, &mockHTTPClient, metricsService)

	t.Run("successful", func(t *testing.T) {
		transactionHash := "6bc97bddc21811c626839baf4ab574f4f9f7ddbebb44d286ae504396d4e752da"
		params := entities.RPCParams{Hash: transactionHash}

		payload := map[string]interface{}{
			"jsonrpc": "2.0",
			"id":      1,
			"method":  "getTransaction",
			"params":  params,
		}
		jsonData, _ := json.Marshal(payload)

		httpResponse := http.Response{
			StatusCode: http.StatusOK,
			Body: io.NopCloser(strings.NewReader(`{
				"jsonrpc": "2.0",
				"id": 8675309,
				"result": {
					"status": "SUCCESS",
					"latestLedger": 2540076,
					"latestLedgerCloseTime": "1700086333",
					"oldestLedger": 2538637,
					"oldestLedgerCloseTime": "1700078796",
					"applicationOrder": 1,
					"envelopeXdr": "AAAAAgAAAADGFY14/R1KD0VGtTbi5Yp4d7LuMW0iQbLM/AUiGKj5owCpsoQAJY3OAAAjqgAAAAEAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAEAAAAAAAAAGAAAAAAAAAABhhOwI+RL18Zpk7cqI5pRRf0L96jE8i+0x3ekhuBh2cUAAAARc2V0X2N1cnJlbmN5X3JhdGUAAAAAAAACAAAADwAAAANldXIAAAAACQAAAAAAAAAAAAAAAAARCz4AAAABAAAAAAAAAAAAAAABhhOwI+RL18Zpk7cqI5pRRf0L96jE8i+0x3ekhuBh2cUAAAARc2V0X2N1cnJlbmN5X3JhdGUAAAAAAAACAAAADwAAAANldXIAAAAACQAAAAAAAAAAAAAAAAARCz4AAAAAAAAAAQAAAAAAAAABAAAAB4408vVXuLU3mry897TfPpYjjsSN7n42REos241RddYdAAAAAQAAAAYAAAABhhOwI+RL18Zpk7cqI5pRRf0L96jE8i+0x3ekhuBh2cUAAAAUAAAAAQFvcYAAAImAAAAHxAAAAAAAAAACAAAAARio+aMAAABATbFMyom/TUz87wHex0LoYZA8jbNJkXbaDSgmOdk+wSBFJuMuta+/vSlro0e0vK2+1FqD/zWHZeYig4pKmM3rDA==",
					"resultXdr": "AAAAAAARFy8AAAAAAAAAAQAAAAAAAAAYAAAAAMu8SHUN67hTUJOz3q+IrH9M/4dCVXaljeK6x1Ss20YWAAAAAA==",
					"resultMetaXdr": "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",
					"ledger": 2540064,
					"createdAt": "1700086268"
				}
			}`)),
		}

		mockHTTPClient.
			On("Post", rpcURL, "application/json", bytes.NewBuffer(jsonData)).
			Return(&httpResponse, nil).
			Once()

		result, err := rpcService.GetTransaction(transactionHash)
		require.NoError(t, err)

		assert.Equal(t, entities.RPCGetTransactionResult{
			Status:                "SUCCESS",
			LatestLedger:          2540076,
			LatestLedgerCloseTime: "1700086333",
			OldestLedger:          2538637,
			OldestLedgerCloseTime: "1700078796",
			ApplicationOrder:      1,
			EnvelopeXDR:           "AAAAAgAAAADGFY14/R1KD0VGtTbi5Yp4d7LuMW0iQbLM/AUiGKj5owCpsoQAJY3OAAAjqgAAAAEAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAEAAAAAAAAAGAAAAAAAAAABhhOwI+RL18Zpk7cqI5pRRf0L96jE8i+0x3ekhuBh2cUAAAARc2V0X2N1cnJlbmN5X3JhdGUAAAAAAAACAAAADwAAAANldXIAAAAACQAAAAAAAAAAAAAAAAARCz4AAAABAAAAAAAAAAAAAAABhhOwI+RL18Zpk7cqI5pRRf0L96jE8i+0x3ekhuBh2cUAAAARc2V0X2N1cnJlbmN5X3JhdGUAAAAAAAACAAAADwAAAANldXIAAAAACQAAAAAAAAAAAAAAAAARCz4AAAAAAAAAAQAAAAAAAAABAAAAB4408vVXuLU3mry897TfPpYjjsSN7n42REos241RddYdAAAAAQAAAAYAAAABhhOwI+RL18Zpk7cqI5pRRf0L96jE8i+0x3ekhuBh2cUAAAAUAAAAAQFvcYAAAImAAAAHxAAAAAAAAAACAAAAARio+aMAAABATbFMyom/TUz87wHex0LoYZA8jbNJkXbaDSgmOdk+wSBFJuMuta+/vSlro0e0vK2+1FqD/zWHZeYig4pKmM3rDA==",
			ResultXDR:             "AAAAAAARFy8AAAAAAAAAAQAAAAAAAAAYAAAAAMu8SHUN67hTUJOz3q+IrH9M/4dCVXaljeK6x1Ss20YWAAAAAA==",
			ResultMetaXDR:         "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",
			Ledger:                2540064,
			CreatedAt:             "1700086268",
			ErrorResultXDR:        "",
		}, result)
	})

	t.Run("rpc_request_fails", func(t *testing.T) {
		mockHTTPClient.
			On("Post", rpcURL, "application/json", mock.Anything).
			Return(&http.Response{}, errors.New("connection failed")).
			Once()

		result, err := rpcService.GetTransaction("hash")
		require.Error(t, err)

		assert.Equal(t, entities.RPCGetTransactionResult{}, result)
		assert.Equal(t, "sending getTransaction request: sending POST request to RPC: connection failed", err.Error())
	})
}

func TestGetTransactions(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()

	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()
	sqlxDB, err := dbConnectionPool.SqlxDB(context.Background())
	require.NoError(t, err)
	metricsService := metrics.NewMetricsService(sqlxDB)
	mockHTTPClient := utils.MockHTTPClient{}
	rpcURL := "http://api.vibrantapp.com/soroban/rpc"
	rpcService, _ := NewRPCService(rpcURL, &mockHTTPClient, metricsService)

	t.Run("rpc_request_fails", func(t *testing.T) {
		mockHTTPClient.
			On("Post", rpcURL, "application/json", mock.Anything).
			Return(&http.Response{}, errors.New("connection failed")).
			Once()

		result, err := rpcService.GetTransactions(10, "", 5)
		require.Error(t, err)

		assert.Equal(t, entities.RPCGetTransactionsResult{}, result)
		assert.Equal(t, "sending getTransactions request: sending POST request to RPC: connection failed", err.Error())
	})

	t.Run("successful", func(t *testing.T) {
		params := entities.RPCParams{StartLedger: 10, Pagination: entities.RPCPagination{Limit: 5}}

		payload := map[string]interface{}{
			"jsonrpc": "2.0",
			"id":      1,
			"method":  "getTransactions",
			"params":  params,
		}
		jsonData, _ := json.Marshal(payload)

		httpResponse := http.Response{
			StatusCode: http.StatusOK,
			Body: io.NopCloser(strings.NewReader(`{
				"jsonrpc": "2.0",
				"id": 8675309,
				"result": {
    				"transactions": [
						{
							"status": "SUCCESS",
							"applicationOrder": 1,
							"feeBump": false,
							"envelopeXdr": "AAAAAgAAAACDz21Q3CTITlGqRus3/96/05EDivbtfJncNQKt64BTbAAAASwAAKkyAAXlMwAAAAEAAAAAAAAAAAAAAABmWeASAAAAAQAAABR3YWxsZXQ6MTcxMjkwNjMzNjUxMAAAAAEAAAABAAAAAIPPbVDcJMhOUapG6zf/3r/TkQOK9u18mdw1Aq3rgFNsAAAAAQAAAABwOSvou8mtwTtCkysVioO35TSgyRir2+WGqO8FShG/GAAAAAFVQUgAAAAAAO371tlrHUfK+AvmQvHje1jSUrvJb3y3wrJ7EplQeqTkAAAAAAX14QAAAAAAAAAAAeuAU2wAAABAn+6A+xXvMasptAm9BEJwf5Y9CLLQtV44TsNqS8ocPmn4n8Rtyb09SBiFoMv8isYgeQU5nAHsIwBNbEKCerusAQ==",
							"resultXdr": "AAAAAAAAAGT/////AAAAAQAAAAAAAAAB////+gAAAAA=",
							"resultMetaXdr": "AAAAAwAAAAAAAAACAAAAAwAc0RsAAAAAAAAAAIPPbVDcJMhOUapG6zf/3r/TkQOK9u18mdw1Aq3rgFNsAAAAF0YpYBQAAKkyAAXlMgAAAAsAAAAAAAAAAAAAAAABAAAAAAAAAAAAAAEAAAAAAAAAAAAAAAAAAAAAAAAAAgAAAAAAAAAAAAAAAAAAAAMAAAAAABzRGgAAAABmWd/VAAAAAAAAAAEAHNEbAAAAAAAAAACDz21Q3CTITlGqRus3/96/05EDivbtfJncNQKt64BTbAAAABdGKWAUAACpMgAF5TMAAAALAAAAAAAAAAAAAAAAAQAAAAAAAAAAAAABAAAAAAAAAAAAAAAAAAAAAAAAAAIAAAAAAAAAAAAAAAAAAAADAAAAAAAc0RsAAAAAZlnf2gAAAAAAAAAAAAAAAAAAAAA=",
							"ledger": 1888539,
							"createdAt": 1717166042
						}
					]
				}
			}`)),
		}

		mockHTTPClient.
			On("Post", rpcURL, "application/json", bytes.NewBuffer(jsonData)).
			Return(&httpResponse, nil).
			Once()

		resp, err := rpcService.GetTransactions(10, "", 5)
		require.Equal(t, entities.RPCStatus("SUCCESS"), resp.Transactions[0].Status)
		require.Equal(t, int64(1888539), resp.Transactions[0].Ledger)
		require.NoError(t, err)
	})
}

func TestSendGetHealth(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()

	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()
	sqlxDB, err := dbConnectionPool.SqlxDB(context.Background())
	require.NoError(t, err)
	metricsService := metrics.NewMetricsService(sqlxDB)
	mockHTTPClient := utils.MockHTTPClient{}
	rpcURL := "http://api.vibrantapp.com/soroban/rpc"
	rpcService, _ := NewRPCService(rpcURL, &mockHTTPClient, metricsService)

	t.Run("successful", func(t *testing.T) {
		payload := map[string]interface{}{
			"jsonrpc": "2.0",
			"id":      1,
			"method":  "getHealth",
		}
		jsonData, _ := json.Marshal(payload)

		httpResponse := http.Response{
			StatusCode: http.StatusOK,
			Body: io.NopCloser(strings.NewReader(`{
				"jsonrpc": "2.0",
				"id": 8675309,
				"result": {
					"status": "healthy"
				}
			}`)),
		}

		mockHTTPClient.
			On("Post", rpcURL, "application/json", bytes.NewBuffer(jsonData)).
			Return(&httpResponse, nil).
			Once()

		result, err := rpcService.GetHealth()
		require.NoError(t, err)
		assert.Equal(t, entities.RPCGetHealthResult{Status: "healthy"}, result)
	})

	t.Run("rpc_request_fails", func(t *testing.T) {
		mockHTTPClient.
			On("Post", rpcURL, "application/json", mock.Anything).
			Return(&http.Response{}, errors.New("connection failed")).
			Once()

		result, err := rpcService.GetHealth()
		require.Error(t, err)
		assert.Equal(t, entities.RPCGetHealthResult{}, result)
		assert.Equal(t, "sending getHealth request: sending POST request to RPC: connection failed", err.Error())
	})
}

func TestTrackRPCServiceHealth_HealthyService(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()

	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()
	sqlxDB, err := dbConnectionPool.SqlxDB(context.Background())
	require.NoError(t, err)
	metricsService := metrics.NewMetricsService(sqlxDB)

	mockHTTPClient := &utils.MockHTTPClient{}
	rpcURL := "http://test-url-track-rpc-service-health"
	rpcService, err := NewRPCService(rpcURL, mockHTTPClient, metricsService)
	require.NoError(t, err)

	healthResult := entities.RPCGetHealthResult{
		Status:                "healthy",
		LatestLedger:          100,
		OldestLedger:          1,
		LedgerRetentionWindow: 0,
	}

	// Mock the HTTP response for GetHealth
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	mockResponse := &http.Response{
		Body: io.NopCloser(bytes.NewBuffer([]byte(`{
			"jsonrpc": "2.0",
			"id": 1,
			"result": {
				"status": "healthy",
				"latestLedger": 100,
				"oldestLedger": 1,
				"ledgerRetentionWindow": 0
			}
		}`))),
	}
	mockHTTPClient.On("Post", rpcURL, "application/json", mock.Anything).Return(mockResponse, nil).Run(func(args mock.Arguments) {
		cancel()
	})
	rpcService.TrackRPCServiceHealth(ctx)

	// Get result from heartbeat channel
	select {
	case result := <-rpcService.GetHeartbeatChannel():
		assert.Equal(t, healthResult, result)
	case <-time.After(10 * time.Second):
		t.Fatal("timeout waiting for heartbeat")
	}

	mockHTTPClient.AssertExpectations(t)
}

func TestTrackRPCServiceHealth_UnhealthyService(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 70*time.Second)
	defer cancel()
	
	dbt := dbtest.Open(t)
	defer dbt.Close()

	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()
	sqlxDB, err := dbConnectionPool.SqlxDB(ctx)
	require.NoError(t, err)
	metricsService := metrics.NewMetricsService(sqlxDB)
	getLogs := log.DefaultLogger.StartTest(log.WarnLevel)

	mockHTTPClient := &utils.MockHTTPClient{}
	rpcURL := "http://test-url-track-rpc-service-health"
	rpcService, err := NewRPCService(rpcURL, mockHTTPClient, metricsService)
	require.NoError(t, err)

	// Mock error response for GetHealth with a valid http.Response
	mockResponse := &http.Response{
		Body: io.NopCloser(bytes.NewBuffer([]byte(`{
			"jsonrpc": "2.0",
			"id": 1,
			"error": {
				"code": -32601,
				"message": "rpc error"
			}
		}`))),
	}
	mockHTTPClient.On("Post", rpcURL, "application/json", mock.Anything).
		Return(mockResponse, nil)

	// The ctx will timeout after 70 seconds, which is enough for the warning to trigger
	rpcService.TrackRPCServiceHealth(ctx)

	entries := getLogs()
	testFailed := true
	for _, entry := range entries {
		if strings.Contains(entry.Message, "rpc service unhealthy for over 1m0s") {
			testFailed = false
		}
	}
	assert.False(t, testFailed)
	mockHTTPClient.AssertExpectations(t)
}

func TestTrackRPCService_ContextCancelled(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	dbt := dbtest.Open(t)
	defer dbt.Close()

	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()
	sqlxDB, err := dbConnectionPool.SqlxDB(ctx)
	require.NoError(t, err)
	metricsService := metrics.NewMetricsService(sqlxDB)
	mockHTTPClient := &utils.MockHTTPClient{}
	rpcURL := "http://test-url-track-rpc-service-health"
	rpcService, err := NewRPCService(rpcURL, mockHTTPClient, metricsService)
	require.NoError(t, err)

	rpcService.TrackRPCServiceHealth(ctx)

	// Verify channel is closed after context cancellation
	time.Sleep(100 * time.Millisecond)
	_, ok := <-rpcService.GetHeartbeatChannel()
	assert.False(t, ok, "channel should be closed")

	mockHTTPClient.AssertNotCalled(t, "Post")
}
