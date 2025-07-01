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

	"github.com/stellar/go/network"
	"github.com/stellar/go/support/log"
	"github.com/stellar/go/xdr"
	"github.com/stellar/stellar-rpc/protocol"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

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

func Test_rpcService_NewRPCService(t *testing.T) {
	testCases := []struct {
		name              string
		rpcURL            string
		networkPassphrase string
		httpClient        utils.HTTPClient
		metricsService    metrics.MetricsService
		wantErrContains   string
		wantResult        *rpcService
	}{
		{
			name:            "🔴rpcURL_is_empty",
			rpcURL:          "",
			wantErrContains: "rpcURL is required",
		},
		{
			name:              "🔴networkPassphrase_is_empty",
			rpcURL:            "https://soroban-testnet.stellar.org",
			networkPassphrase: "",
			wantErrContains:   "networkPassphrase is required",
		},
		{
			name:              "🔴httpClient_is_nil",
			rpcURL:            "https://soroban-testnet.stellar.org",
			networkPassphrase: "Test SDF Network ; September 2015",
			httpClient:        nil,
			wantErrContains:   "httpClient is required",
		},
		{
			name:              "🔴httpClient_is_nil",
			rpcURL:            "https://soroban-testnet.stellar.org",
			networkPassphrase: "Test SDF Network ; September 2015",
			httpClient:        &http.Client{Timeout: 30 * time.Second},
			wantErrContains:   "metricsService is required",
		},
		{
			name:              "🟢successful",
			rpcURL:            "https://soroban-testnet.stellar.org",
			networkPassphrase: "Test SDF Network ; September 2015",
			httpClient:        &http.Client{Timeout: 30 * time.Second},
			metricsService:    metrics.NewMockMetricsService(),
			wantResult: &rpcService{
				rpcURL:                     "https://soroban-testnet.stellar.org",
				networkPassphrase:          "Test SDF Network ; September 2015",
				httpClient:                 &http.Client{Timeout: 30 * time.Second},
				healthCheckWarningInterval: defaultHealthCheckWarningInterval,
				healthCheckTickInterval:    defaultHealthCheckTickInterval,
				metricsService:             metrics.NewMockMetricsService(),
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			rpcService, err := NewRPCService(tc.rpcURL, tc.networkPassphrase, tc.httpClient, tc.metricsService)
			if tc.wantErrContains != "" {
				assert.ErrorContains(t, err, tc.wantErrContains)
				assert.Nil(t, rpcService)
			} else {
				assert.NoError(t, err)
				require.Equal(t, cap(rpcService.heartbeatChannel), 1)
				require.Equal(t, len(rpcService.heartbeatChannel), 0)
				tc.wantResult.heartbeatChannel = rpcService.heartbeatChannel
				assert.Equal(t, tc.wantResult, rpcService)
			}
		})
	}
}

func TestSendRPCRequest(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()

	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()
	mockMetricsService := metrics.NewMockMetricsService()
	mockHTTPClient := utils.MockHTTPClient{}
	rpcURL := "http://api.vibrantapp.com/soroban/rpc"
	rpcService, err := NewRPCService(rpcURL, network.TestNetworkPassphrase, &mockHTTPClient, mockMetricsService)
	require.NoError(t, err)

	t.Run("successful", func(t *testing.T) {
		mockMetricsService.On("IncRPCRequests", "sendTransaction").Once()
		mockMetricsService.On("IncRPCEndpointSuccess", "sendTransaction").Once()
		mockMetricsService.On("ObserveRPCRequestDuration", "sendTransaction", mock.AnythingOfType("float64")).Once()
		defer mockMetricsService.AssertExpectations(t)

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
		mockMetricsService.On("IncRPCRequests", "sendTransaction").Once()
		mockMetricsService.On("IncRPCEndpointFailure", "sendTransaction").Once()
		mockMetricsService.On("ObserveRPCRequestDuration", "sendTransaction", mock.AnythingOfType("float64")).Once()
		defer mockMetricsService.AssertExpectations(t)

		mockHTTPClient.
			On("Post", rpcURL, "application/json", mock.Anything).
			Return(&http.Response{}, errors.New("connection failed")).
			Once()

		resp, err := rpcService.sendRPCRequest("sendTransaction", entities.RPCParams{})

		assert.Nil(t, resp)
		assert.Equal(t, "sending POST request to RPC: connection failed", err.Error())
	})

	t.Run("unmarshaling_rpc_response_fails", func(t *testing.T) {
		mockMetricsService.On("IncRPCRequests", "sendTransaction").Once()
		mockMetricsService.On("IncRPCEndpointFailure", "sendTransaction").Once()
		mockMetricsService.On("ObserveRPCRequestDuration", "sendTransaction", mock.AnythingOfType("float64")).Once()
		defer mockMetricsService.AssertExpectations(t)

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
		mockMetricsService.On("IncRPCRequests", "sendTransaction").Once()
		mockMetricsService.On("IncRPCEndpointFailure", "sendTransaction").Once()
		mockMetricsService.On("ObserveRPCRequestDuration", "sendTransaction", mock.AnythingOfType("float64")).Once()
		defer mockMetricsService.AssertExpectations(t)

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
		assert.ErrorContains(t, err, "parsing RPC response JSON body {invalid-json: invalid character 'i' looking for beginning of object key string")
	})

	t.Run("response_has_no_result_field", func(t *testing.T) {
		mockMetricsService.On("IncRPCRequests", "sendTransaction").Once()
		mockMetricsService.On("IncRPCEndpointFailure", "sendTransaction").Once()
		mockMetricsService.On("ObserveRPCRequestDuration", "sendTransaction", mock.AnythingOfType("float64")).Once()
		defer mockMetricsService.AssertExpectations(t)

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
	mockMetricsService := metrics.NewMockMetricsService()
	mockHTTPClient := utils.MockHTTPClient{}
	rpcURL := "http://api.vibrantapp.com/soroban/rpc"
	rpcService, err := NewRPCService(rpcURL, network.TestNetworkPassphrase, &mockHTTPClient, mockMetricsService)
	require.NoError(t, err)

	t.Run("successful", func(t *testing.T) {
		mockMetricsService.On("IncRPCRequests", "sendTransaction").Once()
		mockMetricsService.On("IncRPCEndpointSuccess", "sendTransaction").Once()
		mockMetricsService.On("ObserveRPCRequestDuration", "sendTransaction", mock.AnythingOfType("float64")).Once()
		defer mockMetricsService.AssertExpectations(t)

		transactionXDR := "AAAAAgAAAABYJgX6SmA2tGVDv3GXfOWbkeL869ahE0e5DG9HnXQw/QAAAGQAAjpnAAAAAQAAAAEAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAEAAAAAAAAAAQAAAACxaDFEbbssZfrbRgFxTYIygITSQxsUpDmneN2gAZBEFQAAAAAAAAAABfXhAAAAAAAAAAAA"
		params := entities.RPCParams{Transaction: transactionXDR}

		payload := map[string]interface{}{
			"jsonrpc": "2.0",
			"id":      1,
			"method":  "sendTransaction",
			"params":  params,
		}
		jsonData, err := json.Marshal(payload)
		require.NoError(t, err)

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
		mockMetricsService.On("IncRPCRequests", "sendTransaction").Once()
		mockMetricsService.On("IncRPCEndpointFailure", "sendTransaction").Once()
		mockMetricsService.On("ObserveRPCRequestDuration", "sendTransaction", mock.AnythingOfType("float64")).Once()
		defer mockMetricsService.AssertExpectations(t)

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

func Test_rpcService_SimulateTransaction(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()
	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	const rpcURL = "http://api.vibrantapp.com/soroban/rpc"
	const transactionXDR = "AAAAAgAAAACnroqZn2p1MGBHWWDhZOaG3H73hXYtdc4Jz27c287ITQAAAGQAAAAAAAAAAQAAAAEAAAAAAAAAAAAAAABoCqJrAAAAAAAAAAEAAAAAAAAAGAAAAAAAAAAB15KLcsJwPM/q9+uf9O9NUEpVqLl5/JtFDqLIQrTRzmEAAAAIdHJhbnNmZXIAAAADAAAAEgAAAAAAAAAAEG5rRhQ6188E3xuAkXVVe82tgIW2yZyDyH43k9/YHKUAAAASAAAAAAAAAAAQbmtGFDrXzwTfG4CRdVV7za2AhbbJnIPIfjeT39gcpQAAAAoAAAAAAAAAAAAAAAAF9eEAAAAAAAAAAAAAAAAA"

	testCases := []struct {
		name             string
		httpResponseCode int
		httpResponseBody string
		httpResponseErr  error
		wantResultFn     func(t *testing.T) entities.RPCSimulateTransactionResult
		wantErrContains  string
	}{
		{
			name:             "🟢successful_call_and_response",
			httpResponseCode: http.StatusOK,
			httpResponseBody: `{
				"jsonrpc": "2.0",
				"id": 8675309,
				"result": {
					"transactionData": "AAAAAAAAAAMAAAAGAAAAAS8eFExnIzKJx87I4pHQOM7ArlmAblnfTrHHCzuoKIpfAAAAFAAAAAEAAAAGAAAAAdeSi3LCcDzP6vfrn/TvTVBKVai5efybRQ6iyEK00c5hAAAAFAAAAAEAAAAH19EQnu7DQcCAFCPhrYa4QCddH5+GrI4TDOceUD3GshcAAAACAAAABgAAAAEvHhRMZyMyicfOyOKR0DjOwK5ZgG5Z306xxws7qCiKXwAAABVq7Bx20hyRtAAAAAAAAAAGAAAAAdeSi3LCcDzP6vfrn/TvTVBKVai5efybRQ6iyEK00c5hAAAAEAAAAAEAAAACAAAADwAAAAdCYWxhbmNlAAAAABIAAAABLx4UTGcjMonHzsjikdA4zsCuWYBuWd9OsccLO6goil8AAAABACwRSwAAKsQAAAEoAAAAAAAHTNw=",
					"events": [
						"AAAAAQAAAAAAAAAAAAAAAgAAAAAAAAADAAAADwAAAAdmbl9jYWxsAAAAAA0AAAAg15KLcsJwPM/q9+uf9O9NUEpVqLl5/JtFDqLIQrTRzmEAAAAPAAAACHRyYW5zZmVyAAAAEAAAAAEAAAADAAAAEgAAAAEvHhRMZyMyicfOyOKR0DjOwK5ZgG5Z306xxws7qCiKXwAAABIAAAABLx4UTGcjMonHzsjikdA4zsCuWYBuWd9OsccLO6goil8AAAAKAAAAAAAAAAAAAAAAAJiWgA==",
						"AAAAAQAAAAAAAAAB15KLcsJwPM/q9+uf9O9NUEpVqLl5/JtFDqLIQrTRzmEAAAACAAAAAAAAAAMAAAAPAAAAB2ZuX2NhbGwAAAAADQAAACAvHhRMZyMyicfOyOKR0DjOwK5ZgG5Z306xxws7qCiKXwAAAA8AAAAMX19jaGVja19hdXRoAAAAEAAAAAEAAAADAAAADQAAACAxIzp5Z9rHxuL2zPLeC4cem6Phs9cZLXvifVfl4OHopAAAABAAAAABAAAAAQAAABEAAAABAAAAAgAAAA8AAAAKcHVibGljX2tleQAAAAAADQAAACAv8qggHT54FDsmFiw8qkXOT8kyCK/xCNetbJ66m6zs4gAAAA8AAAAJc2lnbmF0dXJlAAAAAAAADQAAAEDvh1BPg2Hsjrxax2R3O776ouwU/OvW6ac3+id9lYxDNIL575GAzoWcOvvOHuFCI0tXxiKkK1BSa62QaLRDh5gOAAAAEAAAAAEAAAABAAAAEAAAAAEAAAACAAAADwAAAAhDb250cmFjdAAAABEAAAABAAAAAwAAAA8AAAAEYXJncwAAABAAAAABAAAAAwAAABIAAAABLx4UTGcjMonHzsjikdA4zsCuWYBuWd9OsccLO6goil8AAAASAAAAAS8eFExnIzKJx87I4pHQOM7ArlmAblnfTrHHCzuoKIpfAAAACgAAAAAAAAAAAAAAAACYloAAAAAPAAAACGNvbnRyYWN0AAAAEgAAAAHXkotywnA8z+r365/0701QSlWouXn8m0UOoshCtNHOYQAAAA8AAAAHZm5fbmFtZQAAAAAPAAAACHRyYW5zZmVy",
						"AAAAAQAAAAAAAAABLx4UTGcjMonHzsjikdA4zsCuWYBuWd9OsccLO6goil8AAAACAAAAAAAAAAIAAAAPAAAACWZuX3JldHVybgAAAAAAAA8AAAAMX19jaGVja19hdXRoAAAAAQ==",
						"AAAAAQAAAAAAAAAB15KLcsJwPM/q9+uf9O9NUEpVqLl5/JtFDqLIQrTRzmEAAAABAAAAAAAAAAQAAAAPAAAACHRyYW5zZmVyAAAAEgAAAAEvHhRMZyMyicfOyOKR0DjOwK5ZgG5Z306xxws7qCiKXwAAABIAAAABLx4UTGcjMonHzsjikdA4zsCuWYBuWd9OsccLO6goil8AAAAOAAAABm5hdGl2ZQAAAAAACgAAAAAAAAAAAAAAAACYloA=",
						"AAAAAQAAAAAAAAAB15KLcsJwPM/q9+uf9O9NUEpVqLl5/JtFDqLIQrTRzmEAAAACAAAAAAAAAAIAAAAPAAAACWZuX3JldHVybgAAAAAAAA8AAAAIdHJhbnNmZXIAAAAB"
					],
					"minResourceFee": "478428",
					"results": [
						{
							"auth": [
								"AAAAAQAAAAEvHhRMZyMyicfOyOKR0DjOwK5ZgG5Z306xxws7qCiKX2rsHHbSHJG0AB++cAAAABAAAAABAAAAAQAAABEAAAABAAAAAgAAAA8AAAAKcHVibGljX2tleQAAAAAADQAAACAv8qggHT54FDsmFiw8qkXOT8kyCK/xCNetbJ66m6zs4gAAAA8AAAAJc2lnbmF0dXJlAAAAAAAADQAAAEDvh1BPg2Hsjrxax2R3O776ouwU/OvW6ac3+id9lYxDNIL575GAzoWcOvvOHuFCI0tXxiKkK1BSa62QaLRDh5gOAAAAAAAAAAHXkotywnA8z+r365/0701QSlWouXn8m0UOoshCtNHOYQAAAAh0cmFuc2ZlcgAAAAMAAAASAAAAAS8eFExnIzKJx87I4pHQOM7ArlmAblnfTrHHCzuoKIpfAAAAEgAAAAEvHhRMZyMyicfOyOKR0DjOwK5ZgG5Z306xxws7qCiKXwAAAAoAAAAAAAAAAAAAAAAAmJaAAAAAAA=="
							],
							"xdr": "AAAAAQ=="
						}
					],
					"stateChanges": [
						{
							"type": "created",
							"key": "AAAABgAAAAEvHhRMZyMyicfOyOKR0DjOwK5ZgG5Z306xxws7qCiKXwAAABVq7Bx20hyRtAAAAAA=",
							"before": null,
							"after": "AAAAAAAAAAYAAAAAAAAAAS8eFExnIzKJx87I4pHQOM7ArlmAblnfTrHHCzuoKIpfAAAAFWrsHHbSHJG0AAAAAAAAAAEAAAAA"
						}
					],
					"latestLedger": 621041,
					"restorePreamble": {
						"minResourceFee": "478428",
						"transactionData": "AAAAAAAAAAMAAAAGAAAAAS8eFExnIzKJx87I4pHQOM7ArlmAblnfTrHHCzuoKIpfAAAAFAAAAAEAAAAGAAAAAdeSi3LCcDzP6vfrn/TvTVBKVai5efybRQ6iyEK00c5hAAAAFAAAAAEAAAAH19EQnu7DQcCAFCPhrYa4QCddH5+GrI4TDOceUD3GshcAAAACAAAABgAAAAEvHhRMZyMyicfOyOKR0DjOwK5ZgG5Z306xxws7qCiKXwAAABVq7Bx20hyRtAAAAAAAAAAGAAAAAdeSi3LCcDzP6vfrn/TvTVBKVai5efybRQ6iyEK00c5hAAAAEAAAAAEAAAACAAAADwAAAAdCYWxhbmNlAAAAABIAAAABLx4UTGcjMonHzsjikdA4zsCuWYBuWd9OsccLO6goil8AAAABACwRSwAAKsQAAAEoAAAAAAAHTNw="
					}
				}
			}`,
			wantResultFn: func(t *testing.T) entities.RPCSimulateTransactionResult {
				var sorobanTxData xdr.SorobanTransactionData
				err = xdr.SafeUnmarshalBase64("AAAAAAAAAAMAAAAGAAAAAS8eFExnIzKJx87I4pHQOM7ArlmAblnfTrHHCzuoKIpfAAAAFAAAAAEAAAAGAAAAAdeSi3LCcDzP6vfrn/TvTVBKVai5efybRQ6iyEK00c5hAAAAFAAAAAEAAAAH19EQnu7DQcCAFCPhrYa4QCddH5+GrI4TDOceUD3GshcAAAACAAAABgAAAAEvHhRMZyMyicfOyOKR0DjOwK5ZgG5Z306xxws7qCiKXwAAABVq7Bx20hyRtAAAAAAAAAAGAAAAAdeSi3LCcDzP6vfrn/TvTVBKVai5efybRQ6iyEK00c5hAAAAEAAAAAEAAAACAAAADwAAAAdCYWxhbmNlAAAAABIAAAABLx4UTGcjMonHzsjikdA4zsCuWYBuWd9OsccLO6goil8AAAABACwRSwAAKsQAAAEoAAAAAAAHTNw=", &sorobanTxData)
				require.NoError(t, err)

				var authEntry xdr.SorobanAuthorizationEntry
				err = xdr.SafeUnmarshalBase64("AAAAAQAAAAEvHhRMZyMyicfOyOKR0DjOwK5ZgG5Z306xxws7qCiKX2rsHHbSHJG0AB++cAAAABAAAAABAAAAAQAAABEAAAABAAAAAgAAAA8AAAAKcHVibGljX2tleQAAAAAADQAAACAv8qggHT54FDsmFiw8qkXOT8kyCK/xCNetbJ66m6zs4gAAAA8AAAAJc2lnbmF0dXJlAAAAAAAADQAAAEDvh1BPg2Hsjrxax2R3O776ouwU/OvW6ac3+id9lYxDNIL575GAzoWcOvvOHuFCI0tXxiKkK1BSa62QaLRDh5gOAAAAAAAAAAHXkotywnA8z+r365/0701QSlWouXn8m0UOoshCtNHOYQAAAAh0cmFuc2ZlcgAAAAMAAAASAAAAAS8eFExnIzKJx87I4pHQOM7ArlmAblnfTrHHCzuoKIpfAAAAEgAAAAEvHhRMZyMyicfOyOKR0DjOwK5ZgG5Z306xxws7qCiKXwAAAAoAAAAAAAAAAAAAAAAAmJaAAAAAAA==", &authEntry)
				require.NoError(t, err)

				return entities.RPCSimulateTransactionResult{
					TransactionData: sorobanTxData,
					LatestLedger:    621041,
					MinResourceFee:  "478428",
					RestorePreamble: entities.RPCRestorePreamble{
						MinResourceFee:  "478428",
						TransactionData: sorobanTxData,
					},
					Results: []entities.RPCSimulateHostFunctionResult{
						{
							XDR:  xdr.ScVal{Type: xdr.ScValTypeScvVoid},
							Auth: []xdr.SorobanAuthorizationEntry{authEntry},
						},
					},
					Events: []string{
						"AAAAAQAAAAAAAAAAAAAAAgAAAAAAAAADAAAADwAAAAdmbl9jYWxsAAAAAA0AAAAg15KLcsJwPM/q9+uf9O9NUEpVqLl5/JtFDqLIQrTRzmEAAAAPAAAACHRyYW5zZmVyAAAAEAAAAAEAAAADAAAAEgAAAAEvHhRMZyMyicfOyOKR0DjOwK5ZgG5Z306xxws7qCiKXwAAABIAAAABLx4UTGcjMonHzsjikdA4zsCuWYBuWd9OsccLO6goil8AAAAKAAAAAAAAAAAAAAAAAJiWgA==",
						"AAAAAQAAAAAAAAAB15KLcsJwPM/q9+uf9O9NUEpVqLl5/JtFDqLIQrTRzmEAAAACAAAAAAAAAAMAAAAPAAAAB2ZuX2NhbGwAAAAADQAAACAvHhRMZyMyicfOyOKR0DjOwK5ZgG5Z306xxws7qCiKXwAAAA8AAAAMX19jaGVja19hdXRoAAAAEAAAAAEAAAADAAAADQAAACAxIzp5Z9rHxuL2zPLeC4cem6Phs9cZLXvifVfl4OHopAAAABAAAAABAAAAAQAAABEAAAABAAAAAgAAAA8AAAAKcHVibGljX2tleQAAAAAADQAAACAv8qggHT54FDsmFiw8qkXOT8kyCK/xCNetbJ66m6zs4gAAAA8AAAAJc2lnbmF0dXJlAAAAAAAADQAAAEDvh1BPg2Hsjrxax2R3O776ouwU/OvW6ac3+id9lYxDNIL575GAzoWcOvvOHuFCI0tXxiKkK1BSa62QaLRDh5gOAAAAEAAAAAEAAAABAAAAEAAAAAEAAAACAAAADwAAAAhDb250cmFjdAAAABEAAAABAAAAAwAAAA8AAAAEYXJncwAAABAAAAABAAAAAwAAABIAAAABLx4UTGcjMonHzsjikdA4zsCuWYBuWd9OsccLO6goil8AAAASAAAAAS8eFExnIzKJx87I4pHQOM7ArlmAblnfTrHHCzuoKIpfAAAACgAAAAAAAAAAAAAAAACYloAAAAAPAAAACGNvbnRyYWN0AAAAEgAAAAHXkotywnA8z+r365/0701QSlWouXn8m0UOoshCtNHOYQAAAA8AAAAHZm5fbmFtZQAAAAAPAAAACHRyYW5zZmVy",
						"AAAAAQAAAAAAAAABLx4UTGcjMonHzsjikdA4zsCuWYBuWd9OsccLO6goil8AAAACAAAAAAAAAAIAAAAPAAAACWZuX3JldHVybgAAAAAAAA8AAAAMX19jaGVja19hdXRoAAAAAQ==",
						"AAAAAQAAAAAAAAAB15KLcsJwPM/q9+uf9O9NUEpVqLl5/JtFDqLIQrTRzmEAAAABAAAAAAAAAAQAAAAPAAAACHRyYW5zZmVyAAAAEgAAAAEvHhRMZyMyicfOyOKR0DjOwK5ZgG5Z306xxws7qCiKXwAAABIAAAABLx4UTGcjMonHzsjikdA4zsCuWYBuWd9OsccLO6goil8AAAAOAAAABm5hdGl2ZQAAAAAACgAAAAAAAAAAAAAAAACYloA=",
						"AAAAAQAAAAAAAAAB15KLcsJwPM/q9+uf9O9NUEpVqLl5/JtFDqLIQrTRzmEAAAACAAAAAAAAAAIAAAAPAAAACWZuX3JldHVybgAAAAAAAA8AAAAIdHJhbnNmZXIAAAAB",
					},
					StateChanges: []entities.RPCSimulateStateChange{
						{
							Type:   "created",
							Key:    "AAAABgAAAAEvHhRMZyMyicfOyOKR0DjOwK5ZgG5Z306xxws7qCiKXwAAABVq7Bx20hyRtAAAAAA=",
							Before: nil,
							After:  utils.PointOf("AAAAAAAAAAYAAAAAAAAAAS8eFExnIzKJx87I4pHQOM7ArlmAblnfTrHHCzuoKIpfAAAAFWrsHHbSHJG0AAAAAAAAAAEAAAAA"),
						},
					},
				}
			},
		},
		{
			name:             "🟡successful_call_error_result",
			httpResponseCode: http.StatusOK,
			httpResponseBody: `{
				"jsonrpc": "2.0",
				"id": 8675309,
				"result": {
					"error": "HostError: Error(Contract, #6)\n\nEvent log (newest first):\n   0: [Diagnostic Event] contract:CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG2FB2RMQQVU2HHGCYSC, topics:[error, Error(Contract, #6)], data:[\"account entry is missing\", GAIG422GCQ5NPTYE34NYBELVKV543LMAQW3MTHEDZB7DPE673AOKLEXO]\n   1: [Diagnostic Event] topics:[fn_call, CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG2FB2RMQQVU2HHGCYSC, transfer], data:[GAIG422GCQ5NPTYE34NYBELVKV543LMAQW3MTHEDZB7DPE673AOKLEXO, GAIG422GCQ5NPTYE34NYBELVKV543LMAQW3MTHEDZB7DPE673AOKLEXO, 100000000]\n",
					"events": [
						"AAAAAAAAAAAAAAAAAAAAAgAAAAAAAAADAAAADwAAAAdmbl9jYWxsAAAAAA0AAAAg15KLcsJwPM/q9+uf9O9NUEpVqLl5/JtFDqLIQrTRzmEAAAAPAAAACHRyYW5zZmVyAAAAEAAAAAEAAAADAAAAEgAAAAAAAAAAEG5rRhQ6188E3xuAkXVVe82tgIW2yZyDyH43k9/YHKUAAAASAAAAAAAAAAAQbmtGFDrXzwTfG4CRdVV7za2AhbbJnIPIfjeT39gcpQAAAAoAAAAAAAAAAAAAAAAF9eEA",
						"AAAAAAAAAAAAAAAB15KLcsJwPM/q9+uf9O9NUEpVqLl5/JtFDqLIQrTRzmEAAAACAAAAAAAAAAIAAAAPAAAABWVycm9yAAAAAAAAAgAAAAAAAAAGAAAAEAAAAAEAAAACAAAADgAAABhhY2NvdW50IGVudHJ5IGlzIG1pc3NpbmcAAAASAAAAAAAAAAAQbmtGFDrXzwTfG4CRdVV7za2AhbbJnIPIfjeT39gcpQ=="
					],
					"latestLedger": 622844
				}
			}`,
			wantResultFn: func(t *testing.T) entities.RPCSimulateTransactionResult {
				return entities.RPCSimulateTransactionResult{
					Error: "HostError: Error(Contract, #6)\n\nEvent log (newest first):\n   0: [Diagnostic Event] contract:CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG2FB2RMQQVU2HHGCYSC, topics:[error, Error(Contract, #6)], data:[\"account entry is missing\", GAIG422GCQ5NPTYE34NYBELVKV543LMAQW3MTHEDZB7DPE673AOKLEXO]\n   1: [Diagnostic Event] topics:[fn_call, CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG2FB2RMQQVU2HHGCYSC, transfer], data:[GAIG422GCQ5NPTYE34NYBELVKV543LMAQW3MTHEDZB7DPE673AOKLEXO, GAIG422GCQ5NPTYE34NYBELVKV543LMAQW3MTHEDZB7DPE673AOKLEXO, 100000000]\n",
					Events: []string{
						"AAAAAAAAAAAAAAAAAAAAAgAAAAAAAAADAAAADwAAAAdmbl9jYWxsAAAAAA0AAAAg15KLcsJwPM/q9+uf9O9NUEpVqLl5/JtFDqLIQrTRzmEAAAAPAAAACHRyYW5zZmVyAAAAEAAAAAEAAAADAAAAEgAAAAAAAAAAEG5rRhQ6188E3xuAkXVVe82tgIW2yZyDyH43k9/YHKUAAAASAAAAAAAAAAAQbmtGFDrXzwTfG4CRdVV7za2AhbbJnIPIfjeT39gcpQAAAAoAAAAAAAAAAAAAAAAF9eEA",
						"AAAAAAAAAAAAAAAB15KLcsJwPM/q9+uf9O9NUEpVqLl5/JtFDqLIQrTRzmEAAAACAAAAAAAAAAIAAAAPAAAABWVycm9yAAAAAAAAAgAAAAAAAAAGAAAAEAAAAAEAAAACAAAADgAAABhhY2NvdW50IGVudHJ5IGlzIG1pc3NpbmcAAAASAAAAAAAAAAAQbmtGFDrXzwTfG4CRdVV7za2AhbbJnIPIfjeT39gcpQ==",
					},
					LatestLedger: 622844,
				}
			},
		},
		{
			name:             "🔴rpc_request_fails",
			httpResponseCode: http.StatusBadRequest,
			httpResponseErr:  errors.New("connection failed"),
			wantErrContains:  "sending simulateTransaction request: sending POST request to RPC: connection failed",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Mock Metrics
			mMetricsService := metrics.NewMockMetricsService()
			mMetricsService.
				On("IncRPCRequests", "simulateTransaction").Once().
				On("ObserveRPCRequestDuration", "simulateTransaction", mock.AnythingOfType("float64")).Once()
			if tc.httpResponseErr == nil {
				mMetricsService.On("IncRPCEndpointSuccess", "simulateTransaction").Once()
			} else {
				mMetricsService.On("IncRPCEndpointFailure", "simulateTransaction").Once()
			}

			// Mock HTTP Client
			payload := map[string]any{
				"jsonrpc": "2.0",
				"id":      1,
				"method":  "simulateTransaction",
				"params":  entities.RPCParams{Transaction: transactionXDR},
			}
			jsonData, err := json.Marshal(payload)
			require.NoError(t, err)

			mHTTPClient := utils.MockHTTPClient{}
			mHTTPClient.
				On("Post", rpcURL, "application/json", bytes.NewBuffer(jsonData)).
				Return(&http.Response{
					StatusCode: tc.httpResponseCode,
					Body:       io.NopCloser(strings.NewReader(tc.httpResponseBody)),
				}, tc.httpResponseErr).
				Once()

			// Simulate Transaction
			rpcService, err := NewRPCService(rpcURL, network.TestNetworkPassphrase, &mHTTPClient, mMetricsService)
			require.NoError(t, err)
			simulateTransactionResult, err := rpcService.SimulateTransaction(transactionXDR, entities.RPCResourceConfig{})

			// Assert
			if tc.wantErrContains == "" {
				assert.Equal(t, tc.wantResultFn(t), simulateTransactionResult)
				require.NoError(t, err)
			} else {
				assert.Error(t, err)
				assert.ErrorContains(t, err, tc.wantErrContains)
			}

			mMetricsService.AssertExpectations(t)
			mHTTPClient.AssertExpectations(t)
		})
	}
}

func TestGetTransaction(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()

	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()
	mockMetricsService := metrics.NewMockMetricsService()
	mockHTTPClient := utils.MockHTTPClient{}
	rpcURL := "http://api.vibrantapp.com/soroban/rpc"
	rpcService, err := NewRPCService(rpcURL, network.TestNetworkPassphrase, &mockHTTPClient, mockMetricsService)
	require.NoError(t, err)

	t.Run("successful", func(t *testing.T) {
		mockMetricsService.On("IncRPCRequests", "getTransaction").Once()
		mockMetricsService.On("IncRPCEndpointSuccess", "getTransaction").Once()
		mockMetricsService.On("ObserveRPCRequestDuration", "getTransaction", mock.AnythingOfType("float64")).Once()
		defer mockMetricsService.AssertExpectations(t)

		transactionHash := "6bc97bddc21811c626839baf4ab574f4f9f7ddbebb44d286ae504396d4e752da"
		params := entities.RPCParams{Hash: transactionHash}

		payload := map[string]interface{}{
			"jsonrpc": "2.0",
			"id":      1,
			"method":  "getTransaction",
			"params":  params,
		}
		jsonData, err := json.Marshal(payload)
		require.NoError(t, err)

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
					"resultMetaXdr": "AAAAAwAAAAAAAAACAAAAAwAmwiAAAAAAAAAAAMYVjXj9HUoPRUa1NuLlinh3su4xbSJBssz8BSIYqPmjAAAAFUHZob0AJY3OAAAjqQAAAAAAAAAAAAAAAAAAAAABAAAAAAAAAAAAAAEAAAAAAAAAAAAAAAAAAAAAAAAAAgAAAAAAAAAAAAAAAAAAAAMAAAAAACbCHwAAAABlVUH3AAAAAAAAAAEAJsIgAAAAAAAAAADGFY14/R1KD0VGtTbi5Yp4d7LuMW0iQbLM/AUiGKj5owAAABVB2aG9ACWNzgAAI6oAAAAAAAAAAAAAAAAAAAAAAQAAAAAAAAAAAAABAAAAAAAAAAAAAAAAAAAAAAAAAAIAAAAAAAAAAAAAAAAAAAADAAAAAAAmwiAAAAAAZVVB/AAAAAAAAAABAAAAAgAAAAMAJsIfAAAABgAAAAAAAAABhhOwI+RL18Zpk7cqI5pRRf0L96jE8i+0x3ekhuBh2cUAAAAUAAAAAQAAABMAAAAAjjTy9Ve4tTeavLz3tN8+liOOxI3ufjZESizbjVF11h0AAAABAAAABQAAABAAAAABAAAAAQAAAA8AAAAJQ29yZVN0YXRlAAAAAAAAEQAAAAEAAAAGAAAADwAAAAVhZG1pbgAAAAAAABIAAAAAAAAAADn1LT+CCK/HiHMChoEi/AtPrkos4XRR2E45Pr25lb3/AAAADwAAAAljb2xfdG9rZW4AAAAAAAASAAAAAdeSi3LCcDzP6vfrn/TvTVBKVai5efybRQ6iyEK00c5hAAAADwAAAAxvcmFjbGVfYWRtaW4AAAASAAAAAAAAAADGFY14/R1KD0VGtTbi5Yp4d7LuMW0iQbLM/AUiGKj5owAAAA8AAAAKcGFuaWNfbW9kZQAAAAAAAAAAAAAAAAAPAAAAEHByb3RvY29sX21hbmFnZXIAAAASAAAAAAAAAAAtSfyAwmj05lZ0WduHsQYQZgvahCNVtZyqS2HRC99kyQAAAA8AAAANc3RhYmxlX2lzc3VlcgAAAAAAABIAAAAAAAAAAEM5BlXva0R5UN6SCMY+6evwJa4mY/f062z0TKLnqN4wAAAAEAAAAAEAAAACAAAADwAAAAhDdXJyZW5jeQAAAA8AAAADZXVyAAAAABEAAAABAAAABQAAAA8AAAAGYWN0aXZlAAAAAAAAAAAAAQAAAA8AAAAIY29udHJhY3QAAAASAAAAAUGpebFxuPbvxZFzOxh8TWAxUwFgraPxPuJEY/8yhiYEAAAADwAAAAxkZW5vbWluYXRpb24AAAAPAAAAA2V1cgAAAAAPAAAAC2xhc3RfdXBkYXRlAAAAAAUAAAAAZVVBvgAAAA8AAAAEcmF0ZQAAAAkAAAAAAAAAAAAAAAAAEQb8AAAAEAAAAAEAAAACAAAADwAAAAhDdXJyZW5jeQAAAA8AAAADdXNkAAAAABEAAAABAAAABQAAAA8AAAAGYWN0aXZlAAAAAAAAAAAAAQAAAA8AAAAIY29udHJhY3QAAAASAAAAATUEqdkvrE2LnSiwOwed3v4VEaulOEiS1rxQw6rJkfxCAAAADwAAAAxkZW5vbWluYXRpb24AAAAPAAAAA3VzZAAAAAAPAAAAC2xhc3RfdXBkYXRlAAAAAAUAAAAAZVVB9wAAAA8AAAAEcmF0ZQAAAAkAAAAAAAAAAAAAAAAAEnzuAAAAEAAAAAEAAAACAAAADwAAAApWYXVsdHNJbmZvAAAAAAAPAAAAA2V1cgAAAAARAAAAAQAAAAgAAAAPAAAADGRlbm9taW5hdGlvbgAAAA8AAAADZXVyAAAAAA8AAAAKbG93ZXN0X2tleQAAAAAAEAAAAAEAAAACAAAADwAAAARTb21lAAAAEQAAAAEAAAADAAAADwAAAAdhY2NvdW50AAAAABIAAAAAAAAAAGKaH7iFUU2kfGOJGONeYuJ2U2QUeQ+zOEfYZvAoeHDsAAAADwAAAAxkZW5vbWluYXRpb24AAAAPAAAAA2V1cgAAAAAPAAAABWluZGV4AAAAAAAACQAAAAAAAAAAAAAAA7msoAAAAAAPAAAADG1pbl9jb2xfcmF0ZQAAAAkAAAAAAAAAAAAAAAAAp9jAAAAADwAAABFtaW5fZGVidF9jcmVhdGlvbgAAAAAAAAkAAAAAAAAAAAAAAAA7msoAAAAADwAAABBvcGVuaW5nX2NvbF9yYXRlAAAACQAAAAAAAAAAAAAAAACveeAAAAAPAAAACXRvdGFsX2NvbAAAAAAAAAkAAAAAAAAAAAAAAAlQL5AAAAAADwAAAAp0b3RhbF9kZWJ0AAAAAAAJAAAAAAAAAAAAAAAAlQL5AAAAAA8AAAAMdG90YWxfdmF1bHRzAAAABQAAAAAAAAABAAAAEAAAAAEAAAACAAAADwAAAApWYXVsdHNJbmZvAAAAAAAPAAAAA3VzZAAAAAARAAAAAQAAAAgAAAAPAAAADGRlbm9taW5hdGlvbgAAAA8AAAADdXNkAAAAAA8AAAAKbG93ZXN0X2tleQAAAAAAEAAAAAEAAAACAAAADwAAAARTb21lAAAAEQAAAAEAAAADAAAADwAAAAdhY2NvdW50AAAAABIAAAAAAAAAAGKaH7iFUU2kfGOJGONeYuJ2U2QUeQ+zOEfYZvAoeHDsAAAADwAAAAxkZW5vbWluYXRpb24AAAAPAAAAA3VzZAAAAAAPAAAABWluZGV4AAAAAAAACQAAAAAAAAAAAAAAA7msoAAAAAAPAAAADG1pbl9jb2xfcmF0ZQAAAAkAAAAAAAAAAAAAAAAAp9jAAAAADwAAABFtaW5fZGVidF9jcmVhdGlvbgAAAAAAAAkAAAAAAAAAAAAAAAA7msoAAAAADwAAABBvcGVuaW5nX2NvbF9yYXRlAAAACQAAAAAAAAAAAAAAAACveeAAAAAPAAAACXRvdGFsX2NvbAAAAAAAAAkAAAAAAAAAAAAAABF2WS4AAAAADwAAAAp0b3RhbF9kZWJ0AAAAAAAJAAAAAAAAAAAAAAAA7msoAAAAAA8AAAAMdG90YWxfdmF1bHRzAAAABQAAAAAAAAACAAAAAAAAAAEAJsIgAAAABgAAAAAAAAABhhOwI+RL18Zpk7cqI5pRRf0L96jE8i+0x3ekhuBh2cUAAAAUAAAAAQAAABMAAAAAjjTy9Ve4tTeavLz3tN8+liOOxI3ufjZESizbjVF11h0AAAABAAAABQAAABAAAAABAAAAAQAAAA8AAAAJQ29yZVN0YXRlAAAAAAAAEQAAAAEAAAAGAAAADwAAAAVhZG1pbgAAAAAAABIAAAAAAAAAADn1LT+CCK/HiHMChoEi/AtPrkos4XRR2E45Pr25lb3/AAAADwAAAAljb2xfdG9rZW4AAAAAAAASAAAAAdeSi3LCcDzP6vfrn/TvTVBKVai5efybRQ6iyEK00c5hAAAADwAAAAxvcmFjbGVfYWRtaW4AAAASAAAAAAAAAADGFY14/R1KD0VGtTbi5Yp4d7LuMW0iQbLM/AUiGKj5owAAAA8AAAAKcGFuaWNfbW9kZQAAAAAAAAAAAAAAAAAPAAAAEHByb3RvY29sX21hbmFnZXIAAAASAAAAAAAAAAAtSfyAwmj05lZ0WduHsQYQZgvahCNVtZyqS2HRC99kyQAAAA8AAAANc3RhYmxlX2lzc3VlcgAAAAAAABIAAAAAAAAAAEM5BlXva0R5UN6SCMY+6evwJa4mY/f062z0TKLnqN4wAAAAEAAAAAEAAAACAAAADwAAAAhDdXJyZW5jeQAAAA8AAAADZXVyAAAAABEAAAABAAAABQAAAA8AAAAGYWN0aXZlAAAAAAAAAAAAAQAAAA8AAAAIY29udHJhY3QAAAASAAAAAUGpebFxuPbvxZFzOxh8TWAxUwFgraPxPuJEY/8yhiYEAAAADwAAAAxkZW5vbWluYXRpb24AAAAPAAAAA2V1cgAAAAAPAAAAC2xhc3RfdXBkYXRlAAAAAAUAAAAAZVVB/AAAAA8AAAAEcmF0ZQAAAAkAAAAAAAAAAAAAAAAAEQs+AAAAEAAAAAEAAAACAAAADwAAAAhDdXJyZW5jeQAAAA8AAAADdXNkAAAAABEAAAABAAAABQAAAA8AAAAGYWN0aXZlAAAAAAAAAAAAAQAAAA8AAAAIY29udHJhY3QAAAASAAAAATUEqdkvrE2LnSiwOwed3v4VEaulOEiS1rxQw6rJkfxCAAAADwAAAAxkZW5vbWluYXRpb24AAAAPAAAAA3VzZAAAAAAPAAAAC2xhc3RfdXBkYXRlAAAAAAUAAAAAZVVB9wAAAA8AAAAEcmF0ZQAAAAkAAAAAAAAAAAAAAAAAEnzuAAAAEAAAAAEAAAACAAAADwAAAApWYXVsdHNJbmZvAAAAAAAPAAAAA2V1cgAAAAARAAAAAQAAAAgAAAAPAAAADGRlbm9taW5hdGlvbgAAAA8AAAADZXVyAAAAAA8AAAAKbG93ZXN0X2tleQAAAAAAEAAAAAEAAAACAAAADwAAAARTb21lAAAAEQAAAAEAAAADAAAADwAAAAdhY2NvdW50AAAAABIAAAAAAAAAAGKaH7iFUU2kfGOJGONeYuJ2U2QUeQ+zOEfYZvAoeHDsAAAADwAAAAxkZW5vbWluYXRpb24AAAAPAAAAA2V1cgAAAAAPAAAABWluZGV4AAAAAAAACQAAAAAAAAAAAAAAA7msoAAAAAAPAAAADG1pbl9jb2xfcmF0ZQAAAAkAAAAAAAAAAAAAAAAAp9jAAAAADwAAABFtaW5fZGVidF9jcmVhdGlvbgAAAAAAAAkAAAAAAAAAAAAAAAA7msoAAAAADwAAABBvcGVuaW5nX2NvbF9yYXRlAAAACQAAAAAAAAAAAAAAAACveeAAAAAPAAAACXRvdGFsX2NvbAAAAAAAAAkAAAAAAAAAAAAAAAlQL5AAAAAADwAAAAp0b3RhbF9kZWJ0AAAAAAAJAAAAAAAAAAAAAAAAlQL5AAAAAA8AAAAMdG90YWxfdmF1bHRzAAAABQAAAAAAAAABAAAAEAAAAAEAAAACAAAADwAAAApWYXVsdHNJbmZvAAAAAAAPAAAAA3VzZAAAAAARAAAAAQAAAAgAAAAPAAAADGRlbm9taW5hdGlvbgAAAA8AAAADdXNkAAAAAA8AAAAKbG93ZXN0X2tleQAAAAAAEAAAAAEAAAACAAAADwAAAARTb21lAAAAEQAAAAEAAAADAAAADwAAAAdhY2NvdW50AAAAABIAAAAAAAAAAGKaH7iFUU2kfGOJGONeYuJ2U2QUeQ+zOEfYZvAoeHDsAAAADwAAAAxkZW5vbWluYXRpb24AAAAPAAAAA3VzZAAAAAAPAAAABWluZGV4AAAAAAAACQAAAAAAAAAAAAAAA7msoAAAAAAPAAAADG1pbl9jb2xfcmF0ZQAAAAkAAAAAAAAAAAAAAAAAp9jAAAAADwAAABFtaW5fZGVidF9jcmVhdGlvbgAAAAAAAAkAAAAAAAAAAAAAAAA7msoAAAAADwAAABBvcGVuaW5nX2NvbF9yYXRlAAAACQAAAAAAAAAAAAAAAACveeAAAAAPAAAACXRvdGFsX2NvbAAAAAAAAAkAAAAAAAAAAAAAABF2WS4AAAAADwAAAAp0b3RhbF9kZWJ0AAAAAAAJAAAAAAAAAAAAAAAA7msoAAAAAA8AAAAMdG90YWxfdmF1bHRzAAAABQAAAAAAAAACAAAAAAAAAAAAAAABAAAAAAAAAAAAAAABAAAAFQAAAAEAAAAAAAAAAAAAAAIAAAAAAAAAAwAAAA8AAAAHZm5fY2FsbAAAAAANAAAAIIYTsCPkS9fGaZO3KiOaUUX9C/eoxPIvtMd3pIbgYdnFAAAADwAAABFzZXRfY3VycmVuY3lfcmF0ZQAAAAAAABAAAAABAAAAAgAAAA8AAAADZXVyAAAAAAkAAAAAAAAAAAAAAAAAEQs+AAAAAQAAAAAAAAABhhOwI+RL18Zpk7cqI5pRRf0L96jE8i+0x3ekhuBh2cUAAAACAAAAAAAAAAIAAAAPAAAACWZuX3JldHVybgAAAAAAAA8AAAARc2V0X2N1cnJlbmN5X3JhdGUAAAAAAAABAAAAAAAAAAAAAAAAAAAAAgAAAAAAAAACAAAADwAAAAxjb3JlX21ldHJpY3MAAAAPAAAACnJlYWRfZW50cnkAAAAAAAUAAAAAAAAAAgAAAAAAAAAAAAAAAAAAAAIAAAAAAAAAAgAAAA8AAAAMY29yZV9tZXRyaWNzAAAADwAAAAt3cml0ZV9lbnRyeQAAAAAFAAAAAAAAAAEAAAAAAAAAAAAAAAAAAAACAAAAAAAAAAIAAAAPAAAADGNvcmVfbWV0cmljcwAAAA8AAAAQbGVkZ2VyX3JlYWRfYnl0ZQAAAAUAAAAAAACJaAAAAAAAAAAAAAAAAAAAAAIAAAAAAAAAAgAAAA8AAAAMY29yZV9tZXRyaWNzAAAADwAAABFsZWRnZXJfd3JpdGVfYnl0ZQAAAAAAAAUAAAAAAAAHxAAAAAAAAAAAAAAAAAAAAAIAAAAAAAAAAgAAAA8AAAAMY29yZV9tZXRyaWNzAAAADwAAAA1yZWFkX2tleV9ieXRlAAAAAAAABQAAAAAAAABUAAAAAAAAAAAAAAAAAAAAAgAAAAAAAAACAAAADwAAAAxjb3JlX21ldHJpY3MAAAAPAAAADndyaXRlX2tleV9ieXRlAAAAAAAFAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAACAAAAAAAAAAIAAAAPAAAADGNvcmVfbWV0cmljcwAAAA8AAAAOcmVhZF9kYXRhX2J5dGUAAAAAAAUAAAAAAAAH6AAAAAAAAAAAAAAAAAAAAAIAAAAAAAAAAgAAAA8AAAAMY29yZV9tZXRyaWNzAAAADwAAAA93cml0ZV9kYXRhX2J5dGUAAAAABQAAAAAAAAfEAAAAAAAAAAAAAAAAAAAAAgAAAAAAAAACAAAADwAAAAxjb3JlX21ldHJpY3MAAAAPAAAADnJlYWRfY29kZV9ieXRlAAAAAAAFAAAAAAAAgYAAAAAAAAAAAAAAAAAAAAACAAAAAAAAAAIAAAAPAAAADGNvcmVfbWV0cmljcwAAAA8AAAAPd3JpdGVfY29kZV9ieXRlAAAAAAUAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAIAAAAAAAAAAgAAAA8AAAAMY29yZV9tZXRyaWNzAAAADwAAAAplbWl0X2V2ZW50AAAAAAAFAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAACAAAAAAAAAAIAAAAPAAAADGNvcmVfbWV0cmljcwAAAA8AAAAPZW1pdF9ldmVudF9ieXRlAAAAAAUAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAIAAAAAAAAAAgAAAA8AAAAMY29yZV9tZXRyaWNzAAAADwAAAAhjcHVfaW5zbgAAAAUAAAAAATLTQAAAAAAAAAAAAAAAAAAAAAIAAAAAAAAAAgAAAA8AAAAMY29yZV9tZXRyaWNzAAAADwAAAAhtZW1fYnl0ZQAAAAUAAAAAACqhewAAAAAAAAAAAAAAAAAAAAIAAAAAAAAAAgAAAA8AAAAMY29yZV9tZXRyaWNzAAAADwAAABFpbnZva2VfdGltZV9uc2VjcwAAAAAAAAUAAAAAABFfSQAAAAAAAAAAAAAAAAAAAAIAAAAAAAAAAgAAAA8AAAAMY29yZV9tZXRyaWNzAAAADwAAAA9tYXhfcndfa2V5X2J5dGUAAAAABQAAAAAAAAAwAAAAAAAAAAAAAAAAAAAAAgAAAAAAAAACAAAADwAAAAxjb3JlX21ldHJpY3MAAAAPAAAAEG1heF9yd19kYXRhX2J5dGUAAAAFAAAAAAAAB+gAAAAAAAAAAAAAAAAAAAACAAAAAAAAAAIAAAAPAAAADGNvcmVfbWV0cmljcwAAAA8AAAAQbWF4X3J3X2NvZGVfYnl0ZQAAAAUAAAAAAACBgAAAAAAAAAAAAAAAAAAAAAIAAAAAAAAAAgAAAA8AAAAMY29yZV9tZXRyaWNzAAAADwAAABNtYXhfZW1pdF9ldmVudF9ieXRlAAAAAAUAAAAAAAAAAA==",
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
		mockMetricsService.On("IncRPCRequests", "getTransaction").Once()
		mockMetricsService.On("IncRPCEndpointFailure", "getTransaction").Once()
		mockMetricsService.On("ObserveRPCRequestDuration", "getTransaction", mock.AnythingOfType("float64")).Once()
		defer mockMetricsService.AssertExpectations(t)

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
	mockMetricsService := metrics.NewMockMetricsService()
	mockHTTPClient := utils.MockHTTPClient{}
	rpcURL := "http://api.vibrantapp.com/soroban/rpc"
	rpcService, err := NewRPCService(rpcURL, network.TestNetworkPassphrase, &mockHTTPClient, mockMetricsService)
	require.NoError(t, err)

	t.Run("rpc_request_fails", func(t *testing.T) {
		mockMetricsService.On("IncRPCRequests", "getTransactions").Once()
		mockMetricsService.On("IncRPCEndpointFailure", "getTransactions").Once()
		mockMetricsService.On("ObserveRPCRequestDuration", "getTransactions", mock.AnythingOfType("float64")).Once()
		defer mockMetricsService.AssertExpectations(t)

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
		mockMetricsService.On("IncRPCRequests", "getTransactions").Once()
		mockMetricsService.On("IncRPCEndpointSuccess", "getTransactions").Once()
		mockMetricsService.On("ObserveRPCRequestDuration", "getTransactions", mock.AnythingOfType("float64")).Once()
		defer mockMetricsService.AssertExpectations(t)

		params := entities.RPCParams{StartLedger: 10, Pagination: entities.RPCPagination{Limit: 5}}

		payload := map[string]interface{}{
			"jsonrpc": "2.0",
			"id":      1,
			"method":  "getTransactions",
			"params":  params,
		}
		jsonData, err := json.Marshal(payload)
		require.NoError(t, err)

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
	mockMetricsService := metrics.NewMockMetricsService()
	mockHTTPClient := utils.MockHTTPClient{}
	rpcURL := "http://api.vibrantapp.com/soroban/rpc"
	rpcService, err := NewRPCService(rpcURL, network.TestNetworkPassphrase, &mockHTTPClient, mockMetricsService)
	require.NoError(t, err)

	t.Run("successful", func(t *testing.T) {
		mockMetricsService.On("IncRPCRequests", "getHealth").Once()
		mockMetricsService.On("IncRPCEndpointSuccess", "getHealth").Once()
		mockMetricsService.On("ObserveRPCRequestDuration", "getHealth", mock.AnythingOfType("float64")).Once()
		defer mockMetricsService.AssertExpectations(t)

		payload := map[string]interface{}{
			"jsonrpc": "2.0",
			"id":      1,
			"method":  "getHealth",
		}
		jsonData, err := json.Marshal(payload)
		require.NoError(t, err)

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
		mockMetricsService.On("IncRPCRequests", "getHealth").Once()
		mockMetricsService.On("IncRPCEndpointFailure", "getHealth").Once()
		mockMetricsService.On("ObserveRPCRequestDuration", "getHealth", mock.AnythingOfType("float64")).Once()
		defer mockMetricsService.AssertExpectations(t)

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

func Test_rpcService_GetLedgers(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()
	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	const rpcURL = "https://test.com/soroban-rpc"

	t.Run("🟢successful", func(t *testing.T) {
		mockMetricsService := metrics.NewMockMetricsService()
		mockMetricsService.
			On("IncRPCRequests", "getLedgers").Once().
			On("IncRPCEndpointSuccess", "getLedgers").Once().
			On("ObserveRPCRequestDuration", "getLedgers", mock.AnythingOfType("float64")).Once()
		defer mockMetricsService.AssertExpectations(t)

		payload := map[string]any{
			"jsonrpc": "2.0",
			"id":      1,
			"method":  "getLedgers",
			"params": entities.RPCParams{
				StartLedger: 1541075,
				Pagination: entities.RPCPagination{
					Limit: 1,
				},
			},
		}
		jsonData, err := json.Marshal(payload)
		require.NoError(t, err)

		mockHTTPClient := utils.MockHTTPClient{}
		mockHTTPClient.
			On("Post", rpcURL, "application/json", bytes.NewBuffer(jsonData)).
			Return(&http.Response{
				StatusCode: http.StatusOK,
				Body: io.NopCloser(strings.NewReader(`{
					"jsonrpc": "2.0",
					"id": 8675309,
					"result": {
						"ledgers": [
						{
							"hash": "f0174cd1dad2b9304af43c9a3ed30900e738d2d0f88940d49d776c51bcfa610d",
							"sequence": 1541075,
							"ledgerCloseTime": "1750117639",
							"headerXdr": "8BdM0drSuTBK9DyaPtMJAOc40tD4iUDUnXdsUbz6YQ0AAAAWZfBjwPwfuchZSCct99MtE4yNfMuH0ishqJI3+m+vz9aw7CHKFwey/sLAvUFPrqMz3wS3efPbVR4hGaKsW7b6kwAAAABoUK0HAAAAAAAAAAEAAAAAqCTNGLyddQZNKZpbW6ykO8OqLzJpOBU9jC+btctt8DMAAABACMD13DKnK60i/BL7Mp0H4vWLhEXK1tzZ1h3AHKRV56KZSoz2ybKa2P9fQuWdvUhVXTroQz1LL3zRHeoyUPp4C98/YZgEqS/bQFcZLcQ910jqd4rcUrxJjOgFJMAUuBEZ08HM8t09UZJ1Kqg6cuLLvK6IV3+m24+jVo/lTAPkWJEAF4PTDeC2s6dkAAAAAAEKk0jb3QAAAAAAAAAAAAB+pwAAAGQATEtAAAAAyErGE6gBB1x9QL8XPn3RWBGlWkkhSzINUNtpHEgvUn0Y62DCzECDr9D1930epUoKD/aiJy08uliELqIZSfcqk1u0dEs3T0QyOh071TGfSuMRy+VYJD1rKa+7JQTYNwADLhRpfTb42PTjRzh3ChN9hbTQIDrhnu7KnZl+cPDflR4eAAAAAAAAAAA=",
							"metadataXdr": "AAAAAQAAAADwF0zR2tK5MEr0PJo+0wkA5zjS0PiJQNSdd2xRvPphDQAAABZl8GPA/B+5yFlIJy330y0TjI18y4fSKyGokjf6b6/P1rDsIcoXB7L+wsC9QU+uozPfBLd589tVHiEZoqxbtvqTAAAAAGhQrQcAAAAAAAAAAQAAAACoJM0YvJ11Bk0pmltbrKQ7w6ovMmk4FT2ML5u1y23wMwAAAEAIwPXcMqcrrSL8EvsynQfi9YuERcrW3NnWHcAcpFXnoplKjPbJsprY/19C5Z29SFVdOuhDPUsvfNEd6jJQ+ngL3z9hmASpL9tAVxktxD3XSOp3itxSvEmM6AUkwBS4ERnTwczy3T1RknUqqDpy4su8rohXf6bbj6NWj+VMA+RYkQAXg9MN4Lazp2QAAAAAAQqTSNvdAAAAAAAAAAAAAH6nAAAAZABMS0AAAADISsYTqAEHXH1Avxc+fdFYEaVaSSFLMg1Q22kcSC9SfRjrYMLMQIOv0PX3fR6lSgoP9qInLTy6WIQuohlJ9yqTW7R0SzdPRDI6HTvVMZ9K4xHL5VgkPWspr7slBNg3AAMuFGl9NvjY9ONHOHcKE32FtNAgOuGe7sqdmX5w8N+VHh4AAAAAAAAAAAAAAAFl8GPA/B+5yFlIJy330y0TjI18y4fSKyGokjf6b6/P1gAAAAIAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA/ZvdkAAAAAAAAAAA=="
						}
						],
						"latestLedger": 1541079,
						"latestLedgerCloseTime": 1750117659,
						"oldestLedger": 1420120,
						"oldestLedgerCloseTime": 1749512372,
						"cursor": "1541075"
					}
				}`)),
			}, nil).
			Once()

		rpcService, err := NewRPCService(rpcURL, network.TestNetworkPassphrase, &mockHTTPClient, mockMetricsService)
		require.NoError(t, err)

		result, err := rpcService.GetLedgers(1541075, 1)
		require.NoError(t, err)

		assert.Equal(t, GetLedgersResponse{
			LatestLedger:          1541079,
			LatestLedgerCloseTime: 1750117659,
			OldestLedger:          1420120,
			OldestLedgerCloseTime: 1749512372,
			Cursor:                "1541075",
			Ledgers: []protocol.LedgerInfo{
				{
					Sequence:        1541075,
					Hash:            "f0174cd1dad2b9304af43c9a3ed30900e738d2d0f88940d49d776c51bcfa610d",
					LedgerHeader:    "8BdM0drSuTBK9DyaPtMJAOc40tD4iUDUnXdsUbz6YQ0AAAAWZfBjwPwfuchZSCct99MtE4yNfMuH0ishqJI3+m+vz9aw7CHKFwey/sLAvUFPrqMz3wS3efPbVR4hGaKsW7b6kwAAAABoUK0HAAAAAAAAAAEAAAAAqCTNGLyddQZNKZpbW6ykO8OqLzJpOBU9jC+btctt8DMAAABACMD13DKnK60i/BL7Mp0H4vWLhEXK1tzZ1h3AHKRV56KZSoz2ybKa2P9fQuWdvUhVXTroQz1LL3zRHeoyUPp4C98/YZgEqS/bQFcZLcQ910jqd4rcUrxJjOgFJMAUuBEZ08HM8t09UZJ1Kqg6cuLLvK6IV3+m24+jVo/lTAPkWJEAF4PTDeC2s6dkAAAAAAEKk0jb3QAAAAAAAAAAAAB+pwAAAGQATEtAAAAAyErGE6gBB1x9QL8XPn3RWBGlWkkhSzINUNtpHEgvUn0Y62DCzECDr9D1930epUoKD/aiJy08uliELqIZSfcqk1u0dEs3T0QyOh071TGfSuMRy+VYJD1rKa+7JQTYNwADLhRpfTb42PTjRzh3ChN9hbTQIDrhnu7KnZl+cPDflR4eAAAAAAAAAAA=",
					LedgerCloseTime: 1750117639,
					LedgerMetadata:  "AAAAAQAAAADwF0zR2tK5MEr0PJo+0wkA5zjS0PiJQNSdd2xRvPphDQAAABZl8GPA/B+5yFlIJy330y0TjI18y4fSKyGokjf6b6/P1rDsIcoXB7L+wsC9QU+uozPfBLd589tVHiEZoqxbtvqTAAAAAGhQrQcAAAAAAAAAAQAAAACoJM0YvJ11Bk0pmltbrKQ7w6ovMmk4FT2ML5u1y23wMwAAAEAIwPXcMqcrrSL8EvsynQfi9YuERcrW3NnWHcAcpFXnoplKjPbJsprY/19C5Z29SFVdOuhDPUsvfNEd6jJQ+ngL3z9hmASpL9tAVxktxD3XSOp3itxSvEmM6AUkwBS4ERnTwczy3T1RknUqqDpy4su8rohXf6bbj6NWj+VMA+RYkQAXg9MN4Lazp2QAAAAAAQqTSNvdAAAAAAAAAAAAAH6nAAAAZABMS0AAAADISsYTqAEHXH1Avxc+fdFYEaVaSSFLMg1Q22kcSC9SfRjrYMLMQIOv0PX3fR6lSgoP9qInLTy6WIQuohlJ9yqTW7R0SzdPRDI6HTvVMZ9K4xHL5VgkPWspr7slBNg3AAMuFGl9NvjY9ONHOHcKE32FtNAgOuGe7sqdmX5w8N+VHh4AAAAAAAAAAAAAAAFl8GPA/B+5yFlIJy330y0TjI18y4fSKyGokjf6b6/P1gAAAAIAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA/ZvdkAAAAAAAAAAA==",
				},
			},
		}, result)
	})

	t.Run("🔴rpc_request_fails", func(t *testing.T) {
		mockMetricsService := metrics.NewMockMetricsService()
		mockMetricsService.
			On("IncRPCRequests", "getLedgers").Once().
			On("IncRPCEndpointFailure", "getLedgers").Once().
			On("ObserveRPCRequestDuration", "getLedgers", mock.AnythingOfType("float64")).Once()
		defer mockMetricsService.AssertExpectations(t)

		mockHTTPClient := utils.MockHTTPClient{}
		mockHTTPClient.
			On("Post", rpcURL, "application/json", mock.Anything).
			Return(&http.Response{}, errors.New("connection failed")).
			Once()

		rpcService, err := NewRPCService(rpcURL, network.TestNetworkPassphrase, &mockHTTPClient, mockMetricsService)
		require.NoError(t, err)

		result, err := rpcService.GetLedgers(1541075, 1)
		require.Error(t, err)

		assert.Equal(t, GetLedgersResponse{}, result)
		assert.Equal(t, "sending getLedgers request: sending POST request to RPC: connection failed", err.Error())
	})
}

func TestTrackRPCServiceHealth_HealthyService(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()

	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	mockMetricsService := metrics.NewMockMetricsService()
	mockMetricsService.On("IncRPCRequests", "getHealth").Once()
	mockMetricsService.On("IncRPCEndpointSuccess", "getHealth").Once()
	mockMetricsService.On("ObserveRPCRequestDuration", "getHealth", mock.AnythingOfType("float64")).Once()
	mockMetricsService.On("SetRPCServiceHealth", true).Once()
	mockMetricsService.On("SetRPCLatestLedger", int64(100)).Once()
	defer mockMetricsService.AssertExpectations(t)

	mockHTTPClient := &utils.MockHTTPClient{}
	rpcURL := "http://test-url-track-rpc-service-health"
	rpcService, err := NewRPCService(rpcURL, network.TestNetworkPassphrase, mockHTTPClient, mockMetricsService)
	require.NoError(t, err)

	healthResult := entities.RPCGetHealthResult{
		Status:                "healthy",
		LatestLedger:          100,
		OldestLedger:          1,
		LedgerRetentionWindow: 0,
	}

	// Mock the HTTP response for GetHealth
	ctx, cancel := context.WithTimeout(context.Background(), rpcService.HealthCheckTickInterval()*2)
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
	rpcService.TrackRPCServiceHealth(ctx, nil)

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
	healthCheckTickInterval := 300 * time.Millisecond
	healthCheckWarningInterval := 400 * time.Millisecond
	contextTimeout := healthCheckWarningInterval + time.Millisecond*190
	ctx, cancel := context.WithTimeout(context.Background(), contextTimeout)
	defer cancel()

	dbt := dbtest.Open(t)
	defer dbt.Close()

	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	mockMetricsService := metrics.NewMockMetricsService()
	mockMetricsService.On("IncRPCRequests", "getHealth").Once()
	mockMetricsService.On("IncRPCRequests", "getHealth").Maybe()
	mockMetricsService.On("IncRPCEndpointFailure", "getHealth").Once()
	mockMetricsService.On("IncRPCEndpointFailure", "getHealth").Maybe()
	mockMetricsService.On("ObserveRPCRequestDuration", "getHealth", mock.AnythingOfType("float64")).Once()
	mockMetricsService.On("ObserveRPCRequestDuration", "getHealth", mock.AnythingOfType("float64")).Maybe()
	mockMetricsService.On("SetRPCServiceHealth", false).Once()
	mockMetricsService.On("SetRPCServiceHealth", false).Maybe()
	defer mockMetricsService.AssertExpectations(t)
	getLogs := log.DefaultLogger.StartTest(log.WarnLevel)

	mockHTTPClient := &utils.MockHTTPClient{}
	defer mockHTTPClient.AssertExpectations(t)
	rpcURL := "http://test-url-track-rpc-service-health"
	rpcService, err := NewRPCService(rpcURL, network.TestNetworkPassphrase, mockHTTPClient, mockMetricsService)
	require.NoError(t, err)
	rpcService.healthCheckTickInterval = healthCheckTickInterval
	rpcService.healthCheckWarningInterval = healthCheckWarningInterval

	// Mock error response for GetHealth with a valid http.Response
	getHealthRequestBody, err := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": 1, "method": "getHealth"})
	require.NoError(t, err)
	getHealthResponseBody := `{
		"jsonrpc": "2.0",
		"id": 1,
		"error": {
			"code": -32601,
			"message": "rpc error"
		}
	}`
	mockResponse := &http.Response{
		Body: io.NopCloser(strings.NewReader(getHealthResponseBody)),
	}
	mockHTTPClient.On("Post", rpcURL, "application/json", bytes.NewBuffer(getHealthRequestBody)).
		Return(mockResponse, nil)

	// The ctx will timeout after {contextTimeout}, which is enough for the warning to trigger
	rpcService.TrackRPCServiceHealth(ctx, nil)

	entries := getLogs()
	testSucceeded := false
	logMessages := []string{}
	for _, entry := range entries {
		logMessages = append(logMessages, entry.Message)
		if strings.Contains(entry.Message, "RPC service unhealthy for over "+healthCheckWarningInterval.String()) {
			testSucceeded = true
			break
		}
	}
	assert.Truef(t, testSucceeded, "couldn't find log entry containing %q in %v", "rpc service unhealthy for over "+healthCheckWarningInterval.String(), logMessages)
}

func TestTrackRPCService_ContextCancelled(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	dbt := dbtest.Open(t)
	defer dbt.Close()

	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()
	mockMetricsService := metrics.NewMockMetricsService()
	mockHTTPClient := &utils.MockHTTPClient{}
	rpcURL := "http://test-url-track-rpc-service-health"
	rpcService, err := NewRPCService(rpcURL, network.TestNetworkPassphrase, mockHTTPClient, mockMetricsService)
	require.NoError(t, err)

	rpcService.TrackRPCServiceHealth(ctx, nil)

	// Verify channel is closed after context cancellation
	time.Sleep(100 * time.Millisecond)
	_, ok := <-rpcService.GetHeartbeatChannel()
	assert.False(t, ok, "channel should be closed")

	mockHTTPClient.AssertNotCalled(t, "Post")
}
