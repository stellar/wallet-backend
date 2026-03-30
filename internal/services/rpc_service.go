package services

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/stellar/go-stellar-sdk/xdr"
	"github.com/stellar/stellar-rpc/protocol"

	"github.com/stellar/wallet-backend/internal/entities"
	"github.com/stellar/wallet-backend/internal/metrics"
	"github.com/stellar/wallet-backend/internal/utils"
)

const (
	defaultHealthCheckTickInterval    = 5 * time.Second
	defaultHealthCheckWarningInterval = 60 * time.Second
	getHealthMethodName               = "getHealth"
)

type RPCService interface {
	GetTransaction(transactionHash string) (entities.RPCGetTransactionResult, error)
	GetTransactions(startLedger int64, startCursor string, limit int) (entities.RPCGetTransactionsResult, error)
	SendTransaction(transactionXDR string) (entities.RPCSendTransactionResult, error)
	GetHealth() (entities.RPCGetHealthResult, error)
	GetLedgers(startLedger uint32, limit uint32) (GetLedgersResponse, error)
	GetLedgerEntries(keys []string) (entities.RPCGetLedgerEntriesResult, error)
	GetAccountLedgerSequence(address string) (int64, error)
	SimulateTransaction(transactionXDR string, resourceConfig entities.RPCResourceConfig) (entities.RPCSimulateTransactionResult, error)
	NetworkPassphrase() string
}

type rpcService struct {
	rpcURL                     string
	httpClient                 utils.HTTPClient
	metrics                    *metrics.RPCMetrics
	healthCheckWarningInterval time.Duration
	healthCheckTickInterval    time.Duration
	networkPassphrase          string
}

var PageLimit = 200

var _ RPCService = (*rpcService)(nil)

func NewRPCService(rpcURL, networkPassphrase string, httpClient utils.HTTPClient, rpcMetrics *metrics.RPCMetrics) (*rpcService, error) {
	if rpcURL == "" {
		return nil, errors.New("rpcURL is required")
	}
	if networkPassphrase == "" {
		return nil, errors.New("networkPassphrase is required")
	}
	if httpClient == nil {
		return nil, errors.New("httpClient is required")
	}
	if rpcMetrics == nil {
		return nil, errors.New("rpcMetrics is required")
	}

	return &rpcService{
		rpcURL:                     rpcURL,
		httpClient:                 httpClient,
		metrics:                    rpcMetrics,
		healthCheckWarningInterval: defaultHealthCheckWarningInterval,
		healthCheckTickInterval:    defaultHealthCheckTickInterval,
		networkPassphrase:          networkPassphrase,
	}, nil
}

func (r *rpcService) GetTransaction(transactionHash string) (entities.RPCGetTransactionResult, error) {
	startTime := time.Now()
	r.metrics.MethodCallsTotal.WithLabelValues("GetTransaction").Inc()
	defer func() {
		duration := time.Since(startTime).Seconds()
		r.metrics.MethodDuration.WithLabelValues("GetTransaction").Observe(duration)
	}()

	resultBytes, err := r.sendRPCRequest("getTransaction", entities.RPCParams{Hash: transactionHash})
	if err != nil {
		r.metrics.MethodErrorsTotal.WithLabelValues("GetTransaction", "rpc_error").Inc()
		return entities.RPCGetTransactionResult{}, fmt.Errorf("sending getTransaction request: %w", err)
	}

	var result entities.RPCGetTransactionResult
	err = json.Unmarshal(resultBytes, &result)
	if err != nil {
		r.metrics.MethodErrorsTotal.WithLabelValues("GetTransaction", "json_unmarshal_error").Inc()
		return entities.RPCGetTransactionResult{}, fmt.Errorf("parsing getTransaction result JSON: %w", err)
	}

	return result, nil
}

func (r *rpcService) GetTransactions(startLedger int64, startCursor string, limit int) (entities.RPCGetTransactionsResult, error) {
	startTime := time.Now()
	r.metrics.MethodCallsTotal.WithLabelValues("GetTransactions").Inc()
	defer func() {
		duration := time.Since(startTime).Seconds()
		r.metrics.MethodDuration.WithLabelValues("GetTransactions").Observe(duration)
	}()

	if limit > PageLimit {
		r.metrics.MethodErrorsTotal.WithLabelValues("GetTransactions", "validation_error").Inc()
		return entities.RPCGetTransactionsResult{}, fmt.Errorf("limit cannot exceed %d", PageLimit)
	}
	params := entities.RPCParams{}
	if startCursor != "" {
		params.Pagination = entities.RPCPagination{Cursor: startCursor, Limit: limit}
	} else {
		params.StartLedger = startLedger
		params.Pagination = entities.RPCPagination{Limit: limit}
	}
	resultBytes, err := r.sendRPCRequest("getTransactions", params)
	if err != nil {
		r.metrics.MethodErrorsTotal.WithLabelValues("GetTransactions", "rpc_error").Inc()
		return entities.RPCGetTransactionsResult{}, fmt.Errorf("sending getTransactions request: %w", err)
	}

	var result entities.RPCGetTransactionsResult
	err = json.Unmarshal(resultBytes, &result)
	if err != nil {
		r.metrics.MethodErrorsTotal.WithLabelValues("GetTransactions", "json_unmarshal_error").Inc()
		return entities.RPCGetTransactionsResult{}, fmt.Errorf("parsing getTransactions result JSON: %w", err)
	}
	return result, nil
}

func (r *rpcService) GetHealth() (entities.RPCGetHealthResult, error) {
	startTime := time.Now()
	r.metrics.MethodCallsTotal.WithLabelValues("GetHealth").Inc()
	defer func() {
		duration := time.Since(startTime).Seconds()
		r.metrics.MethodDuration.WithLabelValues("GetHealth").Observe(duration)
	}()

	resultBytes, err := r.sendRPCRequest("getHealth", entities.RPCParams{})
	if err != nil {
		r.metrics.MethodErrorsTotal.WithLabelValues("GetHealth", "rpc_error").Inc()
		r.metrics.ServiceHealth.Set(0)
		return entities.RPCGetHealthResult{}, fmt.Errorf("sending getHealth request: %w", err)
	}

	var result entities.RPCGetHealthResult
	err = json.Unmarshal(resultBytes, &result)
	if err != nil {
		r.metrics.MethodErrorsTotal.WithLabelValues("GetHealth", "json_unmarshal_error").Inc()
		r.metrics.ServiceHealth.Set(0)
		return entities.RPCGetHealthResult{}, fmt.Errorf("parsing getHealth result JSON: %w", err)
	}

	if result.Status == "healthy" {
		r.metrics.ServiceHealth.Set(1)
	} else {
		r.metrics.ServiceHealth.Set(0)
	}
	r.metrics.LatestLedger.Set(float64(result.LatestLedger))

	return result, nil
}

type GetLedgersResponse protocol.GetLedgersResponse

func (r *rpcService) GetLedgers(startLedger uint32, limit uint32) (GetLedgersResponse, error) {
	startTime := time.Now()
	r.metrics.MethodCallsTotal.WithLabelValues("GetLedgers").Inc()
	defer func() {
		duration := time.Since(startTime).Seconds()
		r.metrics.MethodDuration.WithLabelValues("GetLedgers").Observe(duration)
	}()

	resultBytes, err := r.sendRPCRequest("getLedgers", entities.RPCParams{
		StartLedger: int64(startLedger),
		Pagination: entities.RPCPagination{
			Limit: int(limit),
		},
	})
	if err != nil {
		r.metrics.MethodErrorsTotal.WithLabelValues("GetLedgers", "rpc_error").Inc()
		return GetLedgersResponse{}, fmt.Errorf("sending getLedgers request: %w", err)
	}

	var result GetLedgersResponse
	err = json.Unmarshal(resultBytes, &result)
	if err != nil {
		r.metrics.MethodErrorsTotal.WithLabelValues("GetLedgers", "json_unmarshal_error").Inc()
		return GetLedgersResponse{}, fmt.Errorf("parsing getLedgers result JSON: %w", err)
	}

	return result, nil
}

func (r *rpcService) GetLedgerEntries(keys []string) (entities.RPCGetLedgerEntriesResult, error) {
	startTime := time.Now()
	r.metrics.MethodCallsTotal.WithLabelValues("GetLedgerEntries").Inc()
	defer func() {
		duration := time.Since(startTime).Seconds()
		r.metrics.MethodDuration.WithLabelValues("GetLedgerEntries").Observe(duration)
	}()

	resultBytes, err := r.sendRPCRequest("getLedgerEntries", entities.RPCParams{
		LedgerKeys: keys,
	})
	if err != nil {
		r.metrics.MethodErrorsTotal.WithLabelValues("GetLedgerEntries", "rpc_error").Inc()
		return entities.RPCGetLedgerEntriesResult{}, fmt.Errorf("sending getLedgerEntries request: %w", err)
	}

	var result entities.RPCGetLedgerEntriesResult
	err = json.Unmarshal(resultBytes, &result)
	if err != nil {
		r.metrics.MethodErrorsTotal.WithLabelValues("GetLedgerEntries", "json_unmarshal_error").Inc()
		return entities.RPCGetLedgerEntriesResult{}, fmt.Errorf("parsing getLedgerEntries result JSON: %w", err)
	}
	return result, nil
}

func (r *rpcService) SendTransaction(transactionXDR string) (entities.RPCSendTransactionResult, error) {
	startTime := time.Now()
	r.metrics.MethodCallsTotal.WithLabelValues("SendTransaction").Inc()
	defer func() {
		duration := time.Since(startTime).Seconds()
		r.metrics.MethodDuration.WithLabelValues("SendTransaction").Observe(duration)
	}()

	resultBytes, err := r.sendRPCRequest("sendTransaction", entities.RPCParams{Transaction: transactionXDR})
	if err != nil {
		r.metrics.MethodErrorsTotal.WithLabelValues("SendTransaction", "rpc_error").Inc()
		return entities.RPCSendTransactionResult{}, fmt.Errorf("sending sendTransaction request: %w", err)
	}

	var result entities.RPCSendTransactionResult
	err = json.Unmarshal(resultBytes, &result)
	if err != nil {
		r.metrics.MethodErrorsTotal.WithLabelValues("SendTransaction", "json_unmarshal_error").Inc()
		return entities.RPCSendTransactionResult{}, fmt.Errorf("parsing sendTransaction result JSON: %w", err)
	}

	return result, nil
}

func (r *rpcService) SimulateTransaction(transactionXDR string, resourceConfig entities.RPCResourceConfig) (entities.RPCSimulateTransactionResult, error) {
	startTime := time.Now()
	r.metrics.MethodCallsTotal.WithLabelValues("SimulateTransaction").Inc()
	defer func() {
		duration := time.Since(startTime).Seconds()
		r.metrics.MethodDuration.WithLabelValues("SimulateTransaction").Observe(duration)
	}()

	resultBytes, err := r.sendRPCRequest("simulateTransaction", entities.RPCParams{Transaction: transactionXDR, ResourceConfig: resourceConfig})
	if err != nil {
		r.metrics.MethodErrorsTotal.WithLabelValues("SimulateTransaction", "rpc_error").Inc()
		return entities.RPCSimulateTransactionResult{}, fmt.Errorf("sending simulateTransaction request: %w", err)
	}

	var result entities.RPCSimulateTransactionResult
	err = json.Unmarshal(resultBytes, &result)
	if err != nil {
		r.metrics.MethodErrorsTotal.WithLabelValues("SimulateTransaction", "json_unmarshal_error").Inc()
		return entities.RPCSimulateTransactionResult{}, fmt.Errorf("parsing simulateTransaction result JSON: %w", err)
	}

	return result, nil
}

func (r *rpcService) GetAccountLedgerSequence(address string) (int64, error) {
	startTime := time.Now()
	r.metrics.MethodCallsTotal.WithLabelValues("GetAccountLedgerSequence").Inc()
	defer func() {
		duration := time.Since(startTime).Seconds()
		r.metrics.MethodDuration.WithLabelValues("GetAccountLedgerSequence").Observe(duration)
	}()

	keyXdr, err := utils.GetAccountLedgerKey(address)
	if err != nil {
		r.metrics.MethodErrorsTotal.WithLabelValues("GetAccountLedgerSequence", "validation_error").Inc()
		return 0, fmt.Errorf("getting ledger key for account public key: %w", err)
	}
	result, err := r.GetLedgerEntries([]string{keyXdr})
	if err != nil {
		r.metrics.MethodErrorsTotal.WithLabelValues("GetAccountLedgerSequence", "rpc_error").Inc()
		return 0, fmt.Errorf("getting ledger entry for account public key: %w", err)
	}
	if len(result.Entries) == 0 {
		r.metrics.MethodErrorsTotal.WithLabelValues("GetAccountLedgerSequence", "not_found_error").Inc()
		return 0, fmt.Errorf("%w: entry not found for account public key", ErrAccountNotFound)
	}

	var ledgerEntryData xdr.LedgerEntryData
	err = xdr.SafeUnmarshalBase64(result.Entries[0].DataXDR, &ledgerEntryData)
	if err != nil {
		r.metrics.MethodErrorsTotal.WithLabelValues("GetAccountLedgerSequence", "xdr_decode_error").Inc()
		return 0, fmt.Errorf("decoding account entry for account public key: %w", err)
	}
	accountEntry := ledgerEntryData.MustAccount()
	return int64(accountEntry.SeqNum), nil
}

func (r *rpcService) NetworkPassphrase() string {
	return r.networkPassphrase
}

func (r *rpcService) HealthCheckWarningInterval() time.Duration {
	if utils.IsEmpty(r.healthCheckWarningInterval) {
		return defaultHealthCheckWarningInterval
	}
	return r.healthCheckWarningInterval
}

func (r *rpcService) HealthCheckTickInterval() time.Duration {
	if utils.IsEmpty(r.healthCheckTickInterval) {
		return defaultHealthCheckTickInterval
	}
	return r.healthCheckTickInterval
}

func (r *rpcService) sendRPCRequest(method string, params entities.RPCParams) (json.RawMessage, error) {
	startTime := time.Now()
	r.metrics.InFlightRequests.Inc()
	defer func() {
		r.metrics.InFlightRequests.Dec()
		r.metrics.RequestDuration.WithLabelValues(method).Observe(time.Since(startTime).Seconds())
	}()

	payload := map[string]interface{}{
		"jsonrpc": "2.0",
		"id":      1,
		"method":  method,
	}
	// The getHealth method in RPC does not expect any params and returns an error if an empty
	// params interface is sent.
	if method != getHealthMethodName {
		payload["params"] = params
	}

	jsonData, err := json.Marshal(payload)
	if err != nil {
		r.metrics.RequestsTotal.WithLabelValues(method, "failure").Inc()
		return nil, fmt.Errorf("marshaling payload")
	}
	resp, err := r.httpClient.Post(r.rpcURL, "application/json", bytes.NewBuffer(jsonData))
	if err != nil {
		r.metrics.RequestsTotal.WithLabelValues(method, "failure").Inc()
		return nil, fmt.Errorf("sending POST request to RPC: %w", err)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		r.metrics.RequestsTotal.WithLabelValues(method, "failure").Inc()
		return nil, fmt.Errorf("unmarshaling RPC response: %w", err)
	}
	defer utils.DeferredClose(context.TODO(), resp.Body, "closing response body in the sendRPCRequest function")
	r.metrics.ResponseSizeBytes.WithLabelValues(method).Observe(float64(len(body)))

	if resp.StatusCode != http.StatusOK {
		r.metrics.RequestsTotal.WithLabelValues(method, "failure").Inc()
		return nil, fmt.Errorf("RPC returned status code=%d, body=%s", resp.StatusCode, string(body))
	}

	var res entities.RPCResponse
	err = json.Unmarshal(body, &res)
	if err != nil {
		r.metrics.RequestsTotal.WithLabelValues(method, "failure").Inc()
		return nil, fmt.Errorf("parsing RPC response JSON body %v: %w", string(body), err)
	}

	if res.Result == nil {
		r.metrics.RequestsTotal.WithLabelValues(method, "failure").Inc()
		return nil, fmt.Errorf("response %s missing result field", string(body))
	}

	r.metrics.RequestsTotal.WithLabelValues(method, "success").Inc()
	return res.Result, nil
}
