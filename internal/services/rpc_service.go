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
	GetHeartbeatChannel() chan entities.RPCGetHealthResult
	SimulateTransaction(transactionXDR string, resourceConfig entities.RPCResourceConfig) (entities.RPCSimulateTransactionResult, error)
	NetworkPassphrase() string
}

type rpcService struct {
	rpcURL                     string
	httpClient                 utils.HTTPClient
	heartbeatChannel           chan entities.RPCGetHealthResult
	metricsService             metrics.MetricsService
	healthCheckWarningInterval time.Duration
	healthCheckTickInterval    time.Duration
	networkPassphrase          string
}

var PageLimit = 200

var _ RPCService = (*rpcService)(nil)

func NewRPCService(rpcURL, networkPassphrase string, httpClient utils.HTTPClient, metricsService metrics.MetricsService) (*rpcService, error) {
	if rpcURL == "" {
		return nil, errors.New("rpcURL is required")
	}
	if networkPassphrase == "" {
		return nil, errors.New("networkPassphrase is required")
	}
	if httpClient == nil {
		return nil, errors.New("httpClient is required")
	}
	if metricsService == nil {
		return nil, errors.New("metricsService is required")
	}

	heartbeatChannel := make(chan entities.RPCGetHealthResult, 1)
	return &rpcService{
		rpcURL:                     rpcURL,
		httpClient:                 httpClient,
		heartbeatChannel:           heartbeatChannel,
		metricsService:             metricsService,
		healthCheckWarningInterval: defaultHealthCheckWarningInterval,
		healthCheckTickInterval:    defaultHealthCheckTickInterval,
		networkPassphrase:          networkPassphrase,
	}, nil
}

func (r *rpcService) GetHeartbeatChannel() chan entities.RPCGetHealthResult {
	return r.heartbeatChannel
}

func (r *rpcService) GetTransaction(transactionHash string) (entities.RPCGetTransactionResult, error) {
	startTime := time.Now()
	r.metricsService.IncRPCMethodCalls("GetTransaction")
	defer func() {
		duration := time.Since(startTime).Seconds()
		r.metricsService.ObserveRPCMethodDuration("GetTransaction", duration)
	}()

	resultBytes, err := r.sendRPCRequest("getTransaction", entities.RPCParams{Hash: transactionHash})
	if err != nil {
		r.metricsService.IncRPCMethodErrors("GetTransaction", "rpc_error")
		return entities.RPCGetTransactionResult{}, fmt.Errorf("sending getTransaction request: %w", err)
	}

	var result entities.RPCGetTransactionResult
	err = json.Unmarshal(resultBytes, &result)
	if err != nil {
		r.metricsService.IncRPCMethodErrors("GetTransaction", "json_unmarshal_error")
		return entities.RPCGetTransactionResult{}, fmt.Errorf("parsing getTransaction result JSON: %w", err)
	}

	return result, nil
}

func (r *rpcService) GetTransactions(startLedger int64, startCursor string, limit int) (entities.RPCGetTransactionsResult, error) {
	startTime := time.Now()
	r.metricsService.IncRPCMethodCalls("GetTransactions")
	defer func() {
		duration := time.Since(startTime).Seconds()
		r.metricsService.ObserveRPCMethodDuration("GetTransactions", duration)
	}()

	if limit > PageLimit {
		r.metricsService.IncRPCMethodErrors("GetTransactions", "validation_error")
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
		r.metricsService.IncRPCMethodErrors("GetTransactions", "rpc_error")
		return entities.RPCGetTransactionsResult{}, fmt.Errorf("sending getTransactions request: %w", err)
	}

	var result entities.RPCGetTransactionsResult
	err = json.Unmarshal(resultBytes, &result)
	if err != nil {
		r.metricsService.IncRPCMethodErrors("GetTransactions", "json_unmarshal_error")
		return entities.RPCGetTransactionsResult{}, fmt.Errorf("parsing getTransactions result JSON: %w", err)
	}
	return result, nil
}

func (r *rpcService) GetHealth() (entities.RPCGetHealthResult, error) {
	startTime := time.Now()
	r.metricsService.IncRPCMethodCalls("GetHealth")
	defer func() {
		duration := time.Since(startTime).Seconds()
		r.metricsService.ObserveRPCMethodDuration("GetHealth", duration)
	}()

	resultBytes, err := r.sendRPCRequest("getHealth", entities.RPCParams{})
	if err != nil {
		r.metricsService.IncRPCMethodErrors("GetHealth", "rpc_error")
		return entities.RPCGetHealthResult{}, fmt.Errorf("sending getHealth request: %w", err)
	}

	var result entities.RPCGetHealthResult
	err = json.Unmarshal(resultBytes, &result)
	if err != nil {
		r.metricsService.IncRPCMethodErrors("GetHealth", "json_unmarshal_error")
		return entities.RPCGetHealthResult{}, fmt.Errorf("parsing getHealth result JSON: %w", err)
	}

	return result, nil
}

type GetLedgersResponse protocol.GetLedgersResponse

func (r *rpcService) GetLedgers(startLedger uint32, limit uint32) (GetLedgersResponse, error) {
	startTime := time.Now()
	r.metricsService.IncRPCMethodCalls("GetLedgers")
	defer func() {
		duration := time.Since(startTime).Seconds()
		r.metricsService.ObserveRPCMethodDuration("GetLedgers", duration)
	}()

	resultBytes, err := r.sendRPCRequest("getLedgers", entities.RPCParams{
		StartLedger: int64(startLedger),
		Pagination: entities.RPCPagination{
			Limit: int(limit),
		},
	})
	if err != nil {
		r.metricsService.IncRPCMethodErrors("GetLedgers", "rpc_error")
		return GetLedgersResponse{}, fmt.Errorf("sending getLedgers request: %w", err)
	}

	var result GetLedgersResponse
	err = json.Unmarshal(resultBytes, &result)
	if err != nil {
		r.metricsService.IncRPCMethodErrors("GetLedgers", "json_unmarshal_error")
		return GetLedgersResponse{}, fmt.Errorf("parsing getLedgers result JSON: %w", err)
	}

	return result, nil
}

func (r *rpcService) GetLedgerEntries(keys []string) (entities.RPCGetLedgerEntriesResult, error) {
	startTime := time.Now()
	r.metricsService.IncRPCMethodCalls("GetLedgerEntries")
	defer func() {
		duration := time.Since(startTime).Seconds()
		r.metricsService.ObserveRPCMethodDuration("GetLedgerEntries", duration)
	}()

	resultBytes, err := r.sendRPCRequest("getLedgerEntries", entities.RPCParams{
		LedgerKeys: keys,
	})
	if err != nil {
		r.metricsService.IncRPCMethodErrors("GetLedgerEntries", "rpc_error")
		return entities.RPCGetLedgerEntriesResult{}, fmt.Errorf("sending getLedgerEntries request: %w", err)
	}

	var result entities.RPCGetLedgerEntriesResult
	err = json.Unmarshal(resultBytes, &result)
	if err != nil {
		r.metricsService.IncRPCMethodErrors("GetLedgerEntries", "json_unmarshal_error")
		return entities.RPCGetLedgerEntriesResult{}, fmt.Errorf("parsing getLedgerEntries result JSON: %w", err)
	}
	return result, nil
}

func (r *rpcService) SendTransaction(transactionXDR string) (entities.RPCSendTransactionResult, error) {
	startTime := time.Now()
	r.metricsService.IncRPCMethodCalls("SendTransaction")
	defer func() {
		duration := time.Since(startTime).Seconds()
		r.metricsService.ObserveRPCMethodDuration("SendTransaction", duration)
	}()

	resultBytes, err := r.sendRPCRequest("sendTransaction", entities.RPCParams{Transaction: transactionXDR})
	if err != nil {
		r.metricsService.IncRPCMethodErrors("SendTransaction", "rpc_error")
		return entities.RPCSendTransactionResult{}, fmt.Errorf("sending sendTransaction request: %w", err)
	}

	var result entities.RPCSendTransactionResult
	err = json.Unmarshal(resultBytes, &result)
	if err != nil {
		r.metricsService.IncRPCMethodErrors("SendTransaction", "json_unmarshal_error")
		return entities.RPCSendTransactionResult{}, fmt.Errorf("parsing sendTransaction result JSON: %w", err)
	}

	return result, nil
}

func (r *rpcService) SimulateTransaction(transactionXDR string, resourceConfig entities.RPCResourceConfig) (entities.RPCSimulateTransactionResult, error) {
	startTime := time.Now()
	r.metricsService.IncRPCMethodCalls("SimulateTransaction")
	defer func() {
		duration := time.Since(startTime).Seconds()
		r.metricsService.ObserveRPCMethodDuration("SimulateTransaction", duration)
	}()

	resultBytes, err := r.sendRPCRequest("simulateTransaction", entities.RPCParams{Transaction: transactionXDR, ResourceConfig: resourceConfig})
	if err != nil {
		r.metricsService.IncRPCMethodErrors("SimulateTransaction", "rpc_error")
		return entities.RPCSimulateTransactionResult{}, fmt.Errorf("sending simulateTransaction request: %w", err)
	}

	var result entities.RPCSimulateTransactionResult
	err = json.Unmarshal(resultBytes, &result)
	if err != nil {
		r.metricsService.IncRPCMethodErrors("SimulateTransaction", "json_unmarshal_error")
		return entities.RPCSimulateTransactionResult{}, fmt.Errorf("parsing simulateTransaction result JSON: %w", err)
	}

	return result, nil
}

func (r *rpcService) GetAccountLedgerSequence(address string) (int64, error) {
	startTime := time.Now()
	r.metricsService.IncRPCMethodCalls("GetAccountLedgerSequence")
	defer func() {
		duration := time.Since(startTime).Seconds()
		r.metricsService.ObserveRPCMethodDuration("GetAccountLedgerSequence", duration)
	}()

	keyXdr, err := utils.GetAccountLedgerKey(address)
	if err != nil {
		r.metricsService.IncRPCMethodErrors("GetAccountLedgerSequence", "validation_error")
		return 0, fmt.Errorf("getting ledger key for account public key: %w", err)
	}
	result, err := r.GetLedgerEntries([]string{keyXdr})
	if err != nil {
		r.metricsService.IncRPCMethodErrors("GetAccountLedgerSequence", "rpc_error")
		return 0, fmt.Errorf("getting ledger entry for account public key: %w", err)
	}
	if len(result.Entries) == 0 {
		r.metricsService.IncRPCMethodErrors("GetAccountLedgerSequence", "not_found_error")
		return 0, fmt.Errorf("%w: entry not found for account public key", ErrAccountNotFound)
	}

	var ledgerEntryData xdr.LedgerEntryData
	err = xdr.SafeUnmarshalBase64(result.Entries[0].DataXDR, &ledgerEntryData)
	if err != nil {
		r.metricsService.IncRPCMethodErrors("GetAccountLedgerSequence", "xdr_decode_error")
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
	r.metricsService.IncRPCRequests(method)
	defer func() {
		duration := time.Since(startTime).Seconds()
		r.metricsService.ObserveRPCRequestDuration(method, duration)
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
		r.metricsService.IncRPCEndpointFailure(method)
		return nil, fmt.Errorf("marshaling payload")
	}
	resp, err := r.httpClient.Post(r.rpcURL, "application/json", bytes.NewBuffer(jsonData))
	if err != nil {
		r.metricsService.IncRPCEndpointFailure(method)
		return nil, fmt.Errorf("sending POST request to RPC: %w", err)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		r.metricsService.IncRPCEndpointFailure(method)
		return nil, fmt.Errorf("unmarshaling RPC response: %w", err)
	}
	defer utils.DeferredClose(context.TODO(), resp.Body, "closing response body in the sendRPCRequest function")

	if resp.StatusCode != http.StatusOK {
		r.metricsService.IncRPCEndpointFailure(method)
		return nil, fmt.Errorf("RPC returned status code=%d, body=%s", resp.StatusCode, string(body))
	}

	var res entities.RPCResponse
	err = json.Unmarshal(body, &res)
	if err != nil {
		r.metricsService.IncRPCEndpointFailure(method)
		return nil, fmt.Errorf("parsing RPC response JSON body %v: %w", string(body), err)
	}

	if res.Result == nil {
		r.metricsService.IncRPCEndpointFailure(method)
		return nil, fmt.Errorf("response %s missing result field", string(body))
	}

	r.metricsService.IncRPCEndpointSuccess(method)
	return res.Result, nil
}
