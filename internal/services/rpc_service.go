package services

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/stellar/go/support/log"
	"github.com/stellar/go/xdr"

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
	GetLedgerEntries(keys []string) (entities.RPCGetLedgerEntriesResult, error)
	GetAccountLedgerSequence(address string) (int64, error)
	GetHeartbeatChannel() chan entities.RPCGetHealthResult
	// TrackRPCServiceHealth continuously monitors the health of the RPC service and updates metrics.
	// It runs health checks at regular intervals and can be triggered on-demand via healthCheckTrigger.
	//
	// The healthCheckTrigger channel allows external components to request an immediate health check,
	// which is particularly useful when the ingestor needs to catch up with the RPC service.
	TrackRPCServiceHealth(ctx context.Context, healthCheckTrigger chan any)
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
		return nil, errors.New("rpcURL cannot be nil")
	}
	if httpClient == nil {
		return nil, errors.New("httpClient cannot be nil")
	}
	if metricsService == nil {
		return nil, errors.New("metricsService cannot be nil")
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
	resultBytes, err := r.sendRPCRequest("getTransaction", entities.RPCParams{Hash: transactionHash})
	if err != nil {
		return entities.RPCGetTransactionResult{}, fmt.Errorf("sending getTransaction request: %w", err)
	}

	var result entities.RPCGetTransactionResult
	err = json.Unmarshal(resultBytes, &result)
	if err != nil {
		return entities.RPCGetTransactionResult{}, fmt.Errorf("parsing getTransaction result JSON: %w", err)
	}

	return result, nil
}

func (r *rpcService) GetTransactions(startLedger int64, startCursor string, limit int) (entities.RPCGetTransactionsResult, error) {
	if limit > PageLimit {
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
		return entities.RPCGetTransactionsResult{}, fmt.Errorf("sending getTransactions request: %w", err)
	}

	var result entities.RPCGetTransactionsResult
	err = json.Unmarshal(resultBytes, &result)
	if err != nil {
		return entities.RPCGetTransactionsResult{}, fmt.Errorf("parsing getTransactions result JSON: %w", err)
	}
	return result, nil
}

func (r *rpcService) GetHealth() (entities.RPCGetHealthResult, error) {
	resultBytes, err := r.sendRPCRequest("getHealth", entities.RPCParams{})
	if err != nil {
		return entities.RPCGetHealthResult{}, fmt.Errorf("sending getHealth request: %w", err)
	}

	var result entities.RPCGetHealthResult
	err = json.Unmarshal(resultBytes, &result)
	if err != nil {
		return entities.RPCGetHealthResult{}, fmt.Errorf("parsing getHealth result JSON: %w", err)
	}

	return result, nil
}

func (r *rpcService) GetLedgerEntries(keys []string) (entities.RPCGetLedgerEntriesResult, error) {
	resultBytes, err := r.sendRPCRequest("getLedgerEntries", entities.RPCParams{
		LedgerKeys: keys,
	})
	if err != nil {
		return entities.RPCGetLedgerEntriesResult{}, fmt.Errorf("sending getLedgerEntries request: %w", err)
	}

	var result entities.RPCGetLedgerEntriesResult
	err = json.Unmarshal(resultBytes, &result)
	if err != nil {
		return entities.RPCGetLedgerEntriesResult{}, fmt.Errorf("parsing getLedgerEntries result JSON: %w", err)
	}
	return result, nil
}

func (r *rpcService) SendTransaction(transactionXDR string) (entities.RPCSendTransactionResult, error) {
	resultBytes, err := r.sendRPCRequest("sendTransaction", entities.RPCParams{Transaction: transactionXDR})
	if err != nil {
		return entities.RPCSendTransactionResult{}, fmt.Errorf("sending sendTransaction request: %w", err)
	}

	var result entities.RPCSendTransactionResult
	err = json.Unmarshal(resultBytes, &result)
	if err != nil {
		return entities.RPCSendTransactionResult{}, fmt.Errorf("parsing sendTransaction result JSON: %w", err)
	}

	return result, nil
}

func (r *rpcService) SimulateTransaction(transactionXDR string, resourceConfig entities.RPCResourceConfig) (entities.RPCSimulateTransactionResult, error) {
	resultBytes, err := r.sendRPCRequest("simulateTransaction", entities.RPCParams{Transaction: transactionXDR, ResourceConfig: resourceConfig})
	if err != nil {
		return entities.RPCSimulateTransactionResult{}, fmt.Errorf("sending simulateTransaction request: %w", err)
	}

	var result entities.RPCSimulateTransactionResult
	err = json.Unmarshal(resultBytes, &result)
	if err != nil {
		return entities.RPCSimulateTransactionResult{}, fmt.Errorf("parsing simulateTransaction result JSON: %w", err)
	}

	return result, nil
}

func (r *rpcService) GetAccountLedgerSequence(address string) (int64, error) {
	keyXdr, err := utils.GetAccountLedgerKey(address)
	if err != nil {
		return 0, fmt.Errorf("getting ledger key for account public key: %w", err)
	}
	result, err := r.GetLedgerEntries([]string{keyXdr})
	if err != nil {
		return 0, fmt.Errorf("getting ledger entry for account public key: %w", err)
	}
	if len(result.Entries) == 0 {
		return 0, fmt.Errorf("%w: entry not found for account public key", ErrAccountNotFound)
	}

	var ledgerEntryData xdr.LedgerEntryData
	err = xdr.SafeUnmarshalBase64(result.Entries[0].DataXDR, &ledgerEntryData)
	if err != nil {
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

// TrackRPCServiceHealth continuously monitors the health of the RPC service and updates metrics.
// It runs health checks at regular intervals and can be triggered on-demand via healthCheckTrigger.
//
// The healthCheckTrigger channel allows external components to request an immediate health check,
// which is particularly useful when the ingestor needs to catch up with the RPC service.
func (r *rpcService) TrackRPCServiceHealth(ctx context.Context, healthCheckTrigger chan any) {
	healthCheckTicker := time.NewTicker(r.HealthCheckTickInterval())
	warningTicker := time.NewTicker(r.HealthCheckWarningInterval())
	defer func() {
		healthCheckTicker.Stop()
		warningTicker.Stop()
		close(r.heartbeatChannel)
	}()

	performHealthCheck := func() {
		result, err := r.GetHealth()
		if err != nil {
			log.Warnf("RPC health check failed: %v", err)
			r.metricsService.SetRPCServiceHealth(false)
			return
		}

		r.heartbeatChannel <- result
		r.metricsService.SetRPCServiceHealth(true)
		r.metricsService.SetRPCLatestLedger(int64(result.LatestLedger))
		warningTicker.Reset(r.HealthCheckWarningInterval())
	}

	for {
		select {
		case <-ctx.Done():
			return
		case <-warningTicker.C:
			log.Warnf("RPC service unhealthy for over %s", r.HealthCheckWarningInterval())
			r.metricsService.SetRPCServiceHealth(false)
			warningTicker.Reset(r.HealthCheckWarningInterval())
		case <-healthCheckTicker.C:
			performHealthCheck()
		case <-healthCheckTrigger:
			healthCheckTicker.Reset(r.HealthCheckTickInterval())
			performHealthCheck()
		}
	}
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

	var res entities.RPCResponse
	err = json.Unmarshal(body, &res)
	if err != nil {
		r.metricsService.IncRPCEndpointFailure(method)
		return nil, fmt.Errorf("parsing RPC response JSON: %w", err)
	}

	if res.Result == nil {
		r.metricsService.IncRPCEndpointFailure(method)
		return nil, fmt.Errorf("response %s missing result field", string(body))
	}

	r.metricsService.IncRPCEndpointSuccess(method)
	return res.Result, nil
}
