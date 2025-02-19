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
	"github.com/stellar/wallet-backend/internal/entities"
	"github.com/stellar/wallet-backend/internal/metrics"
	"github.com/stellar/wallet-backend/internal/utils"
)

const (
	rpcHealthCheckSleepTime   = 5 * time.Second
	rpcHealthCheckMaxWaitTime = 60 * time.Second
	getHealthMethodName       = "getHealth"
)

type RPCService interface {
	GetTransaction(transactionHash string) (entities.RPCGetTransactionResult, error)
	GetTransactions(startLedger int64, startCursor string, limit int) (entities.RPCGetTransactionsResult, error)
	SendTransaction(transactionXDR string) (entities.RPCSendTransactionResult, error)
	GetHealth() (entities.RPCGetHealthResult, error)
	GetLedgerEntries(keys []string) (entities.RPCGetLedgerEntriesResult, error)
	GetAccountLedgerSequence(address string) (int64, error)
	GetHeartbeatChannel() chan entities.RPCGetHealthResult
	TrackRPCServiceHealth(ctx context.Context)
}

type rpcService struct {
	rpcURL           string
	httpClient       utils.HTTPClient
	heartbeatChannel chan entities.RPCGetHealthResult
	metricsService   metrics.MetricsService
}

var PageLimit = 200

var _ RPCService = (*rpcService)(nil)

func NewRPCService(rpcURL string, httpClient utils.HTTPClient, metricsService metrics.MetricsService) (*rpcService, error) {
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
		rpcURL:           rpcURL,
		httpClient:       httpClient,
		heartbeatChannel: heartbeatChannel,
		metricsService:   metricsService,
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
		return entities.RPCGetHealthResult{}, fmt.Errorf("sending getHealth request: %v", err)
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
		return 0, fmt.Errorf("entry not found for account public key")
	}
	accountEntry, err := utils.GetAccountFromLedgerEntry(result.Entries[0])
	if err != nil {
		return 0, fmt.Errorf("decoding account entry for account public key: %w", err)
	}
	return int64(accountEntry.SeqNum), nil
}

func (r *rpcService) TrackRPCServiceHealth(ctx context.Context) {
	healthCheckTicker := time.NewTicker(rpcHealthCheckSleepTime)
	warningTicker := time.NewTicker(rpcHealthCheckMaxWaitTime)
	defer func() {
		healthCheckTicker.Stop()
		warningTicker.Stop()
		close(r.heartbeatChannel)
	}()

	for {
		select {
		case <-ctx.Done():
			return
		case <-warningTicker.C:
			log.Warn(fmt.Sprintf("rpc service unhealthy for over %s", rpcHealthCheckMaxWaitTime))
			r.metricsService.SetRPCServiceHealth(false)
			warningTicker.Reset(rpcHealthCheckMaxWaitTime)
		case <-healthCheckTicker.C:
			result, err := r.GetHealth()
			if err != nil {
				log.Warnf("rpc health check failed: %v", err)
				r.metricsService.SetRPCServiceHealth(false)
				continue
			}
			r.heartbeatChannel <- result
			r.metricsService.SetRPCServiceHealth(true)
			r.metricsService.SetRPCLatestLedger(int64(result.LatestLedger))
			warningTicker.Reset(rpcHealthCheckMaxWaitTime)
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
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		r.metricsService.IncRPCEndpointFailure(method)
		return nil, fmt.Errorf("unmarshaling RPC response: %w", err)
	}

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
