package services

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"github.com/stellar/wallet-backend/internal/entities"
	"github.com/stellar/wallet-backend/internal/utils"
)

type RPCService interface {
	GetTransaction(transactionHash string) (entities.RPCGetTransactionResult, error)
	GetTransactions(startLedger int64, startCursor string, limit int) (entities.RPCGetTransactionsResult, error)
	SendTransaction(transactionXDR string) (entities.RPCSendTransactionResult, error)
}

type rpcService struct {
	rpcURL     string
	httpClient utils.HTTPClient
}

var PageLimit = 200

var _ RPCService = (*rpcService)(nil)

func NewRPCService(rpcURL string, httpClient utils.HTTPClient) (*rpcService, error) {
	if rpcURL == "" {
		return nil, errors.New("rpcURL cannot be nil")
	}
	if httpClient == nil {
		return nil, errors.New("httpClient cannot be nil")
	}

	return &rpcService{
		rpcURL:     rpcURL,
		httpClient: httpClient,
	}, nil
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

func (r *rpcService) sendRPCRequest(method string, params entities.RPCParams) (json.RawMessage, error) {

	payload := map[string]interface{}{
		"jsonrpc": "2.0",
		"id":      1,
		"method":  method,
		"params":  params,
	}
	jsonData, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("marshaling payload")
	}
	resp, err := r.httpClient.Post(r.rpcURL, "application/json", bytes.NewBuffer(jsonData))
	if err != nil {
		return nil, fmt.Errorf("sending POST request to RPC: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("unmarshaling RPC response: %w", err)
	}

	var res entities.RPCResponse
	err = json.Unmarshal(body, &res)
	if err != nil {
		return nil, fmt.Errorf("parsing RPC response JSON: %w", err)
	}

	if res.Result == nil {
		return nil, fmt.Errorf("response %s missing result field", string(body))
	}

	return res.Result, nil
}
