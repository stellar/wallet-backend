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
	SendTransaction(transactionXDR string) (entities.RPCSendTransactionResult, error)
}

type rpcService struct {
	rpcURL     string
	httpClient utils.HTTPClient
}

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
	resultBytes, err := r.sendRPCRequest("getTransaction", map[string]string{"hash": transactionHash})
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

func (r *rpcService) SendTransaction(transactionXDR string) (entities.RPCSendTransactionResult, error) {
	resultBytes, err := r.sendRPCRequest("sendTransaction", map[string]string{"transaction": transactionXDR})
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

func (r *rpcService) sendRPCRequest(method string, params map[string]string) (json.RawMessage, error) {
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

	return res.Result, nil
}
