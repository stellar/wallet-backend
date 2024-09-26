package services

/*
package services

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"

	xdr3 "github.com/stellar/go-xdr/xdr3"
	"github.com/stellar/go/xdr"
	"github.com/stellar/wallet-backend/internal/tss"
)

type HTTPClient interface {
	Post(url string, t string, body io.Reader) (resp *http.Response, err error)
}

type RPCService interface {
	SendTransaction(transactionXdr string) (tss.RPCSendTxResponse, error)
	GetTransaction(transactionHash string) (tss.RPCGetIngestTxResponse, error)
}

type RPCServiceOptions struct {
	RPCURL     string
	HTTPClient HTTPClient
}

type rpcService struct {
	RPCURL     string
	HTTPClient HTTPClient
}

var _ RPCService = (*rpcService)(nil)

func NewRPCService(cfg RPCServiceOptions) *rpcService {
	return &rpcService{
		RPCURL:     cfg.RPCURL,
		HTTPClient: cfg.HTTPClient,
	}
}

func (r *rpcService) GetTransaction(transactionHash string) (tss.RPCGetIngestTxResponse, error) {
	rpcResponse, err := r.sendRPCRequest("getTransaction", map[string]string{"hash": transactionHash})
	if err != nil {
		return tss.RPCGetIngestTxResponse{Status: tss.ErrorStatus}, fmt.Errorf("RPC Fail: %s", err.Error())
	}
	getIngestTxResponse := tss.RPCGetIngestTxResponse{
		Status:      tss.RPCTXStatus(rpcResponse.RPCResult.Status),
		EnvelopeXDR: rpcResponse.RPCResult.EnvelopeXDR,
		ResultXDR:   rpcResponse.RPCResult.ResultXDR,
	}
	if getIngestTxResponse.Status != tss.NotFoundStatus {
		getIngestTxResponse.CreatedAt, err = strconv.ParseInt(rpcResponse.RPCResult.CreatedAt, 10, 64)
		if err != nil {
			return tss.RPCGetIngestTxResponse{Status: tss.ErrorStatus}, fmt.Errorf("unable to parse createAt: %w", err)
		}
	}
	return getIngestTxResponse, nil
}

func (r *rpcService) SendTransaction(transactionXdr string) (tss.RPCSendTxResponse, error) {
	rpcResponse, err := r.sendRPCRequest("sendTransaction", map[string]string{"transaction": transactionXdr})
	sendTxResponse := tss.RPCSendTxResponse{}
	sendTxResponse.TransactionXDR = transactionXdr
	if err != nil {
		sendTxResponse.Code.OtherCodes = tss.RPCFailCode
		return sendTxResponse, fmt.Errorf("RPC fail: %w", err)
	}
	sendTxResponse.Status = tss.RPCTXStatus(rpcResponse.RPCResult.Status)
	sendTxResponse.TransactionHash = rpcResponse.RPCResult.Hash
	sendTxResponse.Code, err = r.parseErrorResultXDR(rpcResponse.RPCResult.ErrorResultXDR)
	if err != nil {
		return sendTxResponse, fmt.Errorf("parse error result xdr string: %w", err)
	}
	return sendTxResponse, nil
}

func (r *rpcService) sendRPCRequest(method string, params map[string]string) (tss.RPCResponse, error) {
	payload := map[string]interface{}{
		"jsonrpc": "2.0",
		"id":      1,
		"method":  method,
		"params":  params,
	}
	jsonData, err := json.Marshal(payload)

	if err != nil {
		return tss.RPCResponse{}, fmt.Errorf("marshaling payload")
	}

	resp, err := r.HTTPClient.Post(r.RPCURL, "application/json", bytes.NewBuffer(jsonData))
	if err != nil {
		return tss.RPCResponse{}, fmt.Errorf("%s: sending POST request to rpc: %v", method, err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return tss.RPCResponse{}, fmt.Errorf("%s: unmarshaling RPC response", method)
	}
	var res tss.RPCResponse
	err = json.Unmarshal(body, &res)
	if err != nil {
		return tss.RPCResponse{}, fmt.Errorf("%s: parsing RPC response JSON", method)
	}
	if res.RPCResult == (tss.RPCResult{}) {
		return tss.RPCResponse{}, fmt.Errorf("%s: response missing result field", method)
	}
	return res, nil
}

func (r *rpcService) parseErrorResultXDR(errorResultXdr string) (tss.RPCTXCode, error) {
	unMarshalErr := "unable to unmarshal errorResultXdr: %s"
	decodedBytes, err := base64.StdEncoding.DecodeString(errorResultXdr)
	if err != nil {
		return tss.RPCTXCode{OtherCodes: tss.UnMarshalBinaryCode}, fmt.Errorf(unMarshalErr, errorResultXdr)
	}
	var errorResult xdr.TransactionResult
	_, err = xdr3.Unmarshal(bytes.NewReader(decodedBytes), &errorResult)
	if err != nil {
		return tss.RPCTXCode{OtherCodes: tss.UnMarshalBinaryCode}, fmt.Errorf(unMarshalErr, errorResultXdr)
	}
	return tss.RPCTXCode{
		TxResultCode: errorResult.Result.Code,
	}, nil
}
*/
