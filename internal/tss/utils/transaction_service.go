package utils

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"

	"github.com/stellar/go/clients/horizonclient"
	"github.com/stellar/go/txnbuild"
	"github.com/stellar/go/xdr"
	"github.com/stellar/wallet-backend/internal/signing"
	"github.com/stellar/wallet-backend/internal/tss"
	tssErr "github.com/stellar/wallet-backend/internal/tss/errors"
)

var (
	RpcPost                 = http.Post
	UnMarshalRPCResponse    = io.ReadAll
	UnMarshalJSON           = parseJSONBody
	callRPC                 = sendRPCRequest
	UnMarshalErrorResultXdr = parseErrorResultXdr
)

type transactionService struct {
	DistributionAccountSignatureClient signing.SignatureClient
	ChannelAccountSignatureClient      signing.SignatureClient
	HorizonClient                      horizonclient.ClientInterface
	RpcUrl                             string
	BaseFee                            int64
	Ctx                                context.Context
}

type TransactionServiceOptions struct {
	DistributionAccountSignatureClient signing.SignatureClient
	ChannelAccountSignatureClient      signing.SignatureClient
	HorizonClient                      horizonclient.ClientInterface
	RpcUrl                             string
	BaseFee                            int64
	Ctx                                context.Context
}

func (o *TransactionServiceOptions) ValidateOptions() error {
	if o.DistributionAccountSignatureClient == nil {
		return fmt.Errorf("distribution account signature client cannot be nil")
	}

	if o.ChannelAccountSignatureClient == nil {
		return fmt.Errorf("channel account signature client cannot be nil")
	}

	if o.HorizonClient == nil {
		return fmt.Errorf("horizon client cannot be nil")
	}

	if o.RpcUrl == "" {
		return fmt.Errorf("rpc url cannot be empty")
	}

	if o.BaseFee < int64(txnbuild.MinBaseFee) {
		return fmt.Errorf("base fee is lower than the minimum network fee")
	}
	return nil
}

func NewTransactionService(opts TransactionServiceOptions) (TransactionService, error) {
	if err := opts.ValidateOptions(); err != nil {
		return nil, err
	}
	return &transactionService{
		DistributionAccountSignatureClient: opts.DistributionAccountSignatureClient,
		ChannelAccountSignatureClient:      opts.ChannelAccountSignatureClient,
		HorizonClient:                      opts.HorizonClient,
		RpcUrl:                             opts.RpcUrl,
		BaseFee:                            opts.BaseFee,
		Ctx:                                opts.Ctx,
	}, nil
}

func parseJSONBody(body []byte) (map[string]interface{}, error) {
	var res map[string]interface{}
	err := json.Unmarshal(body, &res)
	if err != nil {
		return nil, fmt.Errorf(err.Error())
	}
	return res, nil
}

func parseErrorResultXdr(errorResultXdr string) (tss.RPCTXCode, error) {
	errorResult := xdr.TransactionResult{}
	err := errorResult.UnmarshalBinary([]byte(errorResultXdr))

	if err != nil {
		return tss.RPCTXCode{OtherCodes: tss.UnMarshalBinaryCode}, fmt.Errorf("SendTransaction: unable to unmarshal errorResultXdr: %s", errorResultXdr)
	}
	return tss.RPCTXCode{
		TxResultCode: errorResult.Result.Code,
	}, nil
}

func sendRPCRequest(rpcUrl string, method string, params map[string]string) (map[string]interface{}, error) {
	payload := map[string]interface{}{
		"jsonrpc": "2.0",
		"id":      1,
		"method":  method,
		"params":  params,
	}
	jsonData, _ := json.Marshal(payload)

	resp, err := RpcPost(rpcUrl, "application/json", bytes.NewBuffer(jsonData))
	if err != nil {
		return nil, fmt.Errorf(method+": sending POST request to rpc: %v", err)
	}
	defer resp.Body.Close()

	body, err := UnMarshalRPCResponse(resp.Body)
	if err != nil {
		return nil, fmt.Errorf(method+": unmarshalling rpc response: %v", err)
	}
	res, err := UnMarshalJSON(body)
	if err != nil {
		return nil, fmt.Errorf(method+": parsing rpc response JSON: %v", err)
	}
	return res, nil
}

func (t *transactionService) NetworkPassPhrase() string {
	return t.DistributionAccountSignatureClient.NetworkPassphrase()
}

func (t *transactionService) SignAndBuildNewTransaction(origTxXdr string) (*txnbuild.FeeBumpTransaction, error) {
	genericTx, err := txnbuild.TransactionFromXDR(origTxXdr)
	if err != nil {
		return nil, tssErr.OriginalXdrMalformed
	}
	originalTx, txEmpty := genericTx.Transaction()
	if !txEmpty {
		return nil, tssErr.OriginalXdrMalformed
	}
	channelAccountPublicKey, err := t.ChannelAccountSignatureClient.GetAccountPublicKey(t.Ctx)
	if err != nil {
		return nil, fmt.Errorf("getting channel account public key: %w", err)
	}
	channelAccount, err := t.HorizonClient.AccountDetail(horizonclient.AccountRequest{AccountID: channelAccountPublicKey})
	if err != nil {
		return nil, fmt.Errorf("getting channel account details from horizon: %w", err)
	}
	tx, err := txnbuild.NewTransaction(
		txnbuild.TransactionParams{
			SourceAccount: &channelAccount,
			Operations:    originalTx.Operations(),
			BaseFee:       int64(t.BaseFee),
			Preconditions: txnbuild.Preconditions{
				TimeBounds: txnbuild.NewTimeout(10),
			},
			IncrementSequenceNum: true,
		},
	)
	if err != nil {
		return nil, fmt.Errorf("building transaction: %w", err)
	}
	tx, err = t.ChannelAccountSignatureClient.SignStellarTransaction(t.Ctx, tx, channelAccountPublicKey)
	if err != nil {
		return nil, fmt.Errorf("signing transaction with channel account: %w", err)
	}
	// wrap the transaction in a fee bump tx, signed by the distribution account
	distributionAccountPublicKey, err := t.DistributionAccountSignatureClient.GetAccountPublicKey(t.Ctx)
	if err != nil {
		return nil, fmt.Errorf("getting distribution account public key: %w", err)
	}

	feeBumpTx, err := txnbuild.NewFeeBumpTransaction(
		txnbuild.FeeBumpTransactionParams{
			Inner:      tx,
			FeeAccount: distributionAccountPublicKey,
			BaseFee:    int64(t.BaseFee),
		},
	)
	if err != nil {
		return nil, fmt.Errorf("building fee-bump transaction %w", err)
	}

	feeBumpTx, err = t.DistributionAccountSignatureClient.SignStellarFeeBumpTransaction(t.Ctx, feeBumpTx)
	if err != nil {
		return nil, fmt.Errorf("signing the fee bump transaction with distribution account: %w", err)
	}
	return feeBumpTx, nil
}

func (t *transactionService) SendTransaction(transactionXdr string) (tss.RPCSendTxResponse, error) {
	rpcResponse, err := callRPC(t.RpcUrl, "sendTransaction", map[string]string{"transaction": transactionXdr})
	sendTxResponse := tss.RPCSendTxResponse{}
	sendTxResponse.TransactionXDR = transactionXdr
	if err != nil {
		sendTxResponse.Code.OtherCodes = tss.RPCFailCode
		return sendTxResponse, fmt.Errorf(err.Error())
	}

	if result, ok := rpcResponse["result"].(map[string]interface{}); ok {
		if val, exists := result["status"].(tss.RPCTXStatus); exists {
			sendTxResponse.Status = val
		}
		if val, exists := result["errorResultXdr"].(string); exists {
			sendTxResponse.Code, err = UnMarshalErrorResultXdr(val)
		}
		if hash, exists := result["hash"].(string); exists {
			sendTxResponse.TransactionHash = hash
		}
	}
	return sendTxResponse, err
}

func (t *transactionService) GetTransaction(transactionHash string) (tss.RPCGetIngestTxResponse, error) {
	rpcResponse, err := callRPC(t.RpcUrl, "getTransaction", map[string]string{"hash": transactionHash})
	if err != nil {
		return tss.RPCGetIngestTxResponse{}, fmt.Errorf(err.Error())
	}

	getIngestTxResponse := tss.RPCGetIngestTxResponse{}
	if result, ok := rpcResponse["result"].(map[string]interface{}); ok {
		if status, exists := result["status"].(tss.RPCTXStatus); exists {
			getIngestTxResponse.Status = status
		}
		if envelopeXdr, exists := result["envelopeXdr"].(string); exists {
			getIngestTxResponse.EnvelopeXDR = envelopeXdr
		}
		if resultXdr, exists := result["resultXdr"].(string); exists {
			getIngestTxResponse.ResultXDR = resultXdr
		}
		if createdAt, exists := result["createdAt"].(string); exists {
			// we can supress erroneous createdAt errors as this is not an important field
			createdAtInt, _ := strconv.ParseInt(createdAt, 10, 64)
			getIngestTxResponse.CreatedAt = createdAtInt
		}
	}
	return getIngestTxResponse, nil
}
