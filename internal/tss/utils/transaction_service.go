package utils

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"

	xdr3 "github.com/stellar/go-xdr/xdr3"
	"github.com/stellar/go/clients/horizonclient"
	"github.com/stellar/go/txnbuild"
	"github.com/stellar/go/xdr"
	"github.com/stellar/wallet-backend/internal/signing"
	"github.com/stellar/wallet-backend/internal/tss"
	tsserror "github.com/stellar/wallet-backend/internal/tss/errors"
)

type HTTPClient interface {
	Post(url string, t string, body io.Reader) (resp *http.Response, err error)
}

type TransactionService interface {
	SignAndBuildNewFeeBumpTransaction(ctx context.Context, origTxXdr string) (*txnbuild.FeeBumpTransaction, error)
	SendTransaction(transactionXdr string) (tss.RPCSendTxResponse, error)
	GetTransaction(transactionHash string) (tss.RPCGetIngestTxResponse, error)
}

type transactionService struct {
	DistributionAccountSignatureClient signing.SignatureClient
	ChannelAccountSignatureClient      signing.SignatureClient
	HorizonClient                      horizonclient.ClientInterface
	RPCURL                             string
	BaseFee                            int64
	HTTPClient                         HTTPClient
}

var _ TransactionService = (*transactionService)(nil)

type TransactionServiceOptions struct {
	DistributionAccountSignatureClient signing.SignatureClient
	ChannelAccountSignatureClient      signing.SignatureClient
	HorizonClient                      horizonclient.ClientInterface
	RPCURL                             string
	BaseFee                            int64
	HTTPClient                         HTTPClient
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

	if o.RPCURL == "" {
		return fmt.Errorf("rpc url cannot be empty")
	}

	if o.BaseFee < int64(txnbuild.MinBaseFee) {
		return fmt.Errorf("base fee is lower than the minimum network fee")
	}

	if o.HTTPClient == nil {
		return fmt.Errorf("http client cannot be nil")
	}

	return nil
}

func NewTransactionService(opts TransactionServiceOptions) (*transactionService, error) {
	if err := opts.ValidateOptions(); err != nil {
		return nil, err
	}
	return &transactionService{
		DistributionAccountSignatureClient: opts.DistributionAccountSignatureClient,
		ChannelAccountSignatureClient:      opts.ChannelAccountSignatureClient,
		HorizonClient:                      opts.HorizonClient,
		RPCURL:                             opts.RPCURL,
		BaseFee:                            opts.BaseFee,
		HTTPClient:                         opts.HTTPClient,
	}, nil
}

func (t *transactionService) SignAndBuildNewFeeBumpTransaction(ctx context.Context, origTxXdr string) (*txnbuild.FeeBumpTransaction, error) {
	genericTx, err := txnbuild.TransactionFromXDR(origTxXdr)
	if err != nil {
		return nil, tsserror.OriginalXDRMalformed
	}
	originalTx, txEmpty := genericTx.Transaction()
	if !txEmpty {
		return nil, tsserror.OriginalXDRMalformed
	}
	channelAccountPublicKey, err := t.ChannelAccountSignatureClient.GetAccountPublicKey(ctx)
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
				TimeBounds: txnbuild.NewTimeout(300),
			},
			IncrementSequenceNum: true,
		},
	)
	if err != nil {
		return nil, fmt.Errorf("building transaction: %w", err)
	}
	tx, err = t.ChannelAccountSignatureClient.SignStellarTransaction(ctx, tx, channelAccountPublicKey)
	if err != nil {
		return nil, fmt.Errorf("signing transaction with channel account: %w", err)
	}
	// Wrap the transaction in a fee bump tx, signed by the distribution account
	distributionAccountPublicKey, err := t.DistributionAccountSignatureClient.GetAccountPublicKey(ctx)
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

	feeBumpTx, err = t.DistributionAccountSignatureClient.SignStellarFeeBumpTransaction(ctx, feeBumpTx)
	if err != nil {
		return nil, fmt.Errorf("signing the fee bump transaction with distribution account: %w", err)
	}
	return feeBumpTx, nil
}

func (t *transactionService) parseErrorResultXDR(errorResultXdr string) (tss.RPCTXCode, error) {
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

func (t *transactionService) sendRPCRequest(method string, params map[string]string) (tss.RPCResponse, error) {
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

	resp, err := t.HTTPClient.Post(t.RPCURL, "application/json", bytes.NewBuffer(jsonData))
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

func (t *transactionService) SendTransaction(transactionXdr string) (tss.RPCSendTxResponse, error) {
	rpcResponse, err := t.sendRPCRequest("sendTransaction", map[string]string{"transaction": transactionXdr})
	sendTxResponse := tss.RPCSendTxResponse{}
	sendTxResponse.TransactionXDR = transactionXdr
	if err != nil {
		sendTxResponse.Code.OtherCodes = tss.RPCFailCode
		return sendTxResponse, fmt.Errorf("RPC fail: %w", err)
	}
	sendTxResponse.Status = tss.RPCTXStatus(rpcResponse.RPCResult.Status)
	sendTxResponse.TransactionHash = rpcResponse.RPCResult.Hash
	sendTxResponse.Code, err = t.parseErrorResultXDR(rpcResponse.RPCResult.ErrorResultXDR)
	if err != nil {
		return sendTxResponse, fmt.Errorf("parse error result xdr string: %w", err)
	}
	return sendTxResponse, nil
}

func (t *transactionService) GetTransaction(transactionHash string) (tss.RPCGetIngestTxResponse, error) {
	rpcResponse, err := t.sendRPCRequest("getTransaction", map[string]string{"hash": transactionHash})
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
