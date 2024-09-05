package tss_services

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"

	"github.com/stellar/go/clients/horizonclient"
	"github.com/stellar/go/txnbuild"
	"github.com/stellar/go/xdr"
	"github.com/stellar/wallet-backend/internal/signing"
	"github.com/stellar/wallet-backend/internal/tss"
)

type transactionService struct {
	DistributionAccountSignatureClient signing.SignatureClient
	ChannelAccountSignatureClient      signing.SignatureClient
	HorizonClient                      horizonclient.ClientInterface
	RpcUrl                             string
	BaseFee                            int64
}

type TransactionServiceOptions struct {
	DistributionAccountSignatureClient signing.SignatureClient
	ChannelAccountSignatureClient      signing.SignatureClient
	HorizonClient                      horizonclient.ClientInterface
	RpcUrl                             string
	BaseFee                            int64
}

func (o *TransactionServiceOptions) Validate() error {
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

func (t *transactionService) SignAndBuildNewTransaction(ctx context.Context, origTxXdr string) (*txnbuild.FeeBumpTransaction, error) {
	genericTx, err := txnbuild.TransactionFromXDR(origTxXdr)
	if err != nil {
		return nil, fmt.Errorf("deserializing the transaction xdr: %w", err)
	}
	originalTx, txEmpty := genericTx.Transaction()
	if txEmpty {
		return nil, fmt.Errorf("empty transaction: %w", err)
	}
	channelAccountPublicKey, err := t.ChannelAccountSignatureClient.GetAccountPublicKey(ctx)
	if err != nil {
		return nil, fmt.Errorf("getting channel account public key: %w", err)
	}
	channelAccount, err := t.HorizonClient.AccountDetail(horizonclient.AccountRequest{AccountID: channelAccountPublicKey})
	if err != nil {
		return nil, fmt.Errorf("getting channel account details: %w", err)
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
	tx, err = t.ChannelAccountSignatureClient.SignStellarTransaction(ctx, tx, channelAccountPublicKey)
	if err != nil {
		return nil, fmt.Errorf("signing transaction with channel account: %w", err)
	}
	// wrap the transaction in a fee bump tx, signed by the distribution account
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

func (t *transactionService) SendTransaction(transactionXdr string) (tss.RPCSendTxResponse, error) {
	payload := map[string]interface{}{
		"jsonrpc": "2.0",
		"id":      1,
		"method":  "sendTransaction",
		"params": map[string]string{
			"transaction": transactionXdr,
		},
	}
	jsonData, _ := json.Marshal(payload)

	resp, err := http.Post(t.RpcUrl, "application/json", bytes.NewBuffer(jsonData))
	if err != nil {
		return tss.RPCSendTxResponse{}, fmt.Errorf("sending POST request to rpc: %v", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return tss.RPCSendTxResponse{}, fmt.Errorf("reading rpc response: %v", err)
	}
	var res map[string]interface{}
	err = json.Unmarshal(body, &res)
	if err != nil {
		return tss.RPCSendTxResponse{}, fmt.Errorf("parsing rpc response JSON: %v", err)
	}

	sendTxResponse := tss.RPCSendTxResponse{}
	if result, ok := res["result"].(map[string]interface{}); ok {
		if val, exists := result["errorResultXdr"].(string); exists {
			errorResult := xdr.TransactionResult{}
			errorResult.UnmarshalBinary([]byte(val))
			sendTxResponse.ErrorCode = errorResult.Result.Code.String()
		}
		if val, exists := result["status"].(string); exists {
			sendTxResponse.Status = val
		}
	}
	fmt.Println(sendTxResponse)
	prettyResponse, err := json.MarshalIndent(res, "", "    ")
	if err != nil {
		log.Fatalf("Error formatting rpc JSON response: %v", err)
	}
	fmt.Println(string(prettyResponse))
	return sendTxResponse, nil
}

func NewTransactionService(opts TransactionServiceOptions) (*transactionService, error) {
	/*
		if err := opts.Validate(); err != nil {
			return nil, err
		}
	*/

	return &transactionService{
		DistributionAccountSignatureClient: opts.DistributionAccountSignatureClient,
		ChannelAccountSignatureClient:      opts.ChannelAccountSignatureClient,
		HorizonClient:                      opts.HorizonClient,
		RpcUrl:                             opts.RpcUrl,
		BaseFee:                            opts.BaseFee,
	}, nil
}
