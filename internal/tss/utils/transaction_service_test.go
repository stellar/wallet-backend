package utils

/*

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/stellar/go/clients/horizonclient"
	"github.com/stellar/go/keypair"
	"github.com/stellar/go/protocols/horizon"
	"github.com/stellar/go/txnbuild"
	"github.com/stellar/go/xdr"
	"github.com/stellar/wallet-backend/internal/signing"
	"github.com/stellar/wallet-backend/internal/tss"
	tsserror "github.com/stellar/wallet-backend/internal/tss/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestSignAndBuildNewFeeBumpTransaction(t *testing.T) {
	distributionAccountSignatureClient := signing.SignatureClientMock{}
	defer distributionAccountSignatureClient.AssertExpectations(t)
	channelAccountSignatureClient := signing.SignatureClientMock{}
	defer channelAccountSignatureClient.AssertExpectations(t)
	horizonClient := horizonclient.MockClient{}
	defer horizonClient.AssertExpectations(t)
	txService, _ := NewTransactionService(TransactionServiceOptions{
		DistributionAccountSignatureClient: &distributionAccountSignatureClient,
		ChannelAccountSignatureClient:      &channelAccountSignatureClient,
		HorizonClient:                      &horizonClient,
		RPCURL:                             "http://localhost:8000/soroban/rpc",
		BaseFee:                            114,
		HTTPClient:                         &MockHTTPClient{},
	})

	txStr, _ := BuildTestTransaction().Base64()

	t.Run("malformed_transaction_string", func(t *testing.T) {
		feeBumpTx, err := txService.SignAndBuildNewFeeBumpTransaction(context.Background(), "abcd")
		assert.Empty(t, feeBumpTx)
		assert.ErrorIs(t, tsserror.OriginalXDRMalformed, err)
	})

	t.Run("channel_account_signature_client_get_account_public_key_err", func(t *testing.T) {
		channelAccountSignatureClient.
			On("GetAccountPublicKey", context.Background()).
			Return("", errors.New("channel accounts unavailable")).
			Once()

		feeBumpTx, err := txService.SignAndBuildNewFeeBumpTransaction(context.Background(), txStr)
		assert.Empty(t, feeBumpTx)
		assert.Equal(t, "getting channel account public key: channel accounts unavailable", err.Error())
	})

	t.Run("horizon_client_get_account_detail_err", func(t *testing.T) {
		channelAccount := keypair.MustRandom()
		channelAccountSignatureClient.
			On("GetAccountPublicKey", context.Background()).
			Return(channelAccount.Address(), nil).
			Once()

		horizonClient.
			On("AccountDetail", horizonclient.AccountRequest{
				AccountID: channelAccount.Address(),
			}).
			Return(horizon.Account{}, errors.New("horizon down")).
			Once()

		feeBumpTx, err := txService.SignAndBuildNewFeeBumpTransaction(context.Background(), txStr)
		assert.Empty(t, feeBumpTx)
		assert.Equal(t, "getting channel account details from horizon: horizon down", err.Error())
	})

	t.Run("horizon_client_sign_stellar_transaction_w_channel_account_err", func(t *testing.T) {
		channelAccount := keypair.MustRandom()
		channelAccountSignatureClient.
			On("GetAccountPublicKey", context.Background()).
			Return(channelAccount.Address(), nil).
			Once().
			On("SignStellarTransaction", context.Background(), mock.AnythingOfType("*txnbuild.Transaction"), []string{channelAccount.Address()}).
			Return(nil, errors.New("unable to sign")).
			Once()

		horizonClient.
			On("AccountDetail", horizonclient.AccountRequest{
				AccountID: channelAccount.Address(),
			}).
			Return(horizon.Account{AccountID: channelAccount.Address(), Sequence: 1}, nil).
			Once()

		feeBumpTx, err := txService.SignAndBuildNewFeeBumpTransaction(context.Background(), txStr)
		assert.Empty(t, feeBumpTx)
		assert.Equal(t, "signing transaction with channel account: unable to sign", err.Error())
	})

	t.Run("distribution_account_signature_client_get_account_public_key_err", func(t *testing.T) {
		channelAccount := keypair.MustRandom()
		signedTx := txnbuild.Transaction{}
		channelAccountSignatureClient.
			On("GetAccountPublicKey", context.Background()).
			Return(channelAccount.Address(), nil).
			Once().
			On("SignStellarTransaction", context.Background(), mock.AnythingOfType("*txnbuild.Transaction"), []string{channelAccount.Address()}).
			Return(&signedTx, nil).
			Once()

		distributionAccountSignatureClient.
			On("GetAccountPublicKey", context.Background()).
			Return("", errors.New("client down")).
			Once()

		horizonClient.
			On("AccountDetail", horizonclient.AccountRequest{
				AccountID: channelAccount.Address(),
			}).
			Return(horizon.Account{AccountID: channelAccount.Address(), Sequence: 1}, nil).
			Once()

		feeBumpTx, err := txService.SignAndBuildNewFeeBumpTransaction(context.Background(), txStr)
		assert.Empty(t, feeBumpTx)
		assert.Equal(t, "getting distribution account public key: client down", err.Error())
	})

	t.Run("horizon_client_sign_stellar_transaction_w_distribition_account_err", func(t *testing.T) {
		account := keypair.MustRandom()
		signedTx := BuildTestTransaction()
		channelAccountSignatureClient.
			On("GetAccountPublicKey", context.Background()).
			Return(account.Address(), nil).
			Once().
			On("SignStellarTransaction", context.Background(), mock.AnythingOfType("*txnbuild.Transaction"), []string{account.Address()}).
			Return(signedTx, nil).
			Once()

		distributionAccountSignatureClient.
			On("GetAccountPublicKey", context.Background()).
			Return(account.Address(), nil).
			Once().
			On("SignStellarFeeBumpTransaction", context.Background(), mock.AnythingOfType("*txnbuild.FeeBumpTransaction")).
			Return(nil, errors.New("unable to sign")).
			Once()

		horizonClient.
			On("AccountDetail", horizonclient.AccountRequest{
				AccountID: account.Address(),
			}).
			Return(horizon.Account{AccountID: account.Address(), Sequence: 1}, nil).
			Once()

		feeBumpTx, err := txService.SignAndBuildNewFeeBumpTransaction(context.Background(), txStr)
		assert.Empty(t, feeBumpTx)
		assert.Equal(t, "signing the fee bump transaction with distribution account: unable to sign", err.Error())
	})

	t.Run("returns_signed_tx", func(t *testing.T) {
		account := keypair.MustRandom()
		signedTx := BuildTestTransaction()
		testFeeBumpTx, _ := txnbuild.NewFeeBumpTransaction(
			txnbuild.FeeBumpTransactionParams{
				Inner:      signedTx,
				FeeAccount: account.Address(),
				BaseFee:    int64(100),
			},
		)
		channelAccountSignatureClient.
			On("GetAccountPublicKey", context.Background()).
			Return(account.Address(), nil).
			Once().
			On("SignStellarTransaction", context.Background(), mock.AnythingOfType("*txnbuild.Transaction"), []string{account.Address()}).
			Return(signedTx, nil).
			Once()

		distributionAccountSignatureClient.
			On("GetAccountPublicKey", context.Background()).
			Return(account.Address(), nil).
			Once().
			On("SignStellarFeeBumpTransaction", context.Background(), mock.AnythingOfType("*txnbuild.FeeBumpTransaction")).
			Return(testFeeBumpTx, nil).
			Once()

		horizonClient.
			On("AccountDetail", horizonclient.AccountRequest{
				AccountID: account.Address(),
			}).
			Return(horizon.Account{AccountID: account.Address(), Sequence: 1}, nil).
			Once()

		feeBumpTx, err := txService.SignAndBuildNewFeeBumpTransaction(context.Background(), txStr)
		assert.Equal(t, feeBumpTx, testFeeBumpTx)
		assert.Empty(t, err)
	})
}

func TestParseErrorResultXDR(t *testing.T) {
	distributionAccountSignatureClient := signing.SignatureClientMock{}
	defer distributionAccountSignatureClient.AssertExpectations(t)
	channelAccountSignatureClient := signing.SignatureClientMock{}
	defer channelAccountSignatureClient.AssertExpectations(t)
	horizonClient := horizonclient.MockClient{}
	defer horizonClient.AssertExpectations(t)
	txService, _ := NewTransactionService(TransactionServiceOptions{
		DistributionAccountSignatureClient: &distributionAccountSignatureClient,
		ChannelAccountSignatureClient:      &channelAccountSignatureClient,
		HorizonClient:                      &horizonClient,
		RPCURL:                             "http://localhost:8000/soroban/rpc",
		BaseFee:                            114,
		HTTPClient:                         &MockHTTPClient{},
	})

	t.Run("errorResultXdr_empty", func(t *testing.T) {
		_, err := txService.parseErrorResultXDR("")
		assert.Equal(t, "unable to unmarshal errorResultXdr: ", err.Error())
	})

	t.Run("errorResultXdr_invalid", func(t *testing.T) {
		_, err := txService.parseErrorResultXDR("ABCD")
		assert.Equal(t, "unable to unmarshal errorResultXdr: ABCD", err.Error())
	})

	t.Run("errorResultXdr_valid", func(t *testing.T) {
		resp, err := txService.parseErrorResultXDR("AAAAAAAAAMj////9AAAAAA==")
		assert.Equal(t, xdr.TransactionResultCodeTxTooLate, resp.TxResultCode)
		assert.Empty(t, err)
	})
}

type errorReader struct{}

func (e *errorReader) Read(p []byte) (n int, err error) {
	return 0, fmt.Errorf("read error")
}

func (e *errorReader) Close() error {
	return nil
}

func TestSendRPCRequest(t *testing.T) {
	mockHTTPClient := MockHTTPClient{}
	rpcURL := "http://localhost:8000/soroban/rpc"
	txService, _ := NewTransactionService(TransactionServiceOptions{
		DistributionAccountSignatureClient: &signing.SignatureClientMock{},
		ChannelAccountSignatureClient:      &signing.SignatureClientMock{},
		HorizonClient:                      &horizonclient.MockClient{},
		RPCURL:                             rpcURL,
		BaseFee:                            114,
		HTTPClient:                         &mockHTTPClient,
	})
	method := "sendTransaction"
	params := tss.RPCParams{Transaction: "ABCD"}
	payload := map[string]interface{}{
		"jsonrpc": "2.0",
		"id":      1,
		"method":  method,
		"params":  params,
	}
	jsonData, _ := json.Marshal(payload)
	t.Run("rpc_post_call_fails", func(t *testing.T) {
		mockHTTPClient.
			On("Post", rpcURL, "application/json", bytes.NewBuffer(jsonData)).
			Return(&http.Response{}, errors.New("RPC Connection fail")).
			Once()

		resp, err := txService.sendRPCRequest(method, params)

		assert.Empty(t, resp)
		assert.Equal(t, "sendTransaction: sending POST request to rpc: RPC Connection fail", err.Error())
	})

	t.Run("unmarshaling_rpc_response_fails", func(t *testing.T) {
		httpResponse := &http.Response{
			StatusCode: http.StatusOK,
			Body:       io.NopCloser(&errorReader{}),
		}
		mockHTTPClient.
			On("Post", rpcURL, "application/json", bytes.NewBuffer(jsonData)).
			Return(httpResponse, nil).
			Once()

		resp, err := txService.sendRPCRequest(method, params)

		assert.Empty(t, resp)
		assert.Equal(t, "sendTransaction: unmarshaling RPC response", err.Error())
	})

	t.Run("unmarshaling_json_fails", func(t *testing.T) {
		httpResponse := &http.Response{
			StatusCode: http.StatusOK,
			Body:       io.NopCloser(strings.NewReader(`{invalid-json`)),
		}
		mockHTTPClient.
			On("Post", rpcURL, "application/json", bytes.NewBuffer(jsonData)).
			Return(httpResponse, nil).
			Once()

		resp, err := txService.sendRPCRequest(method, params)

		assert.Empty(t, resp)
		assert.Equal(t, "sendTransaction: parsing RPC response JSON", err.Error())
	})

	t.Run("response_has_no_result_field", func(t *testing.T) {
		httpResponse := &http.Response{
			StatusCode: http.StatusOK,
			Body:       io.NopCloser(strings.NewReader(`{"status": "success"}`)),
		}
		mockHTTPClient.
			On("Post", rpcURL, "application/json", bytes.NewBuffer(jsonData)).
			Return(httpResponse, nil).
			Once()

		resp, _ := txService.sendRPCRequest(method, params)
		assert.Empty(t, resp)
	})

	t.Run("response_has_status_field", func(t *testing.T) {
		httpResponse := &http.Response{
			StatusCode: http.StatusOK,
			Body:       io.NopCloser(strings.NewReader(`{"result": {"status": "PENDING"}}`)),
		}
		mockHTTPClient.
			On("Post", rpcURL, "application/json", bytes.NewBuffer(jsonData)).
			Return(httpResponse, nil).
			Once()

		resp, err := txService.sendRPCRequest(method, params)

		assert.Equal(t, "PENDING", resp.Status)
		assert.Empty(t, err)
	})

	t.Run("response_has_envelopexdr_field", func(t *testing.T) {
		httpResponse := &http.Response{
			StatusCode: http.StatusOK,
			Body:       io.NopCloser(strings.NewReader(`{"result": {"envelopeXdr": "exdr"}}`)),
		}
		mockHTTPClient.
			On("Post", rpcURL, "application/json", bytes.NewBuffer(jsonData)).
			Return(httpResponse, nil).
			Once()

		resp, err := txService.sendRPCRequest(method, params)

		assert.Equal(t, "exdr", resp.EnvelopeXDR)
		assert.Empty(t, err)
	})

	t.Run("response_has_resultxdr_field", func(t *testing.T) {
		httpResponse := &http.Response{
			StatusCode: http.StatusOK,
			Body:       io.NopCloser(strings.NewReader(`{"result": {"resultXdr": "rxdr"}}`)),
		}
		mockHTTPClient.
			On("Post", rpcURL, "application/json", bytes.NewBuffer(jsonData)).
			Return(httpResponse, nil).
			Once()

		resp, err := txService.sendRPCRequest(method, params)

		assert.Equal(t, "rxdr", resp.ResultXDR)
		assert.Empty(t, err)
	})

	t.Run("response_has_errorresultxdr_field", func(t *testing.T) {
		httpResponse := &http.Response{
			StatusCode: http.StatusOK,
			Body:       io.NopCloser(strings.NewReader(`{"result": {"errorResultXdr": "exdr"}}`)),
		}
		mockHTTPClient.
			On("Post", rpcURL, "application/json", bytes.NewBuffer(jsonData)).
			Return(httpResponse, nil).
			Once()

		resp, err := txService.sendRPCRequest(method, params)

		assert.Equal(t, "exdr", resp.ErrorResultXDR)
		assert.Empty(t, err)
	})

	t.Run("response_has_hash_field", func(t *testing.T) {
		httpResponse := &http.Response{
			StatusCode: http.StatusOK,
			Body:       io.NopCloser(strings.NewReader(`{"result": {"hash": "hash"}}`)),
		}
		mockHTTPClient.
			On("Post", rpcURL, "application/json", bytes.NewBuffer(jsonData)).
			Return(httpResponse, nil).
			Once()

		resp, err := txService.sendRPCRequest(method, params)

		assert.Equal(t, "hash", resp.Hash)
		assert.Empty(t, err)
	})

	t.Run("response_has_createdat_field", func(t *testing.T) {
		httpResponse := &http.Response{
			StatusCode: http.StatusOK,
			Body:       io.NopCloser(strings.NewReader(`{"result": {"createdAt": "1234"}}`)),
		}
		mockHTTPClient.
			On("Post", rpcURL, "application/json", bytes.NewBuffer(jsonData)).
			Return(httpResponse, nil).
			Once()

		resp, err := txService.sendRPCRequest(method, params)

		assert.Equal(t, "1234", resp.CreatedAt)
		assert.Empty(t, err)
	})
}

func TestSendTransaction(t *testing.T) {
	mockHTTPClient := MockHTTPClient{}
	rpcURL := "http://localhost:8000/soroban/rpc"
	txService, _ := NewTransactionService(TransactionServiceOptions{
		DistributionAccountSignatureClient: &signing.SignatureClientMock{},
		ChannelAccountSignatureClient:      &signing.SignatureClientMock{},
		HorizonClient:                      &horizonclient.MockClient{},
		RPCURL:                             rpcURL,
		BaseFee:                            114,
		HTTPClient:                         &mockHTTPClient,
	})
	method := "sendTransaction"
	params := tss.RPCParams{Transaction: "ABCD"}
	payload := map[string]interface{}{
		"jsonrpc": "2.0",
		"id":      1,
		"method":  method,
		"params":  params,
	}
	jsonData, _ := json.Marshal(payload)

	t.Run("rpc_request_fails", func(t *testing.T) {
		mockHTTPClient.
			On("Post", rpcURL, "application/json", bytes.NewBuffer(jsonData)).
			Return(&http.Response{}, errors.New("RPC Connection fail")).
			Once()

		resp, err := txService.SendTransaction("ABCD")

		assert.Equal(t, tss.ErrorStatus, resp.Status)
		assert.Equal(t, tss.RPCFailCode, resp.Code.OtherCodes)
		assert.Equal(t, "RPC fail: sendTransaction: sending POST request to rpc: RPC Connection fail", err.Error())

	})
	t.Run("response_has_unparsable_errorResultXdr", func(t *testing.T) {
		httpResponse := &http.Response{
			StatusCode: http.StatusOK,
			Body:       io.NopCloser(strings.NewReader(`{"result": {"status": "ERROR", "errorResultXdr": "ABC123"}}`)),
		}
		mockHTTPClient.
			On("Post", rpcURL, "application/json", bytes.NewBuffer(jsonData)).
			Return(httpResponse, nil).
			Once()

		resp, err := txService.SendTransaction("ABCD")

		assert.Equal(t, tss.ErrorStatus, resp.Status)
		assert.Equal(t, tss.UnMarshalBinaryCode, resp.Code.OtherCodes)
		assert.Equal(t, "unable to unmarshal errorResultXdr: ABC123", err.Error())
	})
	t.Run("response_has_empty_errorResultXdr_wth_status", func(t *testing.T) {
		httpResponse := &http.Response{
			StatusCode: http.StatusOK,
			Body:       io.NopCloser(strings.NewReader(`{"result": {"status": "PENDING", "errorResultXdr": ""}}`)),
		}
		mockHTTPClient.
			On("Post", rpcURL, "application/json", bytes.NewBuffer(jsonData)).
			Return(httpResponse, nil).
			Once()

		resp, err := txService.SendTransaction("ABCD")

		assert.Equal(t, tss.PendingStatus, resp.Status)
		assert.Equal(t, tss.UnMarshalBinaryCode, resp.Code.OtherCodes)
		assert.Equal(t, "unable to unmarshal errorResultXdr: ", err.Error())
	})
	t.Run("response_has_errorResultXdr", func(t *testing.T) {
		httpResponse := &http.Response{
			StatusCode: http.StatusOK,
			Body:       io.NopCloser(strings.NewReader(`{"result": {"status": "ERROR", "errorResultXdr": "AAAAAAAAAMj////9AAAAAA=="}}`)),
		}
		mockHTTPClient.
			On("Post", rpcURL, "application/json", bytes.NewBuffer(jsonData)).
			Return(httpResponse, nil).
			Once()

		resp, err := txService.SendTransaction("ABCD")

		assert.Equal(t, tss.ErrorStatus, resp.Status)
		assert.Equal(t, xdr.TransactionResultCodeTxTooLate, resp.Code.TxResultCode)
		assert.Empty(t, err)
	})
}

func TestGetTransaction(t *testing.T) {
	mockHTTPClient := MockHTTPClient{}
	rpcURL := "http://localhost:8000/soroban/rpc"
	txService, _ := NewTransactionService(TransactionServiceOptions{
		DistributionAccountSignatureClient: &signing.SignatureClientMock{},
		ChannelAccountSignatureClient:      &signing.SignatureClientMock{},
		HorizonClient:                      &horizonclient.MockClient{},
		RPCURL:                             rpcURL,
		BaseFee:                            114,
		HTTPClient:                         &mockHTTPClient,
	})
	method := "getTransaction"
	params := tss.RPCParams{Hash: "XYZ"}
	payload := map[string]interface{}{
		"jsonrpc": "2.0",
		"id":      1,
		"method":  method,
		"params":  params,
	}
	jsonData, _ := json.Marshal(payload)

	t.Run("rpc_request_fails", func(t *testing.T) {
		mockHTTPClient.
			On("Post", rpcURL, "application/json", bytes.NewBuffer(jsonData)).
			Return(&http.Response{}, errors.New("RPC Connection fail")).
			Once()

		resp, err := txService.GetTransaction("XYZ")

		assert.Equal(t, tss.ErrorStatus, resp.Status)
		assert.Equal(t, "RPC Fail: getTransaction: sending POST request to rpc: RPC Connection fail", err.Error())

	})
	t.Run("unable_to_parse_createdAt", func(t *testing.T) {
		httpResponse := &http.Response{
			StatusCode: http.StatusOK,
			Body:       io.NopCloser(strings.NewReader(`{"result": {"status": "SUCCESS", "createdAt": "ABCD"}}`)),
		}
		mockHTTPClient.
			On("Post", rpcURL, "application/json", bytes.NewBuffer(jsonData)).
			Return(httpResponse, nil).
			Once()

		resp, err := txService.GetTransaction("XYZ")

		assert.Equal(t, tss.ErrorStatus, resp.Status)
		assert.Equal(t, "unable to parse createAt: strconv.ParseInt: parsing \"ABCD\": invalid syntax", err.Error())
	})
	t.Run("response_has_createdAt_resultXdr_field", func(t *testing.T) {
		httpResponse := &http.Response{
			StatusCode: http.StatusOK,
			Body:       io.NopCloser(strings.NewReader(`{"result": {"status": "FAILED", "resultXdr": "AAAAAAAAAMj////9AAAAAA==", "createdAt": "1234567"}}`)),
		}
		mockHTTPClient.
			On("Post", rpcURL, "application/json", bytes.NewBuffer(jsonData)).
			Return(httpResponse, nil).
			Once()

		resp, err := txService.GetTransaction("XYZ")

		assert.Equal(t, tss.FailedStatus, resp.Status)
		assert.Equal(t, xdr.TransactionResultCodeTxTooLate, resp.Code.TxResultCode)
		assert.Equal(t, int64(1234567), resp.CreatedAt)
		assert.Empty(t, err)
	})

}
*/
