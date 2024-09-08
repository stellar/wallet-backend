package tss_services

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"testing"

	"github.com/stellar/go/clients/horizonclient"
	"github.com/stellar/go/keypair"
	"github.com/stellar/go/protocols/horizon"
	"github.com/stellar/go/txnbuild"
	"github.com/stellar/wallet-backend/internal/signing"
	tssErr "github.com/stellar/wallet-backend/internal/tss/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func buildTestTransaction() *txnbuild.Transaction {
	accountToSponsor := keypair.MustRandom()

	tx, _ := txnbuild.NewTransaction(txnbuild.TransactionParams{
		SourceAccount: &txnbuild.SimpleAccount{
			AccountID: accountToSponsor.Address(),
			Sequence:  124,
		},
		IncrementSequenceNum: true,
		Operations: []txnbuild.Operation{
			&txnbuild.Payment{
				Destination: keypair.MustRandom().Address(),
				Amount:      "14",
				Asset:       txnbuild.NativeAsset{},
			},
		},
		BaseFee:       104,
		Preconditions: txnbuild.Preconditions{TimeBounds: txnbuild.NewTimeout(10)},
	})
	return tx
}

func TestValidateOptions(t *testing.T) {
	t.Run("return_error_when_distribution_signature_client_null", func(t *testing.T) {
		opts := TransactionServiceOptions{
			DistributionAccountSignatureClient: nil,
			ChannelAccountSignatureClient:      &signing.SignatureClientMock{},
			HorizonClient:                      &horizonclient.MockClient{},
			RpcUrl:                             "http://localhost:8000/soroban/rpc",
			BaseFee:                            114,
		}
		err := opts.ValidateOptions()
		assert.Equal(t, "distribution account signature client cannot be nil", err.Error())

	})

	t.Run("return_error_when_channel_signature_client_null", func(t *testing.T) {
		opts := TransactionServiceOptions{
			DistributionAccountSignatureClient: &signing.SignatureClientMock{},
			ChannelAccountSignatureClient:      nil,
			HorizonClient:                      &horizonclient.MockClient{},
			RpcUrl:                             "http://localhost:8000/soroban/rpc",
			BaseFee:                            114,
		}
		err := opts.ValidateOptions()
		assert.Equal(t, "channel account signature client cannot be nil", err.Error())
	})

	t.Run("return_error_when_horizon_client_null", func(t *testing.T) {
		opts := TransactionServiceOptions{
			DistributionAccountSignatureClient: &signing.SignatureClientMock{},
			ChannelAccountSignatureClient:      &signing.SignatureClientMock{},
			HorizonClient:                      nil,
			RpcUrl:                             "http://localhost:8000/soroban/rpc",
			BaseFee:                            114,
		}
		err := opts.ValidateOptions()
		assert.Equal(t, "horizon client cannot be nil", err.Error())
	})

	t.Run("return_error_when_rpc_url_empty", func(t *testing.T) {
		opts := TransactionServiceOptions{
			DistributionAccountSignatureClient: &signing.SignatureClientMock{},
			ChannelAccountSignatureClient:      &signing.SignatureClientMock{},
			HorizonClient:                      &horizonclient.MockClient{},
			RpcUrl:                             "",
			BaseFee:                            114,
		}
		err := opts.ValidateOptions()
		assert.Equal(t, "rpc url cannot be empty", err.Error())
	})

	t.Run("return_error_when_base_fee_too_low", func(t *testing.T) {
		opts := TransactionServiceOptions{
			DistributionAccountSignatureClient: &signing.SignatureClientMock{},
			ChannelAccountSignatureClient:      &signing.SignatureClientMock{},
			HorizonClient:                      &horizonclient.MockClient{},
			RpcUrl:                             "http://localhost:8000/soroban/rpc",
			BaseFee:                            txnbuild.MinBaseFee - 10,
		}
		err := opts.ValidateOptions()
		assert.Equal(t, "base fee is lower than the minimum network fee", err.Error())
	})
}

func TestSignAndBuildNewTransaction(t *testing.T) {
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
		RpcUrl:                             "http://localhost:8000/soroban/rpc",
		BaseFee:                            114,
	})

	txStr, _ := buildTestTransaction().Base64()

	t.Run("malformed_transaction_string", func(t *testing.T) {
		feeBumpTx, err := txService.SignAndBuildNewTransaction(context.Background(), "abcd")
		assert.Empty(t, feeBumpTx)
		assert.ErrorIs(t, tssErr.OriginalXdrMalformed, err)
	})

	t.Run("channel_account_signature_client_get_account_public_key_err", func(t *testing.T) {
		channelAccountSignatureClient.
			On("GetAccountPublicKey", context.Background()).
			Return("", errors.New("channel accounts unavailable")).
			Once()

		feeBumpTx, err := txService.SignAndBuildNewTransaction(context.Background(), txStr)
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

		feeBumpTx, err := txService.SignAndBuildNewTransaction(context.Background(), txStr)
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

		feeBumpTx, err := txService.SignAndBuildNewTransaction(context.Background(), txStr)
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

		feeBumpTx, err := txService.SignAndBuildNewTransaction(context.Background(), txStr)
		assert.Empty(t, feeBumpTx)
		assert.Equal(t, "getting distribution account public key: client down", err.Error())
	})

	t.Run("horizon_client_sign_stellar_transaction_w_distribition_account_err", func(t *testing.T) {
		account := keypair.MustRandom()
		signedTx := buildTestTransaction()
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

		feeBumpTx, err := txService.SignAndBuildNewTransaction(context.Background(), txStr)
		assert.Empty(t, feeBumpTx)
		assert.Equal(t, "signing the fee bump transaction with distribution account: unable to sign", err.Error())
	})

	t.Run("returns_signed_tx", func(t *testing.T) {
		account := keypair.MustRandom()
		signedTx := buildTestTransaction()
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

		feeBumpTx, err := txService.SignAndBuildNewTransaction(context.Background(), txStr)
		assert.Equal(t, feeBumpTx, testFeeBumpTx)
		assert.Empty(t, err)
	})
}

type MockPost struct {
	mock.Mock
}

func (m *MockPost) Post(url string, content string, body io.Reader) (*http.Response, error) {
	args := m.Called(url, content, body)
	return args.Get(0).(*http.Response), args.Error(1)
}

type MockUnMarshallRPCResponse struct {
	mock.Mock
}

func (m *MockUnMarshallRPCResponse) ReadAll(r io.Reader) ([]byte, error) {
	args := m.Called(r)
	return args.Get(0).(([]byte)), args.Error(1)

}

type MockUnMarshalJSON struct {
	mock.Mock
}

func (m *MockUnMarshalJSON) UnMarshalJSONBody(body []byte) (map[string]interface{}, error) {
	args := m.Called(body)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(map[string]interface{}), args.Error(1)
}

func TestCallRPC(t *testing.T) {
	mockPost := MockPost{}
	RpcPost = mockPost.Post
	defer func() { RpcPost = http.Post }()
	mockUnMarshalRPCResponse := MockUnMarshallRPCResponse{}
	UnMarshalRPCResponse = mockUnMarshalRPCResponse.ReadAll
	defer func() { UnMarshalRPCResponse = io.ReadAll }()
	mockUnMarshalJSON := MockUnMarshalJSON{}
	UnMarshalJSON = mockUnMarshalJSON.UnMarshalJSONBody
	defer func() { UnMarshalJSON = parseJSONBody }()
	params := map[string]string{"transaction": "ABCD"}
	payload := map[string]interface{}{
		"jsonrpc": "2.0",
		"id":      1,
		"method":  "sendTransaction",
		"params":  params,
	}
	jsonData, _ := json.Marshal(payload)
	rpcUrl := "http://localhost:8000/soroban/rpc"

	t.Run("rpc_post_call_fails", func(t *testing.T) {
		mockPost.
			On("Post", rpcUrl, "application/json", bytes.NewBuffer(jsonData)).
			Return(&http.Response{}, errors.New("connection error")).
			Once()

		response, err := callRPC(rpcUrl, "sendTransaction", params)

		assert.Empty(t, response)
		assert.Equal(t, "sendTransaction: sending POST request to rpc: connection error", err.Error())
	})
	t.Run("unmarshal_rpc_response_fails", func(t *testing.T) {
		mockResponse := &http.Response{
			StatusCode: 200,
			Body:       io.NopCloser(bytes.NewBufferString(`{"mock": "response"}`)),
		}

		mockPost.
			On("Post", rpcUrl, "application/json", bytes.NewBuffer(jsonData)).
			Return(mockResponse, nil).
			Once()

		mockUnMarshalRPCResponse.
			On("ReadAll", mockResponse.Body).
			Return([]byte{}, errors.New("bad string")).
			Once()

		response, err := callRPC(rpcUrl, "sendTransaction", params)

		assert.Empty(t, response)
		assert.Equal(t, "sendTransaction: unmarshalling rpc response: bad string", err.Error())
	})

	t.Run("unmarshal_json_fails", func(t *testing.T) {
		mockResponse := &http.Response{
			StatusCode: 200,
			Body:       io.NopCloser(bytes.NewBufferString(`{"mock": "response"}`)),
		}

		mockPost.
			On("Post", rpcUrl, "application/json", mock.AnythingOfType("*bytes.Buffer")).
			Return(mockResponse, nil).
			Once()

		body := []byte("response")
		mockUnMarshalRPCResponse.
			On("ReadAll", mockResponse.Body).
			Return(body, nil).
			Once()

		mockUnMarshalJSON.
			On("UnMarshalJSONBody", body).
			Return(nil, errors.New("bad json format")).
			Once()

		response, err := callRPC(rpcUrl, "sendTransaction", params)

		assert.Empty(t, response)
		assert.Equal(t, "sendTransaction: parsing rpc response JSON: bad json format", err.Error())
	})
	t.Run("returns_unmarshalled_value", func(t *testing.T) {
		mockResponse := &http.Response{
			StatusCode: 200,
			Body:       io.NopCloser(bytes.NewBufferString(`{"mock": "response"}`)),
		}

		mockPost.
			On("Post", rpcUrl, "application/json", mock.AnythingOfType("*bytes.Buffer")).
			Return(mockResponse, nil).
			Once()

		body := []byte("response")
		mockUnMarshalRPCResponse.
			On("ReadAll", mockResponse.Body).
			Return(body, nil).
			Once()

		expectedResponse := map[string]interface{}{"jsonrpc": "2.0", "id": 1, "result": map[string]interface{}{"status": "SUCCESS", "envelopeXdr": "ABCD"}}

		mockUnMarshalJSON.
			On("UnMarshalJSONBody", body).
			Return(expectedResponse, nil).
			Once()

		rpcResponse, err := callRPC(rpcUrl, "sendTransaction", params)

		assert.Equal(t, rpcResponse, expectedResponse)
		assert.Empty(t, err)
	})
}

type MockCallRPC struct {
	mock.Mock
}

func (m *MockCallRPC) callRPC(rpcUrl string, method string, params map[string]string) (map[string]interface{}, error) {
	args := m.Called(rpcUrl, method, params)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(map[string]interface{}), args.Error(1)
}

type MockUnMarshalErrorResultXdr struct {
	mock.Mock
}

func (m *MockUnMarshalErrorResultXdr) UnMarshalErrorResultXdr(errorResultXdr string) (string, error) {
	args := m.Called(errorResultXdr)
	return args.String(0), args.Error(1)
}

func TestSendTransaction(t *testing.T) {
	mockCallRPC := MockCallRPC{}
	callRPC = mockCallRPC.callRPC
	defer func() { callRPC = sendRPCRequest }()
	mockUnMarshalErrorResultXdr := MockUnMarshalErrorResultXdr{}
	UnMarshalErrorResultXdr = mockUnMarshalErrorResultXdr.UnMarshalErrorResultXdr
	defer func() { UnMarshalErrorResultXdr = parseErrorResultXdr }()
	txService, _ := NewTransactionService(TransactionServiceOptions{
		DistributionAccountSignatureClient: &signing.SignatureClientMock{},
		ChannelAccountSignatureClient:      &signing.SignatureClientMock{},
		HorizonClient:                      &horizonclient.MockClient{},
		RpcUrl:                             "http://localhost:8000/soroban/rpc",
		BaseFee:                            114,
	})
	txXdr, _ := buildTestTransaction().Base64()
	rpcUrl := "http://localhost:8000/soroban/rpc"

	t.Run("call_rpc_returns_error", func(t *testing.T) {
		mockCallRPC.
			On("callRPC", rpcUrl, "sendTransaction", map[string]string{"transaction": txXdr}).
			Return(nil, errors.New("unable to reach rpc server")).
			Once()

		_, err := txService.SendTransaction(txXdr)
		assert.Equal(t, "unable to reach rpc server", err.Error())
	})
	t.Run("error_unmarshaling_error_result_xdr", func(t *testing.T) {
		errorResultXdr := "AAAAAAAAAGT////7AAAAAA=="
		rpcResponse := map[string]interface{}{"jsonrpc": "2.0", "id": 1, "result": map[string]interface{}{"status": "ERROR", "errorResultXdr": errorResultXdr}}
		mockCallRPC.
			On("callRPC", rpcUrl, "sendTransaction", map[string]string{"transaction": txXdr}).
			Return(rpcResponse, nil).
			Once()

		mockUnMarshalErrorResultXdr.
			On("UnMarshalErrorResultXdr", errorResultXdr).
			Return("", errors.New("unable to unmarshal")).
			Once()

		rpcSendTxResponse, err := txService.SendTransaction(txXdr)
		assert.Equal(t, rpcSendTxResponse.Status, "ERROR")
		assert.Empty(t, rpcSendTxResponse.ErrorCode)
		assert.Equal(t, "SendTransaction: unable to unmarshal errorResultXdr: "+errorResultXdr, err.Error())
	})
	t.Run("return_send_tx_response", func(t *testing.T) {
		errorResultXdr := "AAAAAAAAAGT////7AAAAAA=="
		rpcResponse := map[string]interface{}{"jsonrpc": "2.0", "id": 1, "result": map[string]interface{}{"status": "ERROR", "errorResultXdr": errorResultXdr}}
		mockCallRPC.
			On("callRPC", rpcUrl, "sendTransaction", map[string]string{"transaction": txXdr}).
			Return(rpcResponse, nil).
			Once()

		mockUnMarshalErrorResultXdr.
			On("UnMarshalErrorResultXdr", errorResultXdr).
			Return("txError", nil).
			Once()

		rpcSendTxResponse, err := txService.SendTransaction(txXdr)
		assert.Equal(t, rpcSendTxResponse.Status, "ERROR")
		assert.Equal(t, rpcSendTxResponse.ErrorCode, "txError")
		assert.Empty(t, err)
	})
}

func TestGetTransaction(t *testing.T) {
	mockCallRPC := MockCallRPC{}
	callRPC = mockCallRPC.callRPC
	defer func() { callRPC = sendRPCRequest }()
	txService, _ := NewTransactionService(TransactionServiceOptions{
		DistributionAccountSignatureClient: &signing.SignatureClientMock{},
		ChannelAccountSignatureClient:      &signing.SignatureClientMock{},
		HorizonClient:                      &horizonclient.MockClient{},
		RpcUrl:                             "http://localhost:8000/soroban/rpc",
		BaseFee:                            114,
	})
	txHash, _ := buildTestTransaction().HashHex("abcd")
	rpcUrl := "http://localhost:8000/soroban/rpc"

	t.Run("call_rpc_returns_error", func(t *testing.T) {
		mockCallRPC.
			On("callRPC", rpcUrl, "getTransaction", map[string]string{"hash": txHash}).
			Return(nil, errors.New("unable to reach rpc server")).
			Once()

		_, err := txService.GetTransaction(txHash)
		assert.Equal(t, "unable to reach rpc server", err.Error())
	})

	t.Run("returns_resp_with_status", func(t *testing.T) {
		rpcResponse := map[string]interface{}{"jsonrpc": "2.0", "id": 1, "result": map[string]interface{}{"status": "ERROR"}}
		mockCallRPC.
			On("callRPC", rpcUrl, "getTransaction", map[string]string{"hash": txHash}).
			Return(rpcResponse, nil).
			Once()

		getIngestTxResponse, err := txService.GetTransaction(txHash)
		assert.Equal(t, getIngestTxResponse.Status, "ERROR")
		assert.Empty(t, getIngestTxResponse.EnvelopeXdr)
		assert.Empty(t, getIngestTxResponse.ResultXdr)
		assert.Empty(t, getIngestTxResponse.CreatedAt)
		assert.Empty(t, err)
	})

	t.Run("returns_resp_with_envelope_xdr", func(t *testing.T) {
		rpcResponse := map[string]interface{}{"jsonrpc": "2.0", "id": 1, "result": map[string]interface{}{"envelopeXdr": "abcd"}}
		mockCallRPC.
			On("callRPC", rpcUrl, "getTransaction", map[string]string{"hash": txHash}).
			Return(rpcResponse, nil).
			Once()

		getIngestTxResponse, err := txService.GetTransaction(txHash)
		assert.Empty(t, getIngestTxResponse.Status)
		assert.Equal(t, getIngestTxResponse.EnvelopeXdr, "abcd")
		assert.Empty(t, getIngestTxResponse.ResultXdr)
		assert.Empty(t, getIngestTxResponse.CreatedAt)
		assert.Empty(t, err)
	})

	t.Run("returns_resp_with_result_xdr", func(t *testing.T) {
		rpcResponse := map[string]interface{}{"jsonrpc": "2.0", "id": 1, "result": map[string]interface{}{"resultXdr": "abcd"}}
		mockCallRPC.
			On("callRPC", rpcUrl, "getTransaction", map[string]string{"hash": txHash}).
			Return(rpcResponse, nil).
			Once()

		getIngestTxResponse, err := txService.GetTransaction(txHash)
		assert.Empty(t, getIngestTxResponse.Status)
		assert.Empty(t, getIngestTxResponse.EnvelopeXdr)
		assert.Equal(t, getIngestTxResponse.ResultXdr, "abcd")
		assert.Empty(t, getIngestTxResponse.CreatedAt)
		assert.Empty(t, err)
	})

	t.Run("returns_resp_with_created_at", func(t *testing.T) {
		rpcResponse := map[string]interface{}{"jsonrpc": "2.0", "id": 1, "result": map[string]interface{}{"createdAt": "1234"}}
		mockCallRPC.
			On("callRPC", rpcUrl, "getTransaction", map[string]string{"hash": txHash}).
			Return(rpcResponse, nil).
			Once()

		getIngestTxResponse, err := txService.GetTransaction(txHash)
		assert.Empty(t, getIngestTxResponse.Status)
		assert.Empty(t, getIngestTxResponse.EnvelopeXdr)
		assert.Empty(t, getIngestTxResponse.ResultXdr)
		assert.Equal(t, getIngestTxResponse.CreatedAt, int64(1234))
		assert.Empty(t, err)
	})
}
