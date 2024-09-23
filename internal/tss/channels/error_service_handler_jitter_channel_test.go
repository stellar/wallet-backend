package channels

import (
	"context"
	"errors"
	"testing"

	"github.com/stellar/go/xdr"
	"github.com/stellar/wallet-backend/internal/db"
	"github.com/stellar/wallet-backend/internal/db/dbtest"
	"github.com/stellar/wallet-backend/internal/tss"
	"github.com/stellar/wallet-backend/internal/tss/router"
	"github.com/stellar/wallet-backend/internal/tss/store"
	"github.com/stellar/wallet-backend/internal/tss/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestJitterSend(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()

	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()
	store := store.NewStore(dbConnectionPool)
	txServiceMock := utils.TransactionServiceMock{}
	cfg := RPCErrorHandlerServiceJitterChannelConfigs{
		Store:                store,
		TxService:            &txServiceMock,
		MaxBufferSize:        1,
		MaxWorkers:           1,
		MaxRetries:           3,
		MinWaitBtwnRetriesMS: 10,
	}
	channel := NewErrorHandlerServiceJitterChannel(cfg)

	payload := tss.Payload{}
	payload.WebhookURL = "www.stellar.com"
	payload.TransactionHash = "hash"
	payload.TransactionXDR = "xdr"
	txServiceMock.
		On("SignAndBuildNewFeeBumpTransaction", context.Background(), payload.TransactionXDR).
		Return(nil, errors.New("signing failed"))

	_ = store.UpsertTransaction(context.Background(), payload.WebhookURL, payload.TransactionHash, payload.TransactionXDR, tss.NewStatus)

	channel.Send(payload)
	channel.Stop()

	var status string
	err = dbConnectionPool.GetContext(context.Background(), &status, `SELECT current_status FROM tss_transactions WHERE transaction_hash = $1`, payload.TransactionHash)
	require.NoError(t, err)
	assert.Equal(t, status, string(tss.NewStatus))
}

func TestJitterReceive(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()

	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()
	store := store.NewStore(dbConnectionPool)
	txServiceMock := utils.TransactionServiceMock{}
	cfg := RPCErrorHandlerServiceJitterChannelConfigs{
		Store:                store,
		TxService:            &txServiceMock,
		MaxBufferSize:        1,
		MaxWorkers:           1,
		MaxRetries:           3,
		MinWaitBtwnRetriesMS: 10,
	}
	channel := NewErrorHandlerServiceJitterChannel(cfg)

	mockRouter := router.MockRouter{}
	defer mockRouter.AssertExpectations(t)
	channel.SetRouter(&mockRouter)
	networkPass := "passphrase"
	feeBumpTx := utils.BuildTestFeeBumpTransaction()
	feeBumpTxXDR, _ := feeBumpTx.Base64()
	feeBumpTxHash, _ := feeBumpTx.HashHex(networkPass)
	payload := tss.Payload{}
	payload.WebhookURL = "www.stellar.com"
	payload.TransactionHash = "hash"
	payload.TransactionXDR = "xdr"

	_ = store.UpsertTransaction(context.Background(), payload.WebhookURL, payload.TransactionHash, payload.TransactionXDR, tss.NewStatus)

	t.Run("signing_and_submitting_tx_fails", func(t *testing.T) {
		txServiceMock.
			On("SignAndBuildNewFeeBumpTransaction", context.Background(), payload.TransactionXDR).
			Return(nil, errors.New("sign tx failed")).
			Once()

		channel.Receive(payload)

		var txStatus string
		err = dbConnectionPool.GetContext(context.Background(), &txStatus, `SELECT current_status FROM tss_transactions WHERE transaction_hash = $1`, payload.TransactionHash)
		require.NoError(t, err)
		assert.Equal(t, string(tss.NewStatus), txStatus)
	})

	t.Run("payload_gets_routed", func(t *testing.T) {
		sendResp := tss.RPCSendTxResponse{}
		sendResp.Status = tss.ErrorStatus
		sendResp.TransactionHash = feeBumpTxHash
		sendResp.TransactionXDR = feeBumpTxXDR
		sendResp.Code.TxResultCode = xdr.TransactionResultCodeTxTooEarly
		txServiceMock.
			On("SignAndBuildNewFeeBumpTransaction", context.Background(), payload.TransactionXDR).
			Return(feeBumpTx, nil).
			Once().
			On("NetworkPassphrase").
			Return(networkPass).
			Once().
			On("SendTransaction", feeBumpTxXDR).
			Return(sendResp, nil).
			Once()

		mockRouter.
			On("Route", mock.AnythingOfType("tss.Payload")).
			Return().
			Once()

		channel.Receive(payload)

		var txStatus string
		err = dbConnectionPool.GetContext(context.Background(), &txStatus, `SELECT current_status FROM tss_transactions WHERE transaction_hash = $1`, payload.TransactionHash)
		require.NoError(t, err)
		assert.Equal(t, string(tss.ErrorStatus), txStatus)

		var tryStatus int
		err = dbConnectionPool.GetContext(context.Background(), &tryStatus, `SELECT status FROM tss_transaction_submission_tries WHERE try_transaction_hash = $1`, feeBumpTxHash)
		require.NoError(t, err)
		assert.Equal(t, int(xdr.TransactionResultCodeTxTooEarly), tryStatus)
	})

	t.Run("retries", func(t *testing.T) {
		sendResp1 := tss.RPCSendTxResponse{}
		sendResp1.Status = tss.ErrorStatus
		sendResp1.TransactionHash = feeBumpTxHash
		sendResp1.TransactionXDR = feeBumpTxXDR
		sendResp1.Code.TxResultCode = xdr.TransactionResultCodeTxInsufficientFee

		sendResp2 := tss.RPCSendTxResponse{}
		sendResp2.Status = tss.FailedStatus
		sendResp2.TransactionHash = feeBumpTxHash
		sendResp2.TransactionXDR = feeBumpTxXDR
		sendResp2.Code.TxResultCode = xdr.TransactionResultCodeTxFailed
		txServiceMock.
			On("SignAndBuildNewFeeBumpTransaction", context.Background(), payload.TransactionXDR).
			Return(feeBumpTx, nil).
			Twice().
			On("NetworkPassphrase").
			Return(networkPass).
			Twice()

		txServiceMock.
			On("SendTransaction", feeBumpTxXDR).
			Return(sendResp1, nil).
			Once()

		txServiceMock.
			On("SendTransaction", feeBumpTxXDR).
			Return(sendResp2, nil).
			Once()

		mockRouter.
			On("Route", mock.AnythingOfType("tss.Payload")).
			Return().
			Once()

		channel.Receive(payload)

		var txStatus string
		err = dbConnectionPool.GetContext(context.Background(), &txStatus, `SELECT current_status FROM tss_transactions WHERE transaction_hash = $1`, payload.TransactionHash)
		require.NoError(t, err)
		assert.Equal(t, string(tss.FailedStatus), txStatus)

		var tryStatus int
		err = dbConnectionPool.GetContext(context.Background(), &tryStatus, `SELECT status FROM tss_transaction_submission_tries WHERE try_transaction_hash = $1`, feeBumpTxHash)
		require.NoError(t, err)
		assert.Equal(t, int(xdr.TransactionResultCodeTxFailed), tryStatus)
	})

	t.Run("max_retries", func(t *testing.T) {
		sendResp := tss.RPCSendTxResponse{}
		sendResp.Status = tss.ErrorStatus
		sendResp.TransactionHash = feeBumpTxHash
		sendResp.TransactionXDR = feeBumpTxXDR
		sendResp.Code.TxResultCode = xdr.TransactionResultCodeTxInsufficientFee
		txServiceMock.
			On("SignAndBuildNewFeeBumpTransaction", context.Background(), payload.TransactionXDR).
			Return(feeBumpTx, nil).
			Times(3).
			On("NetworkPassphrase").
			Return(networkPass).
			Times(3)

		txServiceMock.
			On("SendTransaction", feeBumpTxXDR).
			Return(sendResp, nil).
			Times(3)

		mockRouter.
			On("Route", mock.AnythingOfType("tss.Payload")).
			Return().
			Once()

		channel.Receive(payload)

		var txStatus string
		err = dbConnectionPool.GetContext(context.Background(), &txStatus, `SELECT current_status FROM tss_transactions WHERE transaction_hash = $1`, payload.TransactionHash)
		require.NoError(t, err)
		assert.Equal(t, string(tss.ErrorStatus), txStatus)

		var tryStatus int
		err = dbConnectionPool.GetContext(context.Background(), &tryStatus, `SELECT status FROM tss_transaction_submission_tries WHERE try_transaction_hash = $1`, feeBumpTxHash)
		require.NoError(t, err)
		assert.Equal(t, int(xdr.TransactionResultCodeTxInsufficientFee), tryStatus)
	})
}
