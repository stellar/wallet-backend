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

func TestSend(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()
	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()
	store := store.NewStore(dbConnectionPool)
	txServiceMock := utils.TransactionServiceMock{}
	cfgs := RPCCallerServiceChannelConfigs{
		Store:         store,
		TxService:     &txServiceMock,
		MaxBufferSize: 10,
		MaxWorkers:    10,
	}
	channel := NewRPCCallerServiceChannel(cfgs)
	payload := tss.Payload{}
	payload.WebhookURL = "www.stellar.com"
	payload.TransactionHash = "hash"
	payload.TransactionXDR = "xdr"
	txServiceMock.
		On("SignAndBuildNewFeeBumpTransaction", context.Background(), payload.TransactionXDR).
		Return(nil, errors.New("signing failed"))
	channel.Send(payload)
	channel.Stop()

	var status string
	err = dbConnectionPool.GetContext(context.Background(), &status, `SELECT current_status FROM tss_transactions WHERE transaction_hash = $1`, payload.TransactionHash)
	require.NoError(t, err)
	assert.Equal(t, status, string(tss.NewStatus))
}

func TestReceive(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()

	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()
	store := store.NewStore(dbConnectionPool)
	txServiceMock := utils.TransactionServiceMock{}
	defer txServiceMock.AssertExpectations(t)
	routerMock := router.MockRouter{}
	defer routerMock.AssertExpectations(t)
	cfgs := RPCCallerServiceChannelConfigs{
		Store:         store,
		TxService:     &txServiceMock,
		Router:        &routerMock,
		MaxBufferSize: 1,
		MaxWorkers:    1,
	}
	networkPass := "passphrase"
	channel := NewRPCCallerServiceChannel(cfgs)
	feeBumpTx := utils.BuildTestFeeBumpTransaction()
	feeBumpTxXDR, _ := feeBumpTx.Base64()
	feeBumpTxHash, _ := feeBumpTx.HashHex(networkPass)
	payload := tss.Payload{}
	payload.WebhookURL = "www.stellar.com"
	payload.TransactionHash = "hash"
	payload.TransactionXDR = "xdr"

	t.Run("fail_on_tx_build_and_sign", func(t *testing.T) {
		txServiceMock.
			On("SignAndBuildNewFeeBumpTransaction", context.Background(), payload.TransactionXDR).
			Return(nil, errors.New("signing failed")).
			Once()
		channel.Receive(payload)

		var status string
		err = dbConnectionPool.GetContext(context.Background(), &status, `SELECT current_status FROM tss_transactions WHERE transaction_hash = $1`, payload.TransactionHash)
		require.NoError(t, err)
		assert.Equal(t, string(tss.NewStatus), status)
	})

	t.Run("sign_and_submit_tx_fails", func(t *testing.T) {
		sendResp := tss.RPCSendTxResponse{}
		sendResp.Code.OtherCodes = tss.RPCFailCode
		txServiceMock.
			On("SignAndBuildNewFeeBumpTransaction", context.Background(), payload.TransactionXDR).
			Return(feeBumpTx, nil).
			Once().
			On("NetworkPassphrase").
			Return(networkPass).
			Once().
			On("SendTransaction", feeBumpTxXDR).
			Return(sendResp, errors.New("RPC Fail")).
			Once()

		channel.Receive(payload)

		var txStatus string
		err = dbConnectionPool.GetContext(context.Background(), &txStatus, `SELECT current_status FROM tss_transactions WHERE transaction_hash = $1`, payload.TransactionHash)
		require.NoError(t, err)
		assert.Equal(t, txStatus, string(tss.NewStatus))

		var tryStatus int
		feeBumpTxHash, _ := feeBumpTx.HashHex(networkPass)
		err = dbConnectionPool.GetContext(context.Background(), &tryStatus, `SELECT status FROM tss_transaction_submission_tries WHERE try_transaction_hash = $1`, feeBumpTxHash)
		require.NoError(t, err)
		assert.Equal(t, int(tss.RPCFailCode), tryStatus)

	})

	t.Run("routes_payload", func(t *testing.T) {
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
		routerMock.
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

	t.Run("does_not_routes_payload", func(t *testing.T) {
		sendResp := tss.RPCSendTxResponse{}
		sendResp.Status = tss.PendingStatus
		sendResp.TransactionHash = feeBumpTxHash
		sendResp.TransactionXDR = feeBumpTxXDR
		sendResp.Code.TxResultCode = xdr.TransactionResultCodeTxSuccess
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
		// this time the router mock is not called

		channel.Receive(payload)

		var txStatus string
		err = dbConnectionPool.GetContext(context.Background(), &txStatus, `SELECT current_status FROM tss_transactions WHERE transaction_hash = $1`, payload.TransactionHash)
		require.NoError(t, err)
		assert.Equal(t, string(tss.PendingStatus), txStatus)

		var tryStatus int
		err = dbConnectionPool.GetContext(context.Background(), &tryStatus, `SELECT status FROM tss_transaction_submission_tries WHERE try_transaction_hash = $1`, feeBumpTxHash)
		require.NoError(t, err)
		assert.Equal(t, int(xdr.TransactionResultCodeTxSuccess), tryStatus)
	})

}
