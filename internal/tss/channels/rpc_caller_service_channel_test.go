package channels

import (
	"context"
	"errors"
	"testing"

	"github.com/stellar/wallet-backend/internal/db"
	"github.com/stellar/wallet-backend/internal/db/dbtest"
	"github.com/stellar/wallet-backend/internal/entities"
	"github.com/stellar/wallet-backend/internal/tss"
	"github.com/stellar/wallet-backend/internal/tss/router"
	"github.com/stellar/wallet-backend/internal/tss/services"
	"github.com/stellar/wallet-backend/internal/tss/store"
	"github.com/stretchr/testify/require"
)

func TestSend(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()
	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()
	store := store.NewStore(dbConnectionPool)
	txManagerMock := services.TransactionManagerMock{}
	routerMock := router.MockRouter{}
	cfgs := RPCCallerServiceChannelConfigs{
		Store:         store,
		TxManager:     &txManagerMock,
		Router:        &routerMock,
		MaxBufferSize: 10,
		MaxWorkers:    10,
	}
	channel := NewRPCCallerServiceChannel(cfgs)
	payload := tss.Payload{}
	payload.WebhookURL = "www.stellar.com"
	payload.TransactionHash = "hash"
	payload.TransactionXDR = "xdr"

	rpcResp := tss.RPCSendTxResponse{
		Status: tss.RPCTXStatus{RPCStatus: entities.TryAgainLaterStatus},
	}
	payload.RpcSubmitTxResponse = rpcResp

	txManagerMock.
		On("BuildAndSubmitTransaction", context.Background(), ChannelName, payload).
		Return(rpcResp, nil).
		Once()

	routerMock.
		On("Route", payload).
		Return(nil).
		Once()

	channel.Send(payload)
	channel.Stop()

	routerMock.AssertCalled(t, "Route", payload)
}

func TestReceivee(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()
	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()
	store := store.NewStore(dbConnectionPool)
	txManagerMock := services.TransactionManagerMock{}
	routerMock := router.MockRouter{}
	cfgs := RPCCallerServiceChannelConfigs{
		Store:         store,
		TxManager:     &txManagerMock,
		Router:        &routerMock,
		MaxBufferSize: 10,
		MaxWorkers:    10,
	}
	channel := NewRPCCallerServiceChannel(cfgs)
	payload := tss.Payload{}
	payload.WebhookURL = "www.stellar.com"
	payload.TransactionHash = "hash"
	payload.TransactionXDR = "xdr"

	t.Run("build_and_submit_tx_fail", func(t *testing.T) {
		txManagerMock.
			On("BuildAndSubmitTransaction", context.Background(), ChannelName, payload).
			Return(tss.RPCSendTxResponse{}, errors.New("build tx failed")).
			Once()

		channel.Receive(payload)

		routerMock.AssertNotCalled(t, "Route", payload)
	})

	t.Run("payload_not_routed", func(t *testing.T) {
		rpcResp := tss.RPCSendTxResponse{
			Status: tss.RPCTXStatus{RPCStatus: entities.PendingStatus},
		}
		payload.RpcSubmitTxResponse = rpcResp

		txManagerMock.
			On("BuildAndSubmitTransaction", context.Background(), ChannelName, payload).
			Return(rpcResp, nil).
			Once()

		channel.Receive(payload)

		routerMock.AssertNotCalled(t, "Route", payload)
	})
	t.Run("payload_routed", func(t *testing.T) {
		rpcResp := tss.RPCSendTxResponse{
			Status: tss.RPCTXStatus{RPCStatus: entities.ErrorStatus},
		}
		payload.RpcSubmitTxResponse = rpcResp

		txManagerMock.
			On("BuildAndSubmitTransaction", context.Background(), ChannelName, payload).
			Return(rpcResp, nil).
			Once()

		routerMock.
			On("Route", payload).
			Return(nil).
			Once()

		channel.Receive(payload)

		routerMock.AssertCalled(t, "Route", payload)
	})

}

/*
func TestReceive(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()

	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()
	store := store.NewStore(dbConnectionPool)
	txServiceMock := services.TransactionServiceMock{}
	defer txServiceMock.AssertExpectations(t)
	routerMock := router.MockRouter{}
	defer routerMock.AssertExpectations(t)
	rpcServiceMock := services.RPCServiceMock{}
	defer rpcServiceMock.AssertExpectations(t)
	txManager := services.NewTransactionManager(services.TransactionManagerConfigs{
		TxService:  &txServiceMock,
		RPCService: &rpcServiceMock,
		Store:      store,
	})
	cfgs := RPCCallerServiceChannelConfigs{
		Store:         store,
		TxManager:     txManager,
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
*/
