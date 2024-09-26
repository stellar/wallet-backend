package services

import (
	"context"
	"errors"
	"testing"

	"github.com/stellar/go/xdr"
	"github.com/stellar/wallet-backend/internal/db"
	"github.com/stellar/wallet-backend/internal/db/dbtest"
	"github.com/stellar/wallet-backend/internal/entities"
	"github.com/stellar/wallet-backend/internal/services"
	"github.com/stellar/wallet-backend/internal/tss"
	"github.com/stellar/wallet-backend/internal/tss/store"
	"github.com/stellar/wallet-backend/internal/tss/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBuildAndSubmitTransaction(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()

	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()
	store := store.NewStore(dbConnectionPool)
	txServiceMock := TransactionServiceMock{}
	rpcServiceMock := services.RPCServiceMock{}
	txManager := NewTransactionManager(TransactionManagerConfigs{
		TxService:  &txServiceMock,
		RPCService: &rpcServiceMock,
		Store:      store,
	})
	networkPass := "passphrase"
	feeBumpTx := utils.BuildTestFeeBumpTransaction()
	feeBumpTxXDR, _ := feeBumpTx.Base64()
	feeBumpTxHash, _ := feeBumpTx.HashHex(networkPass)
	payload := tss.Payload{}
	payload.WebhookURL = "www.stellar.com"
	payload.TransactionHash = "hash"
	payload.TransactionXDR = "xdr"

	t.Run("fail_on_tx_build_and_sign", func(t *testing.T) {
		_ = store.UpsertTransaction(context.Background(), payload.WebhookURL, payload.TransactionHash, payload.TransactionXDR, tss.RPCTXStatus{OtherStatus: tss.NewStatus})
		txServiceMock.
			On("SignAndBuildNewFeeBumpTransaction", context.Background(), payload.TransactionXDR).
			Return(nil, errors.New("signing failed")).
			Once()

		_, err := txManager.BuildAndSubmitTransaction(context.Background(), "channel", payload)

		assert.Equal(t, "channel: Unable to sign/build transaction: signing failed", err.Error())

		var status string
		err = dbConnectionPool.GetContext(context.Background(), &status, `SELECT current_status FROM tss_transactions WHERE transaction_hash = $1`, payload.TransactionHash)
		require.NoError(t, err)
		assert.Equal(t, string(tss.NewStatus), status)
	})

	t.Run("rpc_call_fail", func(t *testing.T) {
		_ = store.UpsertTransaction(context.Background(), payload.WebhookURL, payload.TransactionHash, payload.TransactionXDR, tss.RPCTXStatus{OtherStatus: tss.NewStatus})
		sendResp := entities.RPCSendTransactionResult{Status: string(entities.ErrorStatus)}

		txServiceMock.
			On("SignAndBuildNewFeeBumpTransaction", context.Background(), payload.TransactionXDR).
			Return(feeBumpTx, nil).
			Once().
			On("NetworkPassphrase").
			Return(networkPass).
			Once()
		rpcServiceMock.
			On("SendTransaction", feeBumpTxXDR).
			Return(sendResp, errors.New("RPC down")).
			Once()

		_, err := txManager.BuildAndSubmitTransaction(context.Background(), "channel", payload)

		assert.Equal(t, "channel: RPC fail: RPC fail: RPC down", err.Error())

		var txStatus string
		err = dbConnectionPool.GetContext(context.Background(), &txStatus, `SELECT current_status FROM tss_transactions WHERE transaction_hash = $1`, payload.TransactionHash)
		require.NoError(t, err)
		assert.Equal(t, txStatus, string(tss.NewStatus))

		var tryStatus int
		err = dbConnectionPool.GetContext(context.Background(), &tryStatus, `SELECT status FROM tss_transaction_submission_tries WHERE try_transaction_hash = $1`, feeBumpTxHash)
		require.NoError(t, err)
		assert.Equal(t, int(tss.RPCFailCode), tryStatus)
	})

	t.Run("rpc_resp_empty_errorresult_xdr", func(t *testing.T) {
		_ = store.UpsertTransaction(context.Background(), payload.WebhookURL, payload.TransactionHash, payload.TransactionXDR, tss.RPCTXStatus{OtherStatus: tss.NewStatus})
		sendResp := entities.RPCSendTransactionResult{
			Status:         string(entities.PendingStatus),
			ErrorResultXDR: "",
		}

		txServiceMock.
			On("SignAndBuildNewFeeBumpTransaction", context.Background(), payload.TransactionXDR).
			Return(feeBumpTx, nil).
			Once().
			On("NetworkPassphrase").
			Return(networkPass).
			Once()
		rpcServiceMock.
			On("SendTransaction", feeBumpTxXDR).
			Return(sendResp, nil).
			Once()

		resp, err := txManager.BuildAndSubmitTransaction(context.Background(), "channel", payload)

		assert.Equal(t, entities.PendingStatus, resp.Status.RPCStatus)
		assert.Equal(t, tss.EmptyCode, resp.Code.OtherCodes)
		assert.Empty(t, err)

		var txStatus string
		err = dbConnectionPool.GetContext(context.Background(), &txStatus, `SELECT current_status FROM tss_transactions WHERE transaction_hash = $1`, payload.TransactionHash)
		require.NoError(t, err)
		assert.Equal(t, txStatus, string(entities.PendingStatus))

		var tryStatus int
		err = dbConnectionPool.GetContext(context.Background(), &tryStatus, `SELECT status FROM tss_transaction_submission_tries WHERE try_transaction_hash = $1`, feeBumpTxHash)
		require.NoError(t, err)
		assert.Equal(t, int(tss.EmptyCode), tryStatus)
	})
	t.Run("rpc_resp_has_unparsable_errorresult_xdr", func(t *testing.T) {
		_ = store.UpsertTransaction(context.Background(), payload.WebhookURL, payload.TransactionHash, payload.TransactionXDR, tss.RPCTXStatus{OtherStatus: tss.NewStatus})
		sendResp := entities.RPCSendTransactionResult{
			Status:         string(entities.ErrorStatus),
			ErrorResultXDR: "ABCD",
		}

		txServiceMock.
			On("SignAndBuildNewFeeBumpTransaction", context.Background(), payload.TransactionXDR).
			Return(feeBumpTx, nil).
			Once().
			On("NetworkPassphrase").
			Return(networkPass).
			Once()
		rpcServiceMock.
			On("SendTransaction", feeBumpTxXDR).
			Return(sendResp, nil).
			Once()

		_, err := txManager.BuildAndSubmitTransaction(context.Background(), "channel", payload)

		assert.Equal(t, "channel: RPC fail: parse error result xdr string: unable to unmarshal errorResultXDR: ABCD", err.Error())

		var txStatus string
		err = dbConnectionPool.GetContext(context.Background(), &txStatus, `SELECT current_status FROM tss_transactions WHERE transaction_hash = $1`, payload.TransactionHash)
		require.NoError(t, err)
		assert.Equal(t, txStatus, string(tss.NewStatus))

		var tryStatus int
		err = dbConnectionPool.GetContext(context.Background(), &tryStatus, `SELECT status FROM tss_transaction_submission_tries WHERE try_transaction_hash = $1`, feeBumpTxHash)
		require.NoError(t, err)
		assert.Equal(t, int(tss.UnmarshalBinaryCode), tryStatus)
	})
	t.Run("rpc_returns_response", func(t *testing.T) {
		_ = store.UpsertTransaction(context.Background(), payload.WebhookURL, payload.TransactionHash, payload.TransactionXDR, tss.RPCTXStatus{OtherStatus: tss.NewStatus})
		sendResp := entities.RPCSendTransactionResult{
			Status:         string(entities.ErrorStatus),
			ErrorResultXDR: "AAAAAAAAAMj////9AAAAAA==",
		}

		txServiceMock.
			On("SignAndBuildNewFeeBumpTransaction", context.Background(), payload.TransactionXDR).
			Return(feeBumpTx, nil).
			Once().
			On("NetworkPassphrase").
			Return(networkPass).
			Once()
		rpcServiceMock.
			On("SendTransaction", feeBumpTxXDR).
			Return(sendResp, nil).
			Once()

		resp, err := txManager.BuildAndSubmitTransaction(context.Background(), "channel", payload)

		assert.Equal(t, entities.ErrorStatus, resp.Status.RPCStatus)
		assert.Equal(t, xdr.TransactionResultCodeTxTooLate, resp.Code.TxResultCode)
		assert.Empty(t, err)

		var txStatus string
		err = dbConnectionPool.GetContext(context.Background(), &txStatus, `SELECT current_status FROM tss_transactions WHERE transaction_hash = $1`, payload.TransactionHash)
		require.NoError(t, err)
		assert.Equal(t, string(entities.ErrorStatus), txStatus)

		var tryStatus int
		err = dbConnectionPool.GetContext(context.Background(), &tryStatus, `SELECT status FROM tss_transaction_submission_tries WHERE try_transaction_hash = $1`, feeBumpTxHash)
		require.NoError(t, err)
		assert.Equal(t, int(xdr.TransactionResultCodeTxTooLate), tryStatus)
	})
}
