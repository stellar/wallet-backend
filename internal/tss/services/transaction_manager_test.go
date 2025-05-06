package services

import (
	"context"
	"errors"
	"testing"

	"github.com/stellar/go/xdr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/stellar/wallet-backend/internal/db"
	"github.com/stellar/wallet-backend/internal/db/dbtest"
	"github.com/stellar/wallet-backend/internal/entities"
	"github.com/stellar/wallet-backend/internal/metrics"
	"github.com/stellar/wallet-backend/internal/services"
	"github.com/stellar/wallet-backend/internal/tss"
	"github.com/stellar/wallet-backend/internal/tss/store"
	"github.com/stellar/wallet-backend/internal/tss/utils"
)

func TestBuildAndSubmitTransaction(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()

	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	mockMetricsService := metrics.NewMockMetricsService()
	dbStore, err := store.NewStore(dbConnectionPool, mockMetricsService)
	require.NoError(t, err)
	txServiceMock := TransactionServiceMock{}
	rpcServiceMock := services.RPCServiceMock{}
	txManager := NewTransactionManager(TransactionManagerConfigs{
		TxService:  &txServiceMock,
		RPCService: &rpcServiceMock,
		Store:      dbStore,
	})
	networkPass := "passphrase"
	tx := utils.BuildTestTransaction(t)
	txHash, err := tx.HashHex(networkPass)
	require.NoError(t, err)
	txXDR, err := tx.Base64()
	require.NoError(t, err)
	feeBumpTx := utils.BuildTestFeeBumpTransaction(t)
	feeBumpTxXDR, err := feeBumpTx.Base64()
	require.NoError(t, err)
	feeBumpTxHash, err := feeBumpTx.HashHex(networkPass)
	require.NoError(t, err)
	payload := tss.Payload{}
	payload.WebhookURL = "www.stellar.com"
	payload.TransactionHash = txHash
	payload.TransactionXDR = txXDR

	t.Run("fail_on_building_feebump_tx", func(t *testing.T) {
		mockMetricsService.On("ObserveDBQueryDuration", "INSERT", "tss_transactions", mock.AnythingOfType("float64")).Once()
		mockMetricsService.On("IncDBQuery", "INSERT", "tss_transactions").Once()
		mockMetricsService.On("ObserveDBQueryDuration", "SELECT", "tss_transactions", mock.AnythingOfType("float64")).Once()
		mockMetricsService.On("IncDBQuery", "SELECT", "tss_transactions").Once()
		defer mockMetricsService.AssertExpectations(t)

		err = dbStore.UpsertTransaction(context.Background(), payload.WebhookURL, payload.TransactionHash, payload.TransactionXDR, tss.RPCTXStatus{OtherStatus: tss.NewStatus})
		require.NoError(t, err)
		txServiceMock.
			On("BuildFeeBumpTransaction", context.Background(), tx).
			Return(nil, errors.New("signing failed")).
			Once()
		payload.FeeBump = true

		var txSendResp tss.RPCSendTxResponse
		txSendResp, err = txManager.BuildAndSubmitTransaction(context.Background(), "channel", payload)
		require.Error(t, err)
		assert.Equal(t, tss.RPCSendTxResponse{}, txSendResp)
		assert.Equal(t, "channel: Unable to build fee bump transaction: signing failed", err.Error())

		var tx store.Transaction
		tx, err = dbStore.GetTransaction(context.Background(), payload.TransactionHash)
		require.NoError(t, err)
		assert.Equal(t, string(tss.NewStatus), tx.Status)
	})

	t.Run("rpc_call_fail", func(t *testing.T) {
		mockMetricsService.On("ObserveDBQueryDuration", "INSERT", "tss_transactions", mock.AnythingOfType("float64")).Once()
		mockMetricsService.On("IncDBQuery", "INSERT", "tss_transactions").Once()
		mockMetricsService.On("ObserveDBQueryDuration", "INSERT", "tss_transaction_submission_tries", mock.AnythingOfType("float64")).Times(2)
		mockMetricsService.On("IncDBQuery", "INSERT", "tss_transaction_submission_tries").Times(2)
		mockMetricsService.On("ObserveDBQueryDuration", "SELECT", "tss_transactions", mock.AnythingOfType("float64")).Once()
		mockMetricsService.On("IncDBQuery", "SELECT", "tss_transactions").Once()
		mockMetricsService.On("ObserveDBQueryDuration", "SELECT", "tss_transaction_submission_tries", mock.AnythingOfType("float64")).Once()
		mockMetricsService.On("IncDBQuery", "SELECT", "tss_transaction_submission_tries").Once()
		defer mockMetricsService.AssertExpectations(t)

		err = dbStore.UpsertTransaction(context.Background(), payload.WebhookURL, payload.TransactionHash, payload.TransactionXDR, tss.RPCTXStatus{OtherStatus: tss.NewStatus})
		require.NoError(t, err)
		sendResp := entities.RPCSendTransactionResult{Status: entities.ErrorStatus}

		txServiceMock.
			On("BuildFeeBumpTransaction", context.Background(), tx).
			Return(feeBumpTx, nil).
			Once().
			On("NetworkPassphrase").
			Return(networkPass).
			Once()
		rpcServiceMock.
			On("SendTransaction", feeBumpTxXDR).
			Return(sendResp, errors.New("RPC down")).
			Once()
		payload.FeeBump = true

		var txSendResp tss.RPCSendTxResponse
		txSendResp, err = txManager.BuildAndSubmitTransaction(context.Background(), "channel", payload)

		assert.Equal(t, entities.ErrorStatus, txSendResp.Status.RPCStatus)
		assert.Equal(t, tss.RPCFailCode, txSendResp.Code.OtherCodes)
		assert.Equal(t, "channel: RPC fail: RPC fail: RPC down", err.Error())

		var tx store.Transaction
		tx, err = dbStore.GetTransaction(context.Background(), payload.TransactionHash)
		require.NoError(t, err)
		assert.Equal(t, string(tss.NewStatus), tx.Status)

		var try store.Try
		try, err = dbStore.GetTry(context.Background(), feeBumpTxHash)
		require.NoError(t, err)
		assert.Equal(t, string(entities.ErrorStatus), try.Status)
		assert.Equal(t, int32(tss.RPCFailCode), try.Code)
	})

	t.Run("rpc_resp_empty_errorresult_xdr", func(t *testing.T) {
		mockMetricsService.On("ObserveDBQueryDuration", "INSERT", "tss_transactions", mock.AnythingOfType("float64")).Times(2)
		mockMetricsService.On("IncDBQuery", "INSERT", "tss_transactions").Times(2)
		mockMetricsService.On("ObserveDBQueryDuration", "INSERT", "tss_transaction_submission_tries", mock.AnythingOfType("float64")).Times(2)
		mockMetricsService.On("IncDBQuery", "INSERT", "tss_transaction_submission_tries").Times(2)
		mockMetricsService.On("ObserveDBQueryDuration", "SELECT", "tss_transactions", mock.AnythingOfType("float64")).Once()
		mockMetricsService.On("IncDBQuery", "SELECT", "tss_transactions").Once()
		mockMetricsService.On("ObserveDBQueryDuration", "SELECT", "tss_transaction_submission_tries", mock.AnythingOfType("float64")).Once()
		mockMetricsService.On("IncDBQuery", "SELECT", "tss_transaction_submission_tries").Once()
		defer mockMetricsService.AssertExpectations(t)

		err = dbStore.UpsertTransaction(context.Background(), payload.WebhookURL, payload.TransactionHash, payload.TransactionXDR, tss.RPCTXStatus{OtherStatus: tss.NewStatus})
		require.NoError(t, err)
		sendResp := entities.RPCSendTransactionResult{
			Status:         entities.PendingStatus,
			ErrorResultXDR: "",
		}

		txServiceMock.
			On("BuildFeeBumpTransaction", context.Background(), tx).
			Return(feeBumpTx, nil).
			Once().
			On("NetworkPassphrase").
			Return(networkPass).
			Once()
		rpcServiceMock.
			On("SendTransaction", feeBumpTxXDR).
			Return(sendResp, nil).
			Once()
		payload.FeeBump = true

		var txSendResp tss.RPCSendTxResponse
		txSendResp, err = txManager.BuildAndSubmitTransaction(context.Background(), "channel", payload)

		assert.Equal(t, entities.PendingStatus, txSendResp.Status.RPCStatus)
		assert.Equal(t, tss.EmptyCode, txSendResp.Code.OtherCodes)
		assert.Empty(t, err)

		var tx store.Transaction
		tx, err = dbStore.GetTransaction(context.Background(), payload.TransactionHash)
		require.NoError(t, err)
		assert.Equal(t, string(entities.PendingStatus), tx.Status)

		var try store.Try
		try, err = dbStore.GetTry(context.Background(), feeBumpTxHash)
		require.NoError(t, err)
		assert.Equal(t, string(entities.PendingStatus), try.Status)
		assert.Equal(t, int32(tss.EmptyCode), try.Code)
	})

	t.Run("rpc_resp_has_unparsable_errorresult_xdr", func(t *testing.T) {
		mockMetricsService.On("ObserveDBQueryDuration", "INSERT", "tss_transactions", mock.AnythingOfType("float64")).Once()
		mockMetricsService.On("IncDBQuery", "INSERT", "tss_transactions").Once()
		mockMetricsService.On("ObserveDBQueryDuration", "INSERT", "tss_transaction_submission_tries", mock.AnythingOfType("float64")).Times(2)
		mockMetricsService.On("IncDBQuery", "INSERT", "tss_transaction_submission_tries").Times(2)
		mockMetricsService.On("ObserveDBQueryDuration", "SELECT", "tss_transactions", mock.AnythingOfType("float64")).Once()
		mockMetricsService.On("IncDBQuery", "SELECT", "tss_transactions").Once()
		mockMetricsService.On("ObserveDBQueryDuration", "SELECT", "tss_transaction_submission_tries", mock.AnythingOfType("float64")).Once()
		mockMetricsService.On("IncDBQuery", "SELECT", "tss_transaction_submission_tries").Once()
		defer mockMetricsService.AssertExpectations(t)

		err = dbStore.UpsertTransaction(context.Background(), payload.WebhookURL, payload.TransactionHash, payload.TransactionXDR, tss.RPCTXStatus{OtherStatus: tss.NewStatus})
		require.NoError(t, err)
		sendResp := entities.RPCSendTransactionResult{
			Status:         entities.ErrorStatus,
			ErrorResultXDR: "ABCD",
		}

		txServiceMock.
			On("BuildFeeBumpTransaction", context.Background(), tx).
			Return(feeBumpTx, nil).
			Once().
			On("NetworkPassphrase").
			Return(networkPass).
			Once()
		rpcServiceMock.
			On("SendTransaction", feeBumpTxXDR).
			Return(sendResp, nil).
			Once()
		payload.FeeBump = true

		var txSendResp tss.RPCSendTxResponse
		txSendResp, err = txManager.BuildAndSubmitTransaction(context.Background(), "channel", payload)
		require.Error(t, err)

		assert.Equal(t, entities.ErrorStatus, txSendResp.Status.RPCStatus)
		assert.Equal(t, tss.UnmarshalBinaryCode, txSendResp.Code.OtherCodes)
		assert.ErrorContains(t, err, "channel: RPC fail: parse error result xdr string: unable to parse: unable to unmarshal errorResultXDR ABCD")

		var tx store.Transaction
		tx, err = dbStore.GetTransaction(context.Background(), payload.TransactionHash)
		require.NoError(t, err)
		assert.Equal(t, string(tss.NewStatus), tx.Status)

		var try store.Try
		try, err = dbStore.GetTry(context.Background(), feeBumpTxHash)
		require.NoError(t, err)
		assert.Equal(t, string(entities.ErrorStatus), try.Status)
		assert.Equal(t, int32(tss.UnmarshalBinaryCode), try.Code)
	})

	t.Run("rpc_returns_response", func(t *testing.T) {
		mockMetricsService.On("ObserveDBQueryDuration", "INSERT", "tss_transactions", mock.AnythingOfType("float64")).Times(2)
		mockMetricsService.On("IncDBQuery", "INSERT", "tss_transactions").Times(2)
		mockMetricsService.On("ObserveDBQueryDuration", "INSERT", "tss_transaction_submission_tries", mock.AnythingOfType("float64")).Times(2)
		mockMetricsService.On("IncDBQuery", "INSERT", "tss_transaction_submission_tries").Times(2)
		mockMetricsService.On("ObserveDBQueryDuration", "SELECT", "tss_transactions", mock.AnythingOfType("float64")).Once()
		mockMetricsService.On("IncDBQuery", "SELECT", "tss_transactions").Once()
		mockMetricsService.On("ObserveDBQueryDuration", "SELECT", "tss_transaction_submission_tries", mock.AnythingOfType("float64")).Once()
		mockMetricsService.On("IncDBQuery", "SELECT", "tss_transaction_submission_tries").Once()
		defer mockMetricsService.AssertExpectations(t)

		err = dbStore.UpsertTransaction(context.Background(), payload.WebhookURL, payload.TransactionHash, payload.TransactionXDR, tss.RPCTXStatus{OtherStatus: tss.NewStatus})
		require.NoError(t, err)
		sendResp := entities.RPCSendTransactionResult{
			Status:         entities.ErrorStatus,
			ErrorResultXDR: "AAAAAAAAAMj////9AAAAAA==",
		}

		txServiceMock.
			On("BuildFeeBumpTransaction", context.Background(), tx).
			Return(feeBumpTx, nil).
			Once().
			On("NetworkPassphrase").
			Return(networkPass).
			Once()
		rpcServiceMock.
			On("SendTransaction", feeBumpTxXDR).
			Return(sendResp, nil).
			Once()
		payload.FeeBump = true

		var txSendResp tss.RPCSendTxResponse
		txSendResp, err = txManager.BuildAndSubmitTransaction(context.Background(), "channel", payload)

		assert.Equal(t, entities.ErrorStatus, txSendResp.Status.RPCStatus)
		assert.Equal(t, xdr.TransactionResultCodeTxTooLate, txSendResp.Code.TxResultCode)
		assert.Empty(t, err)

		var tx store.Transaction
		tx, err = dbStore.GetTransaction(context.Background(), payload.TransactionHash)
		require.NoError(t, err)
		assert.Equal(t, string(entities.ErrorStatus), tx.Status)

		var try store.Try
		try, err = dbStore.GetTry(context.Background(), feeBumpTxHash)
		require.NoError(t, err)
		assert.Equal(t, string(entities.ErrorStatus), try.Status)
		assert.Equal(t, int32(xdr.TransactionResultCodeTxTooLate), try.Code)
	})

	t.Run("feebump_is_false", func(t *testing.T) {
		mockMetricsService.On("ObserveDBQueryDuration", "INSERT", "tss_transactions", mock.AnythingOfType("float64")).Times(2)
		mockMetricsService.On("IncDBQuery", "INSERT", "tss_transactions").Times(2)
		mockMetricsService.On("ObserveDBQueryDuration", "INSERT", "tss_transaction_submission_tries", mock.AnythingOfType("float64")).Times(2)
		mockMetricsService.On("IncDBQuery", "INSERT", "tss_transaction_submission_tries").Times(2)
		mockMetricsService.On("ObserveDBQueryDuration", "SELECT", "tss_transactions", mock.AnythingOfType("float64")).Once()
		mockMetricsService.On("IncDBQuery", "SELECT", "tss_transactions").Once()
		mockMetricsService.On("ObserveDBQueryDuration", "SELECT", "tss_transaction_submission_tries", mock.AnythingOfType("float64")).Once()
		mockMetricsService.On("IncDBQuery", "SELECT", "tss_transaction_submission_tries").Once()
		defer mockMetricsService.AssertExpectations(t)

		err = dbStore.UpsertTransaction(context.Background(), payload.WebhookURL, payload.TransactionHash, payload.TransactionXDR, tss.RPCTXStatus{OtherStatus: tss.NewStatus})
		require.NoError(t, err)
		sendResp := entities.RPCSendTransactionResult{
			Status:         entities.ErrorStatus,
			ErrorResultXDR: "AAAAAAAAAMj////9AAAAAA==",
		}
		txServiceMock.
			On("NetworkPassphrase").
			Return(networkPass).
			Once()
		rpcServiceMock.
			On("SendTransaction", txXDR).
			Return(sendResp, nil).
			Once()
		rpcServiceMock.
			On("SendTransaction", feeBumpTxXDR).
			Return(sendResp, nil).
			Once()
		payload.FeeBump = false

		txSendResp, err := txManager.BuildAndSubmitTransaction(context.Background(), "channel", payload)

		assert.Equal(t, entities.ErrorStatus, txSendResp.Status.RPCStatus)
		assert.Equal(t, xdr.TransactionResultCodeTxTooLate, txSendResp.Code.TxResultCode)
		assert.Empty(t, err)

		tx, err := dbStore.GetTransaction(context.Background(), payload.TransactionHash)
		require.NoError(t, err)
		assert.Equal(t, string(entities.ErrorStatus), tx.Status)

		try, err := dbStore.GetTry(context.Background(), txHash)
		require.NoError(t, err)
		assert.Equal(t, string(entities.ErrorStatus), try.Status)
		assert.Equal(t, int32(xdr.TransactionResultCodeTxTooLate), try.Code)
	})
}
