package channels

import (
	"context"
	"errors"
	"testing"

	"github.com/stellar/go/xdr"
	"github.com/stellar/wallet-backend/internal/db"
	"github.com/stellar/wallet-backend/internal/db/dbtest"
	"github.com/stellar/wallet-backend/internal/tss"
	"github.com/stellar/wallet-backend/internal/tss/store"
	"github.com/stellar/wallet-backend/internal/tss/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSignAndSubmitTransaction(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()

	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()
	store := store.NewStore(dbConnectionPool)
	txServiceMock := utils.TransactionServiceMock{}
	networkPass := "passphrase"
	feeBumpTx := utils.BuildTestFeeBumpTransaction()
	feeBumpTxXDR, _ := feeBumpTx.Base64()
	feeBumpTxHash, _ := feeBumpTx.HashHex(networkPass)
	payload := tss.Payload{}
	payload.WebhookURL = "www.stellar.com"
	payload.TransactionHash = "hash"
	payload.TransactionXDR = "xdr"

	_ = store.UpsertTransaction(context.Background(), payload.WebhookURL, payload.TransactionHash, payload.TransactionXDR, tss.NewStatus)
	t.Run("fail_on_tx_build_and_sign", func(t *testing.T) {
		txServiceMock.
			On("SignAndBuildNewFeeBumpTransaction", context.Background(), payload.TransactionXDR).
			Return(nil, errors.New("signing failed")).
			Once()

		_, err := SignAndSubmitTransaction(context.Background(), "channel", payload, store, &txServiceMock)

		assert.Equal(t, "channel: Unable to sign/build transaction: signing failed", err.Error())

		var status string
		err = dbConnectionPool.GetContext(context.Background(), &status, `SELECT current_status FROM tss_transactions WHERE transaction_hash = $1`, payload.TransactionHash)
		require.NoError(t, err)
		assert.Equal(t, string(tss.NewStatus), status)
	})

	t.Run("rpc_call_fail", func(t *testing.T) {
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

		_, err := SignAndSubmitTransaction(context.Background(), "channel", payload, store, &txServiceMock)

		assert.Equal(t, "channel: RPC fail: RPC Fail", err.Error())

		var txStatus string
		err = dbConnectionPool.GetContext(context.Background(), &txStatus, `SELECT current_status FROM tss_transactions WHERE transaction_hash = $1`, payload.TransactionHash)
		require.NoError(t, err)
		assert.Equal(t, txStatus, string(tss.NewStatus))

		var tryStatus int
		err = dbConnectionPool.GetContext(context.Background(), &tryStatus, `SELECT status FROM tss_transaction_submission_tries WHERE try_transaction_hash = $1`, feeBumpTxHash)
		require.NoError(t, err)
		assert.Equal(t, int(tss.RPCFailCode), tryStatus)
	})

	t.Run("rpc_resp_unmarshaling_error", func(t *testing.T) {
		sendResp := tss.RPCSendTxResponse{}
		sendResp.Code.OtherCodes = tss.UnMarshalBinaryCode
		txServiceMock.
			On("SignAndBuildNewFeeBumpTransaction", context.Background(), payload.TransactionXDR).
			Return(feeBumpTx, nil).
			Once().
			On("NetworkPassphrase").
			Return(networkPass).
			Once().
			On("SendTransaction", feeBumpTxXDR).
			Return(sendResp, errors.New("unable to unmarshal")).
			Once()

		_, err := SignAndSubmitTransaction(context.Background(), "channel", payload, store, &txServiceMock)

		assert.Equal(t, "channel: RPC fail: unable to unmarshal", err.Error())

		var txStatus string
		err = dbConnectionPool.GetContext(context.Background(), &txStatus, `SELECT current_status FROM tss_transactions WHERE transaction_hash = $1`, payload.TransactionHash)
		require.NoError(t, err)
		assert.Equal(t, txStatus, string(tss.NewStatus))

		var tryStatus int
		err = dbConnectionPool.GetContext(context.Background(), &tryStatus, `SELECT status FROM tss_transaction_submission_tries WHERE try_transaction_hash = $1`, feeBumpTxHash)
		require.NoError(t, err)
		assert.Equal(t, int(tss.UnMarshalBinaryCode), tryStatus)
	})
	t.Run("rpc_returns_response", func(t *testing.T) {
		sendResp := tss.RPCSendTxResponse{}
		sendResp.Status = tss.TryAgainLaterStatus
		sendResp.TransactionHash = feeBumpTxHash
		sendResp.TransactionXDR = feeBumpTxXDR
		sendResp.Code.TxResultCode = xdr.TransactionResultCodeTxInsufficientFee
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

		resp, err := SignAndSubmitTransaction(context.Background(), "channel", payload, store, &txServiceMock)

		assert.Equal(t, tss.TryAgainLaterStatus, resp.Status)
		assert.Equal(t, xdr.TransactionResultCodeTxInsufficientFee, resp.Code.TxResultCode)
		assert.Empty(t, err)

		var txStatus string
		err = dbConnectionPool.GetContext(context.Background(), &txStatus, `SELECT current_status FROM tss_transactions WHERE transaction_hash = $1`, payload.TransactionHash)
		require.NoError(t, err)
		assert.Equal(t, string(tss.TryAgainLaterStatus), txStatus)

		var tryStatus int
		err = dbConnectionPool.GetContext(context.Background(), &tryStatus, `SELECT status FROM tss_transaction_submission_tries WHERE try_transaction_hash = $1`, feeBumpTxHash)
		require.NoError(t, err)
		assert.Equal(t, int(xdr.TransactionResultCodeTxInsufficientFee), tryStatus)
	})
}
