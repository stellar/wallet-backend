package services

import (
	"context"
	"testing"

	"github.com/stellar/go/xdr"
	"github.com/stellar/wallet-backend/internal/db"
	"github.com/stellar/wallet-backend/internal/db/dbtest"
	"github.com/stellar/wallet-backend/internal/entities"
	"github.com/stellar/wallet-backend/internal/services"
	"github.com/stellar/wallet-backend/internal/tss"
	"github.com/stellar/wallet-backend/internal/tss/router"
	"github.com/stellar/wallet-backend/internal/tss/store"
	"github.com/stellar/wallet-backend/internal/tss/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRouteNewTransactions(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()

	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()
	store, _ := store.NewStore(dbConnectionPool)
	mockRouter := router.MockRouter{}
	mockRPCSerive := services.RPCServiceMock{}
	populator, _ := NewPoolPopulator(&mockRouter, store, &mockRPCSerive)
	t.Run("tx_has_no_try", func(t *testing.T) {
		_ = store.UpsertTransaction(context.Background(), "localhost:8000/webhook", "hash", "xdr", tss.RPCTXStatus{OtherStatus: tss.NewStatus})

		expectedPayload := tss.Payload{
			TransactionHash:     "hash",
			TransactionXDR:      "xdr",
			WebhookURL:          "localhost:8000/webhook",
			RpcSubmitTxResponse: tss.RPCSendTxResponse{Status: tss.RPCTXStatus{OtherStatus: tss.NewStatus}},
		}
		mockRouter.
			On("Route", expectedPayload).
			Return(nil).
			Once()

		err := populator.routeNewTransactions()
		assert.Empty(t, err)
	})

	t.Run("tx_has_try", func(t *testing.T) {
		_ = store.UpsertTransaction(context.Background(), "localhost:8000/webhook", "hash", "xdr", tss.RPCTXStatus{OtherStatus: tss.NewStatus})
		_ = store.UpsertTry(context.Background(), "hash", "feebumphash", "feebumpxdr", tss.RPCTXStatus{OtherStatus: tss.NewStatus}, tss.RPCTXCode{OtherCodes: tss.NewCode}, "ABCD")

		rpcGetTransacrionResp := entities.RPCGetTransactionResult{
			Status:      entities.ErrorStatus,
			EnvelopeXDR: "envelopexdr",
			ResultXDR:   "AAAAAAARFy8AAAAAAAAAAQAAAAAAAAAYAAAAAMu8SHUN67hTUJOz3q+IrH9M/4dCVXaljeK6x1Ss20YWAAAAAA==",
			CreatedAt:   "1234",
		}

		mockRPCSerive.
			On("GetTransaction", "feebumphash").
			Return(rpcGetTransacrionResp, nil).
			Once()

		getIngestTxResp, _ := tss.ParseToRPCGetIngestTxResponse(rpcGetTransacrionResp, nil)
		expectedPayload := tss.Payload{
			TransactionHash:        "hash",
			TransactionXDR:         "xdr",
			WebhookURL:             "localhost:8000/webhook",
			RpcGetIngestTxResponse: getIngestTxResp,
		}
		mockRouter.
			On("Route", expectedPayload).
			Return(nil).
			Once()

		err := populator.routeNewTransactions()
		assert.Empty(t, err)
	})

	t.Run("tx_not_found_timebounds_not_exceeded", func(t *testing.T) {
		feeBumpTx := utils.BuildTestFeeBumpTransaction()
		txXDRStr, _ := feeBumpTx.Base64()
		_ = store.UpsertTransaction(context.Background(), "localhost:8000/webhook", "hash", "xdr", tss.RPCTXStatus{OtherStatus: tss.NewStatus})
		_ = store.UpsertTry(context.Background(), "hash", "feebumphash", txXDRStr, tss.RPCTXStatus{OtherStatus: tss.NewStatus}, tss.RPCTXCode{OtherCodes: tss.NewCode}, "ABCD")

		rpcGetTransacrionResp := entities.RPCGetTransactionResult{
			Status:      entities.NotFoundStatus,
			EnvelopeXDR: "envelopexdr",
		}

		mockRPCSerive.
			On("GetTransaction", "feebumphash").
			Return(rpcGetTransacrionResp, nil).
			Once()

		err := populator.routeNewTransactions()
		assert.Empty(t, err)
	})
}

func TestRouteErrorTransactions(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()

	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()
	store, _ := store.NewStore(dbConnectionPool)
	mockRouter := router.MockRouter{}
	mockRPCSerive := services.RPCServiceMock{}
	populator, _ := NewPoolPopulator(&mockRouter, store, &mockRPCSerive)

	t.Run("tx_has_final_error_code", func(t *testing.T) {
		_ = store.UpsertTransaction(context.Background(), "localhost:8000/webhook", "hash", "xdr", tss.RPCTXStatus{RPCStatus: entities.ErrorStatus})
		_ = store.UpsertTry(context.Background(), "hash", "feebumphash", "feebumpxdr", tss.RPCTXStatus{RPCStatus: entities.ErrorStatus}, tss.RPCTXCode{TxResultCode: xdr.TransactionResultCodeTxInsufficientBalance}, "ABCD")

		expectedPayload := tss.Payload{
			TransactionHash: "hash",
			TransactionXDR:  "xdr",
			WebhookURL:      "localhost:8000/webhook",
			RpcSubmitTxResponse: tss.RPCSendTxResponse{
				TransactionHash: "feebumphash",
				TransactionXDR:  "feebumpxdr",
				Status:          tss.RPCTXStatus{RPCStatus: entities.ErrorStatus},
				Code:            tss.RPCTXCode{TxResultCode: xdr.TransactionResultCodeTxInsufficientBalance},
				ErrorResultXDR:  "ABCD",
			},
		}

		mockRouter.
			On("Route", expectedPayload).
			Return(nil).
			Once()

		err := populator.routeErrorTransactions()
		assert.Empty(t, err)
	})
	t.Run("tx_timebounds_not_exceeded", func(t *testing.T) {
		feeBumpTx := utils.BuildTestFeeBumpTransaction()
		txXDRStr, _ := feeBumpTx.Base64()
		_ = store.UpsertTransaction(context.Background(), "localhost:8000/webhook", "hash", "xdr", tss.RPCTXStatus{RPCStatus: entities.ErrorStatus})
		_ = store.UpsertTry(context.Background(), "hash", "feebumphash", txXDRStr, tss.RPCTXStatus{RPCStatus: entities.ErrorStatus}, tss.RPCTXCode{OtherCodes: tss.RPCFailCode}, "ABCD")

		err := populator.routeErrorTransactions()
		assert.Empty(t, err)
	})
}

func TestRouteFinalTransactions(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()

	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()
	store, _ := store.NewStore(dbConnectionPool)
	mockRouter := router.MockRouter{}
	mockRPCSerive := services.RPCServiceMock{}
	populator, _ := NewPoolPopulator(&mockRouter, store, &mockRPCSerive)

	t.Run("route_successful_tx", func(t *testing.T) {
		_ = store.UpsertTransaction(context.Background(), "localhost:8000/webhook", "hash", "xdr", tss.RPCTXStatus{RPCStatus: entities.SuccessStatus})
		_ = store.UpsertTry(context.Background(), "hash", "feebumphash", "feebumpxdr", tss.RPCTXStatus{RPCStatus: entities.SuccessStatus}, tss.RPCTXCode{TxResultCode: xdr.TransactionResultCodeTxSuccess}, "ABCD")

		expectedPayload := tss.Payload{
			TransactionHash: "hash",
			TransactionXDR:  "xdr",
			WebhookURL:      "localhost:8000/webhook",
			RpcGetIngestTxResponse: tss.RPCGetIngestTxResponse{
				Status:      entities.SuccessStatus,
				Code:        tss.RPCTXCode{TxResultCode: xdr.TransactionResultCodeTxSuccess},
				EnvelopeXDR: "feebumpxdr",
				ResultXDR:   "ABCD",
			},
		}

		mockRouter.
			On("Route", expectedPayload).
			Return(nil).
			Once()

		err = populator.routeFinalTransactions(tss.RPCTXStatus{RPCStatus: entities.SuccessStatus})
		assert.Empty(t, err)
	})
}

func TestNotSentTransactions(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()

	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()
	store, _ := store.NewStore(dbConnectionPool)
	mockRouter := router.MockRouter{}
	mockRPCSerive := services.RPCServiceMock{}
	populator, _ := NewPoolPopulator(&mockRouter, store, &mockRPCSerive)

	t.Run("routes_not_sent_txns", func(t *testing.T) {
		_ = store.UpsertTransaction(context.Background(), "localhost:8000/webhook", "hash", "xdr", tss.RPCTXStatus{OtherStatus: tss.NotSentStatus})
		_ = store.UpsertTry(context.Background(), "hash", "feebumphash", "feebumpxdr", tss.RPCTXStatus{RPCStatus: entities.SuccessStatus}, tss.RPCTXCode{TxResultCode: xdr.TransactionResultCodeTxSuccess}, "ABCD")

		expectedPayload := tss.Payload{
			TransactionHash: "hash",
			TransactionXDR:  "xdr",
			WebhookURL:      "localhost:8000/webhook",
			RpcSubmitTxResponse: tss.RPCSendTxResponse{
				TransactionHash: "feebumphash",
				TransactionXDR:  "feebumpxdr",
				Status:          tss.RPCTXStatus{RPCStatus: entities.SuccessStatus},
				Code:            tss.RPCTXCode{TxResultCode: xdr.TransactionResultCodeTxSuccess},
				ErrorResultXDR:  "ABCD",
			},
		}

		mockRouter.
			On("Route", expectedPayload).
			Return(nil).
			Once()

		err = populator.routeNotSentTransactions()
		assert.Empty(t, err)
	})
}
