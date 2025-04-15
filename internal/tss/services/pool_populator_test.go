package services

import (
	"context"
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
	"github.com/stellar/wallet-backend/internal/tss/router"
	"github.com/stellar/wallet-backend/internal/tss/store"
)

func TestRouteNewTransactions(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()

	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	mockMetricsService := metrics.NewMockMetricsService()
	store, _ := store.NewStore(dbConnectionPool, mockMetricsService)
	mockRouter := router.MockRouter{}
	mockRPCSerive := services.RPCServiceMock{}
	populator, _ := NewPoolPopulator(&mockRouter, store, &mockRPCSerive)
	t.Run("tx_has_no_try", func(t *testing.T) {
		mockMetricsService.On("ObserveDBQueryDuration", "SELECT", "tss_transaction_submission_tries", mock.AnythingOfType("float64")).Once()
		mockMetricsService.On("IncDBQuery", "SELECT", "tss_transaction_submission_tries").Once()
		mockMetricsService.On("ObserveDBQueryDuration", "INSERT", "tss_transactions", mock.AnythingOfType("float64")).Once()
		mockMetricsService.On("IncDBQuery", "INSERT", "tss_transactions").Once()
		mockMetricsService.On("ObserveDBQueryDuration", "SELECT", "tss_transactions", mock.AnythingOfType("float64")).Once()
		mockMetricsService.On("IncDBQuery", "SELECT", "tss_transactions").Once()
		defer mockMetricsService.AssertExpectations(t)

		_ = store.UpsertTransaction(context.Background(), "localhost:8000/webhook", "hash", "xdr", tss.RPCTXStatus{OtherStatus: tss.NewStatus})

		expectedPayload := tss.Payload{
			TransactionHash:     "hash",
			TransactionXDR:      "xdr",
			WebhookURL:          "localhost:8000/webhook",
			RPCSubmitTxResponse: tss.RPCSendTxResponse{Status: tss.RPCTXStatus{OtherStatus: tss.NewStatus}},
		}
		mockRouter.
			On("Route", expectedPayload).
			Return(nil).
			Once()

		err := populator.routeNewTransactions(context.Background())
		assert.Empty(t, err)
	})

	t.Run("tx_has_try", func(t *testing.T) {
		mockMetricsService.On("ObserveDBQueryDuration", "SELECT", "tss_transaction_submission_tries", mock.AnythingOfType("float64")).Once()
		mockMetricsService.On("IncDBQuery", "SELECT", "tss_transaction_submission_tries").Once()
		mockMetricsService.On("ObserveDBQueryDuration", "INSERT", "tss_transactions", mock.AnythingOfType("float64")).Once()
		mockMetricsService.On("IncDBQuery", "INSERT", "tss_transactions").Once()
		mockMetricsService.On("ObserveDBQueryDuration", "INSERT", "tss_transaction_submission_tries", mock.AnythingOfType("float64")).Once()
		mockMetricsService.On("IncDBQuery", "INSERT", "tss_transaction_submission_tries").Once()
		mockMetricsService.On("ObserveDBQueryDuration", "SELECT", "tss_transactions", mock.AnythingOfType("float64")).Once()
		mockMetricsService.On("IncDBQuery", "SELECT", "tss_transactions").Once()
		defer mockMetricsService.AssertExpectations(t)

		_ = store.UpsertTransaction(context.Background(), "localhost:8000/webhook", "hash", "xdr", tss.RPCTXStatus{OtherStatus: tss.NewStatus})
		_ = store.UpsertTry(context.Background(), "hash", "feebumphash", "feebumpxdr", tss.RPCTXStatus{OtherStatus: tss.NewStatus}, tss.RPCTXCode{OtherCodes: tss.NewCode}, "ABCD")

		expectedPayload := tss.Payload{
			TransactionHash:     "hash",
			TransactionXDR:      "xdr",
			WebhookURL:          "localhost:8000/webhook",
			RPCSubmitTxResponse: tss.RPCSendTxResponse{Status: tss.RPCTXStatus{OtherStatus: tss.NewStatus}},
		}

		mockRouter.
			On("Route", expectedPayload).
			Return(nil).
			Once()

		err := populator.routeNewTransactions(context.Background())
		assert.Empty(t, err)
	})
}

func TestRouteErrorTransactions(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()

	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	mockMetricsService := metrics.NewMockMetricsService()
	store, _ := store.NewStore(dbConnectionPool, mockMetricsService)
	mockRouter := router.MockRouter{}
	mockRPCSerive := services.RPCServiceMock{}
	populator, _ := NewPoolPopulator(&mockRouter, store, &mockRPCSerive)

	t.Run("tx_has_final_error_code", func(t *testing.T) {
		mockMetricsService.On("ObserveDBQueryDuration", "SELECT", "tss_transactions", mock.AnythingOfType("float64")).Once()
		mockMetricsService.On("IncDBQuery", "SELECT", "tss_transactions").Once()
		mockMetricsService.On("ObserveDBQueryDuration", "INSERT", "tss_transactions", mock.AnythingOfType("float64")).Once()
		mockMetricsService.On("IncDBQuery", "INSERT", "tss_transactions").Once()
		mockMetricsService.On("ObserveDBQueryDuration", "INSERT", "tss_transaction_submission_tries", mock.AnythingOfType("float64")).Once()
		mockMetricsService.On("IncDBQuery", "INSERT", "tss_transaction_submission_tries").Once()
		mockMetricsService.On("ObserveDBQueryDuration", "SELECT", "tss_transaction_submission_tries", mock.AnythingOfType("float64")).Once()
		mockMetricsService.On("IncDBQuery", "SELECT", "tss_transaction_submission_tries").Once()
		defer mockMetricsService.AssertExpectations(t)

		_ = store.UpsertTransaction(context.Background(), "localhost:8000/webhook", "hash", "xdr", tss.RPCTXStatus{RPCStatus: entities.ErrorStatus})
		_ = store.UpsertTry(context.Background(), "hash", "feebumphash", "feebumpxdr", tss.RPCTXStatus{RPCStatus: entities.ErrorStatus}, tss.RPCTXCode{TxResultCode: xdr.TransactionResultCodeTxInsufficientBalance}, "ABCD")

		expectedPayload := tss.Payload{
			TransactionHash: "hash",
			TransactionXDR:  "xdr",
			WebhookURL:      "localhost:8000/webhook",
			RPCSubmitTxResponse: tss.RPCSendTxResponse{
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

		err := populator.routeErrorTransactions(context.Background())
		assert.Empty(t, err)
	})

	t.Run("latest_try_rpc_call_failed", func(t *testing.T) {
		mockMetricsService.On("ObserveDBQueryDuration", "SELECT", "tss_transactions", mock.AnythingOfType("float64")).Once()
		mockMetricsService.On("IncDBQuery", "SELECT", "tss_transactions").Once()
		mockMetricsService.On("ObserveDBQueryDuration", "INSERT", "tss_transactions", mock.AnythingOfType("float64")).Once()
		mockMetricsService.On("IncDBQuery", "INSERT", "tss_transactions").Once()
		mockMetricsService.On("ObserveDBQueryDuration", "INSERT", "tss_transaction_submission_tries", mock.AnythingOfType("float64")).Once()
		mockMetricsService.On("IncDBQuery", "INSERT", "tss_transaction_submission_tries").Once()
		mockMetricsService.On("ObserveDBQueryDuration", "SELECT", "tss_transaction_submission_tries", mock.AnythingOfType("float64")).Once()
		mockMetricsService.On("IncDBQuery", "SELECT", "tss_transaction_submission_tries").Once()
		defer mockMetricsService.AssertExpectations(t)

		_ = store.UpsertTransaction(context.Background(), "localhost:8000/webhook", "hash", "xdr", tss.RPCTXStatus{RPCStatus: entities.ErrorStatus})
		_ = store.UpsertTry(context.Background(), "hash", "feebumphash", "feebumpxdr", tss.RPCTXStatus{RPCStatus: entities.ErrorStatus}, tss.RPCTXCode{OtherCodes: tss.RPCFailCode}, "ABCD")

		expectedPayload := tss.Payload{
			TransactionHash: "hash",
			TransactionXDR:  "xdr",
			WebhookURL:      "localhost:8000/webhook",
			RPCSubmitTxResponse: tss.RPCSendTxResponse{
				TransactionHash: "feebumphash",
				TransactionXDR:  "feebumpxdr",
				Status:          tss.RPCTXStatus{RPCStatus: entities.TryAgainLaterStatus},
			},
		}

		mockRouter.
			On("Route", expectedPayload).
			Return(nil).
			Once()

		err := populator.routeErrorTransactions(context.Background())
		assert.Empty(t, err)
	})
}

func TestRouteFinalTransactions(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()

	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	mockMetricsService := metrics.NewMockMetricsService()
	store, _ := store.NewStore(dbConnectionPool, mockMetricsService)
	mockRouter := router.MockRouter{}
	mockRPCSerive := services.RPCServiceMock{}
	populator, _ := NewPoolPopulator(&mockRouter, store, &mockRPCSerive)

	t.Run("route_successful_tx", func(t *testing.T) {
		mockMetricsService.On("ObserveDBQueryDuration", "SELECT", "tss_transactions", mock.AnythingOfType("float64")).Once()
		mockMetricsService.On("IncDBQuery", "SELECT", "tss_transactions").Once()
		mockMetricsService.On("ObserveDBQueryDuration", "INSERT", "tss_transactions", mock.AnythingOfType("float64")).Once()
		mockMetricsService.On("IncDBQuery", "INSERT", "tss_transactions").Once()
		mockMetricsService.On("ObserveDBQueryDuration", "INSERT", "tss_transaction_submission_tries", mock.AnythingOfType("float64")).Once()
		mockMetricsService.On("IncDBQuery", "INSERT", "tss_transaction_submission_tries").Once()
		mockMetricsService.On("ObserveDBQueryDuration", "SELECT", "tss_transaction_submission_tries", mock.AnythingOfType("float64")).Once()
		mockMetricsService.On("IncDBQuery", "SELECT", "tss_transaction_submission_tries").Once()
		defer mockMetricsService.AssertExpectations(t)

		_ = store.UpsertTransaction(context.Background(), "localhost:8000/webhook", "hash", "xdr", tss.RPCTXStatus{RPCStatus: entities.SuccessStatus})
		_ = store.UpsertTry(context.Background(), "hash", "feebumphash", "feebumpxdr", tss.RPCTXStatus{RPCStatus: entities.SuccessStatus}, tss.RPCTXCode{TxResultCode: xdr.TransactionResultCodeTxSuccess}, "ABCD")

		expectedPayload := tss.Payload{
			TransactionHash: "hash",
			TransactionXDR:  "xdr",
			WebhookURL:      "localhost:8000/webhook",
			RPCGetIngestTxResponse: tss.RPCGetIngestTxResponse{
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

		err = populator.routeFinalTransactions(context.Background(), tss.RPCTXStatus{RPCStatus: entities.SuccessStatus})
		assert.Empty(t, err)
	})
}

func TestNotSentTransactions(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()

	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	mockMetricsService := metrics.NewMockMetricsService()
	store, _ := store.NewStore(dbConnectionPool, mockMetricsService)
	mockRouter := router.MockRouter{}
	mockRPCSerive := services.RPCServiceMock{}
	populator, _ := NewPoolPopulator(&mockRouter, store, &mockRPCSerive)

	t.Run("routes_not_sent_txns", func(t *testing.T) {
		mockMetricsService.On("ObserveDBQueryDuration", "SELECT", "tss_transactions", mock.AnythingOfType("float64")).Once()
		mockMetricsService.On("IncDBQuery", "SELECT", "tss_transactions").Once()
		mockMetricsService.On("ObserveDBQueryDuration", "INSERT", "tss_transactions", mock.AnythingOfType("float64")).Once()
		mockMetricsService.On("IncDBQuery", "INSERT", "tss_transactions").Once()
		mockMetricsService.On("ObserveDBQueryDuration", "INSERT", "tss_transaction_submission_tries", mock.AnythingOfType("float64")).Once()
		mockMetricsService.On("IncDBQuery", "INSERT", "tss_transaction_submission_tries").Once()
		mockMetricsService.On("ObserveDBQueryDuration", "SELECT", "tss_transaction_submission_tries", mock.AnythingOfType("float64")).Once()
		mockMetricsService.On("IncDBQuery", "SELECT", "tss_transaction_submission_tries").Once()
		defer mockMetricsService.AssertExpectations(t)

		_ = store.UpsertTransaction(context.Background(), "localhost:8000/webhook", "hash", "xdr", tss.RPCTXStatus{OtherStatus: tss.NotSentStatus})
		_ = store.UpsertTry(context.Background(), "hash", "feebumphash", "feebumpxdr", tss.RPCTXStatus{RPCStatus: entities.SuccessStatus}, tss.RPCTXCode{TxResultCode: xdr.TransactionResultCodeTxSuccess}, "ABCD")

		expectedPayload := tss.Payload{
			TransactionHash: "hash",
			TransactionXDR:  "xdr",
			WebhookURL:      "localhost:8000/webhook",
			RPCSubmitTxResponse: tss.RPCSendTxResponse{
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

		err = populator.routeNotSentTransactions(context.Background())
		assert.Empty(t, err)
	})
}
