package channels

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/stellar/wallet-backend/internal/db"
	"github.com/stellar/wallet-backend/internal/db/dbtest"
	"github.com/stellar/wallet-backend/internal/entities"
	"github.com/stellar/wallet-backend/internal/metrics"
	"github.com/stellar/wallet-backend/internal/tss"
	"github.com/stellar/wallet-backend/internal/tss/router"
	"github.com/stellar/wallet-backend/internal/tss/services"
	"github.com/stellar/wallet-backend/internal/tss/store"
)

func TestSend(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()
	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	mockMetricsService := metrics.NewMockMetricsService()
	store, _ := store.NewStore(dbConnectionPool, mockMetricsService)
	txManagerMock := services.TransactionManagerMock{}
	routerMock := router.MockRouter{}
	cfgs := RPCCallerChannelConfigs{
		Store:          store,
		TxManager:      &txManagerMock,
		Router:         &routerMock,
		MaxBufferSize:  10,
		MaxWorkers:     10,
		MetricsService: mockMetricsService,
	}

	mockMetricsService.On("RegisterPoolMetrics", RPCCallerChannelName, mock.AnythingOfType("*pond.WorkerPool")).Once()
	mockMetricsService.On("ObserveDBQueryDuration", "INSERT", "tss_transactions", mock.AnythingOfType("float64")).Once()
	mockMetricsService.On("IncDBQuery", "INSERT", "tss_transactions").Once()
	mockMetricsService.On("RecordTSSTransactionStatusTransition", string(tss.NewStatus), mock.AnythingOfType("string")).Once()
	defer mockMetricsService.AssertExpectations(t)

	channel := NewRPCCallerChannel(cfgs)
	payload := tss.Payload{}
	payload.WebhookURL = "www.stellar.com"
	payload.TransactionHash = "hash"
	payload.TransactionXDR = "xdr"

	rpcResp := tss.RPCSendTxResponse{
		Status: tss.RPCTXStatus{RPCStatus: entities.TryAgainLaterStatus},
	}
	payload.RPCSubmitTxResponse = rpcResp

	txManagerMock.
		On("BuildAndSubmitTransaction", context.Background(), RPCCallerChannelName, payload).
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

func TestReceive(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()
	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	mockMetricsService := metrics.NewMockMetricsService()
	store, _ := store.NewStore(dbConnectionPool, mockMetricsService)
	txManagerMock := services.TransactionManagerMock{}
	routerMock := router.MockRouter{}
	cfgs := RPCCallerChannelConfigs{
		Store:          store,
		TxManager:      &txManagerMock,
		Router:         &routerMock,
		MaxBufferSize:  10,
		MaxWorkers:     10,
		MetricsService: mockMetricsService,
	}

	mockMetricsService.On("RegisterPoolMetrics", RPCCallerChannelName, mock.AnythingOfType("*pond.WorkerPool")).Once()
	defer mockMetricsService.AssertExpectations(t)

	channel := NewRPCCallerChannel(cfgs)
	payload := tss.Payload{}
	payload.WebhookURL = "www.stellar.com"
	payload.TransactionHash = "hash"
	payload.TransactionXDR = "xdr"

	t.Run("build_and_submit_tx_fail", func(t *testing.T) {
		mockMetricsService.On("ObserveDBQueryDuration", "INSERT", "tss_transactions", mock.AnythingOfType("float64")).Once()
		mockMetricsService.On("IncDBQuery", "INSERT", "tss_transactions").Once()
		defer mockMetricsService.AssertExpectations(t)

		txManagerMock.
			On("BuildAndSubmitTransaction", context.Background(), RPCCallerChannelName, payload).
			Return(tss.RPCSendTxResponse{}, errors.New("build tx failed")).
			Once()

		channel.Receive(payload)

		routerMock.AssertNotCalled(t, "Route", payload)
	})

	t.Run("payload_routed", func(t *testing.T) {
		mockMetricsService.On("ObserveDBQueryDuration", "INSERT", "tss_transactions", mock.AnythingOfType("float64")).Once()
		mockMetricsService.On("IncDBQuery", "INSERT", "tss_transactions").Once()
		mockMetricsService.On("RecordTSSTransactionStatusTransition", string(tss.NewStatus), mock.AnythingOfType("string")).Once()
		defer mockMetricsService.AssertExpectations(t)

		rpcResp := tss.RPCSendTxResponse{
			Status: tss.RPCTXStatus{RPCStatus: entities.ErrorStatus},
		}
		payload.RPCSubmitTxResponse = rpcResp

		txManagerMock.
			On("BuildAndSubmitTransaction", context.Background(), RPCCallerChannelName, payload).
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
