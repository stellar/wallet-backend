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
)

func TestNonJitterSend(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()
	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	mockMetricsService := metrics.NewMockMetricsService()
	txManagerMock := services.TransactionManagerMock{}
	routerMock := router.MockRouter{}
	cfg := ErrorNonJitterChannelConfigs{
		TxManager:         &txManagerMock,
		Router:            &routerMock,
		MaxBufferSize:     1,
		MaxWorkers:        1,
		MaxRetries:        3,
		WaitBtwnRetriesMS: 10,
		MetricsService:    mockMetricsService,
	}

	mockMetricsService.On("RegisterPoolMetrics", ErrorNonJitterChannelName, mock.AnythingOfType("*pond.WorkerPool")).Once()
	mockMetricsService.On("RecordTSSTransactionStatusTransition", mock.AnythingOfType("string"), mock.AnythingOfType("string")).Once()
	defer mockMetricsService.AssertExpectations(t)

	channel := NewErrorNonJitterChannel(cfg)

	payload := tss.Payload{}
	payload.WebhookURL = "www.stellar.com"
	payload.TransactionHash = "hash"
	payload.TransactionXDR = "xdr"

	rpcResp := tss.RPCSendTxResponse{
		Status: tss.RPCTXStatus{RPCStatus: entities.ErrorStatus},
		Code:   tss.RPCTXCode{TxResultCode: tss.JitterErrorCodes[0]},
	}
	payload.RPCSubmitTxResponse = rpcResp

	txManagerMock.
		On("BuildAndSubmitTransaction", context.Background(), ErrorNonJitterChannelName, payload).
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

func TestNonJitterReceive(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()
	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	mockMetricsService := metrics.NewMockMetricsService()
	txManagerMock := services.TransactionManagerMock{}
	routerMock := router.MockRouter{}
	cfg := ErrorNonJitterChannelConfigs{
		TxManager:         &txManagerMock,
		Router:            &routerMock,
		MaxBufferSize:     1,
		MaxWorkers:        1,
		MaxRetries:        3,
		WaitBtwnRetriesMS: 10,
		MetricsService:    mockMetricsService,
	}

	mockMetricsService.On("RegisterPoolMetrics", ErrorNonJitterChannelName, mock.AnythingOfType("*pond.WorkerPool")).Once()
	defer mockMetricsService.AssertExpectations(t)

	channel := NewErrorNonJitterChannel(cfg)
	payload := tss.Payload{}
	payload.WebhookURL = "www.stellar.com"
	payload.TransactionHash = "hash"
	payload.TransactionXDR = "xdr"

	t.Run("build_and_submit_tx_fail", func(t *testing.T) {
		txManagerMock.
			On("BuildAndSubmitTransaction", context.Background(), ErrorNonJitterChannelName, payload).
			Return(tss.RPCSendTxResponse{}, errors.New("build tx failed")).
			Once()

		channel.Receive(payload)

		routerMock.AssertNotCalled(t, "Route", payload)
	})

	t.Run("retries", func(t *testing.T) {
		mockMetricsService.On("RecordTSSTransactionStatusTransition", mock.AnythingOfType("string"), mock.AnythingOfType("string")).Once()
		defer mockMetricsService.AssertExpectations(t)

		sendResp1 := tss.RPCSendTxResponse{
			Status: tss.RPCTXStatus{RPCStatus: entities.ErrorStatus},
			Code:   tss.RPCTXCode{TxResultCode: tss.NonJitterErrorCodes[0]},
		}
		sendResp2 := tss.RPCSendTxResponse{
			Status: tss.RPCTXStatus{RPCStatus: entities.ErrorStatus},
			Code:   tss.RPCTXCode{TxResultCode: tss.JitterErrorCodes[0]},
		}

		txManagerMock.
			On("BuildAndSubmitTransaction", context.Background(), ErrorNonJitterChannelName, mock.AnythingOfType("tss.Payload")).
			Return(sendResp1, nil).
			Once()
		txManagerMock.
			On("BuildAndSubmitTransaction", context.Background(), ErrorNonJitterChannelName, mock.AnythingOfType("tss.Payload")).
			Return(sendResp2, nil).
			Once()
		routerMock.
			On("Route", mock.AnythingOfType("tss.Payload")).
			Return(nil).
			Once()

		channel.Receive(payload)
	})

	t.Run("max_retries", func(t *testing.T) {
		sendResp := tss.RPCSendTxResponse{
			Status: tss.RPCTXStatus{RPCStatus: entities.ErrorStatus},
			Code:   tss.RPCTXCode{TxResultCode: tss.NonJitterErrorCodes[0]},
		}

		txManagerMock.
			On("BuildAndSubmitTransaction", context.Background(), ErrorNonJitterChannelName, mock.AnythingOfType("tss.Payload")).
			Return(sendResp, nil).
			Times(3)
		routerMock.
			On("Route", mock.AnythingOfType("tss.Payload")).
			Return(nil).
			Once()

		channel.Receive(payload)
	})
}
