package channels

import (
	"context"
	"errors"
	"testing"

	"github.com/stellar/wallet-backend/internal/db"
	"github.com/stellar/wallet-backend/internal/db/dbtest"
	"github.com/stellar/wallet-backend/internal/entities"
	"github.com/stellar/wallet-backend/internal/metrics"
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
	sqlxDB, err := dbConnectionPool.SqlxDB(context.Background())
	require.NoError(t, err)
	metricsService := metrics.NewMetricsService(sqlxDB)
	store, _ := store.NewStore(dbConnectionPool, metricsService)
	txManagerMock := services.TransactionManagerMock{}
	routerMock := router.MockRouter{}
	cfgs := RPCCallerChannelConfigs{
		Store:         store,
		TxManager:     &txManagerMock,
		Router:        &routerMock,
		MaxBufferSize: 10,
		MaxWorkers:    10,
		MetricsService: metricsService,
	}
	channel := NewRPCCallerChannel(cfgs)
	payload := tss.Payload{}
	payload.WebhookURL = "www.stellar.com"
	payload.TransactionHash = "hash"
	payload.TransactionXDR = "xdr"

	rpcResp := tss.RPCSendTxResponse{
		Status: tss.RPCTXStatus{RPCStatus: entities.TryAgainLaterStatus},
	}
	payload.RpcSubmitTxResponse = rpcResp

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
	sqlxDB, err := dbConnectionPool.SqlxDB(context.Background())
	require.NoError(t, err)
	metricsService := metrics.NewMetricsService(sqlxDB)
	store, _ := store.NewStore(dbConnectionPool, metricsService)
	txManagerMock := services.TransactionManagerMock{}
	routerMock := router.MockRouter{}
	cfgs := RPCCallerChannelConfigs{
		Store:         store,
		TxManager:     &txManagerMock,
		Router:        &routerMock,
		MaxBufferSize: 10,
		MaxWorkers:    10,
		MetricsService: metricsService,
	}
	channel := NewRPCCallerChannel(cfgs)
	payload := tss.Payload{}
	payload.WebhookURL = "www.stellar.com"
	payload.TransactionHash = "hash"
	payload.TransactionXDR = "xdr"

	t.Run("build_and_submit_tx_fail", func(t *testing.T) {
		txManagerMock.
			On("BuildAndSubmitTransaction", context.Background(), RPCCallerChannelName, payload).
			Return(tss.RPCSendTxResponse{}, errors.New("build tx failed")).
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
