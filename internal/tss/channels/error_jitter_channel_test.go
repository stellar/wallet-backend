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
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestJitterSend(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()
	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN, nil)
	require.NoError(t, err)
	defer dbConnectionPool.Close()
	txManagerMock := services.TransactionManagerMock{}
	routerMock := router.MockRouter{}
	cfg := ErrorJitterChannelConfigs{
		TxManager:            &txManagerMock,
		Router:               &routerMock,
		MaxBufferSize:        1,
		MaxWorkers:           1,
		MaxRetries:           3,
		MinWaitBtwnRetriesMS: 10,
	}

	channel := NewErrorJitterChannel(cfg)

	payload := tss.Payload{}
	payload.WebhookURL = "www.stellar.com"
	payload.TransactionHash = "hash"
	payload.TransactionXDR = "xdr"

	rpcResp := tss.RPCSendTxResponse{
		Status: tss.RPCTXStatus{RPCStatus: entities.ErrorStatus},
		Code:   tss.RPCTXCode{TxResultCode: tss.NonJitterErrorCodes[0]},
	}
	payload.RpcSubmitTxResponse = rpcResp

	txManagerMock.
		On("BuildAndSubmitTransaction", context.Background(), ErrorJitterChannelName, payload).
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

func TestJitterReceive(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()
	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN, nil)
	require.NoError(t, err)
	defer dbConnectionPool.Close()
	txManagerMock := services.TransactionManagerMock{}
	routerMock := router.MockRouter{}
	cfg := ErrorJitterChannelConfigs{
		TxManager:            &txManagerMock,
		Router:               &routerMock,
		MaxBufferSize:        1,
		MaxWorkers:           1,
		MaxRetries:           3,
		MinWaitBtwnRetriesMS: 10,
	}

	channel := NewErrorJitterChannel(cfg)

	payload := tss.Payload{}
	payload.WebhookURL = "www.stellar.com"
	payload.TransactionHash = "hash"
	payload.TransactionXDR = "xdr"

	t.Run("build_and_submit_tx_fail", func(t *testing.T) {
		txManagerMock.
			On("BuildAndSubmitTransaction", context.Background(), ErrorJitterChannelName, payload).
			Return(tss.RPCSendTxResponse{}, errors.New("build tx failed")).
			Once()

		channel.Receive(payload)

		routerMock.AssertNotCalled(t, "Route", payload)
	})
	t.Run("retries", func(t *testing.T) {
		sendResp1 := tss.RPCSendTxResponse{
			Status: tss.RPCTXStatus{RPCStatus: entities.ErrorStatus},
			Code:   tss.RPCTXCode{TxResultCode: tss.JitterErrorCodes[0]},
		}
		sendResp2 := tss.RPCSendTxResponse{
			Status: tss.RPCTXStatus{RPCStatus: entities.ErrorStatus},
			Code:   tss.RPCTXCode{TxResultCode: tss.NonJitterErrorCodes[0]},
		}

		txManagerMock.
			On("BuildAndSubmitTransaction", context.Background(), ErrorJitterChannelName, mock.AnythingOfType("tss.Payload")).
			Return(sendResp1, nil).
			Once()
		txManagerMock.
			On("BuildAndSubmitTransaction", context.Background(), ErrorJitterChannelName, mock.AnythingOfType("tss.Payload")).
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
			Code:   tss.RPCTXCode{TxResultCode: tss.JitterErrorCodes[0]},
		}

		txManagerMock.
			On("BuildAndSubmitTransaction", context.Background(), ErrorJitterChannelName, mock.AnythingOfType("tss.Payload")).
			Return(sendResp, nil).
			Times(3)
		routerMock.
			On("Route", mock.AnythingOfType("tss.Payload")).
			Return(nil).
			Once()

		channel.Receive(payload)
	})
}
