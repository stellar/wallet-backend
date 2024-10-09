package router

import (
	"testing"

	"github.com/stellar/wallet-backend/internal/entities"
	"github.com/stellar/wallet-backend/internal/tss"
	"github.com/stretchr/testify/assert"
)

func TestRouter(t *testing.T) {
	rpcCallerChannel := tss.MockChannel{}
	defer rpcCallerChannel.AssertExpectations(t)
	errorJitterChannel := tss.MockChannel{}
	defer errorJitterChannel.AssertExpectations(t)
	errorNonJitterChannel := tss.MockChannel{}
	defer errorNonJitterChannel.AssertExpectations(t)
	webhookChannel := tss.MockChannel{}
	defer webhookChannel.AssertExpectations(t)

	router := NewRouter(RouterConfigs{
		RPCCallerChannel:      &rpcCallerChannel,
		ErrorJitterChannel:    &errorJitterChannel,
		ErrorNonJitterChannel: &errorNonJitterChannel,
		WebhookChannel:        &webhookChannel,
	})
	t.Run("status_new_routes_to_rpc_caller_channel", func(t *testing.T) {
		payload := tss.Payload{}
		payload.RpcSubmitTxResponse.Status = tss.RPCTXStatus{OtherStatus: tss.NewStatus}

		rpcCallerChannel.
			On("Send", payload).
			Return().
			Once()

		_ = router.Route(payload)

		rpcCallerChannel.AssertCalled(t, "Send", payload)
	})
	t.Run("status_try_again_later_routes_to_error_jitter_channel", func(t *testing.T) {
		payload := tss.Payload{}
		payload.RpcSubmitTxResponse.Status = tss.RPCTXStatus{RPCStatus: entities.TryAgainLaterStatus}

		errorJitterChannel.
			On("Send", payload).
			Return().
			Once()

		_ = router.Route(payload)

		errorJitterChannel.AssertCalled(t, "Send", payload)
	})
	t.Run("status_error_routes_to_error_jitter_channel", func(t *testing.T) {
		for _, code := range tss.JitterErrorCodes {
			payload := tss.Payload{
				RpcSubmitTxResponse: tss.RPCSendTxResponse{
					Status: tss.RPCTXStatus{
						RPCStatus: entities.ErrorStatus,
					},
					Code: tss.RPCTXCode{
						TxResultCode: code,
					},
				},
			}
			payload.RpcSubmitTxResponse.Code.TxResultCode = code
			errorJitterChannel.
				On("Send", payload).
				Return().
				Once()

			_ = router.Route(payload)

			errorJitterChannel.AssertCalled(t, "Send", payload)
		}
	})
	t.Run("status_error_routes_to_error_non_jitter_channel", func(t *testing.T) {
		for _, code := range tss.NonJitterErrorCodes {
			payload := tss.Payload{
				RpcSubmitTxResponse: tss.RPCSendTxResponse{
					Status: tss.RPCTXStatus{
						RPCStatus: entities.ErrorStatus,
					},
					Code: tss.RPCTXCode{
						TxResultCode: code,
					},
				},
			}
			payload.RpcSubmitTxResponse.Code.TxResultCode = code
			errorNonJitterChannel.
				On("Send", payload).
				Return().
				Once()

			_ = router.Route(payload)

			errorNonJitterChannel.AssertCalled(t, "Send", payload)
		}
	})
	t.Run("status_error_routes_to_webhook_channel", func(t *testing.T) {
		for _, code := range tss.FinalErrorCodes {
			payload := tss.Payload{
				RpcSubmitTxResponse: tss.RPCSendTxResponse{
					Status: tss.RPCTXStatus{
						RPCStatus: entities.ErrorStatus,
					},
					Code: tss.RPCTXCode{
						TxResultCode: code,
					},
				},
			}
			payload.RpcSubmitTxResponse.Code.TxResultCode = code
			webhookChannel.
				On("Send", payload).
				Return().
				Once()

			_ = router.Route(payload)

			webhookChannel.AssertCalled(t, "Send", payload)
		}
	})
	t.Run("nil_channel_does_not_route", func(t *testing.T) {
		payload := tss.Payload{}

		err := router.Route(payload)

		errorJitterChannel.AssertNotCalled(t, "Send", payload)
		assert.Equal(t, "payload could not be routed - channel is nil", err.Error())
	})
}
