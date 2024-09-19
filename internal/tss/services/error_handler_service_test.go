package services

import (
	"testing"

	"github.com/stellar/go/xdr"
	"github.com/stellar/wallet-backend/internal/tss"
)

func TestProcessPayload(t *testing.T) {
	jitterChannel := tss.MockChannel{}
	defer jitterChannel.AssertExpectations(t)
	nonJitterChannel := tss.MockChannel{}
	defer nonJitterChannel.AssertExpectations(t)

	service := NewErrorHandlerService(ErrorHandlerServiceConfigs{JitterChannel: &jitterChannel, NonJitterChannel: &nonJitterChannel})

	t.Run("status_try_again_later", func(t *testing.T) {
		payload := tss.Payload{}
		payload.RpcSubmitTxResponse.Status = tss.TryAgainLaterStatus

		jitterChannel.
			On("Send", payload).
			Return().
			Once()

		service.ProcessPayload(payload)
	})
	t.Run("code_tx_too_early", func(t *testing.T) {
		payload := tss.Payload{}
		payload.RpcSubmitTxResponse.Status = tss.ErrorStatus
		payload.RpcSubmitTxResponse.Code.TxResultCode = xdr.TransactionResultCodeTxTooEarly

		nonJitterChannel.
			On("Send", payload).
			Return().
			Once()

		service.ProcessPayload(payload)
	})

	t.Run("code_tx_insufficient_fee", func(t *testing.T) {
		payload := tss.Payload{}
		payload.RpcSubmitTxResponse.Status = tss.ErrorStatus
		payload.RpcSubmitTxResponse.Code.TxResultCode = xdr.TransactionResultCodeTxInsufficientFee

		jitterChannel.
			On("Send", payload).
			Return().
			Once()

		service.ProcessPayload(payload)
	})
}
