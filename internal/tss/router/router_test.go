package router

import (
	"testing"

	"github.com/stellar/go/xdr"
	"github.com/stellar/wallet-backend/internal/tss"
	"github.com/stellar/wallet-backend/internal/tss/services"
)

func TestRouter(t *testing.T) {
	errorHandlerService := services.MockService{}
	defer errorHandlerService.AssertExpectations(t)
	webhookHandlerService := services.MockService{}
	router := NewRouter(RouterConfigs{ErrorHandlerService: &errorHandlerService, WebhookHandlerService: &webhookHandlerService})
	t.Run("status_try_again_later", func(t *testing.T) {
		payload := tss.Payload{}
		payload.RpcSubmitTxResponse.Status = tss.TryAgainLaterStatus

		errorHandlerService.
			On("ProcessPayload", payload).
			Return().
			Once()

		router.Route(payload)
	})
	t.Run("error_status_route_to_error_handler_service", func(t *testing.T) {
		payload := tss.Payload{}
		payload.RpcSubmitTxResponse.Status = tss.ErrorStatus
		payload.RpcSubmitTxResponse.Code.TxResultCode = xdr.TransactionResultCodeTxInsufficientFee

		errorHandlerService.
			On("ProcessPayload", payload).
			Return().
			Once()

		router.Route(payload)
	})
	t.Run("error_status_route_to_webhook_handler_service", func(t *testing.T) {
		payload := tss.Payload{}
		payload.RpcSubmitTxResponse.Status = tss.ErrorStatus
		payload.RpcSubmitTxResponse.Code.TxResultCode = xdr.TransactionResultCodeTxInsufficientBalance

		webhookHandlerService.
			On("ProcessPayload", payload).
			Return().
			Once()

		router.Route(payload)
	})
}
