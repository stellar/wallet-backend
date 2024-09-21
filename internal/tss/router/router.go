package router

import (
	"slices"

	"github.com/stellar/go/xdr"
	"github.com/stellar/wallet-backend/internal/tss"
	"github.com/stellar/wallet-backend/internal/tss/services"
)

type Router interface {
	Route(payload tss.Payload)
}

type RouterConfigs struct {
	ErrorHandlerService   services.Service
	WebhookHandlerService services.Service
}

type router struct {
	ErrorHandlerService   services.Service
	WebhookHandlerService services.Service
}

var _ Router = (*router)(nil)

var FinalErrorCodes = []xdr.TransactionResultCode{
	xdr.TransactionResultCodeTxSuccess,
	xdr.TransactionResultCodeTxFailed,
	xdr.TransactionResultCodeTxMissingOperation,
	xdr.TransactionResultCodeTxInsufficientBalance,
	xdr.TransactionResultCodeTxBadAuthExtra,
	xdr.TransactionResultCodeTxMalformed,
}

var RetryErrorCodes = []xdr.TransactionResultCode{
	xdr.TransactionResultCodeTxTooLate,
	xdr.TransactionResultCodeTxInsufficientFee,
	xdr.TransactionResultCodeTxInternalError,
	xdr.TransactionResultCodeTxBadSeq,
}

func NewRouter(cfg RouterConfigs) Router {
	return &router{
		ErrorHandlerService:   cfg.ErrorHandlerService,
		WebhookHandlerService: cfg.WebhookHandlerService,
	}
}

func (r *router) Route(payload tss.Payload) {
	switch payload.RpcSubmitTxResponse.Status {
	case tss.TryAgainLaterStatus:
		r.ErrorHandlerService.ProcessPayload(payload)
	case tss.ErrorStatus:
		if payload.RpcSubmitTxResponse.Code.OtherCodes == tss.NoCode {
			if slices.Contains(RetryErrorCodes, payload.RpcSubmitTxResponse.Code.TxResultCode) {
				r.ErrorHandlerService.ProcessPayload(payload)
			} else if slices.Contains(FinalErrorCodes, payload.RpcSubmitTxResponse.Code.TxResultCode) {
				r.WebhookHandlerService.ProcessPayload(payload)
			}
		}
		// if Code.OtherCodes = {RPCFailCode, UnMarshall, do nothing, as this should be rare. Let the ticker task take care of this}
	default:
		// PENDING = wait to ingest this transaction via getTransactions()
		return
	}
}
