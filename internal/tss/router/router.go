package router

import (
	"fmt"
	"slices"

	"github.com/stellar/go/xdr"
	"github.com/stellar/wallet-backend/internal/entities"
	"github.com/stellar/wallet-backend/internal/tss"
)

type Router interface {
	Route(payload tss.Payload) error
}

type RouterConfigs struct {
	RPCCallerChannel      tss.Channel
	ErrorJitterChannel    tss.Channel
	ErrorNonJitterChannel tss.Channel
	WebhookChannel        tss.Channel
}

type router struct {
	RPCCallerChannel      tss.Channel
	ErrorJitterChannel    tss.Channel
	ErrorNonJitterChannel tss.Channel
	WebhookChannel        tss.Channel
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

var NonJitterErrorCodes = []xdr.TransactionResultCode{
	xdr.TransactionResultCodeTxTooEarly,
	xdr.TransactionResultCodeTxTooLate,
	xdr.TransactionResultCodeTxBadSeq,
}

var JitterErrorCodes = []xdr.TransactionResultCode{
	xdr.TransactionResultCodeTxInsufficientFee,
	xdr.TransactionResultCodeTxInternalError,
}

func NewRouter(cfg RouterConfigs) Router {
	return &router{
		RPCCallerChannel:      cfg.RPCCallerChannel,
		ErrorJitterChannel:    cfg.ErrorJitterChannel,
		ErrorNonJitterChannel: cfg.ErrorNonJitterChannel,
		WebhookChannel:        cfg.WebhookChannel,
	}
}

func (r *router) Route(payload tss.Payload) error {
	var channel tss.Channel
	if payload.RpcSubmitTxResponse.Status.Status() != "" {
		switch payload.RpcSubmitTxResponse.Status.Status() {
		case string(tss.NewStatus):
			channel = r.RPCCallerChannel
		case string(entities.TryAgainLaterStatus):
			channel = r.ErrorJitterChannel
		case string(entities.ErrorStatus):
			if payload.RpcSubmitTxResponse.Code.OtherCodes == tss.NoCode {
				if slices.Contains(JitterErrorCodes, payload.RpcSubmitTxResponse.Code.TxResultCode) {
					channel = r.ErrorJitterChannel
				} else if slices.Contains(NonJitterErrorCodes, payload.RpcSubmitTxResponse.Code.TxResultCode) {
					channel = r.ErrorNonJitterChannel
				} else if slices.Contains(FinalErrorCodes, payload.RpcSubmitTxResponse.Code.TxResultCode) {
					channel = r.WebhookChannel
				}
			}
		default:
			// Do nothing for PENDING / DUPLICATE statuses
			return nil
		}
	}
	if channel == nil {
		return fmt.Errorf("payload could not be routed - channel is nil")
	}
	channel.Send(payload)
	return nil
}
