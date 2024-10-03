package router

import (
	"fmt"
	"slices"

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
				if slices.Contains(tss.JitterErrorCodes, payload.RpcSubmitTxResponse.Code.TxResultCode) {
					channel = r.ErrorJitterChannel
				} else if slices.Contains(tss.NonJitterErrorCodes, payload.RpcSubmitTxResponse.Code.TxResultCode) {
					channel = r.ErrorNonJitterChannel
				} else if slices.Contains(tss.FinalErrorCodes, payload.RpcSubmitTxResponse.Code.TxResultCode) {
					channel = r.WebhookChannel
				}
			}
		case string(entities.SuccessStatus):
			channel = r.WebhookChannel
		case string(entities.FailedStatus):
			channel = r.WebhookChannel
		default:
			// Do nothing for PENDING / DUPLICATE statuses
			return nil
		}
	} else if payload.RpcGetIngestTxResponse.Status != "" {
		channel = r.WebhookChannel
	}
	if channel == nil {
		return fmt.Errorf("payload could not be routed - channel is nil")
	}
	channel.Send(payload)
	return nil
}
