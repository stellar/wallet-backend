package services

import (
	"fmt"
	"slices"

	"github.com/stellar/wallet-backend/internal/tss"
)

type errorHandlerService struct {
	JitterChannel    tss.Channel
	NonJitterChannel tss.Channel
}

type ErrorHandlerServiceConfigs struct {
	JitterChannel    tss.Channel
	NonJitterChannel tss.Channel
}

func NewErrorHandlerService(cfg ErrorHandlerServiceConfigs) *errorHandlerService {
	return &errorHandlerService{
		JitterChannel:    cfg.JitterChannel,
		NonJitterChannel: cfg.NonJitterChannel,
	}
}

func (p *errorHandlerService) ProcessPayload(payload tss.Payload) {
	if payload.RpcSubmitTxResponse.Status == tss.TryAgainLaterStatus {
		fmt.Println("TRY AGAIN LATER")
		fmt.Println(payload)
		p.JitterChannel.Send(payload)
	} else {
		if slices.Contains(tss.NonJitterErrorCodes, payload.RpcSubmitTxResponse.Code.TxResultCode) {
			p.NonJitterChannel.Send(payload)
		} else if slices.Contains(tss.JitterErrorCodes, payload.RpcSubmitTxResponse.Code.TxResultCode) {
			p.JitterChannel.Send(payload)
		}
	}
}
