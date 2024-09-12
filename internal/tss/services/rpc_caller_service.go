package services

import (
	"github.com/stellar/wallet-backend/internal/tss"
)

type rpcCallerService struct {
	channel tss.Channel
}

func NewRPCCallerService(channel tss.Channel) Service {
	return &rpcCallerService{
		channel: channel,
	}
}

func (p *rpcCallerService) ProcessPayload(payload tss.Payload) {
	p.channel.Send(payload)
}
