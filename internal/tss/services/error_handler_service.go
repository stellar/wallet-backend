package services

import (
	"github.com/stellar/wallet-backend/internal/tss"
)

type errorHandlerService struct {
	channel tss.Channel
}

func NewErrorHandlerService(channel tss.Channel) Service {
	return &errorHandlerService{
		channel: channel,
	}
}

func (p *errorHandlerService) ProcessPayload(payload tss.Payload) {
	// fill in later
}
