package services

import (
	"github.com/stellar/wallet-backend/internal/tss"
)

// nolint:unused
type errorHandlerService struct {
	channel tss.Channel
}

func NewErrorHandlerService(channel tss.Channel) Service {
	return &rpcCallerService{
		channel: channel,
	}
}

// nolint:unused
func (p *errorHandlerService) ProcessPayload(payload tss.Payload) {
	// fill in later
}
