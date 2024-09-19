package services

import (
	"github.com/stellar/wallet-backend/internal/tss"
)

type webhookHandlerService struct {
	channel tss.Channel
}

func NewWebhookHandlerService(channel tss.Channel) Service {
	return &webhookHandlerService{
		channel: channel,
	}
}

func (p *webhookHandlerService) ProcessPayload(payload tss.Payload) {
	// fill in later
}
