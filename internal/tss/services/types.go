package services

import "github.com/stellar/wallet-backend/internal/tss"

type Service interface {
	ProcessPayload(payload tss.Payload)
}
