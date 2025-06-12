package processors

import (
	"context"

	"github.com/stellar/go/ingest"
	cache "github.com/stellar/wallet-backend/internal/store"
)

type TokenTransferProcessor struct {
	contractStore cache.ContractStore
}

func NewTokenTransferProcessor(contractStore cache.ContractStore) *TokenTransferProcessor {
	return &TokenTransferProcessor{contractStore: contractStore}
}

func (p *TokenTransferProcessor) Process(ctx context.Context, transaction ingest.LedgerTransaction) error {
	return nil
}
