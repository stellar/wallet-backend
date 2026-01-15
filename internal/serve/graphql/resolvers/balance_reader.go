// Package resolvers provides GraphQL resolver implementations.
// This file contains the BalanceReader adapter for account balance queries.
package resolvers

import (
	"context"

	"github.com/stellar/wallet-backend/internal/data"
)

// balanceReaderAdapter wraps TrustlineBalanceModelInterface to implement BalanceReader.
// This adapter allows the GraphQL resolver to use a consistent interface for balance reads.
type balanceReaderAdapter struct {
	trustlineBalanceModel data.TrustlineBalanceModelInterface
}

// NewBalanceReader creates a BalanceReader from the underlying model interfaces.
func NewBalanceReader(trustlineBalanceModel data.TrustlineBalanceModelInterface) BalanceReader {
	return &balanceReaderAdapter{
		trustlineBalanceModel: trustlineBalanceModel,
	}
}

// GetTrustlineBalances retrieves all trustline balances for an account.
func (r *balanceReaderAdapter) GetTrustlineBalances(ctx context.Context, accountAddress string) ([]data.TrustlineBalance, error) {
	return r.trustlineBalanceModel.GetByAccount(ctx, accountAddress)
}
