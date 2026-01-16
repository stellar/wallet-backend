// Package resolvers provides GraphQL resolver implementations.
// This file contains the BalanceReader adapter for account balance queries.
package resolvers

import (
	"context"
	"fmt"

	"github.com/stellar/wallet-backend/internal/data"
)

// balanceReaderAdapter wraps TrustlineBalanceModelInterface and NativeBalanceModelInterface to implement BalanceReader.
// This adapter allows the GraphQL resolver to use a consistent interface for balance reads.
type balanceReaderAdapter struct {
	trustlineBalanceModel data.TrustlineBalanceModelInterface
	nativeBalanceModel    data.NativeBalanceModelInterface
}

// NewBalanceReader creates a BalanceReader from the underlying model interfaces.
func NewBalanceReader(trustlineBalanceModel data.TrustlineBalanceModelInterface, nativeBalanceModel data.NativeBalanceModelInterface) BalanceReader {
	return &balanceReaderAdapter{
		trustlineBalanceModel: trustlineBalanceModel,
		nativeBalanceModel:    nativeBalanceModel,
	}
}

// GetTrustlineBalances retrieves all trustline balances for an account.
func (r *balanceReaderAdapter) GetTrustlineBalances(ctx context.Context, accountAddress string) ([]data.TrustlineBalance, error) {
	balances, err := r.trustlineBalanceModel.GetByAccount(ctx, accountAddress)
	if err != nil {
		return nil, fmt.Errorf("getting trustline balances: %w", err)
	}
	return balances, nil
}

// GetNativeBalance retrieves the native XLM balance for an account.
func (r *balanceReaderAdapter) GetNativeBalance(ctx context.Context, accountAddress string) (*data.NativeBalance, error) {
	balance, err := r.nativeBalanceModel.GetByAccount(ctx, accountAddress)
	if err != nil {
		return nil, fmt.Errorf("getting native balance: %w", err)
	}
	return balance, nil
}
