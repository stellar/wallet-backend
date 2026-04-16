// Package resolvers provides GraphQL resolver implementations.
// This file contains the BalanceReader adapter for account balance queries.
package resolvers

import (
	"context"
	"fmt"

	"github.com/google/uuid"

	"github.com/stellar/wallet-backend/internal/data"
)

// balanceReaderAdapter wraps TrustlineBalanceModelInterface, NativeBalanceModelInterface, and SACBalanceModelInterface to implement BalanceReader.
// This adapter allows the GraphQL resolver to use a consistent interface for balance reads.
type balanceReaderAdapter struct {
	trustlineBalanceModel data.TrustlineBalanceModelInterface
	nativeBalanceModel    data.NativeBalanceModelInterface
	sacBalanceModel       data.SACBalanceModelInterface
}

// NewBalanceReader creates a BalanceReader from the underlying model interfaces.
func NewBalanceReader(trustlineBalanceModel data.TrustlineBalanceModelInterface, nativeBalanceModel data.NativeBalanceModelInterface, sacBalanceModel data.SACBalanceModelInterface) BalanceReader {
	return &balanceReaderAdapter{
		trustlineBalanceModel: trustlineBalanceModel,
		nativeBalanceModel:    nativeBalanceModel,
		sacBalanceModel:       sacBalanceModel,
	}
}

// GetTrustlineBalances retrieves trustline balances for an account. Pass nil
// limit/cursor to fetch all rows, or provide them for keyset pagination.
func (r *balanceReaderAdapter) GetTrustlineBalances(ctx context.Context, accountAddress string, limit *int32, cursor *uuid.UUID, sortOrder data.SortOrder) ([]data.TrustlineBalance, error) {
	balances, err := r.trustlineBalanceModel.GetByAccount(ctx, accountAddress, limit, cursor, sortOrder)
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

// GetSACBalances retrieves SAC balances for a contract address. Pass nil
// limit/cursor to fetch all rows, or provide them for keyset pagination.
func (r *balanceReaderAdapter) GetSACBalances(ctx context.Context, accountAddress string, limit *int32, cursor *uuid.UUID, sortOrder data.SortOrder) ([]data.SACBalance, error) {
	balances, err := r.sacBalanceModel.GetByAccount(ctx, accountAddress, limit, cursor, sortOrder)
	if err != nil {
		return nil, fmt.Errorf("getting SAC balances: %w", err)
	}
	return balances, nil
}
