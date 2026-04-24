// Package resolvers provides GraphQL resolver implementations.
// This file contains the BalanceReader adapter for account balance queries.
package resolvers

import (
	"context"
	"fmt"

	"github.com/google/uuid"

	"github.com/stellar/wallet-backend/internal/data"
	sep41data "github.com/stellar/wallet-backend/internal/data/sep41"
)

// balanceReaderAdapter wraps balance model interfaces to implement BalanceReader.
// This adapter allows the GraphQL resolver to use a consistent interface for balance reads.
type balanceReaderAdapter struct {
	trustlineBalanceModel data.TrustlineBalanceModelInterface
	nativeBalanceModel    data.NativeBalanceModelInterface
	sacBalanceModel       data.SACBalanceModelInterface
	sep41BalanceModel     sep41data.BalanceModelInterface
	sep41AllowanceModel   sep41data.AllowanceModelInterface
}

// NewBalanceReader creates a BalanceReader from the underlying model interfaces.
func NewBalanceReader(
	trustlineBalanceModel data.TrustlineBalanceModelInterface,
	nativeBalanceModel data.NativeBalanceModelInterface,
	sacBalanceModel data.SACBalanceModelInterface,
	sep41BalanceModel sep41data.BalanceModelInterface,
	sep41AllowanceModel sep41data.AllowanceModelInterface,
) BalanceReader {
	return &balanceReaderAdapter{
		trustlineBalanceModel: trustlineBalanceModel,
		nativeBalanceModel:    nativeBalanceModel,
		sacBalanceModel:       sacBalanceModel,
		sep41BalanceModel:     sep41BalanceModel,
		sep41AllowanceModel:   sep41AllowanceModel,
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

// GetSEP41Balances retrieves SEP-41 (pure, non-SAC) balances for an account. Pass nil
// limit/cursor to fetch all rows, or provide them for keyset pagination.
func (r *balanceReaderAdapter) GetSEP41Balances(ctx context.Context, accountAddress string, limit *int32, cursor *uuid.UUID, sortOrder data.SortOrder) ([]sep41data.Balance, error) {
	if r.sep41BalanceModel == nil {
		return nil, nil
	}
	// Convert data.SortOrder to the package-local alias defined in sep41 to avoid an import cycle.
	sep41Sort := sep41data.SortASC
	if sortOrder == data.DESC {
		sep41Sort = sep41data.SortDESC
	}
	balances, err := r.sep41BalanceModel.GetByAccount(ctx, accountAddress, limit, cursor, sep41Sort)
	if err != nil {
		return nil, fmt.Errorf("getting SEP-41 balances: %w", err)
	}
	return balances, nil
}

// GetSEP41Allowances retrieves active SEP-41 allowances issued by the given owner.
func (r *balanceReaderAdapter) GetSEP41Allowances(ctx context.Context, ownerAddress string, currentLedger uint32) ([]sep41data.Allowance, error) {
	if r.sep41AllowanceModel == nil {
		return nil, nil
	}
	allowances, err := r.sep41AllowanceModel.GetByOwner(ctx, ownerAddress, currentLedger)
	if err != nil {
		return nil, fmt.Errorf("getting SEP-41 allowances: %w", err)
	}
	return allowances, nil
}
