// AccountModel provides data access methods for account-related queries
// including fee bump eligibility checks and batch lookups for dataloaders.
package data

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/stellar/wallet-backend/internal/db"
	"github.com/stellar/wallet-backend/internal/indexer/types"
	"github.com/stellar/wallet-backend/internal/metrics"
	"github.com/stellar/wallet-backend/internal/utils"
)

type AccountModel struct {
	DB             *pgxpool.Pool
	MetricsService metrics.MetricsService
}

// IsAccountFeeBumpEligible checks whether an account is eligible to have its transaction fee-bumped. Channel Accounts should be
// eligible because some of the transactions will have the channel accounts as the source account (i. e. create account sponsorship).
func (m *AccountModel) IsAccountFeeBumpEligible(ctx context.Context, address string) (bool, error) {
	const query = `SELECT EXISTS(SELECT 1 FROM channel_accounts WHERE public_key = $1)`
	start := time.Now()
	exists, err := db.QueryOne[bool](ctx, m.DB, query, address)
	duration := time.Since(start).Seconds()
	m.MetricsService.ObserveDBQueryDuration("IsAccountFeeBumpEligible", "channel_accounts", duration)
	if err != nil {
		m.MetricsService.IncDBQueryError("IsAccountFeeBumpEligible", "channel_accounts", utils.GetDBErrorType(err))
		return false, fmt.Errorf("checking if account %s is fee bump eligible: %w", address, err)
	}
	m.MetricsService.IncDBQuery("IsAccountFeeBumpEligible", "channel_accounts")
	return exists, nil
}

// BatchGetByToIDs gets the accounts that are associated with the given transaction ToIDs.
func (m *AccountModel) BatchGetByToIDs(ctx context.Context, toIDs []int64, columns string) ([]*types.AccountWithToID, error) {
	query := `
		SELECT account_id AS stellar_address, tx_to_id
		FROM transactions_accounts
		WHERE tx_to_id = ANY($1)`
	var accounts []*types.AccountWithToID
	start := time.Now()
	rows, err := m.DB.Query(ctx, query, toIDs)
	if err != nil {
		m.MetricsService.IncDBQueryError("BatchGetByToIDs", "transactions_accounts", utils.GetDBErrorType(err))
		return nil, fmt.Errorf("getting accounts by transaction ToIDs: %w", err)
	}
	accounts, err := pgx.CollectRows(rows, pgx.RowToStructByNameLax[types.AccountWithToID])
	duration := time.Since(start).Seconds()
	m.MetricsService.ObserveDBQueryDuration("BatchGetByToIDs", "transactions_accounts", duration)
	m.MetricsService.ObserveDBBatchSize("BatchGetByToIDs", "transactions_accounts", len(toIDs))
	if err != nil {
		m.MetricsService.IncDBQueryError("BatchGetByToIDs", "transactions_accounts", utils.GetDBErrorType(err))
		return nil, fmt.Errorf("getting accounts by transaction ToIDs: %w", err)
	}
	m.MetricsService.IncDBQuery("BatchGetByToIDs", "transactions_accounts")
	return accounts, nil
}

// BatchGetByOperationIDs gets the accounts that are associated with the given operation IDs.
func (m *AccountModel) BatchGetByOperationIDs(ctx context.Context, operationIDs []int64, columns string) ([]*types.AccountWithOperationID, error) {
	query := `
		SELECT account_id AS stellar_address, operation_id
		FROM operations_accounts
		WHERE operation_id = ANY($1)`
	var accounts []*types.AccountWithOperationID
	start := time.Now()
	rows, err := m.DB.Query(ctx, query, operationIDs)
	if err != nil {
		m.MetricsService.IncDBQueryError("BatchGetByOperationIDs", "operations_accounts", utils.GetDBErrorType(err))
		return nil, fmt.Errorf("getting accounts by operation IDs: %w", err)
	}
	accounts, err := pgx.CollectRows(rows, pgx.RowToStructByNameLax[types.AccountWithOperationID])
	duration := time.Since(start).Seconds()
	m.MetricsService.ObserveDBQueryDuration("BatchGetByOperationIDs", "operations_accounts", duration)
	m.MetricsService.ObserveDBBatchSize("BatchGetByOperationIDs", "operations_accounts", len(operationIDs))
	if err != nil {
		m.MetricsService.IncDBQueryError("BatchGetByOperationIDs", "operations_accounts", utils.GetDBErrorType(err))
		return nil, fmt.Errorf("getting accounts by operation IDs: %w", err)
	}
	m.MetricsService.IncDBQuery("BatchGetByOperationIDs", "operations_accounts")
	return accounts, nil
}
