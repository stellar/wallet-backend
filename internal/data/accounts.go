// AccountModel provides data access methods for account-related queries
// including fee bump eligibility checks and batch lookups for dataloaders.
package data

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/lib/pq"

	"github.com/stellar/wallet-backend/internal/db"
	"github.com/stellar/wallet-backend/internal/indexer/types"
	"github.com/stellar/wallet-backend/internal/metrics"
	"github.com/stellar/wallet-backend/internal/utils"
)

type AccountModel struct {
	DB             db.ConnectionPool
	MetricsService metrics.MetricsService
}

// IsAccountFeeBumpEligible checks whether an account is eligible to have its transaction fee-bumped. Channel Accounts should be
// eligible because some of the transactions will have the channel accounts as the source account (i. e. create account sponsorship).
func (m *AccountModel) IsAccountFeeBumpEligible(ctx context.Context, address string) (bool, error) {
	const query = `SELECT EXISTS(SELECT 1 FROM channel_accounts WHERE public_key = $1)`
	var exists bool
	start := time.Now()
	err := m.DB.GetContext(ctx, &exists, query, address)
	duration := time.Since(start).Seconds()
	m.MetricsService.ObserveDBQueryDuration("IsAccountFeeBumpEligible", "accounts", duration)
	if err != nil {
		m.MetricsService.IncDBQueryError("IsAccountFeeBumpEligible", "accounts", utils.GetDBErrorType(err))
		return false, fmt.Errorf("checking if account %s is fee bump eligible: %w", address, err)
	}
	m.MetricsService.IncDBQuery("IsAccountFeeBumpEligible", "accounts")
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
	err := m.DB.SelectContext(ctx, &accounts, query, pq.Array(toIDs))
	duration := time.Since(start).Seconds()
	m.MetricsService.ObserveDBQueryDuration("BatchGetByToIDs", "accounts", duration)
	m.MetricsService.ObserveDBBatchSize("BatchGetByToIDs", "accounts", len(toIDs))
	if err != nil {
		m.MetricsService.IncDBQueryError("BatchGetByToIDs", "accounts", utils.GetDBErrorType(err))
		return nil, fmt.Errorf("getting accounts by transaction ToIDs: %w", err)
	}
	m.MetricsService.IncDBQuery("BatchGetByToIDs", "accounts")
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
	err := m.DB.SelectContext(ctx, &accounts, query, pq.Array(operationIDs))
	duration := time.Since(start).Seconds()
	m.MetricsService.ObserveDBQueryDuration("BatchGetByOperationIDs", "accounts", duration)
	m.MetricsService.ObserveDBBatchSize("BatchGetByOperationIDs", "accounts", len(operationIDs))
	if err != nil {
		m.MetricsService.IncDBQueryError("BatchGetByOperationIDs", "accounts", utils.GetDBErrorType(err))
		return nil, fmt.Errorf("getting accounts by operation IDs: %w", err)
	}
	m.MetricsService.IncDBQuery("BatchGetByOperationIDs", "accounts")
	return accounts, nil
}

// BatchGetByStateChangeIDs gets the accounts that are associated with the given state change IDs.
func (m *AccountModel) BatchGetByStateChangeIDs(ctx context.Context, scToIDs []int64, scOpIDs []int64, scOrders []int64, columns string) ([]*types.AccountWithStateChangeID, error) {
	// Build tuples for the IN clause. Since (to_id, operation_id, state_change_order) is the primary key of state_changes,
	// it will be faster to search on this tuple.
	tuples := make([]string, len(scOrders))
	for i := range scOrders {
		tuples[i] = fmt.Sprintf("(%d, %d, %d)", scToIDs[i], scOpIDs[i], scOrders[i])
	}

	query := fmt.Sprintf(`
		SELECT account_id AS stellar_address, CONCAT(to_id, '-', operation_id, '-', state_change_order) AS state_change_id
		FROM state_changes
		WHERE (to_id, operation_id, state_change_order) IN (%s)
		ORDER BY ledger_created_at DESC
	`, strings.Join(tuples, ", "))

	var accountsWithStateChanges []*types.AccountWithStateChangeID
	start := time.Now()
	err := m.DB.SelectContext(ctx, &accountsWithStateChanges, query)
	duration := time.Since(start).Seconds()
	m.MetricsService.ObserveDBQueryDuration("BatchGetByStateChangeIDs", "accounts", duration)
	m.MetricsService.ObserveDBBatchSize("BatchGetByStateChangeIDs", "accounts", len(scOrders))
	if err != nil {
		m.MetricsService.IncDBQueryError("BatchGetByStateChangeIDs", "accounts", utils.GetDBErrorType(err))
		return nil, fmt.Errorf("getting accounts by state change IDs: %w", err)
	}
	m.MetricsService.IncDBQuery("BatchGetByStateChangeIDs", "accounts")
	return accountsWithStateChanges, nil
}
