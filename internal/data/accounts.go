// AccountModel provides data access methods for account-related queries
// including fee bump eligibility checks and batch lookups for dataloaders.
package data

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/stellar/wallet-backend/internal/db"
	"github.com/stellar/wallet-backend/internal/indexer/types"
	"github.com/stellar/wallet-backend/internal/metrics"
	"github.com/stellar/wallet-backend/internal/utils"
)

type AccountModel struct {
	DB             *pgxpool.Pool
	Metrics *metrics.DBMetrics
}

// IsAccountFeeBumpEligible checks whether an account is eligible to have its transaction fee-bumped. Channel Accounts should be
// eligible because some of the transactions will have the channel accounts as the source account (i. e. create account sponsorship).
func (m *AccountModel) IsAccountFeeBumpEligible(ctx context.Context, address string) (bool, error) {
	const query = `SELECT EXISTS(SELECT 1 FROM channel_accounts WHERE public_key = $1)`
	start := time.Now()
	exists, err := db.QueryOne[bool](ctx, m.DB, query, address)
	duration := time.Since(start).Seconds()
	m.Metrics.QueryDuration.WithLabelValues("IsAccountFeeBumpEligible", "channel_accounts").Observe(duration)
	if err != nil {
		m.Metrics.QueryErrors.WithLabelValues("IsAccountFeeBumpEligible", "channel_accounts", utils.GetDBErrorType(err)).Inc()
		return false, fmt.Errorf("checking if account %s is fee bump eligible: %w", address, err)
	}
	m.Metrics.QueriesTotal.WithLabelValues("IsAccountFeeBumpEligible", "channel_accounts").Inc()
	return exists, nil
}

// BatchGetByToIDs gets the accounts that are associated with the given transaction ToIDs.
func (m *AccountModel) BatchGetByToIDs(ctx context.Context, toIDs []int64, columns string) ([]*types.AccountWithToID, error) {
	query := `
		SELECT account_id AS stellar_address, tx_to_id
		FROM transactions_accounts
		WHERE tx_to_id = ANY($1)`
	start := time.Now()
	accounts, err := db.QueryManyPtrs[types.AccountWithToID](ctx, m.DB, query, toIDs)
	duration := time.Since(start).Seconds()
	m.Metrics.QueryDuration.WithLabelValues("BatchGetByToIDs", "transactions_accounts").Observe(duration)
	m.Metrics.BatchSize.WithLabelValues("BatchGetByToIDs", "transactions_accounts").Observe(float64(len(toIDs)))
	if err != nil {
		m.Metrics.QueryErrors.WithLabelValues("BatchGetByToIDs", "transactions_accounts", utils.GetDBErrorType(err)).Inc()
		return nil, fmt.Errorf("getting accounts by transaction ToIDs: %w", err)
	}
	m.Metrics.QueriesTotal.WithLabelValues("BatchGetByToIDs", "transactions_accounts").Inc()
	return accounts, nil
}

// BatchGetByOperationIDs gets the accounts that are associated with the given operation IDs.
func (m *AccountModel) BatchGetByOperationIDs(ctx context.Context, operationIDs []int64, columns string) ([]*types.AccountWithOperationID, error) {
	query := `
		SELECT account_id AS stellar_address, operation_id
		FROM operations_accounts
		WHERE operation_id = ANY($1)`
	start := time.Now()
	accounts, err := db.QueryManyPtrs[types.AccountWithOperationID](ctx, m.DB, query, operationIDs)
	duration := time.Since(start).Seconds()
	m.Metrics.QueryDuration.WithLabelValues("BatchGetByOperationIDs", "operations_accounts").Observe(duration)
	m.Metrics.BatchSize.WithLabelValues("BatchGetByOperationIDs", "operations_accounts").Observe(float64(len(operationIDs)))
	if err != nil {
		m.Metrics.QueryErrors.WithLabelValues("BatchGetByOperationIDs", "operations_accounts", utils.GetDBErrorType(err)).Inc()
		return nil, fmt.Errorf("getting accounts by operation IDs: %w", err)
	}
	m.Metrics.QueriesTotal.WithLabelValues("BatchGetByOperationIDs", "operations_accounts").Inc()
	return accounts, nil
}
