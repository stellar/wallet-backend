package data

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/lib/pq"

	"github.com/stellar/wallet-backend/internal/db"
	"github.com/stellar/wallet-backend/internal/indexer/types"
	"github.com/stellar/wallet-backend/internal/metrics"
	"github.com/stellar/wallet-backend/internal/utils"
)

var (
	ErrAccountAlreadyExists = errors.New("account already exists")
	ErrAccountNotFound      = errors.New("account not found")
)

type AccountModel struct {
	DB             db.ConnectionPool
	MetricsService metrics.MetricsService
}

// isDuplicateError checks if the error is a PostgreSQL unique violation
func isDuplicateError(err error) bool {
	var pqErr *pq.Error
	return err != nil && errors.As(err, &pqErr) && pqErr.Code == "23505"
}

func (m *AccountModel) Get(ctx context.Context, address string) (*types.Account, error) {
	const query = `SELECT * FROM accounts WHERE stellar_address = $1`
	var account types.Account
	start := time.Now()
	err := m.DB.GetContext(ctx, &account, query, address)
	duration := time.Since(start).Seconds()
	m.MetricsService.ObserveDBQueryDuration("Get", "accounts", duration)
	if err != nil {
		m.MetricsService.IncDBQueryError("Get", "accounts", utils.GetDBErrorType(err))
		return nil, fmt.Errorf("getting account %s: %w", address, err)
	}
	m.MetricsService.IncDBQuery("Get", "accounts")
	return &account, nil
}

func (m *AccountModel) GetAll(ctx context.Context) ([]string, error) {
	const query = `SELECT stellar_address FROM accounts`
	start := time.Now()
	accounts := []string{}
	err := m.DB.SelectContext(ctx, &accounts, query)
	duration := time.Since(start).Seconds()
	m.MetricsService.ObserveDBQueryDuration("GetAll", "accounts", duration)
	if err != nil {
		m.MetricsService.IncDBQueryError("GetAll", "accounts", utils.GetDBErrorType(err))
		return nil, fmt.Errorf("getting all accounts: %w", err)
	}
	m.MetricsService.IncDBQuery("GetAll", "accounts")
	return accounts, nil
}

func (m *AccountModel) Insert(ctx context.Context, address string) error {
	const query = `INSERT INTO accounts (stellar_address) VALUES ($1)`
	start := time.Now()
	_, err := m.DB.ExecContext(ctx, query, address)
	duration := time.Since(start).Seconds()
	m.MetricsService.ObserveDBQueryDuration("Insert", "accounts", duration)
	if err != nil {
		m.MetricsService.IncDBQueryError("Insert", "accounts", utils.GetDBErrorType(err))
		if isDuplicateError(err) {
			return ErrAccountAlreadyExists
		}
		return fmt.Errorf("inserting address %s: %w", address, err)
	}
	m.MetricsService.IncDBQuery("Insert", "accounts")
	return nil
}

// BatchInsert inserts multiple accounts into the database. Uses ON CONFLICT DO NOTHING
// to handle pre-existing accounts idempotently.
func (m *AccountModel) BatchInsert(ctx context.Context, sqlExecuter db.SQLExecuter, addresses []string) error {
	if len(addresses) == 0 {
		return nil
	}
	if sqlExecuter == nil {
		sqlExecuter = m.DB
	}

	// Sort addresses to ensure consistent lock acquisition order across parallel workers.
	// This prevents deadlocks when multiple batches try to insert overlapping account sets.
	sort.Strings(addresses)

	const query = `
		INSERT INTO accounts (stellar_address)
		SELECT UNNEST($1::text[])
		ON CONFLICT (stellar_address) DO NOTHING
	`

	start := time.Now()
	_, err := sqlExecuter.ExecContext(ctx, query, pq.Array(addresses))
	duration := time.Since(start).Seconds()
	m.MetricsService.ObserveDBQueryDuration("BatchInsert", "accounts", duration)
	m.MetricsService.ObserveDBBatchSize("BatchInsert", "accounts", len(addresses))
	if err != nil {
		m.MetricsService.IncDBQueryError("BatchInsert", "accounts", utils.GetDBErrorType(err))
		return fmt.Errorf("batch inserting accounts: %w", err)
	}
	m.MetricsService.IncDBQuery("BatchInsert", "accounts")
	return nil
}

func (m *AccountModel) Delete(ctx context.Context, address string) error {
	const query = `DELETE FROM accounts WHERE stellar_address = $1`
	start := time.Now()
	result, err := m.DB.ExecContext(ctx, query, address)
	duration := time.Since(start).Seconds()
	m.MetricsService.ObserveDBQueryDuration("Delete", "accounts", duration)
	if err != nil {
		m.MetricsService.IncDBQueryError("Delete", "accounts", utils.GetDBErrorType(err))
		return fmt.Errorf("deleting address %s: %w", address, err)
	}

	// Check if any rows were affected to determine if account existed
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		m.MetricsService.IncDBQueryError("Delete", "accounts", utils.GetDBErrorType(err))
		return fmt.Errorf("checking rows affected for address %s: %w", address, err)
	}
	if rowsAffected == 0 {
		return ErrAccountNotFound
	}

	m.MetricsService.IncDBQuery("Delete", "accounts")
	return nil
}

// BatchGetByIDs returns the subset of provided account IDs that exist in the accounts table.
func (m *AccountModel) BatchGetByIDs(ctx context.Context, accountIDs []string) ([]string, error) {
	if len(accountIDs) == 0 {
		return []string{}, nil
	}

	const query = `SELECT stellar_address FROM accounts WHERE stellar_address = ANY($1)`
	start := time.Now()
	existingAccounts := []string{}
	err := m.DB.SelectContext(ctx, &existingAccounts, query, pq.Array(accountIDs))
	duration := time.Since(start).Seconds()
	m.MetricsService.ObserveDBQueryDuration("BatchGetByIDs", "accounts", duration)
	m.MetricsService.ObserveDBBatchSize("BatchGetByIDs", "accounts", len(accountIDs))
	if err != nil {
		m.MetricsService.IncDBQueryError("BatchGetByIDs", "accounts", utils.GetDBErrorType(err))
		return nil, fmt.Errorf("batch getting accounts by IDs: %w", err)
	}
	m.MetricsService.IncDBQuery("BatchGetByIDs", "accounts")
	return existingAccounts, nil
}

// IsAccountFeeBumpEligible checks whether an account is eligible to have its transaction fee-bumped. Channel Accounts should be
// eligible because some of the transactions will have the channel accounts as the source account (i. e. create account sponsorship).
func (m *AccountModel) IsAccountFeeBumpEligible(ctx context.Context, address string) (bool, error) {
	const query = `
		SELECT
			EXISTS(
				SELECT stellar_address FROM accounts WHERE stellar_address = $1
				UNION
				SELECT public_key FROM channel_accounts WHERE public_key = $1
			)
	`
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

// BatchGetByTxHashes gets the accounts that are associated with the given transaction hashes.
func (m *AccountModel) BatchGetByTxHashes(ctx context.Context, txHashes []string, columns string) ([]*types.AccountWithTxHash, error) {
	columns = prepareColumnsWithID(columns, types.Account{}, "accounts", "stellar_address")
	query := fmt.Sprintf(`
		SELECT %s, transactions_accounts.tx_hash
		FROM transactions_accounts
		INNER JOIN accounts
		ON transactions_accounts.account_id = accounts.stellar_address
		WHERE transactions_accounts.tx_hash = ANY($1)`, columns)
	var accounts []*types.AccountWithTxHash
	start := time.Now()
	err := m.DB.SelectContext(ctx, &accounts, query, pq.Array(txHashes))
	duration := time.Since(start).Seconds()
	m.MetricsService.ObserveDBQueryDuration("BatchGetByTxHashes", "accounts", duration)
	m.MetricsService.ObserveDBBatchSize("BatchGetByTxHashes", "accounts", len(txHashes))
	if err != nil {
		m.MetricsService.IncDBQueryError("BatchGetByTxHashes", "accounts", utils.GetDBErrorType(err))
		return nil, fmt.Errorf("getting accounts by transaction hashes: %w", err)
	}
	m.MetricsService.IncDBQuery("BatchGetByTxHashes", "accounts")
	return accounts, nil
}

// BatchGetByOperationIDs gets the accounts that are associated with the given operation IDs.
func (m *AccountModel) BatchGetByOperationIDs(ctx context.Context, operationIDs []int64, columns string) ([]*types.AccountWithOperationID, error) {
	columns = prepareColumnsWithID(columns, types.Account{}, "accounts", "stellar_address")
	query := fmt.Sprintf(`
		SELECT %s, operations_accounts.operation_id
		FROM operations_accounts
		INNER JOIN accounts
		ON operations_accounts.account_id = accounts.stellar_address
		WHERE operations_accounts.operation_id = ANY($1)`, columns)
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
func (m *AccountModel) BatchGetByStateChangeIDs(ctx context.Context, scToIDs []int64, scOrders []int64, columns string) ([]*types.AccountWithStateChangeID, error) {
	columns = prepareColumnsWithID(columns, types.Account{}, "accounts", "stellar_address")

	// Build tuples for the IN clause. Since (to_id, state_change_order) is the primary key of state_changes,
	// it will be faster to search on this tuple.
	tuples := make([]string, len(scOrders))
	for i := range scOrders {
		tuples[i] = fmt.Sprintf("(%d, %d)", scToIDs[i], scOrders[i])
	}

	query := fmt.Sprintf(`
		SELECT %s, CONCAT(state_changes.to_id, '-', state_changes.state_change_order) AS state_change_id
		FROM accounts
		INNER JOIN state_changes ON accounts.stellar_address = state_changes.account_id
		WHERE (state_changes.to_id, state_changes.state_change_order) IN (%s)
		ORDER BY accounts.created_at DESC
	`, columns, strings.Join(tuples, ", "))

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
