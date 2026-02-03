package data

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
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
	err := m.DB.GetContext(ctx, &account, query, types.StellarAddress(address))
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
	var addresses []types.StellarAddress
	err := m.DB.SelectContext(ctx, &addresses, query)
	duration := time.Since(start).Seconds()
	m.MetricsService.ObserveDBQueryDuration("GetAll", "accounts", duration)
	if err != nil {
		m.MetricsService.IncDBQueryError("GetAll", "accounts", utils.GetDBErrorType(err))
		return nil, fmt.Errorf("getting all accounts: %w", err)
	}
	m.MetricsService.IncDBQuery("GetAll", "accounts")
	// Convert []StellarAddress to []string
	result := make([]string, len(addresses))
	for i, addr := range addresses {
		result[i] = string(addr)
	}
	return result, nil
}

func (m *AccountModel) Insert(ctx context.Context, address string) error {
	const query = `INSERT INTO accounts (stellar_address) VALUES ($1)`
	start := time.Now()
	_, err := m.DB.ExecContext(ctx, query, types.StellarAddress(address))
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

func (m *AccountModel) Delete(ctx context.Context, address string) error {
	const query = `DELETE FROM accounts WHERE stellar_address = $1`
	start := time.Now()
	result, err := m.DB.ExecContext(ctx, query, types.StellarAddress(address))
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
func (m *AccountModel) BatchGetByIDs(ctx context.Context, dbTx pgx.Tx, accountIDs []string) ([]string, error) {
	if len(accountIDs) == 0 {
		return []string{}, nil
	}

	// Convert string addresses to [][]byte for BYTEA array comparison
	byteAddresses := make([][]byte, len(accountIDs))
	for i, addr := range accountIDs {
		addrBytes, err := types.StellarAddress(addr).Value()
		if err != nil {
			return nil, fmt.Errorf("converting address %s to bytes: %w", addr, err)
		}
		byteAddresses[i] = addrBytes.([]byte)
	}

	const query = `SELECT stellar_address FROM accounts WHERE stellar_address = ANY($1)`
	start := time.Now()
	rows, err := dbTx.Query(ctx, query, byteAddresses)
	if err != nil {
		m.MetricsService.IncDBQueryError("BatchGetByIDs", "accounts", utils.GetDBErrorType(err))
		return nil, fmt.Errorf("querying accounts by IDs: %w", err)
	}
	// Scan as []byte and convert back to string addresses
	byteResults, err := pgx.CollectRows(rows, pgx.RowTo[[]byte])
	if err != nil {
		m.MetricsService.IncDBQueryError("BatchGetByIDs", "accounts", utils.GetDBErrorType(err))
		return nil, fmt.Errorf("collecting rows: %w", err)
	}
	existingAccounts := make([]string, len(byteResults))
	for i, b := range byteResults {
		var addr types.StellarAddress
		if err := addr.Scan(b); err != nil {
			return nil, fmt.Errorf("scanning address: %w", err)
		}
		existingAccounts[i] = string(addr)
	}
	duration := time.Since(start).Seconds()
	m.MetricsService.ObserveDBQueryDuration("BatchGetByIDs", "accounts", duration)
	m.MetricsService.ObserveDBBatchSize("BatchGetByIDs", "accounts", len(accountIDs))
	m.MetricsService.IncDBQuery("BatchGetByIDs", "accounts")
	return existingAccounts, nil
}

// IsAccountFeeBumpEligible checks whether an account is eligible to have its transaction fee-bumped. Channel Accounts should be
// eligible because some of the transactions will have the channel accounts as the source account (i. e. create account sponsorship).
func (m *AccountModel) IsAccountFeeBumpEligible(ctx context.Context, address string) (bool, error) {
	// accounts.stellar_address is BYTEA, channel_accounts.public_key is VARCHAR
	const query = `
		SELECT
			EXISTS(
				SELECT stellar_address FROM accounts WHERE stellar_address = $1
				UNION
				SELECT public_key FROM channel_accounts WHERE public_key = $2
			)
	`
	var exists bool
	start := time.Now()
	err := m.DB.GetContext(ctx, &exists, query, types.StellarAddress(address), address)
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
