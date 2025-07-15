package data

import (
	"context"
	"fmt"
	"time"

	"github.com/lib/pq"

	"github.com/stellar/wallet-backend/internal/db"
	"github.com/stellar/wallet-backend/internal/indexer/types"
	"github.com/stellar/wallet-backend/internal/metrics"
)

type AccountModel struct {
	DB             db.ConnectionPool
	MetricsService metrics.MetricsService
}

func (m *AccountModel) Get(ctx context.Context, address string) (*types.Account, error) {
	const query = `SELECT * FROM accounts WHERE stellar_address = $1`
	var account types.Account
	start := time.Now()
	err := m.DB.GetContext(ctx, &account, query, address)
	duration := time.Since(start).Seconds()
	m.MetricsService.ObserveDBQueryDuration("SELECT", "accounts", duration)
	if err != nil {
		return nil, fmt.Errorf("getting account %s: %w", address, err)
	}
	m.MetricsService.IncDBQuery("SELECT", "accounts")
	return &account, nil
}

func (m *AccountModel) Insert(ctx context.Context, address string) error {
	const query = `INSERT INTO accounts (stellar_address) VALUES ($1) ON CONFLICT DO NOTHING`
	start := time.Now()
	_, err := m.DB.ExecContext(ctx, query, address)
	duration := time.Since(start).Seconds()
	m.MetricsService.ObserveDBQueryDuration("INSERT", "accounts", duration)
	if err != nil {
		return fmt.Errorf("inserting address %s: %w", address, err)
	}
	m.MetricsService.IncDBQuery("INSERT", "accounts")

	return nil
}

func (m *AccountModel) Delete(ctx context.Context, address string) error {
	const query = `DELETE FROM accounts WHERE stellar_address = $1`
	start := time.Now()
	_, err := m.DB.ExecContext(ctx, query, address)
	duration := time.Since(start).Seconds()
	m.MetricsService.ObserveDBQueryDuration("DELETE", "accounts", duration)
	if err != nil {
		return fmt.Errorf("deleting address %s: %w", address, err)
	}
	m.MetricsService.IncDBQuery("DELETE", "accounts")
	return nil
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
	m.MetricsService.ObserveDBQueryDuration("SELECT", "accounts", duration)
	if err != nil {
		return false, fmt.Errorf("checking if account %s is fee bump eligible: %w", address, err)
	}
	m.MetricsService.IncDBQuery("SELECT", "accounts")
	return exists, nil
}

func (m *AccountModel) BatchGetByTxHash(ctx context.Context, txHashes []string) ([]*types.AccountWithTxHash, error) {
	const query = `
		SELECT accounts.*, transactions_accounts.tx_hash 
		FROM transactions_accounts 
		INNER JOIN accounts 
		ON transactions_accounts.account_id = accounts.stellar_address 
		WHERE transactions_accounts.tx_hash = ANY($1)`
	var accounts []*types.AccountWithTxHash
	start := time.Now()
	err := m.DB.SelectContext(ctx, &accounts, query, pq.Array(txHashes))
	duration := time.Since(start).Seconds()
	m.MetricsService.ObserveDBQueryDuration("SELECT", "accounts", duration)
	if err != nil {
		return nil, fmt.Errorf("getting accounts by transaction hashes: %w", err)
	}
	m.MetricsService.IncDBQuery("SELECT", "accounts")
	return accounts, nil
}

func (m *AccountModel) BatchGetByOperationID(ctx context.Context, operationIDs []int64) ([]*types.AccountWithOperationID, error) {
	const query = `
		SELECT accounts.*, operations_accounts.operation_id 
		FROM operations_accounts 
		INNER JOIN accounts 
		ON operations_accounts.account_id = accounts.stellar_address 
		WHERE operations_accounts.operation_id = ANY($1)`
	var accounts []*types.AccountWithOperationID
	start := time.Now()
	err := m.DB.SelectContext(ctx, &accounts, query, pq.Array(operationIDs))
	duration := time.Since(start).Seconds()
	m.MetricsService.ObserveDBQueryDuration("SELECT", "accounts", duration)
	if err != nil {
		return nil, fmt.Errorf("getting accounts by operation IDs: %w", err)
	}
	m.MetricsService.IncDBQuery("SELECT", "accounts")
	return accounts, nil
}
