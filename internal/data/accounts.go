package data

import (
	"context"
	"fmt"
	"time"

	"github.com/lib/pq"

	"github.com/stellar/wallet-backend/internal/db"
	"github.com/stellar/wallet-backend/internal/metrics"
)

type AccountModel struct {
	DB             db.ConnectionPool
	MetricsService metrics.MetricsService
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

// GetExisting returns only the addresses from the input list that exist in the database.
func (m *AccountModel) GetExisting(ctx context.Context, dbTx db.Transaction, stellarAddresses []string) ([]string, error) {
	var sqlExecuter db.SQLExecuter = dbTx
	if sqlExecuter == nil {
		sqlExecuter = m.DB
	}

	const query = "SELECT stellar_address FROM accounts WHERE stellar_address = ANY($1)"
	var existingAddresses []string
	start := time.Now()
	err := sqlExecuter.SelectContext(ctx, &existingAddresses, query, pq.Array(stellarAddresses))
	duration := time.Since(start).Seconds()
	m.MetricsService.ObserveDBQueryDuration("SELECT", "accounts", duration)
	if err != nil {
		return nil, fmt.Errorf("getting existing addresses: %w", err)
	}
	m.MetricsService.IncDBQuery("SELECT", "accounts")
	return existingAddresses, nil
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
