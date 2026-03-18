// Package data provides data access layer for account-contract token mappings.
// This file handles PostgreSQL storage of account-to-contract relationships.
package data

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/stellar/wallet-backend/internal/db"
	"github.com/stellar/wallet-backend/internal/metrics"
)

// AccountContractTokensModelInterface defines the interface for account-contract mapping operations.
type AccountContractTokensModelInterface interface {
	// Read operations (for API/balances queries)
	GetByAccount(ctx context.Context, accountAddress string) ([]*Contract, error)

	// Write operations (for initial population and live ingestion)
	BatchInsert(ctx context.Context, dbTx pgx.Tx, contractsByAccount map[string][]uuid.UUID) error
}

// AccountContractTokensModel implements AccountContractTokensModelInterface.
type AccountContractTokensModel struct {
	DB             *pgxpool.Pool
	Metrics *metrics.DBMetrics
}

var _ AccountContractTokensModelInterface = (*AccountContractTokensModel)(nil)

// GetByAccount retrieves all contract tokens for an account with full metadata via JOIN.
func (m *AccountContractTokensModel) GetByAccount(ctx context.Context, accountAddress string) ([]*Contract, error) {
	if accountAddress == "" {
		return nil, fmt.Errorf("empty account address")
	}

	const query = `
		SELECT ct.id, ct.contract_id, ct.type, ct.code, ct.issuer,
		       ct.name, ct.symbol, ct.decimals, ct.created_at, ct.updated_at
		FROM account_contract_tokens ac
		INNER JOIN contract_tokens ct ON ct.id = ac.contract_id
		WHERE ac.account_address = $1`

	start := time.Now()
	contracts, err := db.QueryManyPtrs[Contract](ctx, m.DB, query, accountAddress)
	duration := time.Since(start).Seconds()
	m.Metrics.QueryDuration.WithLabelValues("GetByAccount", "account_contract_tokens").Observe(duration)
	if err != nil {
		m.Metrics.QueryErrors.WithLabelValues("GetByAccount", "account_contract_tokens", "query_error").Inc()
		return nil, fmt.Errorf("querying contracts for %s: %w", accountAddress, err)
	}
	m.Metrics.QueriesTotal.WithLabelValues("GetByAccount", "account_contract_tokens").Inc()
	return contracts, nil
}

// BatchInsert inserts contract IDs for multiple accounts (contracts are never removed).
func (m *AccountContractTokensModel) BatchInsert(ctx context.Context, dbTx pgx.Tx, contractsByAccount map[string][]uuid.UUID) error {
	if len(contractsByAccount) == 0 {
		return nil
	}

	start := time.Now()

	// Flatten to parallel arrays for UNNEST
	var addresses []string
	var contractIDs []uuid.UUID
	for accountAddress, ids := range contractsByAccount {
		for _, id := range ids {
			addresses = append(addresses, accountAddress)
			contractIDs = append(contractIDs, id)
		}
	}

	if len(addresses) == 0 {
		return nil
	}

	const query = `
		INSERT INTO account_contract_tokens (account_address, contract_id)
		SELECT unnest($1::text[]), unnest($2::uuid[])
		ON CONFLICT DO NOTHING`

	_, err := dbTx.Exec(ctx, query, addresses, contractIDs)
	if err != nil {
		return fmt.Errorf("batch inserting contract tokens: %w", err)
	}

	m.Metrics.QueryDuration.WithLabelValues("BatchInsert", "account_contract_tokens").Observe(time.Since(start).Seconds())
	m.Metrics.QueriesTotal.WithLabelValues("BatchInsert", "account_contract_tokens").Inc()
	return nil
}
