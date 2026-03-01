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
	MetricsService metrics.MetricsService
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
	rows, err := m.DB.Query(ctx, query, accountAddress)
	if err != nil {
		m.MetricsService.IncDBQueryError("GetByAccount", "account_contract_tokens", "query_error")
		return nil, fmt.Errorf("querying contracts for %s: %w", accountAddress, err)
	}
	defer rows.Close()

	var contracts []*Contract
	for rows.Next() {
		var c Contract
		if err := rows.Scan(&c.ID, &c.ContractID, &c.Type, &c.Code, &c.Issuer,
			&c.Name, &c.Symbol, &c.Decimals, &c.CreatedAt, &c.UpdatedAt); err != nil {
			return nil, fmt.Errorf("scanning contract: %w", err)
		}
		contracts = append(contracts, &c)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating contracts: %w", err)
	}

	m.MetricsService.ObserveDBQueryDuration("GetByAccount", "account_contract_tokens", time.Since(start).Seconds())
	m.MetricsService.IncDBQuery("GetByAccount", "account_contract_tokens")
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

	m.MetricsService.ObserveDBQueryDuration("BatchInsert", "account_contract_tokens", time.Since(start).Seconds())
	m.MetricsService.IncDBQuery("BatchInsert", "account_contract_tokens")
	return nil
}
