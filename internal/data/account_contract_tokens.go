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
	"github.com/stellar/wallet-backend/internal/utils"
)

// AccountContractTokensModelInterface defines the interface for account-contract mapping operations.
type AccountContractTokensModelInterface interface {
	// Read operations (for API/balances queries)
	GetByAccount(ctx context.Context, accountAddress string) ([]*Contract, error)
	// GetSEP41ByAccountPaginated returns only SEP-41 contracts ordered by the
	// junction-table contract UUID so the GraphQL balances connection can page
	// without loading every contract membership first.
	GetSEP41ByAccountPaginated(ctx context.Context, accountAddress string, limit *int32, cursor *uuid.UUID, sortOrder SortOrder) ([]*Contract, error)

	// Write operations (for initial population and live ingestion)
	BatchInsert(ctx context.Context, dbTx pgx.Tx, contractsByAccount map[string][]uuid.UUID) error
}

// AccountContractTokensModel implements AccountContractTokensModelInterface.
type AccountContractTokensModel struct {
	DB      *pgxpool.Pool
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
	m.Metrics.QueriesTotal.WithLabelValues("GetByAccount", "account_contract_tokens").Inc()
	if err != nil {
		m.Metrics.QueryErrors.WithLabelValues("GetByAccount", "account_contract_tokens", utils.GetDBErrorType(err)).Inc()
		return nil, fmt.Errorf("querying contracts for %s: %w", accountAddress, err)
	}
	return contracts, nil
}

// GetSEP41ByAccountPaginated retrieves SEP-41 contracts for an account ordered
// by contract UUID. The cursor is the last seen contract UUID from the previous page.
func (m *AccountContractTokensModel) GetSEP41ByAccountPaginated(ctx context.Context, accountAddress string, limit *int32, cursor *uuid.UUID, sortOrder SortOrder) ([]*Contract, error) {
	if accountAddress == "" {
		return nil, fmt.Errorf("empty account address")
	}

	query := `
		SELECT ct.id, ct.contract_id, ct.type, ct.code, ct.issuer,
		       ct.name, ct.symbol, ct.decimals, ct.created_at, ct.updated_at
		FROM account_contract_tokens ac
		INNER JOIN contract_tokens ct ON ct.id = ac.contract_id
		WHERE ac.account_address = $1
		  AND ct.type = 'SEP41'`
	args := []interface{}{accountAddress}
	argIndex := 2

	if cursor != nil {
		// Cursor comparisons implement keyset pagination over the junction PK.
		op := ">"
		if sortOrder == DESC {
			op = "<"
		}
		query += fmt.Sprintf(" AND ac.contract_id %s $%d", op, argIndex)
		args = append(args, *cursor)
		argIndex++
	}

	if sortOrder == DESC {
		query += " ORDER BY ac.contract_id DESC"
	} else {
		query += " ORDER BY ac.contract_id ASC"
	}

	if limit != nil {
		query += fmt.Sprintf(" LIMIT $%d", argIndex)
		args = append(args, *limit)
	}

	start := time.Now()
	contracts, err := db.QueryManyPtrs[Contract](ctx, m.DB, query, args...)
	duration := time.Since(start).Seconds()
	m.Metrics.QueryDuration.WithLabelValues("GetSEP41ByAccountPaginated", "account_contract_tokens").Observe(duration)
	m.Metrics.QueriesTotal.WithLabelValues("GetSEP41ByAccountPaginated", "account_contract_tokens").Inc()
	if err != nil {
		m.Metrics.QueryErrors.WithLabelValues("GetSEP41ByAccountPaginated", "account_contract_tokens", utils.GetDBErrorType(err)).Inc()
		return nil, fmt.Errorf("querying paginated SEP-41 contracts for %s: %w", accountAddress, err)
	}
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
	duration := time.Since(start).Seconds()
	m.Metrics.QueryDuration.WithLabelValues("BatchInsert", "account_contract_tokens").Observe(duration)
	m.Metrics.BatchSize.WithLabelValues("BatchInsert", "account_contract_tokens").Observe(float64(len(addresses)))
	m.Metrics.QueriesTotal.WithLabelValues("BatchInsert", "account_contract_tokens").Inc()
	if err != nil {
		m.Metrics.QueryErrors.WithLabelValues("BatchInsert", "account_contract_tokens", utils.GetDBErrorType(err)).Inc()
		return fmt.Errorf("batch inserting contract tokens: %w", err)
	}
	return nil
}
