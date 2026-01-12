// Package data provides data access layer for contract token operations.
// This file handles PostgreSQL storage of Soroban contract token metadata.
package data

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/lib/pq"

	"github.com/stellar/wallet-backend/internal/db"
	"github.com/stellar/wallet-backend/internal/metrics"
	"github.com/stellar/wallet-backend/internal/utils"
)

// ContractModelInterface defines the interface for contract token operations.
type ContractModelInterface interface {
	GetByContractID(ctx context.Context, contractID string) (*Contract, error)
	GetAllContractIDs(ctx context.Context) ([]string, error)
	BatchGetByIDs(ctx context.Context, ids []int64) ([]*Contract, error)
	BatchGetOrInsert(ctx context.Context, dbTx pgx.Tx, contracts []*Contract) (map[string]int64, error)
	BatchInsert(ctx context.Context, dbTx pgx.Tx, contracts []*Contract) (map[string]int64, error)
}

// ContractModel implements ContractModelInterface.
type ContractModel struct {
	DB             db.ConnectionPool
	MetricsService metrics.MetricsService
}

var _ ContractModelInterface = (*ContractModel)(nil)

// Contract represents a Soroban contract token.
type Contract struct {
	ID         int64     `db:"id" json:"id"`
	ContractID string    `db:"contract_id" json:"contractId"`
	Type       string    `db:"type" json:"type"`
	Code       *string   `db:"code" json:"code"`
	Issuer     *string   `db:"issuer" json:"issuer"`
	Name       *string   `db:"name" json:"name"`
	Symbol     *string   `db:"symbol" json:"symbol"`
	Decimals   uint32    `db:"decimals" json:"decimals"`
	CreatedAt  time.Time `db:"created_at" json:"createdAt"`
	UpdatedAt  time.Time `db:"updated_at" json:"updatedAt"`
}

// contractRow is used for scanning rows from pgx queries.
type contractRow struct {
	ID         int64
	ContractID string
}

// GetByContractID retrieves a contract by its contract address (C...).
func (m *ContractModel) GetByContractID(ctx context.Context, contractID string) (*Contract, error) {
	start := time.Now()
	contract := &Contract{}
	err := m.DB.GetContext(ctx, contract, "SELECT * FROM contract_tokens WHERE contract_id = $1", contractID)
	duration := time.Since(start).Seconds()
	m.MetricsService.ObserveDBQueryDuration("GetByContractID", "contract_tokens", duration)
	if err != nil {
		m.MetricsService.IncDBQueryError("GetByContractID", "contract_tokens", utils.GetDBErrorType(err))
		return nil, fmt.Errorf("getting contract by contract_id %s: %w", contractID, err)
	}
	m.MetricsService.IncDBQuery("GetByContractID", "contract_tokens")
	return contract, nil
}

// GetAllContractIDs returns all contract addresses (C...) from the database.
func (m *ContractModel) GetAllContractIDs(ctx context.Context) ([]string, error) {
	const query = `SELECT contract_id FROM contract_tokens`

	start := time.Now()
	rows, err := m.DB.QueryxContext(ctx, query)
	if err != nil {
		m.MetricsService.IncDBQueryError("GetAllContractIDs", "contract_tokens", utils.GetDBErrorType(err))
		return nil, fmt.Errorf("querying contract IDs: %w", err)
	}
	defer utils.DeferredClose(ctx, rows, "closing contract IDs rows")

	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			m.MetricsService.IncDBQueryError("GetAllContractIDs", "contract_tokens", utils.GetDBErrorType(err))
			return nil, fmt.Errorf("scanning contract ID: %w", err)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		m.MetricsService.IncDBQueryError("GetAllContractIDs", "contract_tokens", utils.GetDBErrorType(err))
		return nil, fmt.Errorf("iterating contract rows: %w", err)
	}

	duration := time.Since(start).Seconds()
	m.MetricsService.ObserveDBQueryDuration("GetAllContractIDs", "contract_tokens", duration)
	m.MetricsService.IncDBQuery("GetAllContractIDs", "contract_tokens")
	return ids, nil
}

// BatchGetByIDs retrieves contracts by their numeric IDs.
func (m *ContractModel) BatchGetByIDs(ctx context.Context, ids []int64) ([]*Contract, error) {
	if len(ids) == 0 {
		return nil, nil
	}

	start := time.Now()
	var contracts []*Contract
	err := m.DB.SelectContext(ctx, &contracts, "SELECT * FROM contract_tokens WHERE id = ANY($1)", pq.Array(ids))
	duration := time.Since(start).Seconds()
	m.MetricsService.ObserveDBQueryDuration("BatchGetByIDs", "contract_tokens", duration)
	if err != nil {
		m.MetricsService.IncDBQueryError("BatchGetByIDs", "contract_tokens", utils.GetDBErrorType(err))
		return nil, fmt.Errorf("getting contracts by IDs: %w", err)
	}
	m.MetricsService.IncDBQuery("BatchGetByIDs", "contract_tokens")
	return contracts, nil
}

// BatchGetOrInsert returns numeric IDs for multiple contracts, creating any that don't exist.
// Uses batch INSERT ... ON CONFLICT for atomic upsert behavior.
// Returns a map of contract_id (C...) -> numeric id.
func (m *ContractModel) BatchGetOrInsert(ctx context.Context, dbTx pgx.Tx, contracts []*Contract) (map[string]int64, error) {
	if len(contracts) == 0 {
		return make(map[string]int64), nil
	}

	contractIDs := make([]string, len(contracts))
	for i, c := range contracts {
		contractIDs[i] = c.ContractID
	}

	// Step 1: Try to get existing rows first (cheap SELECT)
	const selectQuery = `
		SELECT id, contract_id FROM contract_tokens
		WHERE contract_id = ANY($1)
	`

	start := time.Now()
	rows, err := dbTx.Query(ctx, selectQuery, contractIDs)
	if err != nil {
		return nil, fmt.Errorf("selecting contracts: %w", err)
	}

	existingContracts, err := pgx.CollectRows(rows, pgx.RowToStructByPos[contractRow])
	if err != nil {
		return nil, fmt.Errorf("collecting contract rows: %w", err)
	}

	result := make(map[string]int64, len(contracts))
	for _, c := range existingContracts {
		result[c.ContractID] = c.ID
	}

	// Step 2: If all found, we're done (fast path - most common case)
	if len(result) == len(contracts) {
		m.MetricsService.ObserveDBQueryDuration("BatchGetOrInsert", "contract_tokens", time.Since(start).Seconds())
		m.MetricsService.IncDBQuery("BatchGetOrInsert", "contract_tokens")
		return result, nil
	}

	// Step 3: Insert only missing ones
	var newContracts []*Contract
	for _, c := range contracts {
		if _, exists := result[c.ContractID]; !exists {
			newContracts = append(newContracts, c)
		}
	}

	newContractIDs := make([]string, len(newContracts))
	types := make([]string, len(newContracts))
	codes := make([]*string, len(newContracts))
	issuers := make([]*string, len(newContracts))
	names := make([]*string, len(newContracts))
	symbols := make([]*string, len(newContracts))
	decimals := make([]uint32, len(newContracts))

	for i, c := range newContracts {
		newContractIDs[i] = c.ContractID
		types[i] = c.Type
		codes[i] = c.Code
		issuers[i] = c.Issuer
		names[i] = c.Name
		symbols[i] = c.Symbol
		decimals[i] = c.Decimals
	}

	const insertQuery = `
		INSERT INTO contract_tokens (contract_id, type, code, issuer, name, symbol, decimals)
		SELECT * FROM UNNEST($1::text[], $2::text[], $3::text[], $4::text[], $5::text[], $6::text[], $7::smallint[])
		ON CONFLICT (contract_id) DO UPDATE SET contract_id = EXCLUDED.contract_id
		RETURNING id, contract_id
	`

	rows, err = dbTx.Query(ctx, insertQuery, newContractIDs, types, codes, issuers, names, symbols, decimals)
	if err != nil {
		return nil, fmt.Errorf("inserting contracts: %w", err)
	}

	insertedContracts, err := pgx.CollectRows(rows, pgx.RowToStructByPos[contractRow])
	if err != nil {
		return nil, fmt.Errorf("collecting inserted contract rows: %w", err)
	}

	for _, c := range insertedContracts {
		result[c.ContractID] = c.ID
	}

	m.MetricsService.ObserveDBQueryDuration("BatchGetOrInsert", "contract_tokens", time.Since(start).Seconds())
	m.MetricsService.IncDBQuery("BatchGetOrInsert", "contract_tokens")
	return result, nil
}

// BatchInsert inserts multiple contracts and returns their numeric IDs.
// Uses batch INSERT with UNNEST for efficient bulk operations during checkpoint population.
// ON CONFLICT DO UPDATE ensures RETURNING works for all rows (existing + new).
// Returns a map of contract_id (C...) -> numeric id.
func (m *ContractModel) BatchInsert(ctx context.Context, dbTx pgx.Tx, contracts []*Contract) (map[string]int64, error) {
	if len(contracts) == 0 {
		return make(map[string]int64), nil
	}

	contractIDs := make([]string, len(contracts))
	types := make([]string, len(contracts))
	codes := make([]*string, len(contracts))
	issuers := make([]*string, len(contracts))
	names := make([]*string, len(contracts))
	symbols := make([]*string, len(contracts))
	decimals := make([]uint32, len(contracts))

	for i, c := range contracts {
		contractIDs[i] = c.ContractID
		types[i] = c.Type
		codes[i] = c.Code
		issuers[i] = c.Issuer
		names[i] = c.Name
		symbols[i] = c.Symbol
		decimals[i] = c.Decimals
	}

	const query = `
		INSERT INTO contract_tokens (contract_id, type, code, issuer, name, symbol, decimals)
		SELECT * FROM UNNEST($1::text[], $2::text[], $3::text[], $4::text[], $5::text[], $6::text[], $7::smallint[])
		ON CONFLICT (contract_id) DO UPDATE SET contract_id = EXCLUDED.contract_id
		RETURNING id, contract_id
	`

	start := time.Now()
	rows, err := dbTx.Query(ctx, query, contractIDs, types, codes, issuers, names, symbols, decimals)
	if err != nil {
		m.MetricsService.IncDBQueryError("BatchInsert", "contract_tokens", utils.GetDBErrorType(err))
		return nil, fmt.Errorf("batch inserting contracts: %w", err)
	}

	insertedContracts, err := pgx.CollectRows(rows, pgx.RowToStructByPos[contractRow])
	if err != nil {
		m.MetricsService.IncDBQueryError("BatchInsert", "contract_tokens", utils.GetDBErrorType(err))
		return nil, fmt.Errorf("collecting inserted contract rows: %w", err)
	}

	result := make(map[string]int64, len(contracts))
	for _, c := range insertedContracts {
		result[c.ContractID] = c.ID
	}

	duration := time.Since(start).Seconds()
	m.MetricsService.ObserveDBQueryDuration("BatchInsert", "contract_tokens", duration)
	m.MetricsService.ObserveDBBatchSize("BatchInsert", "contract_tokens", len(contracts))
	m.MetricsService.IncDBQuery("BatchInsert", "contract_tokens")

	return result, nil
}
