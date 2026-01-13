// Package data provides data access layer for contract token operations.
// This file handles PostgreSQL storage of Soroban contract token metadata.
package data

import (
	"context"
	"fmt"
	"hash/fnv"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/lib/pq"

	"github.com/stellar/wallet-backend/internal/db"
	"github.com/stellar/wallet-backend/internal/metrics"
	"github.com/stellar/wallet-backend/internal/utils"
)

// DeterministicContractID computes a deterministic 64-bit ID for a contract token
// using FNV-1a hash of the contract address (C...). This enables streaming checkpoint
// processing without needing DB roundtrips to get auto-generated IDs.
func DeterministicContractID(contractID string) int64 {
	h := fnv.New64a()
	h.Write([]byte(contractID))
	return int64(h.Sum64())
}

// ContractModelInterface defines the interface for contract token operations.
type ContractModelInterface interface {
	GetByContractID(ctx context.Context, contractID string) (*Contract, error)
	GetAllContractIDs(ctx context.Context) ([]string, error)
	BatchGetByIDs(ctx context.Context, ids []int64) ([]*Contract, error)
	// BatchInsert inserts multiple contracts with pre-computed IDs.
	// Uses INSERT ... ON CONFLICT DO NOTHING for idempotent operations.
	// Contracts must have their ID field set via DeterministicContractID before calling.
	BatchInsert(ctx context.Context, dbTx pgx.Tx, contracts []*Contract) error
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

// BatchInsert inserts multiple contracts with pre-computed deterministic IDs.
// Uses INSERT ... ON CONFLICT DO NOTHING for idempotent operations.
// Contracts must have their ID field set via DeterministicContractID before calling.
func (m *ContractModel) BatchInsert(ctx context.Context, dbTx pgx.Tx, contracts []*Contract) error {
	if len(contracts) == 0 {
		return nil
	}

	ids := make([]int64, len(contracts))
	contractIDs := make([]string, len(contracts))
	types := make([]string, len(contracts))
	codes := make([]*string, len(contracts))
	issuers := make([]*string, len(contracts))
	names := make([]*string, len(contracts))
	symbols := make([]*string, len(contracts))
	decimals := make([]uint32, len(contracts))

	for i, c := range contracts {
		ids[i] = c.ID
		contractIDs[i] = c.ContractID
		types[i] = c.Type
		codes[i] = c.Code
		issuers[i] = c.Issuer
		names[i] = c.Name
		symbols[i] = c.Symbol
		decimals[i] = c.Decimals
	}

	const query = `
		INSERT INTO contract_tokens (id, contract_id, type, code, issuer, name, symbol, decimals)
		SELECT * FROM UNNEST($1::bigint[], $2::text[], $3::text[], $4::text[], $5::text[], $6::text[], $7::text[], $8::smallint[])
		ON CONFLICT DO NOTHING
	`

	start := time.Now()
	_, err := dbTx.Exec(ctx, query, ids, contractIDs, types, codes, issuers, names, symbols, decimals)
	if err != nil {
		m.MetricsService.IncDBQueryError("BatchInsert", "contract_tokens", utils.GetDBErrorType(err))
		return fmt.Errorf("batch inserting contracts: %w", err)
	}

	m.MetricsService.ObserveDBQueryDuration("BatchInsert", "contract_tokens", time.Since(start).Seconds())
	m.MetricsService.ObserveDBBatchSize("BatchInsert", "contract_tokens", len(contracts))
	m.MetricsService.IncDBQuery("BatchInsert", "contract_tokens")
	return nil
}
