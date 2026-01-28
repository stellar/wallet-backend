// Package data provides data access layer for contract token operations.
// This file handles PostgreSQL storage of Soroban contract token metadata.
package data

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/lib/pq"

	"github.com/stellar/wallet-backend/internal/db"
	"github.com/stellar/wallet-backend/internal/metrics"
	"github.com/stellar/wallet-backend/internal/utils"
)

// contractNamespace is derived from table name for reproducibility.
var contractNamespace = uuid.NewSHA1(uuid.NameSpaceDNS, []byte("contract_tokens"))

// DeterministicContractID computes a deterministic UUID for a contract token
// using UUID v5 (SHA-1) of the contract address (C...). This enables streaming checkpoint
// processing without needing DB roundtrips to get auto-generated IDs.
func DeterministicContractID(contractID string) uuid.UUID {
	return uuid.NewSHA1(contractNamespace, []byte(contractID))
}

// ContractModelInterface defines the interface for contract token operations.
type ContractModelInterface interface {
	GetByContractID(ctx context.Context, contractID string) (*Contract, error)
	GetExisting(ctx context.Context, dbTx pgx.Tx, contractIDs []string) ([]string, error)
	BatchGetByIDs(ctx context.Context, ids []uuid.UUID) ([]*Contract, error)
	// BatchInsert inserts multiple contracts with pre-computed IDs.
	// Uses INSERT ... ON CONFLICT (contract_id) DO NOTHING for idempotent operations.
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
	ID         uuid.UUID `db:"id" json:"id"`
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

// GetExisting returns which of the given contract IDs exist in the database.
func (m *ContractModel) GetExisting(ctx context.Context, dbTx pgx.Tx, contractIDs []string) ([]string, error) {
	if len(contractIDs) == 0 {
		return nil, nil
	}

	const query = `SELECT contract_id FROM contract_tokens WHERE contract_id = ANY($1)`

	start := time.Now()
	rows, err := dbTx.Query(ctx, query, contractIDs)
	if err != nil {
		m.MetricsService.IncDBQueryError("GetExisting", "contract_tokens", utils.GetDBErrorType(err))
		return nil, fmt.Errorf("querying existing contract IDs: %w", err)
	}
	defer rows.Close()

	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			m.MetricsService.IncDBQueryError("GetExisting", "contract_tokens", utils.GetDBErrorType(err))
			return nil, fmt.Errorf("scanning contract ID: %w", err)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		m.MetricsService.IncDBQueryError("GetExisting", "contract_tokens", utils.GetDBErrorType(err))
		return nil, fmt.Errorf("iterating contract rows: %w", err)
	}

	duration := time.Since(start).Seconds()
	m.MetricsService.ObserveDBQueryDuration("GetExisting", "contract_tokens", duration)
	m.MetricsService.IncDBQuery("GetExisting", "contract_tokens")
	return ids, nil
}

// BatchGetByIDs retrieves contracts by their UUIDs.
func (m *ContractModel) BatchGetByIDs(ctx context.Context, ids []uuid.UUID) ([]*Contract, error) {
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
// Uses INSERT ... ON CONFLICT (contract_id) DO NOTHING for idempotent operations.
// Contracts must have their ID field set via DeterministicContractID before calling.
func (m *ContractModel) BatchInsert(ctx context.Context, dbTx pgx.Tx, contracts []*Contract) error {
	if len(contracts) == 0 {
		return nil
	}

	ids := make([]uuid.UUID, len(contracts))
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
		SELECT * FROM UNNEST($1::uuid[], $2::text[], $3::text[], $4::text[], $5::text[], $6::text[], $7::text[], $8::smallint[])
		ON CONFLICT (contract_id) DO NOTHING
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
