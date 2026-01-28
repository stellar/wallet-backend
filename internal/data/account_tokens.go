// Package data provides data access layer for account token operations.
// This file handles PostgreSQL storage of account-to-token relationships (trustlines and contracts).
package data

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/stellar/wallet-backend/internal/db"
	"github.com/stellar/wallet-backend/internal/metrics"
)

// TrustlineChanges represents add/remove operations for an account's trustlines.
type TrustlineChanges struct {
	AddIDs    []uuid.UUID
	RemoveIDs []uuid.UUID
}

// AccountTokensModelInterface defines the interface for account token operations.
type AccountTokensModelInterface interface {
	// Trustline and contract tokens read operations (for API/balances queries)
	GetTrustlineAssetIDs(ctx context.Context, accountAddress string) ([]uuid.UUID, error)
	GetContractIDs(ctx context.Context, accountAddress string) ([]uuid.UUID, error)

	// Trustline and contract tokens write operations (for live ingestion)
	BatchUpsertTrustlines(ctx context.Context, dbTx pgx.Tx, changes map[string]*TrustlineChanges) error
	BatchAddContracts(ctx context.Context, dbTx pgx.Tx, contractsByAccount map[string][]uuid.UUID) error

	// Bulk operations (for initial population)
	BulkInsertTrustlines(ctx context.Context, dbTx pgx.Tx, trustlinesByAccount map[string][]uuid.UUID) error
	BulkInsertContracts(ctx context.Context, dbTx pgx.Tx, contractsByAccount map[string][]uuid.UUID) error
}

// AccountTokensModel implements AccountTokensModelInterface.
type AccountTokensModel struct {
	DB             db.ConnectionPool
	MetricsService metrics.MetricsService
}

var _ AccountTokensModelInterface = (*AccountTokensModel)(nil)

// GetTrustlineAssetIDs retrieves asset IDs for a single account.
func (m *AccountTokensModel) GetTrustlineAssetIDs(ctx context.Context, accountAddress string) ([]uuid.UUID, error) {
	if accountAddress == "" {
		return nil, fmt.Errorf("empty account address")
	}

	const query = `SELECT asset_id FROM account_trustlines WHERE account_address = $1`

	start := time.Now()
	rows, err := m.DB.PgxPool().Query(ctx, query, accountAddress)
	if err != nil {
		m.MetricsService.IncDBQueryError("GetTrustlineAssetIDs", "account_trustlines", "query_error")
		return nil, fmt.Errorf("querying trustline asset IDs for %s: %w", accountAddress, err)
	}
	defer rows.Close()

	var assetIDs []uuid.UUID
	for rows.Next() {
		var id uuid.UUID
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("scanning asset ID: %w", err)
		}
		assetIDs = append(assetIDs, id)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating asset IDs: %w", err)
	}

	m.MetricsService.ObserveDBQueryDuration("GetTrustlineAssetIDs", "account_trustlines", time.Since(start).Seconds())
	m.MetricsService.IncDBQuery("GetTrustlineAssetIDs", "account_trustlines")
	return assetIDs, nil
}

// GetContractIDs retrieves contract IDs for a single account.
func (m *AccountTokensModel) GetContractIDs(ctx context.Context, accountAddress string) ([]uuid.UUID, error) {
	if accountAddress == "" {
		return nil, fmt.Errorf("empty account address")
	}

	const query = `SELECT contract_id FROM account_contracts WHERE account_address = $1`

	start := time.Now()
	rows, err := m.DB.PgxPool().Query(ctx, query, accountAddress)
	if err != nil {
		m.MetricsService.IncDBQueryError("GetContractIDs", "account_contracts", "query_error")
		return nil, fmt.Errorf("querying contract IDs for %s: %w", accountAddress, err)
	}
	defer rows.Close()

	var contractIDs []uuid.UUID
	for rows.Next() {
		var id uuid.UUID
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("scanning contract ID: %w", err)
		}
		contractIDs = append(contractIDs, id)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating contract IDs: %w", err)
	}

	m.MetricsService.ObserveDBQueryDuration("GetContractIDs", "account_contracts", time.Since(start).Seconds())
	m.MetricsService.IncDBQuery("GetContractIDs", "account_contracts")
	return contractIDs, nil
}

// BatchUpsertTrustlines adds/removes trustlines for multiple accounts.
func (m *AccountTokensModel) BatchUpsertTrustlines(ctx context.Context, dbTx pgx.Tx, changes map[string]*TrustlineChanges) error {
	if len(changes) == 0 {
		return nil
	}

	start := time.Now()
	batch := &pgx.Batch{}

	const insertQuery = `
		INSERT INTO account_trustlines (account_address, asset_id)
		SELECT $1, unnest($2::uuid[])
		ON CONFLICT DO NOTHING`

	const deleteQuery = `
		DELETE FROM account_trustlines
		WHERE account_address = $1 AND asset_id = ANY($2::uuid[])`

	for accountAddress, change := range changes {
		if len(change.AddIDs) > 0 {
			batch.Queue(insertQuery, accountAddress, change.AddIDs)
		}
		if len(change.RemoveIDs) > 0 {
			batch.Queue(deleteQuery, accountAddress, change.RemoveIDs)
		}
	}

	if batch.Len() == 0 {
		return nil
	}

	br := dbTx.SendBatch(ctx, batch)
	for i := 0; i < batch.Len(); i++ {
		if _, err := br.Exec(); err != nil {
			_ = br.Close() //nolint:errcheck // cleanup on error path
			return fmt.Errorf("upserting trustlines: %w", err)
		}
	}
	if err := br.Close(); err != nil {
		return fmt.Errorf("closing trustline batch: %w", err)
	}

	m.MetricsService.ObserveDBQueryDuration("BatchUpsertTrustlines", "account_trustlines", time.Since(start).Seconds())
	m.MetricsService.IncDBQuery("BatchUpsertTrustlines", "account_trustlines")
	return nil
}

// BatchAddContracts adds contract IDs for multiple accounts (contracts are never removed).
func (m *AccountTokensModel) BatchAddContracts(ctx context.Context, dbTx pgx.Tx, contractsByAccount map[string][]uuid.UUID) error {
	if len(contractsByAccount) == 0 {
		return nil
	}

	start := time.Now()

	const query = `
		INSERT INTO account_contracts (account_address, contract_id)
		SELECT $1, unnest($2::uuid[])
		ON CONFLICT DO NOTHING`

	batch := &pgx.Batch{}
	for accountAddress, contractIDs := range contractsByAccount {
		if len(contractIDs) == 0 {
			continue
		}
		batch.Queue(query, accountAddress, contractIDs)
	}

	if batch.Len() == 0 {
		return nil
	}

	br := dbTx.SendBatch(ctx, batch)
	for i := 0; i < batch.Len(); i++ {
		if _, err := br.Exec(); err != nil {
			_ = br.Close() //nolint:errcheck // cleanup on error path
			return fmt.Errorf("adding contracts: %w", err)
		}
	}
	if err := br.Close(); err != nil {
		return fmt.Errorf("closing contracts batch: %w", err)
	}

	m.MetricsService.ObserveDBQueryDuration("BatchAddContracts", "account_contracts", time.Since(start).Seconds())
	m.MetricsService.IncDBQuery("BatchAddContracts", "account_contracts")
	return nil
}

// BulkInsertTrustlines performs bulk insert using COPY protocol for speed.
func (m *AccountTokensModel) BulkInsertTrustlines(ctx context.Context, dbTx pgx.Tx, trustlinesByAccount map[string][]uuid.UUID) error {
	if len(trustlinesByAccount) == 0 {
		return nil
	}

	start := time.Now()

	// Build rows for COPY
	type trustlineRow struct {
		address     string
		trustlineID uuid.UUID
	}

	var rows []trustlineRow
	for addr, ids := range trustlinesByAccount {
		for _, id := range ids {
			rows = append(rows, trustlineRow{
				address:     addr,
				trustlineID: id,
			})
		}
	}

	copyCount, err := dbTx.CopyFrom(
		ctx,
		pgx.Identifier{"account_trustlines"},
		[]string{"account_address", "asset_id"},
		pgx.CopyFromSlice(len(rows), func(i int) ([]any, error) {
			return []any{rows[i].address, rows[i].trustlineID}, nil
		}),
	)
	if err != nil {
		return fmt.Errorf("bulk inserting trustlines via COPY: %w", err)
	}

	if int(copyCount) != len(rows) {
		return fmt.Errorf("expected %d rows copied, got %d", len(rows), copyCount)
	}

	m.MetricsService.ObserveDBQueryDuration("BulkInsertTrustlines", "account_trustlines", time.Since(start).Seconds())
	m.MetricsService.IncDBQuery("BulkInsertTrustlines", "account_trustlines")
	return nil
}

// BulkInsertContracts performs bulk insert for initial population.
func (m *AccountTokensModel) BulkInsertContracts(ctx context.Context, dbTx pgx.Tx, contractsByAccount map[string][]uuid.UUID) error {
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
		INSERT INTO account_contracts (account_address, contract_id)
		SELECT unnest($1::text[]), unnest($2::uuid[])
		ON CONFLICT DO NOTHING`

	_, err := dbTx.Exec(ctx, query, addresses, contractIDs)
	if err != nil {
		return fmt.Errorf("bulk inserting contracts: %w", err)
	}

	m.MetricsService.ObserveDBQueryDuration("BulkInsertContracts", "account_contracts", time.Since(start).Seconds())
	m.MetricsService.IncDBQuery("BulkInsertContracts", "account_contracts")
	return nil
}
