// Package data provides data access layer for account token operations.
// This file handles PostgreSQL storage of account-to-token relationships (trustlines and contracts).
package data

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/stellar/wallet-backend/internal/db"
	"github.com/stellar/wallet-backend/internal/metrics"
)

// TrustlineChanges represents add/remove operations for an account's trustlines.
type TrustlineChanges struct {
	AddIDs    []int64
	RemoveIDs []int64
}

// AccountTokensModelInterface defines the interface for account token operations.
type AccountTokensModelInterface interface {
	// Trustline read operations (for API/balances queries)
	GetTrustlineAssetIDs(ctx context.Context, accountAddress string) ([]int64, error)
	BatchGetTrustlineAssetIDs(ctx context.Context, addresses []string) (map[string][]int64, error)

	// Contract read operations (returns numeric IDs referencing contract_tokens.id)
	GetContractIDs(ctx context.Context, accountAddress string) ([]int64, error)
	BatchGetContractIDs(ctx context.Context, addresses []string) (map[string][]int64, error)

	// Trustline write operations (for live ingestion)
	BatchUpsertTrustlines(ctx context.Context, dbTx pgx.Tx, changes map[string]*TrustlineChanges) error

	// Contract write operations (contracts only add, never remove)
	BatchAddContracts(ctx context.Context, dbTx pgx.Tx, contractsByAccount map[string][]int64) error

	// Bulk operations (for initial population)
	BulkInsertTrustlines(ctx context.Context, dbTx pgx.Tx, trustlinesByAccount map[string][]int64) error
	BulkInsertContracts(ctx context.Context, dbTx pgx.Tx, contractsByAccount map[string][]int64) error
}

// AccountTokensModel implements AccountTokensModelInterface.
type AccountTokensModel struct {
	DB             db.ConnectionPool
	MetricsService metrics.MetricsService
}

var _ AccountTokensModelInterface = (*AccountTokensModel)(nil)

// GetTrustlineAssetIDs retrieves asset IDs for a single account using pgx.
func (m *AccountTokensModel) GetTrustlineAssetIDs(ctx context.Context, accountAddress string) ([]int64, error) {
	if accountAddress == "" {
		return nil, fmt.Errorf("empty account address")
	}

	const query = `
		SELECT asset_ids
		FROM account_trustlines
		WHERE account_address = $1`

	start := time.Now()
	var assetIDsJSON []byte
	err := m.DB.PgxPool().QueryRow(ctx, query, accountAddress).Scan(&assetIDsJSON)
	m.MetricsService.ObserveDBQueryDuration("GetTrustlineAssetIDs", "account_trustlines", time.Since(start).Seconds())

	if errors.Is(err, pgx.ErrNoRows) {
		m.MetricsService.IncDBQuery("GetTrustlineAssetIDs", "account_trustlines")
		return []int64{}, nil
	}
	if err != nil {
		m.MetricsService.IncDBQueryError("GetTrustlineAssetIDs", "account_trustlines", "query_error")
		return nil, fmt.Errorf("getting trustline asset IDs for %s: %w", accountAddress, err)
	}

	var assetIDs []int64
	if err := json.Unmarshal(assetIDsJSON, &assetIDs); err != nil {
		return nil, fmt.Errorf("unmarshaling asset IDs for %s: %w", accountAddress, err)
	}

	m.MetricsService.IncDBQuery("GetTrustlineAssetIDs", "account_trustlines")
	return assetIDs, nil
}

// BatchGetTrustlineAssetIDs retrieves asset IDs for multiple accounts using pgx.
func (m *AccountTokensModel) BatchGetTrustlineAssetIDs(ctx context.Context, addresses []string) (map[string][]int64, error) {
	if len(addresses) == 0 {
		return make(map[string][]int64), nil
	}

	const query = `
		SELECT account_address, asset_ids
		FROM account_trustlines
		WHERE account_address = ANY($1)`

	start := time.Now()
	rows, err := m.DB.PgxPool().Query(ctx, query, addresses)
	if err != nil {
		m.MetricsService.IncDBQueryError("BatchGetTrustlineAssetIDs", "account_trustlines", "query_error")
		return nil, fmt.Errorf("querying trustline asset IDs: %w", err)
	}
	defer rows.Close()

	result := make(map[string][]int64, len(addresses))
	for rows.Next() {
		var accountAddress string
		var assetIDsJSON []byte
		if err := rows.Scan(&accountAddress, &assetIDsJSON); err != nil {
			return nil, fmt.Errorf("scanning trustline row: %w", err)
		}

		var assetIDs []int64
		if err := json.Unmarshal(assetIDsJSON, &assetIDs); err != nil {
			return nil, fmt.Errorf("unmarshaling asset IDs for %s: %w", accountAddress, err)
		}
		result[accountAddress] = assetIDs
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating trustline rows: %w", err)
	}

	m.MetricsService.ObserveDBQueryDuration("BatchGetTrustlineAssetIDs", "account_trustlines", time.Since(start).Seconds())
	m.MetricsService.IncDBQuery("BatchGetTrustlineAssetIDs", "account_trustlines")
	return result, nil
}

// GetContractIDs retrieves numeric contract IDs for a single account using pgx.
func (m *AccountTokensModel) GetContractIDs(ctx context.Context, accountAddress string) ([]int64, error) {
	if accountAddress == "" {
		return nil, fmt.Errorf("empty account address")
	}

	const query = `
		SELECT contract_ids
		FROM account_contracts
		WHERE account_address = $1`

	start := time.Now()
	var contractIDsJSON []byte
	err := m.DB.PgxPool().QueryRow(ctx, query, accountAddress).Scan(&contractIDsJSON)
	m.MetricsService.ObserveDBQueryDuration("GetContractIDs", "account_contracts", time.Since(start).Seconds())

	if errors.Is(err, pgx.ErrNoRows) {
		m.MetricsService.IncDBQuery("GetContractIDs", "account_contracts")
		return []int64{}, nil
	}
	if err != nil {
		m.MetricsService.IncDBQueryError("GetContractIDs", "account_contracts", "query_error")
		return nil, fmt.Errorf("getting contract IDs for %s: %w", accountAddress, err)
	}

	var contractIDs []int64
	if err := json.Unmarshal(contractIDsJSON, &contractIDs); err != nil {
		return nil, fmt.Errorf("unmarshaling contract IDs for %s: %w", accountAddress, err)
	}

	m.MetricsService.IncDBQuery("GetContractIDs", "account_contracts")
	return contractIDs, nil
}

// BatchGetContractIDs retrieves numeric contract IDs for multiple accounts using pgx.
func (m *AccountTokensModel) BatchGetContractIDs(ctx context.Context, addresses []string) (map[string][]int64, error) {
	if len(addresses) == 0 {
		return make(map[string][]int64), nil
	}

	const query = `
		SELECT account_address, contract_ids
		FROM account_contracts
		WHERE account_address = ANY($1)`

	start := time.Now()
	rows, err := m.DB.PgxPool().Query(ctx, query, addresses)
	if err != nil {
		m.MetricsService.IncDBQueryError("BatchGetContractIDs", "account_contracts", "query_error")
		return nil, fmt.Errorf("querying contract IDs: %w", err)
	}
	defer rows.Close()

	result := make(map[string][]int64, len(addresses))
	for rows.Next() {
		var accountAddress string
		var contractIDsJSON []byte
		if err := rows.Scan(&accountAddress, &contractIDsJSON); err != nil {
			return nil, fmt.Errorf("scanning contract row: %w", err)
		}

		var contractIDs []int64
		if err := json.Unmarshal(contractIDsJSON, &contractIDs); err != nil {
			return nil, fmt.Errorf("unmarshaling contract IDs for %s: %w", accountAddress, err)
		}
		result[accountAddress] = contractIDs
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating contract rows: %w", err)
	}

	m.MetricsService.ObserveDBQueryDuration("BatchGetContractIDs", "account_contracts", time.Since(start).Seconds())
	m.MetricsService.IncDBQuery("BatchGetContractIDs", "account_contracts")
	return result, nil
}

// BatchUpsertTrustlines adds/removes asset IDs for multiple accounts atomically.
// For each account, it merges the current asset_ids with new additions and removes specified IDs.
// Uses pgx.Batch for single round-trip efficiency.
func (m *AccountTokensModel) BatchUpsertTrustlines(ctx context.Context, dbTx pgx.Tx, changes map[string]*TrustlineChanges) error {
	if len(changes) == 0 {
		return nil
	}

	start := time.Now()

	const query = `
		INSERT INTO account_trustlines (account_address, asset_ids, updated_at)
		VALUES ($1, $2::jsonb, NOW())
		ON CONFLICT (account_address) DO UPDATE SET
			asset_ids = (
				SELECT COALESCE(jsonb_agg(DISTINCT elem), '[]'::jsonb)
				FROM jsonb_array_elements(account_trustlines.asset_ids || $2::jsonb) elem
				WHERE NOT (elem::bigint = ANY($3::bigint[]))
			),
			updated_at = NOW()`

	batch := &pgx.Batch{}
	addresses := make([]string, 0, len(changes))
	for accountAddress, change := range changes {
		addresses = append(addresses, accountAddress)

		addIDs := change.AddIDs
		if addIDs == nil {
			addIDs = []int64{}
		}
		addJSON, err := json.Marshal(addIDs)
		if err != nil {
			return fmt.Errorf("marshaling add IDs for %s: %w", accountAddress, err)
		}

		removeIDs := change.RemoveIDs
		if removeIDs == nil {
			removeIDs = []int64{}
		}
		batch.Queue(query, accountAddress, addJSON, removeIDs)
	}

	br := dbTx.SendBatch(ctx, batch)
	for range changes {
		if _, err := br.Exec(); err != nil {
			_ = br.Close() //nolint:errcheck // cleanup on error path
			return fmt.Errorf("upserting trustlines: %w", err)
		}
	}
	if err := br.Close(); err != nil {
		return fmt.Errorf("closing trustline batch: %w", err)
	}

	// Clean up accounts with empty arrays
	const cleanupQuery = `
		DELETE FROM account_trustlines
		WHERE account_address = ANY($1) AND asset_ids = '[]'::jsonb`

	_, err := dbTx.Exec(ctx, cleanupQuery, addresses)
	if err != nil {
		return fmt.Errorf("cleaning up empty trustlines: %w", err)
	}

	m.MetricsService.ObserveDBQueryDuration("BatchUpsertTrustlines", "account_trustlines", time.Since(start).Seconds())
	m.MetricsService.IncDBQuery("BatchUpsertTrustlines", "account_trustlines")
	return nil
}

// BatchAddContracts appends numeric contract IDs for multiple accounts (contracts never removed).
// Uses pgx.Batch for single round-trip efficiency.
func (m *AccountTokensModel) BatchAddContracts(ctx context.Context, dbTx pgx.Tx, contractsByAccount map[string][]int64) error {
	if len(contractsByAccount) == 0 {
		return nil
	}

	start := time.Now()

	const query = `
		INSERT INTO account_contracts (account_address, contract_ids, updated_at)
		VALUES ($1, $2::jsonb, NOW())
		ON CONFLICT (account_address) DO UPDATE SET
			contract_ids = (
				SELECT COALESCE(jsonb_agg(DISTINCT elem), '[]'::jsonb)
				FROM jsonb_array_elements(account_contracts.contract_ids || $2::jsonb) elem
			),
			updated_at = NOW()`

	batch := &pgx.Batch{}
	count := 0
	for accountAddress, contractIDs := range contractsByAccount {
		if len(contractIDs) == 0 {
			continue
		}
		idsJSON, err := json.Marshal(contractIDs)
		if err != nil {
			return fmt.Errorf("marshaling contract IDs for %s: %w", accountAddress, err)
		}
		batch.Queue(query, accountAddress, idsJSON)
		count++
	}

	if count == 0 {
		return nil
	}

	br := dbTx.SendBatch(ctx, batch)
	for i := 0; i < count; i++ {
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

// BulkInsertTrustlines performs bulk insert for initial population.
// Uses batch INSERT for efficient loading of many accounts at once.
func (m *AccountTokensModel) BulkInsertTrustlines(ctx context.Context, dbTx pgx.Tx, trustlinesByAccount map[string][]int64) error {
	if len(trustlinesByAccount) == 0 {
		return nil
	}

	start := time.Now()

	// Build batch data - use []string to avoid pgx encoding issues with [][]byte
	addresses := make([]string, 0, len(trustlinesByAccount))
	assetIDsJSONs := make([]string, 0, len(trustlinesByAccount))

	for accountAddress, assetIDs := range trustlinesByAccount {
		if len(assetIDs) == 0 {
			continue
		}

		jsonData, err := json.Marshal(assetIDs)
		if err != nil {
			return fmt.Errorf("marshaling asset IDs for %s: %w", accountAddress, err)
		}
		addresses = append(addresses, accountAddress)
		assetIDsJSONs = append(assetIDsJSONs, string(jsonData))
	}

	if len(addresses) == 0 {
		return nil
	}

	// Use UNNEST for efficient bulk insert
	const query = `
		INSERT INTO account_trustlines (account_address, asset_ids, updated_at)
		SELECT addr, ids::jsonb, NOW()
		FROM UNNEST($1::text[], $2::text[]) AS t(addr, ids)
		ON CONFLICT (account_address) DO UPDATE SET
			asset_ids = EXCLUDED.asset_ids,
			updated_at = NOW()`

	_, err := dbTx.Exec(ctx, query, addresses, assetIDsJSONs)
	if err != nil {
		return fmt.Errorf("bulk inserting trustlines: %w", err)
	}

	m.MetricsService.ObserveDBQueryDuration("BulkInsertTrustlines", "account_trustlines", time.Since(start).Seconds())
	m.MetricsService.IncDBQuery("BulkInsertTrustlines", "account_trustlines")
	return nil
}

// BulkInsertContracts performs bulk insert for initial population.
// Uses batch INSERT for efficient loading of many accounts at once.
func (m *AccountTokensModel) BulkInsertContracts(ctx context.Context, dbTx pgx.Tx, contractsByAccount map[string][]int64) error {
	if len(contractsByAccount) == 0 {
		return nil
	}

	start := time.Now()

	// Build batch data - use []string to avoid pgx encoding issues with [][]byte
	addresses := make([]string, 0, len(contractsByAccount))
	contractIDsJSONs := make([]string, 0, len(contractsByAccount))

	for accountAddress, contractIDs := range contractsByAccount {
		if len(contractIDs) == 0 {
			continue
		}

		jsonData, err := json.Marshal(contractIDs)
		if err != nil {
			return fmt.Errorf("marshaling contract IDs for %s: %w", accountAddress, err)
		}
		addresses = append(addresses, accountAddress)
		contractIDsJSONs = append(contractIDsJSONs, string(jsonData))
	}

	if len(addresses) == 0 {
		return nil
	}

	// Use UNNEST for efficient bulk insert
	const query = `
		INSERT INTO account_contracts (account_address, contract_ids, updated_at)
		SELECT addr, ids::jsonb, NOW()
		FROM UNNEST($1::text[], $2::text[]) AS t(addr, ids)
		ON CONFLICT (account_address) DO UPDATE SET
			contract_ids = EXCLUDED.contract_ids,
			updated_at = NOW()`

	_, err := dbTx.Exec(ctx, query, addresses, contractIDsJSONs)
	if err != nil {
		return fmt.Errorf("bulk inserting contracts: %w", err)
	}

	m.MetricsService.ObserveDBQueryDuration("BulkInsertContracts", "account_contracts", time.Since(start).Seconds())
	m.MetricsService.IncDBQuery("BulkInsertContracts", "account_contracts")
	return nil
}
