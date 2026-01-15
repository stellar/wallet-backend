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

// Trustline contains all fields for a trustline including asset metadata from JOIN.
type Trustline struct {
	AccountAddress     string
	AssetID            uuid.UUID
	Code               string // Asset code from trustline_assets table
	Issuer             string // Asset issuer from trustline_assets table
	Balance            int64
	Limit              int64
	BuyingLiabilities  int64
	SellingLiabilities int64
	Flags              uint32
	LedgerNumber       uint32
}

// NativeBalance contains native XLM balance data for an account.
type NativeBalance struct {
	AccountAddress     string
	Balance            int64
	BuyingLiabilities  int64
	SellingLiabilities int64
	LedgerNumber       uint32
}

// AccountTokensModelInterface defines the interface for account token operations.
type AccountTokensModelInterface interface {
	// Trustline and contract tokens read operations (for API/balances queries)
	GetTrustlines(ctx context.Context, accountAddress string) ([]Trustline, error)
	GetContractIDs(ctx context.Context, accountAddress string) ([]uuid.UUID, error)
	GetNativeBalance(ctx context.Context, accountAddress string) (*NativeBalance, error)

	// Trustline and contract tokens write operations (for live ingestion)
	BatchUpsertTrustlines(ctx context.Context, dbTx pgx.Tx, upserts []Trustline, deletes []Trustline) error
	BatchUpsertNativeBalances(ctx context.Context, dbTx pgx.Tx, upserts []NativeBalance, deletes []string) error

	// Batch operations (for initial population and live ingestion)
	BatchInsertTrustlines(ctx context.Context, dbTx pgx.Tx, trustlines []Trustline) error
	BatchInsertContractTokens(ctx context.Context, dbTx pgx.Tx, contractsByAccount map[string][]uuid.UUID) error
	BulkInsertNativeBalances(ctx context.Context, dbTx pgx.Tx, balances []NativeBalance) error
}

// AccountTokensModel implements AccountTokensModelInterface.
type AccountTokensModel struct {
	DB             db.ConnectionPool
	MetricsService metrics.MetricsService
}

var _ AccountTokensModelInterface = (*AccountTokensModel)(nil)

// GetTrustlines retrieves all trustlines for an account with full data via JOIN.
func (m *AccountTokensModel) GetTrustlines(ctx context.Context, accountAddress string) ([]Trustline, error) {
	if accountAddress == "" {
		return nil, fmt.Errorf("empty account address")
	}

	const query = `
		SELECT at.asset_id, ta.code, ta.issuer,
		       at.balance, at.trust_limit, at.buying_liabilities,
		       at.selling_liabilities, at.flags, at.last_modified_ledger
		FROM account_trustlines at
		INNER JOIN trustline_assets ta ON ta.id = at.asset_id
		WHERE at.account_address = $1`

	start := time.Now()
	rows, err := m.DB.PgxPool().Query(ctx, query, accountAddress)
	if err != nil {
		m.MetricsService.IncDBQueryError("GetTrustlines", "account_trustlines", "query_error")
		return nil, fmt.Errorf("querying trustlines for %s: %w", accountAddress, err)
	}
	defer rows.Close()

	var trustlines []Trustline
	for rows.Next() {
		var tl Trustline
		if err := rows.Scan(&tl.AssetID, &tl.Code, &tl.Issuer, &tl.Balance, &tl.Limit,
			&tl.BuyingLiabilities, &tl.SellingLiabilities, &tl.Flags, &tl.LedgerNumber); err != nil {
			return nil, fmt.Errorf("scanning trustline: %w", err)
		}
		tl.AccountAddress = accountAddress
		trustlines = append(trustlines, tl)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating trustlines: %w", err)
	}

	m.MetricsService.ObserveDBQueryDuration("GetTrustlines", "account_trustlines", time.Since(start).Seconds())
	m.MetricsService.IncDBQuery("GetTrustlines", "account_trustlines")
	return trustlines, nil
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

// BatchUpsertTrustlines performs upserts and deletes with full XDR fields.
// For upserts (ADD/UPDATE): inserts or updates all trustline fields.
// For deletes (REMOVE): removes the trustline row.
func (m *AccountTokensModel) BatchUpsertTrustlines(ctx context.Context, dbTx pgx.Tx, upserts []Trustline, deletes []Trustline) error {
	if len(upserts) == 0 && len(deletes) == 0 {
		return nil
	}

	start := time.Now()
	batch := &pgx.Batch{}

	// Upsert query: insert or update all fields
	const upsertQuery = `
		INSERT INTO account_trustlines (
			account_address, asset_id, balance, trust_limit,
			buying_liabilities, selling_liabilities, flags, last_modified_ledger
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
		ON CONFLICT (account_address, asset_id) DO UPDATE SET
			balance = EXCLUDED.balance,
			trust_limit = EXCLUDED.trust_limit,
			buying_liabilities = EXCLUDED.buying_liabilities,
			selling_liabilities = EXCLUDED.selling_liabilities,
			flags = EXCLUDED.flags,
			last_modified_ledger = EXCLUDED.last_modified_ledger`

	for _, tl := range upserts {
		batch.Queue(upsertQuery,
			tl.AccountAddress,
			tl.AssetID,
			tl.Balance,
			tl.Limit,
			tl.BuyingLiabilities,
			tl.SellingLiabilities,
			tl.Flags,
			tl.LedgerNumber,
		)
	}

	// Delete query
	const deleteQuery = `DELETE FROM account_trustlines WHERE account_address = $1 AND asset_id = $2`

	for _, tl := range deletes {
		batch.Queue(deleteQuery, tl.AccountAddress, tl.AssetID)
	}

	if batch.Len() == 0 {
		return nil
	}

	br := dbTx.SendBatch(ctx, batch)
	for i := 0; i < batch.Len(); i++ {
		if _, err := br.Exec(); err != nil {
			_ = br.Close() //nolint:errcheck // cleanup on error path
			return fmt.Errorf("upserting trustlines with full data: %w", err)
		}
	}
	if err := br.Close(); err != nil {
		return fmt.Errorf("closing trustline full data batch: %w", err)
	}

	m.MetricsService.ObserveDBQueryDuration("BatchUpsertTrustlines", "account_trustlines", time.Since(start).Seconds())
	m.MetricsService.IncDBQuery("BatchUpsertTrustlines", "account_trustlines")
	return nil
}

// BatchInsertTrustlines performs bulk insert using COPY protocol for speed.
func (m *AccountTokensModel) BatchInsertTrustlines(ctx context.Context, dbTx pgx.Tx, trustlines []Trustline) error {
	if len(trustlines) == 0 {
		return nil
	}

	start := time.Now()

	// Build rows for COPY with all trustline fields
	rows := make([][]any, len(trustlines))
	for i, tl := range trustlines {
		rows[i] = []any{
			tl.AccountAddress,
			tl.AssetID,
			tl.Balance,
			tl.LedgerNumber,
			tl.Limit,
			tl.BuyingLiabilities,
			tl.SellingLiabilities,
			tl.Flags,
		}
	}

	copyCount, err := dbTx.CopyFrom(
		ctx,
		pgx.Identifier{"account_trustlines"},
		[]string{
			"account_address",
			"asset_id",
			"balance",
			"last_modified_ledger",
			"trust_limit",
			"buying_liabilities",
			"selling_liabilities",
			"flags",
		},
		pgx.CopyFromRows(rows),
	)
	if err != nil {
		return fmt.Errorf("batch inserting trustlines via COPY: %w", err)
	}

	if int(copyCount) != len(rows) {
		return fmt.Errorf("expected %d rows copied, got %d", len(rows), copyCount)
	}

	m.MetricsService.ObserveDBQueryDuration("BatchInsertTrustlines", "account_trustlines", time.Since(start).Seconds())
	m.MetricsService.IncDBQuery("BatchInsertTrustlines", "account_trustlines")
	return nil
}

// BatchInsertContractTokens inserts contract IDs for multiple accounts (contracts are never removed).
func (m *AccountTokensModel) BatchInsertContractTokens(ctx context.Context, dbTx pgx.Tx, contractsByAccount map[string][]uuid.UUID) error {
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
		return fmt.Errorf("batch inserting contract tokens: %w", err)
	}

	m.MetricsService.ObserveDBQueryDuration("BatchInsertContractTokens", "account_contracts", time.Since(start).Seconds())
	m.MetricsService.IncDBQuery("BatchInsertContractTokens", "account_contracts")
	return nil
}

// GetNativeBalance retrieves native XLM balance for an account.
func (m *AccountTokensModel) GetNativeBalance(ctx context.Context, accountAddress string) (*NativeBalance, error) {
	if accountAddress == "" {
		return nil, fmt.Errorf("empty account address")
	}

	const query = `
		SELECT account_address, balance, buying_liabilities, selling_liabilities, last_modified_ledger
		FROM account_native_balances
		WHERE account_address = $1`

	start := time.Now()
	row := m.DB.PgxPool().QueryRow(ctx, query, accountAddress)

	var nb NativeBalance
	err := row.Scan(&nb.AccountAddress, &nb.Balance, &nb.BuyingLiabilities, &nb.SellingLiabilities, &nb.LedgerNumber)
	if err != nil {
		if err.Error() == "no rows in result set" {
			return nil, nil // Account not found (not funded)
		}
		m.MetricsService.IncDBQueryError("GetNativeBalance", "account_native_balances", "query_error")
		return nil, fmt.Errorf("querying native balance for %s: %w", accountAddress, err)
	}

	m.MetricsService.ObserveDBQueryDuration("GetNativeBalance", "account_native_balances", time.Since(start).Seconds())
	m.MetricsService.IncDBQuery("GetNativeBalance", "account_native_balances")
	return &nb, nil
}

// BatchUpsertNativeBalances upserts and deletes native balances in batch.
func (m *AccountTokensModel) BatchUpsertNativeBalances(ctx context.Context, dbTx pgx.Tx, upserts []NativeBalance, deletes []string) error {
	if len(upserts) == 0 && len(deletes) == 0 {
		return nil
	}

	start := time.Now()
	batch := &pgx.Batch{}

	const upsertQuery = `
		INSERT INTO account_native_balances (account_address, balance, buying_liabilities, selling_liabilities, last_modified_ledger)
		VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT (account_address) DO UPDATE SET
			balance = EXCLUDED.balance,
			buying_liabilities = EXCLUDED.buying_liabilities,
			selling_liabilities = EXCLUDED.selling_liabilities,
			last_modified_ledger = EXCLUDED.last_modified_ledger`

	for _, nb := range upserts {
		batch.Queue(upsertQuery, nb.AccountAddress, nb.Balance, nb.BuyingLiabilities, nb.SellingLiabilities, nb.LedgerNumber)
	}

	const deleteQuery = `DELETE FROM account_native_balances WHERE account_address = $1`
	for _, addr := range deletes {
		batch.Queue(deleteQuery, addr)
	}

	if batch.Len() == 0 {
		return nil
	}

	br := dbTx.SendBatch(ctx, batch)
	for i := 0; i < batch.Len(); i++ {
		if _, err := br.Exec(); err != nil {
			_ = br.Close() //nolint:errcheck // cleanup on error path
			return fmt.Errorf("upserting native balances: %w", err)
		}
	}
	if err := br.Close(); err != nil {
		return fmt.Errorf("closing native balance batch: %w", err)
	}

	m.MetricsService.ObserveDBQueryDuration("BatchUpsertNativeBalances", "account_native_balances", time.Since(start).Seconds())
	m.MetricsService.IncDBQuery("BatchUpsertNativeBalances", "account_native_balances")
	return nil
}

// BulkInsertNativeBalances performs bulk insert for initial checkpoint population.
func (m *AccountTokensModel) BulkInsertNativeBalances(ctx context.Context, dbTx pgx.Tx, balances []NativeBalance) error {
	if len(balances) == 0 {
		return nil
	}

	start := time.Now()

	var rows [][]any
	for _, nb := range balances {
		rows = append(rows, []any{nb.AccountAddress, nb.Balance, nb.BuyingLiabilities, nb.SellingLiabilities, nb.LedgerNumber})
	}

	copyCount, err := dbTx.CopyFrom(
		ctx,
		pgx.Identifier{"account_native_balances"},
		[]string{"account_address", "balance", "buying_liabilities", "selling_liabilities", "last_modified_ledger"},
		pgx.CopyFromRows(rows),
	)
	if err != nil {
		return fmt.Errorf("bulk inserting native balances via COPY: %w", err)
	}

	if int(copyCount) != len(rows) {
		return fmt.Errorf("expected %d rows copied, got %d", len(rows), copyCount)
	}

	m.MetricsService.ObserveDBQueryDuration("BulkInsertNativeBalances", "account_native_balances", time.Since(start).Seconds())
	m.MetricsService.IncDBQuery("BulkInsertNativeBalances", "account_native_balances")
	return nil
}
