// Package data provides data access layer for SAC balance operations.
// This file handles PostgreSQL storage of SAC (Stellar Asset Contract) balances for contract addresses.
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

// SACBalance contains SAC (Stellar Asset Contract) balance data for contract addresses.
// Only contract addresses (C...) have SAC balances stored here; G-addresses use trustlines.
type SACBalance struct {
	AccountAddress    string    // Contract address (C...)
	ContractID        uuid.UUID // Deterministic UUID for the SAC contract
	Balance           string    // Balance as string (handles i128 values)
	IsAuthorized      bool
	IsClawbackEnabled bool
	LedgerNumber      uint32
}

// SACBalanceModelInterface defines the interface for SAC balance operations.
type SACBalanceModelInterface interface {
	// Read operations (for API/balances queries)
	GetByAccount(ctx context.Context, accountAddress string) ([]SACBalance, error)

	// Write operations (for live ingestion)
	BatchUpsert(ctx context.Context, dbTx pgx.Tx, upserts []SACBalance, deletes []SACBalance) error

	// Batch operations (for initial population)
	BatchCopy(ctx context.Context, dbTx pgx.Tx, balances []SACBalance) error
}

// SACBalanceModel implements SACBalanceModelInterface.
type SACBalanceModel struct {
	DB             db.ConnectionPool
	MetricsService metrics.MetricsService
}

var _ SACBalanceModelInterface = (*SACBalanceModel)(nil)

// GetByAccount retrieves all SAC balances for a contract address.
func (m *SACBalanceModel) GetByAccount(ctx context.Context, accountAddress string) ([]SACBalance, error) {
	if accountAddress == "" {
		return nil, fmt.Errorf("empty account address")
	}

	const query = `
		SELECT contract_id, balance, is_authorized, is_clawback_enabled, last_modified_ledger
		FROM account_sac_balances
		WHERE account_address = $1`

	start := time.Now()
	rows, err := m.DB.PgxPool().Query(ctx, query, accountAddress)
	if err != nil {
		m.MetricsService.IncDBQueryError("GetByAccount", "account_sac_balances", "query_error")
		return nil, fmt.Errorf("querying SAC balances for %s: %w", accountAddress, err)
	}
	defer rows.Close()

	var balances []SACBalance
	for rows.Next() {
		var bal SACBalance
		if err := rows.Scan(&bal.ContractID, &bal.Balance, &bal.IsAuthorized, &bal.IsClawbackEnabled, &bal.LedgerNumber); err != nil {
			return nil, fmt.Errorf("scanning SAC balance: %w", err)
		}
		bal.AccountAddress = accountAddress
		balances = append(balances, bal)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating SAC balances: %w", err)
	}

	m.MetricsService.ObserveDBQueryDuration("GetByAccount", "account_sac_balances", time.Since(start).Seconds())
	m.MetricsService.IncDBQuery("GetByAccount", "account_sac_balances")
	return balances, nil
}

// BatchUpsert performs upserts and deletes for SAC balances.
// For upserts (ADD/UPDATE): inserts or updates balance with authorization flags.
// For deletes (REMOVE): removes the balance row.
func (m *SACBalanceModel) BatchUpsert(ctx context.Context, dbTx pgx.Tx, upserts []SACBalance, deletes []SACBalance) error {
	if len(upserts) == 0 && len(deletes) == 0 {
		return nil
	}

	start := time.Now()
	batch := &pgx.Batch{}

	// Upsert query: insert or update all fields
	const upsertQuery = `
		INSERT INTO account_sac_balances (
			account_address, contract_id, balance, is_authorized, is_clawback_enabled, last_modified_ledger
		) VALUES ($1, $2, $3, $4, $5, $6)
		ON CONFLICT (account_address, contract_id) DO UPDATE SET
			balance = EXCLUDED.balance,
			is_authorized = EXCLUDED.is_authorized,
			is_clawback_enabled = EXCLUDED.is_clawback_enabled,
			last_modified_ledger = EXCLUDED.last_modified_ledger`

	for _, bal := range upserts {
		batch.Queue(upsertQuery,
			bal.AccountAddress,
			bal.ContractID,
			bal.Balance,
			bal.IsAuthorized,
			bal.IsClawbackEnabled,
			bal.LedgerNumber,
		)
	}

	// Delete query
	const deleteQuery = `DELETE FROM account_sac_balances WHERE account_address = $1 AND contract_id = $2`

	for _, bal := range deletes {
		batch.Queue(deleteQuery, bal.AccountAddress, bal.ContractID)
	}

	if batch.Len() == 0 {
		return nil
	}

	br := dbTx.SendBatch(ctx, batch)
	for i := 0; i < batch.Len(); i++ {
		if _, err := br.Exec(); err != nil {
			_ = br.Close() //nolint:errcheck // cleanup on error path
			return fmt.Errorf("upserting SAC balances: %w", err)
		}
	}
	if err := br.Close(); err != nil {
		return fmt.Errorf("closing SAC balance batch: %w", err)
	}

	m.MetricsService.ObserveDBQueryDuration("BatchUpsert", "account_sac_balances", time.Since(start).Seconds())
	m.MetricsService.IncDBQuery("BatchUpsert", "account_sac_balances")
	return nil
}

// BatchCopy performs bulk insert using COPY protocol for speed during checkpoint population.
func (m *SACBalanceModel) BatchCopy(ctx context.Context, dbTx pgx.Tx, balances []SACBalance) error {
	if len(balances) == 0 {
		return nil
	}

	start := time.Now()

	// Build rows for COPY
	rows := make([][]any, len(balances))
	for i, bal := range balances {
		rows[i] = []any{
			bal.AccountAddress,
			bal.ContractID,
			bal.Balance,
			bal.IsAuthorized,
			bal.IsClawbackEnabled,
			bal.LedgerNumber,
		}
	}

	copyCount, err := dbTx.CopyFrom(
		ctx,
		pgx.Identifier{"account_sac_balances"},
		[]string{
			"account_address",
			"contract_id",
			"balance",
			"is_authorized",
			"is_clawback_enabled",
			"last_modified_ledger",
		},
		pgx.CopyFromRows(rows),
	)
	if err != nil {
		return fmt.Errorf("batch inserting SAC balances via COPY: %w", err)
	}

	if int(copyCount) != len(rows) {
		return fmt.Errorf("expected %d rows copied, got %d", len(rows), copyCount)
	}

	m.MetricsService.ObserveDBQueryDuration("BatchCopy", "account_sac_balances", time.Since(start).Seconds())
	m.MetricsService.IncDBQuery("BatchCopy", "account_sac_balances")
	return nil
}
