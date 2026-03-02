// Package data provides data access layer for SAC balance operations.
// This file handles PostgreSQL storage of SAC (Stellar Asset Contract) balances for contract addresses.
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

// SACBalance contains SAC (Stellar Asset Contract) balance data for contract addresses.
// Only contract addresses (C...) have SAC balances stored here; G-addresses use trustlines.
// Includes contract metadata from JOIN with contract_tokens table for API responses.
type SACBalance struct {
	AccountAddress    string    `db:"account_address"` // Contract address (C...) of the holder
	ContractID        uuid.UUID `db:"contract_id"`     // Deterministic UUID for the SAC contract
	Balance           string    `db:"balance"`         // Balance as string (handles i128 values)
	IsAuthorized      bool      `db:"is_authorized"`
	IsClawbackEnabled bool      `db:"is_clawback_enabled"`
	LedgerNumber      uint32    `db:"last_modified_ledger"`
	// Contract metadata from JOIN with contract_tokens
	TokenID  string `db:"token_id"` // SAC contract address (C...) used as token identifier in API; aliased from ct.contract_id
	Code     string `db:"code"`     // Asset code (e.g., "USDC")
	Issuer   string `db:"issuer"`   // Asset issuer G-address
	Decimals uint32 `db:"decimals"` // Token decimals
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
	DB             *pgxpool.Pool
	MetricsService metrics.MetricsService
}

var _ SACBalanceModelInterface = (*SACBalanceModel)(nil)

// GetByAccount retrieves all SAC balances for a contract address with contract metadata.
// JOINs with contract_tokens to get code, issuer, and decimals for API responses.
func (m *SACBalanceModel) GetByAccount(ctx context.Context, accountAddress string) ([]SACBalance, error) {
	if accountAddress == "" {
		return nil, fmt.Errorf("empty account address")
	}

	const query = `
		SELECT asb.account_address, asb.contract_id, asb.balance, asb.is_authorized,
		       asb.is_clawback_enabled, asb.last_modified_ledger,
		       ct.contract_id AS token_id, ct.code, ct.issuer, ct.decimals
		FROM sac_balances asb
		INNER JOIN contract_tokens ct ON ct.id = asb.contract_id
		WHERE asb.account_address = $1`

	start := time.Now()
	balances, err := db.QueryMany[SACBalance](ctx, m.DB, query, accountAddress)
	duration := time.Since(start).Seconds()
	m.MetricsService.ObserveDBQueryDuration("GetByAccount", "sac_balances", duration)
	if err != nil {
		m.MetricsService.IncDBQueryError("GetByAccount", "sac_balances", "query_error")
		return nil, fmt.Errorf("querying SAC balances for %s: %w", accountAddress, err)
	}
	m.MetricsService.IncDBQuery("GetByAccount", "sac_balances")
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
		INSERT INTO sac_balances (
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
	const deleteQuery = `DELETE FROM sac_balances WHERE account_address = $1 AND contract_id = $2`

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

	m.MetricsService.ObserveDBQueryDuration("BatchUpsert", "sac_balances", time.Since(start).Seconds())
	m.MetricsService.IncDBQuery("BatchUpsert", "sac_balances")
	return nil
}

// BatchCopy performs bulk insert using COPY protocol for speed during checkpoint population.
func (m *SACBalanceModel) BatchCopy(ctx context.Context, dbTx pgx.Tx, balances []SACBalance) error {
	if len(balances) == 0 {
		return nil
	}

	start := time.Now()

	copyCount, err := dbTx.CopyFrom(
		ctx,
		pgx.Identifier{"sac_balances"},
		[]string{
			"account_address",
			"contract_id",
			"balance",
			"is_authorized",
			"is_clawback_enabled",
			"last_modified_ledger",
		},
		pgx.CopyFromSlice(len(balances), func(i int) ([]any, error) {
			bal := balances[i]
			return []any{
				bal.AccountAddress,
				bal.ContractID,
				bal.Balance,
				bal.IsAuthorized,
				bal.IsClawbackEnabled,
				bal.LedgerNumber,
			}, nil
		}),
	)
	if err != nil {
		return fmt.Errorf("batch inserting SAC balances via COPY: %w", err)
	}

	if int(copyCount) != len(balances) {
		return fmt.Errorf("expected %d rows copied, got %d", len(balances), copyCount)
	}

	m.MetricsService.ObserveDBQueryDuration("BatchCopy", "sac_balances", time.Since(start).Seconds())
	m.MetricsService.IncDBQuery("BatchCopy", "sac_balances")
	return nil
}
