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
	"github.com/stellar/wallet-backend/internal/indexer/types"
	"github.com/stellar/wallet-backend/internal/metrics"
	"github.com/stellar/wallet-backend/internal/utils"
)

// SACBalance contains SAC (Stellar Asset Contract) balance data for contract addresses.
// Only contract addresses (C...) have SAC balances stored here; G-addresses use trustlines.
// Includes contract metadata from JOIN with contract_tokens table for API responses.
type SACBalance struct {
	AccountID         types.AddressBytea `db:"account_id"`  // Contract address (C...) of the holder
	ContractID        uuid.UUID          `db:"contract_id"` // Deterministic UUID for the SAC contract
	Balance           string             `db:"balance"`     // Balance as string (handles i128 values)
	IsAuthorized      bool               `db:"is_authorized"`
	IsClawbackEnabled bool               `db:"is_clawback_enabled"`
	LedgerNumber      uint32             `db:"last_modified_ledger"`
	// Contract metadata from JOIN with contract_tokens
	TokenID  string `db:"token_id"` // SAC contract address (C...) used as token identifier in API; aliased from ct.contract_id
	Code     string `db:"code"`     // Asset code (e.g., "USDC")
	Issuer   string `db:"issuer"`   // Asset issuer G-address
	Decimals uint32 `db:"decimals"` // Token decimals
}

// SACBalanceModelInterface defines the interface for SAC balance operations.
type SACBalanceModelInterface interface {
	// GetByAccount returns SAC balances for a contract holder ordered by
	// contract_id. Pass nil limit/cursor to fetch all rows, or provide them for
	// keyset pagination so GraphQL can page with stable cursors.
	GetByAccount(ctx context.Context, accountAddress string, limit *int32, cursor *uuid.UUID, sortOrder SortOrder) ([]SACBalance, error)

	// Write operations (for live ingestion)
	BatchUpsert(ctx context.Context, dbTx pgx.Tx, upserts []SACBalance, deletes []SACBalance) error

	// Batch operations (for initial population)
	BatchCopy(ctx context.Context, dbTx pgx.Tx, balances []SACBalance) error
}

// SACBalanceModel implements SACBalanceModelInterface.
type SACBalanceModel struct {
	DB      *pgxpool.Pool
	Metrics *metrics.DBMetrics
}

var _ SACBalanceModelInterface = (*SACBalanceModel)(nil)

// GetByAccount retrieves SAC balances for a contract holder ordered by
// contract_id, JOINed with contract_tokens for code/issuer/decimals. Pass nil
// limit/cursor to fetch all rows; provide them for keyset pagination (the
// cursor is the last seen contract UUID from the previous page).
func (m *SACBalanceModel) GetByAccount(ctx context.Context, accountAddress string, limit *int32, cursor *uuid.UUID, sortOrder SortOrder) ([]SACBalance, error) {
	if accountAddress == "" {
		return nil, fmt.Errorf("empty account address")
	}

	query := `
		SELECT asb.account_id, asb.contract_id, asb.balance, asb.is_authorized,
		       asb.is_clawback_enabled, asb.last_modified_ledger,
		       ct.contract_id AS token_id, ct.code, ct.issuer, ct.decimals
		FROM sac_balances asb
		INNER JOIN contract_tokens ct ON ct.id = asb.contract_id
		WHERE asb.account_id = $1`
	args := []interface{}{types.AddressBytea(accountAddress)}
	argIndex := 2

	if cursor != nil {
		// Cursor comparisons implement keyset pagination over the primary key.
		op := ">"
		if sortOrder == DESC {
			op = "<"
		}
		query += fmt.Sprintf(" AND asb.contract_id %s $%d", op, argIndex)
		args = append(args, *cursor)
		argIndex++
	}

	if sortOrder == DESC {
		query += " ORDER BY asb.contract_id DESC"
	} else {
		query += " ORDER BY asb.contract_id ASC"
	}

	if limit != nil {
		query += fmt.Sprintf(" LIMIT $%d", argIndex)
		args = append(args, *limit)
	}

	start := time.Now()
	balances, err := db.QueryMany[SACBalance](ctx, m.DB, query, args...)
	duration := time.Since(start).Seconds()
	m.Metrics.QueryDuration.WithLabelValues("GetByAccount", "sac_balances").Observe(duration)
	m.Metrics.QueriesTotal.WithLabelValues("GetByAccount", "sac_balances").Inc()
	if err != nil {
		m.Metrics.QueryErrors.WithLabelValues("GetByAccount", "sac_balances", utils.GetDBErrorType(err)).Inc()
		return nil, fmt.Errorf("querying paginated SAC balances for %s: %w", accountAddress, err)
	}
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
			account_id, contract_id, balance, is_authorized, is_clawback_enabled, last_modified_ledger
		) VALUES ($1, $2, $3, $4, $5, $6)
		ON CONFLICT (account_id, contract_id) DO UPDATE SET
			balance = EXCLUDED.balance,
			is_authorized = EXCLUDED.is_authorized,
			is_clawback_enabled = EXCLUDED.is_clawback_enabled,
			last_modified_ledger = EXCLUDED.last_modified_ledger`

	for _, bal := range upserts {
		batch.Queue(upsertQuery,
			bal.AccountID,
			bal.ContractID,
			bal.Balance,
			bal.IsAuthorized,
			bal.IsClawbackEnabled,
			bal.LedgerNumber,
		)
	}

	// Delete query
	const deleteQuery = `DELETE FROM sac_balances WHERE account_id = $1 AND contract_id = $2`

	for _, bal := range deletes {
		batch.Queue(deleteQuery, bal.AccountID, bal.ContractID)
	}

	if batch.Len() == 0 {
		return nil
	}

	br := dbTx.SendBatch(ctx, batch)
	for i := 0; i < batch.Len(); i++ {
		if _, err := br.Exec(); err != nil {
			_ = br.Close() //nolint:errcheck // cleanup on error path
			m.Metrics.QueryDuration.WithLabelValues("BatchUpsert", "sac_balances").Observe(time.Since(start).Seconds())
			m.Metrics.QueriesTotal.WithLabelValues("BatchUpsert", "sac_balances").Inc()
			m.Metrics.QueryErrors.WithLabelValues("BatchUpsert", "sac_balances", utils.GetDBErrorType(err)).Inc()
			return fmt.Errorf("upserting SAC balances: %w", err)
		}
	}
	if err := br.Close(); err != nil {
		m.Metrics.QueryDuration.WithLabelValues("BatchUpsert", "sac_balances").Observe(time.Since(start).Seconds())
		m.Metrics.QueriesTotal.WithLabelValues("BatchUpsert", "sac_balances").Inc()
		m.Metrics.QueryErrors.WithLabelValues("BatchUpsert", "sac_balances", utils.GetDBErrorType(err)).Inc()
		return fmt.Errorf("closing SAC balance batch: %w", err)
	}

	m.Metrics.QueryDuration.WithLabelValues("BatchUpsert", "sac_balances").Observe(time.Since(start).Seconds())
	m.Metrics.QueriesTotal.WithLabelValues("BatchUpsert", "sac_balances").Inc()
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
			"account_id",
			"contract_id",
			"balance",
			"is_authorized",
			"is_clawback_enabled",
			"last_modified_ledger",
		},
		pgx.CopyFromSlice(len(balances), func(i int) ([]any, error) {
			bal := balances[i]
			accountIDBytes, err := bal.AccountID.Value()
			if err != nil {
				return nil, fmt.Errorf("converting account address to bytes: %w", err)
			}
			return []any{
				accountIDBytes,
				bal.ContractID,
				bal.Balance,
				bal.IsAuthorized,
				bal.IsClawbackEnabled,
				bal.LedgerNumber,
			}, nil
		}),
	)
	if err != nil {
		m.Metrics.QueryDuration.WithLabelValues("BatchCopy", "sac_balances").Observe(time.Since(start).Seconds())
		m.Metrics.QueriesTotal.WithLabelValues("BatchCopy", "sac_balances").Inc()
		m.Metrics.QueryErrors.WithLabelValues("BatchCopy", "sac_balances", utils.GetDBErrorType(err)).Inc()
		return fmt.Errorf("batch inserting SAC balances via COPY: %w", err)
	}

	if int(copyCount) != len(balances) {
		m.Metrics.QueryDuration.WithLabelValues("BatchCopy", "sac_balances").Observe(time.Since(start).Seconds())
		m.Metrics.QueriesTotal.WithLabelValues("BatchCopy", "sac_balances").Inc()
		m.Metrics.QueryErrors.WithLabelValues("BatchCopy", "sac_balances", "row_count_mismatch").Inc()
		return fmt.Errorf("expected %d rows copied, got %d", len(balances), copyCount)
	}

	m.Metrics.QueryDuration.WithLabelValues("BatchCopy", "sac_balances").Observe(time.Since(start).Seconds())
	m.Metrics.QueriesTotal.WithLabelValues("BatchCopy", "sac_balances").Inc()
	return nil
}
