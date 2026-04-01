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
	"github.com/stellar/wallet-backend/internal/utils"
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
	DB      *pgxpool.Pool
	Metrics *metrics.DBMetrics
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
	m.Metrics.QueryDuration.WithLabelValues("GetByAccount", "sac_balances").Observe(duration)
	m.Metrics.QueriesTotal.WithLabelValues("GetByAccount", "sac_balances").Inc()
	if err != nil {
		m.Metrics.QueryErrors.WithLabelValues("GetByAccount", "sac_balances", utils.GetDBErrorType(err)).Inc()
		return nil, fmt.Errorf("querying SAC balances for %s: %w", accountAddress, err)
	}
	return balances, nil
}

// BatchUpsert performs upserts and deletes for SAC balances using UNNEST-based bulk operations.
// For upserts (ADD/UPDATE): bulk inserts or updates balance with authorization flags via single UNNEST query.
// For deletes (REMOVE): bulk removes balance rows via single UNNEST query.
func (m *SACBalanceModel) BatchUpsert(ctx context.Context, dbTx pgx.Tx, upserts []SACBalance, deletes []SACBalance) error {
	if len(upserts) == 0 && len(deletes) == 0 {
		return nil
	}

	start := time.Now()

	if len(upserts) > 0 {
		accountAddresses := make([]string, len(upserts))
		contractIDs := make([]uuid.UUID, len(upserts))
		balances := make([]string, len(upserts))
		isAuthorized := make([]bool, len(upserts))
		isClawbackEnabled := make([]bool, len(upserts))
		ledgerNumbers := make([]int32, len(upserts))

		for i, bal := range upserts {
			accountAddresses[i] = bal.AccountAddress
			contractIDs[i] = bal.ContractID
			balances[i] = bal.Balance
			isAuthorized[i] = bal.IsAuthorized
			isClawbackEnabled[i] = bal.IsClawbackEnabled
			ledgerNumbers[i] = int32(bal.LedgerNumber)
		}

		const upsertQuery = `
			INSERT INTO sac_balances (
				account_address, contract_id, balance,
				is_authorized, is_clawback_enabled, last_modified_ledger
			)
			SELECT * FROM UNNEST(
				$1::text[], $2::uuid[], $3::text[],
				$4::boolean[], $5::boolean[], $6::int4[]
			)
			ON CONFLICT (account_address, contract_id) DO UPDATE SET
				balance = EXCLUDED.balance,
				is_authorized = EXCLUDED.is_authorized,
				is_clawback_enabled = EXCLUDED.is_clawback_enabled,
				last_modified_ledger = EXCLUDED.last_modified_ledger`

		if _, err := dbTx.Exec(ctx, upsertQuery,
			accountAddresses, contractIDs, balances,
			isAuthorized, isClawbackEnabled, ledgerNumbers,
		); err != nil {
			m.Metrics.QueryDuration.WithLabelValues("BatchUpsert", "sac_balances").Observe(time.Since(start).Seconds())
			m.Metrics.QueriesTotal.WithLabelValues("BatchUpsert", "sac_balances").Inc()
			m.Metrics.QueryErrors.WithLabelValues("BatchUpsert", "sac_balances", utils.GetDBErrorType(err)).Inc()
			return fmt.Errorf("upserting SAC balances: %w", err)
		}
	}

	if len(deletes) > 0 {
		delAccountAddresses := make([]string, len(deletes))
		delContractIDs := make([]uuid.UUID, len(deletes))

		for i, bal := range deletes {
			delAccountAddresses[i] = bal.AccountAddress
			delContractIDs[i] = bal.ContractID
		}

		const deleteQuery = `
			DELETE FROM sac_balances
			WHERE (account_address, contract_id) IN (
				SELECT * FROM UNNEST($1::text[], $2::uuid[])
			)`

		if _, err := dbTx.Exec(ctx, deleteQuery, delAccountAddresses, delContractIDs); err != nil {
			m.Metrics.QueryDuration.WithLabelValues("BatchUpsert", "sac_balances").Observe(time.Since(start).Seconds())
			m.Metrics.QueriesTotal.WithLabelValues("BatchUpsert", "sac_balances").Inc()
			m.Metrics.QueryErrors.WithLabelValues("BatchUpsert", "sac_balances", utils.GetDBErrorType(err)).Inc()
			return fmt.Errorf("deleting SAC balances: %w", err)
		}
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
	defer func() {
		m.Metrics.QueryDuration.WithLabelValues("BatchCopy", "sac_balances").Observe(time.Since(start).Seconds())
		m.Metrics.QueriesTotal.WithLabelValues("BatchCopy", "sac_balances").Inc()
	}()

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
		m.Metrics.QueryErrors.WithLabelValues("BatchCopy", "sac_balances", utils.GetDBErrorType(err)).Inc()
		return fmt.Errorf("batch inserting SAC balances via COPY: %w", err)
	}

	if int(copyCount) != len(balances) {
		m.Metrics.QueryErrors.WithLabelValues("BatchCopy", "sac_balances", "row_count_mismatch").Inc()
		return fmt.Errorf("expected %d rows copied, got %d", len(balances), copyCount)
	}

	return nil
}
