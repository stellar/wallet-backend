// Package data provides data access layer for trustline balance operations.
// This file handles PostgreSQL storage of account trustline balances.
package data

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/stellar/wallet-backend/internal/metrics"
)

// TrustlineBalance contains all fields for a trustline including asset metadata from JOIN.
type TrustlineBalance struct {
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

// TrustlineBalanceModelInterface defines the interface for trustline balance operations.
type TrustlineBalanceModelInterface interface {
	// Read operations (for API/balances queries)
	GetByAccount(ctx context.Context, accountAddress string) ([]TrustlineBalance, error)

	// Write operations (for live ingestion)
	BatchUpsert(ctx context.Context, dbTx pgx.Tx, upserts []TrustlineBalance, deletes []TrustlineBalance) error

	// Batch operations (for initial population)
	BatchCopy(ctx context.Context, dbTx pgx.Tx, balances []TrustlineBalance) error
}

// TrustlineBalanceModel implements TrustlineBalanceModelInterface.
type TrustlineBalanceModel struct {
	DB             *pgxpool.Pool
	MetricsService metrics.MetricsService
}

var _ TrustlineBalanceModelInterface = (*TrustlineBalanceModel)(nil)

// GetByAccount retrieves all trustline balances for an account with full data via JOIN.
func (m *TrustlineBalanceModel) GetByAccount(ctx context.Context, accountAddress string) ([]TrustlineBalance, error) {
	if accountAddress == "" {
		return nil, fmt.Errorf("empty account address")
	}

	const query = `
		SELECT atb.asset_id, ta.code, ta.issuer,
		       atb.balance, atb.trust_limit, atb.buying_liabilities,
		       atb.selling_liabilities, atb.flags, atb.last_modified_ledger
		FROM trustline_balances atb
		INNER JOIN trustline_assets ta ON ta.id = atb.asset_id
		WHERE atb.account_address = $1`

	start := time.Now()
	rows, err := m.DB.Query(ctx, query, accountAddress)
	if err != nil {
		m.MetricsService.IncDBQueryError("GetByAccount", "trustline_balances", "query_error")
		return nil, fmt.Errorf("querying trustline balances for %s: %w", accountAddress, err)
	}
	defer rows.Close()

	var balances []TrustlineBalance
	for rows.Next() {
		var tl TrustlineBalance
		if err := rows.Scan(&tl.AssetID, &tl.Code, &tl.Issuer, &tl.Balance, &tl.Limit,
			&tl.BuyingLiabilities, &tl.SellingLiabilities, &tl.Flags, &tl.LedgerNumber); err != nil {
			return nil, fmt.Errorf("scanning trustline balance: %w", err)
		}
		tl.AccountAddress = accountAddress
		balances = append(balances, tl)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating trustline balances: %w", err)
	}

	m.MetricsService.ObserveDBQueryDuration("GetByAccount", "trustline_balances", time.Since(start).Seconds())
	m.MetricsService.IncDBQuery("GetByAccount", "trustline_balances")
	return balances, nil
}

// BatchUpsert performs upserts and deletes with full XDR fields.
// For upserts (ADD/UPDATE): inserts or updates all trustline fields.
// For deletes (REMOVE): removes the trustline row.
func (m *TrustlineBalanceModel) BatchUpsert(ctx context.Context, dbTx pgx.Tx, upserts []TrustlineBalance, deletes []TrustlineBalance) error {
	if len(upserts) == 0 && len(deletes) == 0 {
		return nil
	}

	start := time.Now()
	batch := &pgx.Batch{}

	// Upsert query: insert or update all fields
	const upsertQuery = `
		INSERT INTO trustline_balances (
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
	const deleteQuery = `DELETE FROM trustline_balances WHERE account_address = $1 AND asset_id = $2`

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
			return fmt.Errorf("upserting trustline balances: %w", err)
		}
	}
	if err := br.Close(); err != nil {
		return fmt.Errorf("closing trustline balance batch: %w", err)
	}

	m.MetricsService.ObserveDBQueryDuration("BatchUpsert", "trustline_balances", time.Since(start).Seconds())
	m.MetricsService.IncDBQuery("BatchUpsert", "trustline_balances")
	return nil
}

// BatchCopy performs bulk insert using COPY protocol for speed.
func (m *TrustlineBalanceModel) BatchCopy(ctx context.Context, dbTx pgx.Tx, balances []TrustlineBalance) error {
	if len(balances) == 0 {
		return nil
	}

	start := time.Now()

	copyCount, err := dbTx.CopyFrom(
		ctx,
		pgx.Identifier{"trustline_balances"},
		[]string{
			"account_address",
			"asset_id",
			"balance",
			"trust_limit",
			"buying_liabilities",
			"selling_liabilities",
			"flags",
			"last_modified_ledger",
		},
		pgx.CopyFromSlice(len(balances), func(i int) ([]any, error) {
			tl := balances[i]
			return []any{
				tl.AccountAddress,
				tl.AssetID,
				tl.Balance,
				tl.Limit,
				tl.BuyingLiabilities,
				tl.SellingLiabilities,
				tl.Flags,
				tl.LedgerNumber,
			}, nil
		}),
	)
	if err != nil {
		return fmt.Errorf("batch inserting trustline balances via COPY: %w", err)
	}

	if int(copyCount) != len(balances) {
		return fmt.Errorf("expected %d rows copied, got %d", len(balances), copyCount)
	}

	m.MetricsService.ObserveDBQueryDuration("BatchCopy", "trustline_balances", time.Since(start).Seconds())
	m.MetricsService.IncDBQuery("BatchCopy", "trustline_balances")
	return nil
}
