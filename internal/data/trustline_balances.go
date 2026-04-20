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

	"github.com/stellar/wallet-backend/internal/db"
	"github.com/stellar/wallet-backend/internal/indexer/types"
	"github.com/stellar/wallet-backend/internal/metrics"
	"github.com/stellar/wallet-backend/internal/utils"
)

// TrustlineBalance contains all fields for a trustline including asset metadata from JOIN.
type TrustlineBalance struct {
	AccountID          types.AddressBytea `db:"account_id"`
	AssetID            uuid.UUID          `db:"asset_id"`
	Code               string             `db:"code"`   // Asset code from trustline_assets table
	Issuer             string             `db:"issuer"` // Asset issuer from trustline_assets table
	Balance            int64              `db:"balance"`
	Limit              int64              `db:"trust_limit"`
	BuyingLiabilities  int64              `db:"buying_liabilities"`
	SellingLiabilities int64              `db:"selling_liabilities"`
	Flags              uint32             `db:"flags"`
	LedgerNumber       uint32             `db:"last_modified_ledger"`
}

// TrustlineBalanceModelInterface defines the interface for trustline balance operations.
type TrustlineBalanceModelInterface interface {
	// GetByAccount returns trustlines for an account ordered by asset_id. Pass nil
	// limit/cursor to fetch all rows, or provide them for keyset pagination so the
	// GraphQL balances connection can page without scanning the full account set.
	GetByAccount(ctx context.Context, accountAddress string, limit *int32, cursor *uuid.UUID, sortOrder SortOrder) ([]TrustlineBalance, error)

	// Write operations (for live ingestion)
	BatchUpsert(ctx context.Context, dbTx pgx.Tx, upserts []TrustlineBalance, deletes []TrustlineBalance) error

	// Batch operations (for initial population)
	BatchCopy(ctx context.Context, dbTx pgx.Tx, balances []TrustlineBalance) error
}

// TrustlineBalanceModel implements TrustlineBalanceModelInterface.
type TrustlineBalanceModel struct {
	DB      *pgxpool.Pool
	Metrics *metrics.DBMetrics
}

var _ TrustlineBalanceModelInterface = (*TrustlineBalanceModel)(nil)

// GetByAccount retrieves trustline balances for an account ordered by asset_id.
// Pass nil limit/cursor to fetch all rows; provide them for keyset pagination
// (the cursor is the last seen asset UUID from the previous page).
func (m *TrustlineBalanceModel) GetByAccount(ctx context.Context, accountAddress string, limit *int32, cursor *uuid.UUID, sortOrder SortOrder) ([]TrustlineBalance, error) {
	if accountAddress == "" {
		return nil, fmt.Errorf("empty account address")
	}

	query := `
		SELECT atb.account_id, atb.asset_id, ta.code, ta.issuer,
		       atb.balance, atb.trust_limit, atb.buying_liabilities,
		       atb.selling_liabilities, atb.flags, atb.last_modified_ledger
		FROM trustline_balances atb
		INNER JOIN trustline_assets ta ON ta.id = atb.asset_id
		WHERE atb.account_id = $1`
	args := []interface{}{types.AddressBytea(accountAddress)}
	argIndex := 2

	if cursor != nil {
		// Cursor comparisons mirror keyset pagination semantics:
		// - ASC pages fetch rows greater than the previous row
		// - DESC pages fetch rows less than the previous row
		op := ">"
		if sortOrder == DESC {
			op = "<"
		}
		query += fmt.Sprintf(" AND atb.asset_id %s $%d", op, argIndex)
		args = append(args, *cursor)
		argIndex++
	}

	if sortOrder == DESC {
		query += " ORDER BY atb.asset_id DESC"
	} else {
		query += " ORDER BY atb.asset_id ASC"
	}

	if limit != nil {
		query += fmt.Sprintf(" LIMIT $%d", argIndex)
		args = append(args, *limit)
	}

	start := time.Now()
	balances, err := db.QueryMany[TrustlineBalance](ctx, m.DB, query, args...)
	duration := time.Since(start).Seconds()
	m.Metrics.QueryDuration.WithLabelValues("GetByAccount", "trustline_balances").Observe(duration)
	m.Metrics.QueriesTotal.WithLabelValues("GetByAccount", "trustline_balances").Inc()
	if err != nil {
		m.Metrics.QueryErrors.WithLabelValues("GetByAccount", "trustline_balances", utils.GetDBErrorType(err)).Inc()
		return nil, fmt.Errorf("querying paginated trustline balances for %s: %w", accountAddress, err)
	}
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
			account_id, asset_id, balance, trust_limit,
			buying_liabilities, selling_liabilities, flags, last_modified_ledger
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
		ON CONFLICT (account_id, asset_id) DO UPDATE SET
			balance = EXCLUDED.balance,
			trust_limit = EXCLUDED.trust_limit,
			buying_liabilities = EXCLUDED.buying_liabilities,
			selling_liabilities = EXCLUDED.selling_liabilities,
			flags = EXCLUDED.flags,
			last_modified_ledger = EXCLUDED.last_modified_ledger`

	for _, tl := range upserts {
		batch.Queue(upsertQuery,
			tl.AccountID,
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
	const deleteQuery = `DELETE FROM trustline_balances WHERE account_id = $1 AND asset_id = $2`

	for _, tl := range deletes {
		batch.Queue(deleteQuery, tl.AccountID, tl.AssetID)
	}

	if batch.Len() == 0 {
		return nil
	}

	br := dbTx.SendBatch(ctx, batch)
	for i := 0; i < batch.Len(); i++ {
		if _, err := br.Exec(); err != nil {
			_ = br.Close() //nolint:errcheck // cleanup on error path
			m.Metrics.QueryDuration.WithLabelValues("BatchUpsert", "trustline_balances").Observe(time.Since(start).Seconds())
			m.Metrics.QueriesTotal.WithLabelValues("BatchUpsert", "trustline_balances").Inc()
			m.Metrics.QueryErrors.WithLabelValues("BatchUpsert", "trustline_balances", utils.GetDBErrorType(err)).Inc()
			return fmt.Errorf("upserting trustline balances: %w", err)
		}
	}
	if err := br.Close(); err != nil {
		m.Metrics.QueryDuration.WithLabelValues("BatchUpsert", "trustline_balances").Observe(time.Since(start).Seconds())
		m.Metrics.QueriesTotal.WithLabelValues("BatchUpsert", "trustline_balances").Inc()
		m.Metrics.QueryErrors.WithLabelValues("BatchUpsert", "trustline_balances", utils.GetDBErrorType(err)).Inc()
		return fmt.Errorf("closing trustline balance batch: %w", err)
	}

	m.Metrics.QueryDuration.WithLabelValues("BatchUpsert", "trustline_balances").Observe(time.Since(start).Seconds())
	m.Metrics.QueriesTotal.WithLabelValues("BatchUpsert", "trustline_balances").Inc()
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
			"account_id",
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
			accountIDBytes, err := tl.AccountID.Value()
			if err != nil {
				return nil, fmt.Errorf("converting account address to bytes: %w", err)
			}
			return []any{
				accountIDBytes,
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
		m.Metrics.QueryDuration.WithLabelValues("BatchCopy", "trustline_balances").Observe(time.Since(start).Seconds())
		m.Metrics.QueriesTotal.WithLabelValues("BatchCopy", "trustline_balances").Inc()
		m.Metrics.QueryErrors.WithLabelValues("BatchCopy", "trustline_balances", utils.GetDBErrorType(err)).Inc()
		return fmt.Errorf("batch inserting trustline balances via COPY: %w", err)
	}

	if int(copyCount) != len(balances) {
		m.Metrics.QueryDuration.WithLabelValues("BatchCopy", "trustline_balances").Observe(time.Since(start).Seconds())
		m.Metrics.QueriesTotal.WithLabelValues("BatchCopy", "trustline_balances").Inc()
		m.Metrics.QueryErrors.WithLabelValues("BatchCopy", "trustline_balances", "row_count_mismatch").Inc()
		return fmt.Errorf("expected %d rows copied, got %d", len(balances), copyCount)
	}

	m.Metrics.QueryDuration.WithLabelValues("BatchCopy", "trustline_balances").Observe(time.Since(start).Seconds())
	m.Metrics.QueriesTotal.WithLabelValues("BatchCopy", "trustline_balances").Inc()
	return nil
}
