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
	"github.com/stellar/wallet-backend/internal/metrics"
	"github.com/stellar/wallet-backend/internal/utils"
)

// TrustlineBalance contains all fields for a trustline including asset metadata from JOIN.
type TrustlineBalance struct {
	AccountAddress     string    `db:"account_address"`
	AssetID            uuid.UUID `db:"asset_id"`
	Code               string    `db:"code"`   // Asset code from trustline_assets table
	Issuer             string    `db:"issuer"` // Asset issuer from trustline_assets table
	Balance            int64     `db:"balance"`
	Limit              int64     `db:"trust_limit"`
	BuyingLiabilities  int64     `db:"buying_liabilities"`
	SellingLiabilities int64     `db:"selling_liabilities"`
	Flags              uint32    `db:"flags"`
	LedgerNumber       uint32    `db:"last_modified_ledger"`
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
	DB      *pgxpool.Pool
	Metrics *metrics.DBMetrics
}

var _ TrustlineBalanceModelInterface = (*TrustlineBalanceModel)(nil)

// GetByAccount retrieves all trustline balances for an account with full data via JOIN.
func (m *TrustlineBalanceModel) GetByAccount(ctx context.Context, accountAddress string) ([]TrustlineBalance, error) {
	if accountAddress == "" {
		return nil, fmt.Errorf("empty account address")
	}

	const query = `
		SELECT atb.account_address, atb.asset_id, ta.code, ta.issuer,
		       atb.balance, atb.trust_limit, atb.buying_liabilities,
		       atb.selling_liabilities, atb.flags, atb.last_modified_ledger
		FROM trustline_balances atb
		INNER JOIN trustline_assets ta ON ta.id = atb.asset_id
		WHERE atb.account_address = $1`

	start := time.Now()
	balances, err := db.QueryMany[TrustlineBalance](ctx, m.DB, query, accountAddress)
	duration := time.Since(start).Seconds()
	m.Metrics.QueryDuration.WithLabelValues("GetByAccount", "trustline_balances").Observe(duration)
	m.Metrics.QueriesTotal.WithLabelValues("GetByAccount", "trustline_balances").Inc()
	if err != nil {
		m.Metrics.QueryErrors.WithLabelValues("GetByAccount", "trustline_balances", utils.GetDBErrorType(err)).Inc()
		return nil, fmt.Errorf("querying trustline balances for %s: %w", accountAddress, err)
	}
	return balances, nil
}

// BatchUpsert performs upserts and deletes using UNNEST-based bulk operations.
// For upserts (ADD/UPDATE): bulk inserts or updates all trustline fields via single UNNEST query.
// For deletes (REMOVE): bulk removes trustline rows via single UNNEST query.
func (m *TrustlineBalanceModel) BatchUpsert(ctx context.Context, dbTx pgx.Tx, upserts []TrustlineBalance, deletes []TrustlineBalance) error {
	if len(upserts) == 0 && len(deletes) == 0 {
		return nil
	}

	start := time.Now()

	if len(upserts) > 0 {
		accountAddresses := make([]string, len(upserts))
		assetIDs := make([]uuid.UUID, len(upserts))
		balances := make([]int64, len(upserts))
		trustLimits := make([]int64, len(upserts))
		buyingLiabilities := make([]int64, len(upserts))
		sellingLiabilities := make([]int64, len(upserts))
		flags := make([]int32, len(upserts))
		ledgerNumbers := make([]int64, len(upserts))

		for i, tl := range upserts {
			accountAddresses[i] = tl.AccountAddress
			assetIDs[i] = tl.AssetID
			balances[i] = tl.Balance
			trustLimits[i] = tl.Limit
			buyingLiabilities[i] = tl.BuyingLiabilities
			sellingLiabilities[i] = tl.SellingLiabilities
			flags[i] = int32(tl.Flags)
			ledgerNumbers[i] = int64(tl.LedgerNumber)
		}

		const upsertQuery = `
			INSERT INTO trustline_balances (
				account_address, asset_id, balance, trust_limit,
				buying_liabilities, selling_liabilities, flags, last_modified_ledger
			)
			SELECT * FROM UNNEST(
				$1::text[], $2::uuid[], $3::bigint[], $4::bigint[],
				$5::bigint[], $6::bigint[], $7::int4[], $8::bigint[]
			)
			ON CONFLICT (account_address, asset_id) DO UPDATE SET
				balance = EXCLUDED.balance,
				trust_limit = EXCLUDED.trust_limit,
				buying_liabilities = EXCLUDED.buying_liabilities,
				selling_liabilities = EXCLUDED.selling_liabilities,
				flags = EXCLUDED.flags,
				last_modified_ledger = EXCLUDED.last_modified_ledger`

		if _, err := dbTx.Exec(ctx, upsertQuery,
			accountAddresses, assetIDs, balances, trustLimits,
			buyingLiabilities, sellingLiabilities, flags, ledgerNumbers,
		); err != nil {
			m.Metrics.QueryDuration.WithLabelValues("BatchUpsert", "trustline_balances").Observe(time.Since(start).Seconds())
			m.Metrics.QueriesTotal.WithLabelValues("BatchUpsert", "trustline_balances").Inc()
			m.Metrics.QueryErrors.WithLabelValues("BatchUpsert", "trustline_balances", utils.GetDBErrorType(err)).Inc()
			return fmt.Errorf("upserting trustline balances: %w", err)
		}
	}

	if len(deletes) > 0 {
		delAccountAddresses := make([]string, len(deletes))
		delAssetIDs := make([]uuid.UUID, len(deletes))

		for i, tl := range deletes {
			delAccountAddresses[i] = tl.AccountAddress
			delAssetIDs[i] = tl.AssetID
		}

		const deleteQuery = `
			DELETE FROM trustline_balances
			WHERE (account_address, asset_id) IN (
				SELECT * FROM UNNEST($1::text[], $2::uuid[])
			)`

		if _, err := dbTx.Exec(ctx, deleteQuery, delAccountAddresses, delAssetIDs); err != nil {
			m.Metrics.QueryDuration.WithLabelValues("BatchUpsert", "trustline_balances").Observe(time.Since(start).Seconds())
			m.Metrics.QueriesTotal.WithLabelValues("BatchUpsert", "trustline_balances").Inc()
			m.Metrics.QueryErrors.WithLabelValues("BatchUpsert", "trustline_balances", utils.GetDBErrorType(err)).Inc()
			return fmt.Errorf("deleting trustline balances: %w", err)
		}
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
	defer func() {
		m.Metrics.QueryDuration.WithLabelValues("BatchCopy", "trustline_balances").Observe(time.Since(start).Seconds())
		m.Metrics.QueriesTotal.WithLabelValues("BatchCopy", "trustline_balances").Inc()
	}()

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
		m.Metrics.QueryErrors.WithLabelValues("BatchCopy", "trustline_balances", utils.GetDBErrorType(err)).Inc()
		return fmt.Errorf("batch inserting trustline balances via COPY: %w", err)
	}

	if int(copyCount) != len(balances) {
		m.Metrics.QueryErrors.WithLabelValues("BatchCopy", "trustline_balances", "row_count_mismatch").Inc()
		return fmt.Errorf("expected %d rows copied, got %d", len(balances), copyCount)
	}

	return nil
}
