// Package data provides data access layer for native XLM balance operations.
// This file handles PostgreSQL storage of account native balances.
package data

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/stellar/wallet-backend/internal/db"
	"github.com/stellar/wallet-backend/internal/indexer/types"
	"github.com/stellar/wallet-backend/internal/metrics"
	"github.com/stellar/wallet-backend/internal/utils"
)

// NativeBalance contains native XLM balance data for an account.
type NativeBalance struct {
	AccountID          types.AddressBytea `db:"account_id"`
	Balance            int64  `db:"balance"`
	MinimumBalance     int64  `db:"minimum_balance"`
	BuyingLiabilities  int64  `db:"buying_liabilities"`
	SellingLiabilities int64  `db:"selling_liabilities"`
	LedgerNumber       uint32 `db:"last_modified_ledger"`
}

// NativeBalanceModelInterface defines the interface for native balance operations.
type NativeBalanceModelInterface interface {
	// Read operations (for API/balances queries)
	GetByAccount(ctx context.Context, accountAddress string) (*NativeBalance, error)

	// Write operations (for live ingestion)
	BatchUpsert(ctx context.Context, dbTx pgx.Tx, upserts []NativeBalance, deletes []types.AddressBytea) error

	// Batch operations (for initial population)
	BatchCopy(ctx context.Context, dbTx pgx.Tx, balances []NativeBalance) error
}

// NativeBalanceModel implements NativeBalanceModelInterface.
type NativeBalanceModel struct {
	DB      *pgxpool.Pool
	Metrics *metrics.DBMetrics
}

var _ NativeBalanceModelInterface = (*NativeBalanceModel)(nil)

// GetByAccount retrieves native XLM balance for an account.
func (m *NativeBalanceModel) GetByAccount(ctx context.Context, accountAddress string) (*NativeBalance, error) {
	if accountAddress == "" {
		return nil, fmt.Errorf("empty account address")
	}

	const query = `
		SELECT account_id, balance, minimum_balance, buying_liabilities, selling_liabilities, last_modified_ledger
		FROM native_balances
		WHERE account_id = $1`

	start := time.Now()
	nb, err := db.QueryOne[NativeBalance](ctx, m.DB, query, types.AddressBytea(accountAddress))
	duration := time.Since(start).Seconds()
	m.Metrics.QueryDuration.WithLabelValues("GetByAccount", "native_balances").Observe(duration)
	m.Metrics.QueriesTotal.WithLabelValues("GetByAccount", "native_balances").Inc()
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		m.Metrics.QueryErrors.WithLabelValues("GetByAccount", "native_balances", utils.GetDBErrorType(err)).Inc()
		return nil, fmt.Errorf("querying native balance for %s: %w", accountAddress, err)
	}
	return &nb, nil
}

// BatchUpsert upserts and deletes native balances in batch.
func (m *NativeBalanceModel) BatchUpsert(ctx context.Context, dbTx pgx.Tx, upserts []NativeBalance, deletes []types.AddressBytea) error {
	if len(upserts) == 0 && len(deletes) == 0 {
		return nil
	}

	start := time.Now()
	batch := &pgx.Batch{}

	const upsertQuery = `
		INSERT INTO native_balances (account_id, balance, minimum_balance, buying_liabilities, selling_liabilities, last_modified_ledger)
		VALUES ($1, $2, $3, $4, $5, $6)
		ON CONFLICT (account_id) DO UPDATE SET
			balance = EXCLUDED.balance,
			minimum_balance = EXCLUDED.minimum_balance,
			buying_liabilities = EXCLUDED.buying_liabilities,
			selling_liabilities = EXCLUDED.selling_liabilities,
			last_modified_ledger = EXCLUDED.last_modified_ledger`

	for _, nb := range upserts {
		batch.Queue(upsertQuery, nb.AccountID, nb.Balance, nb.MinimumBalance, nb.BuyingLiabilities, nb.SellingLiabilities, nb.LedgerNumber)
	}

	const deleteQuery = `DELETE FROM native_balances WHERE account_id = $1`
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
			m.Metrics.QueryDuration.WithLabelValues("BatchUpsert", "native_balances").Observe(time.Since(start).Seconds())
			m.Metrics.QueriesTotal.WithLabelValues("BatchUpsert", "native_balances").Inc()
			m.Metrics.QueryErrors.WithLabelValues("BatchUpsert", "native_balances", utils.GetDBErrorType(err)).Inc()
			return fmt.Errorf("upserting native balances: %w", err)
		}
	}
	if err := br.Close(); err != nil {
		m.Metrics.QueryDuration.WithLabelValues("BatchUpsert", "native_balances").Observe(time.Since(start).Seconds())
		m.Metrics.QueriesTotal.WithLabelValues("BatchUpsert", "native_balances").Inc()
		m.Metrics.QueryErrors.WithLabelValues("BatchUpsert", "native_balances", utils.GetDBErrorType(err)).Inc()
		return fmt.Errorf("closing native balance batch: %w", err)
	}

	m.Metrics.QueryDuration.WithLabelValues("BatchUpsert", "native_balances").Observe(time.Since(start).Seconds())
	m.Metrics.QueriesTotal.WithLabelValues("BatchUpsert", "native_balances").Inc()
	return nil
}

// BatchCopy performs bulk insert using COPY protocol for speed.
func (m *NativeBalanceModel) BatchCopy(ctx context.Context, dbTx pgx.Tx, balances []NativeBalance) error {
	if len(balances) == 0 {
		return nil
	}

	start := time.Now()

	copyCount, err := dbTx.CopyFrom(
		ctx,
		pgx.Identifier{"native_balances"},
		[]string{"account_id", "balance", "minimum_balance", "buying_liabilities", "selling_liabilities", "last_modified_ledger"},
		pgx.CopyFromSlice(len(balances), func(i int) ([]any, error) {
			nb := balances[i]
			accountIDBytes, err := nb.AccountID.Value()
			if err != nil {
				return nil, fmt.Errorf("converting account address to bytes: %w", err)
			}
			return []any{accountIDBytes, nb.Balance, nb.MinimumBalance, nb.BuyingLiabilities, nb.SellingLiabilities, nb.LedgerNumber}, nil
		}),
	)
	if err != nil {
		m.Metrics.QueryDuration.WithLabelValues("BatchCopy", "native_balances").Observe(time.Since(start).Seconds())
		m.Metrics.QueriesTotal.WithLabelValues("BatchCopy", "native_balances").Inc()
		m.Metrics.QueryErrors.WithLabelValues("BatchCopy", "native_balances", utils.GetDBErrorType(err)).Inc()
		return fmt.Errorf("bulk inserting native balances via COPY: %w", err)
	}

	if int(copyCount) != len(balances) {
		m.Metrics.QueryDuration.WithLabelValues("BatchCopy", "native_balances").Observe(time.Since(start).Seconds())
		m.Metrics.QueriesTotal.WithLabelValues("BatchCopy", "native_balances").Inc()
		m.Metrics.QueryErrors.WithLabelValues("BatchCopy", "native_balances", "row_count_mismatch").Inc()
		return fmt.Errorf("expected %d rows copied, got %d", len(balances), copyCount)
	}

	m.Metrics.QueryDuration.WithLabelValues("BatchCopy", "native_balances").Observe(time.Since(start).Seconds())
	m.Metrics.QueriesTotal.WithLabelValues("BatchCopy", "native_balances").Inc()
	return nil
}
