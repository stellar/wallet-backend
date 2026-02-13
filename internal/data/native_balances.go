// Package data provides data access layer for native XLM balance operations.
// This file handles PostgreSQL storage of account native balances.
package data

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/stellar/wallet-backend/internal/db"
	"github.com/stellar/wallet-backend/internal/metrics"
)

// NativeBalance contains native XLM balance data for an account.
type NativeBalance struct {
	AccountAddress     string
	Balance            int64
	MinimumBalance     int64
	BuyingLiabilities  int64
	SellingLiabilities int64
	LedgerNumber       uint32
}

// NativeBalanceModelInterface defines the interface for native balance operations.
type NativeBalanceModelInterface interface {
	// Read operations (for API/balances queries)
	GetByAccount(ctx context.Context, accountAddress string) (*NativeBalance, error)

	// Write operations (for live ingestion)
	BatchUpsert(ctx context.Context, dbTx pgx.Tx, upserts []NativeBalance, deletes []string) error

	// Batch operations (for initial population)
	BatchCopy(ctx context.Context, dbTx pgx.Tx, balances []NativeBalance) error
}

// NativeBalanceModel implements NativeBalanceModelInterface.
type NativeBalanceModel struct {
	DB             db.ConnectionPool
	MetricsService metrics.MetricsService
}

var _ NativeBalanceModelInterface = (*NativeBalanceModel)(nil)

// GetByAccount retrieves native XLM balance for an account.
func (m *NativeBalanceModel) GetByAccount(ctx context.Context, accountAddress string) (*NativeBalance, error) {
	if accountAddress == "" {
		return nil, fmt.Errorf("empty account address")
	}

	const query = `
		SELECT account_address, balance, minimum_balance, buying_liabilities, selling_liabilities, last_modified_ledger
		FROM native_balances
		WHERE account_address = $1`

	start := time.Now()
	row := m.DB.PgxPool().QueryRow(ctx, query, accountAddress)

	var nb NativeBalance
	err := row.Scan(&nb.AccountAddress, &nb.Balance, &nb.MinimumBalance, &nb.BuyingLiabilities, &nb.SellingLiabilities, &nb.LedgerNumber)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		m.MetricsService.IncDBQueryError("GetByAccount", "native_balances", "query_error")
		return nil, fmt.Errorf("querying native balance for %s: %w", accountAddress, err)
	}

	m.MetricsService.ObserveDBQueryDuration("GetByAccount", "native_balances", time.Since(start).Seconds())
	m.MetricsService.IncDBQuery("GetByAccount", "native_balances")
	return &nb, nil
}

// BatchUpsert upserts and deletes native balances in batch.
func (m *NativeBalanceModel) BatchUpsert(ctx context.Context, dbTx pgx.Tx, upserts []NativeBalance, deletes []string) error {
	if len(upserts) == 0 && len(deletes) == 0 {
		return nil
	}

	start := time.Now()
	batch := &pgx.Batch{}

	const upsertQuery = `
		INSERT INTO native_balances (account_address, balance, minimum_balance, buying_liabilities, selling_liabilities, last_modified_ledger)
		VALUES ($1, $2, $3, $4, $5, $6)
		ON CONFLICT (account_address) DO UPDATE SET
			balance = EXCLUDED.balance,
			minimum_balance = EXCLUDED.minimum_balance,
			buying_liabilities = EXCLUDED.buying_liabilities,
			selling_liabilities = EXCLUDED.selling_liabilities,
			last_modified_ledger = EXCLUDED.last_modified_ledger`

	for _, nb := range upserts {
		batch.Queue(upsertQuery, nb.AccountAddress, nb.Balance, nb.MinimumBalance, nb.BuyingLiabilities, nb.SellingLiabilities, nb.LedgerNumber)
	}

	const deleteQuery = `DELETE FROM native_balances WHERE account_address = $1`
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

	m.MetricsService.ObserveDBQueryDuration("BatchUpsert", "native_balances", time.Since(start).Seconds())
	m.MetricsService.IncDBQuery("BatchUpsert", "native_balances")
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
		[]string{"account_address", "balance", "minimum_balance", "buying_liabilities", "selling_liabilities", "last_modified_ledger"},
		pgx.CopyFromSlice(len(balances), func(i int) ([]any, error) {
			nb := balances[i]
			return []any{nb.AccountAddress, nb.Balance, nb.MinimumBalance, nb.BuyingLiabilities, nb.SellingLiabilities, nb.LedgerNumber}, nil
		}),
	)
	if err != nil {
		return fmt.Errorf("bulk inserting native balances via COPY: %w", err)
	}

	if int(copyCount) != len(balances) {
		return fmt.Errorf("expected %d rows copied, got %d", len(balances), copyCount)
	}

	m.MetricsService.ObserveDBQueryDuration("BatchCopy", "native_balances", time.Since(start).Seconds())
	m.MetricsService.IncDBQuery("BatchCopy", "native_balances")
	return nil
}
