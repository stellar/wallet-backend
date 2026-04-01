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
	"github.com/stellar/wallet-backend/internal/metrics"
	"github.com/stellar/wallet-backend/internal/utils"
)

// NativeBalance contains native XLM balance data for an account.
type NativeBalance struct {
	AccountAddress     string `db:"account_address"`
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
	BatchUpsert(ctx context.Context, dbTx pgx.Tx, upserts []NativeBalance, deletes []string) error

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
		SELECT account_address, balance, minimum_balance, buying_liabilities, selling_liabilities, last_modified_ledger
		FROM native_balances
		WHERE account_address = $1`

	start := time.Now()
	nb, err := db.QueryOne[NativeBalance](ctx, m.DB, query, accountAddress)
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

// BatchUpsert upserts and deletes native balances using UNNEST-based bulk operations.
func (m *NativeBalanceModel) BatchUpsert(ctx context.Context, dbTx pgx.Tx, upserts []NativeBalance, deletes []string) error {
	if len(upserts) == 0 && len(deletes) == 0 {
		return nil
	}

	start := time.Now()

	if len(upserts) > 0 {
		accountAddresses := make([]string, len(upserts))
		balances := make([]int64, len(upserts))
		minimumBalances := make([]int64, len(upserts))
		buyingLiabilities := make([]int64, len(upserts))
		sellingLiabilities := make([]int64, len(upserts))
		ledgerNumbers := make([]int64, len(upserts))

		for i, nb := range upserts {
			accountAddresses[i] = nb.AccountAddress
			balances[i] = nb.Balance
			minimumBalances[i] = nb.MinimumBalance
			buyingLiabilities[i] = nb.BuyingLiabilities
			sellingLiabilities[i] = nb.SellingLiabilities
			ledgerNumbers[i] = int64(nb.LedgerNumber)
		}

		const upsertQuery = `
			INSERT INTO native_balances (
				account_address, balance, minimum_balance,
				buying_liabilities, selling_liabilities, last_modified_ledger
			)
			SELECT * FROM UNNEST(
				$1::text[], $2::bigint[], $3::bigint[],
				$4::bigint[], $5::bigint[], $6::bigint[]
			)
			ON CONFLICT (account_address) DO UPDATE SET
				balance = EXCLUDED.balance,
				minimum_balance = EXCLUDED.minimum_balance,
				buying_liabilities = EXCLUDED.buying_liabilities,
				selling_liabilities = EXCLUDED.selling_liabilities,
				last_modified_ledger = EXCLUDED.last_modified_ledger`

		if _, err := dbTx.Exec(ctx, upsertQuery,
			accountAddresses, balances, minimumBalances,
			buyingLiabilities, sellingLiabilities, ledgerNumbers,
		); err != nil {
			m.Metrics.QueryDuration.WithLabelValues("BatchUpsert", "native_balances").Observe(time.Since(start).Seconds())
			m.Metrics.QueriesTotal.WithLabelValues("BatchUpsert", "native_balances").Inc()
			m.Metrics.QueryErrors.WithLabelValues("BatchUpsert", "native_balances", utils.GetDBErrorType(err)).Inc()
			return fmt.Errorf("upserting native balances: %w", err)
		}
	}

	if len(deletes) > 0 {
		const deleteQuery = `DELETE FROM native_balances WHERE account_address = ANY($1::text[])`

		if _, err := dbTx.Exec(ctx, deleteQuery, deletes); err != nil {
			m.Metrics.QueryDuration.WithLabelValues("BatchUpsert", "native_balances").Observe(time.Since(start).Seconds())
			m.Metrics.QueriesTotal.WithLabelValues("BatchUpsert", "native_balances").Inc()
			m.Metrics.QueryErrors.WithLabelValues("BatchUpsert", "native_balances", utils.GetDBErrorType(err)).Inc()
			return fmt.Errorf("deleting native balances: %w", err)
		}
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
	defer func() {
		m.Metrics.QueryDuration.WithLabelValues("BatchCopy", "native_balances").Observe(time.Since(start).Seconds())
		m.Metrics.QueriesTotal.WithLabelValues("BatchCopy", "native_balances").Inc()
	}()

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
		m.Metrics.QueryErrors.WithLabelValues("BatchCopy", "native_balances", utils.GetDBErrorType(err)).Inc()
		return fmt.Errorf("bulk inserting native balances via COPY: %w", err)
	}

	if int(copyCount) != len(balances) {
		m.Metrics.QueryErrors.WithLabelValues("BatchCopy", "native_balances", "row_count_mismatch").Inc()
		return fmt.Errorf("expected %d rows copied, got %d", len(balances), copyCount)
	}

	return nil
}
