// Package data provides data access layer for native XLM balance operations.
// This file handles PostgreSQL storage of account native balances.
package data

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"slices"
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
	AccountID types.AddressBytea `db:"account_id"`
	Balance   int64              `db:"balance"`
	// MinimumBalance is the base reserve requirement in stroops (excludes liabilities):
	// (2 + NumSubEntries + numSponsoring - numSponsored) * baseReserve; matches stellar-core getMinBalance.
	MinimumBalance     int64  `db:"minimum_balance"`
	BuyingLiabilities  int64  `db:"buying_liabilities"`
	SellingLiabilities int64  `db:"selling_liabilities"`
	NumSubEntries      uint32 `db:"num_subentries"`
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
		SELECT account_id, balance, minimum_balance, buying_liabilities, selling_liabilities, num_subentries, last_modified_ledger
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

// BatchUpsert upserts and deletes native balances using UNNEST for efficiency.
func (m *NativeBalanceModel) BatchUpsert(ctx context.Context, dbTx pgx.Tx, upserts []NativeBalance, deletes []types.AddressBytea) error {
	if len(upserts) == 0 && len(deletes) == 0 {
		return nil
	}

	start := time.Now()

	if len(upserts) > 0 {
		type upsertRow struct {
			accountID []byte
			nb        NativeBalance
		}
		rows := make([]upsertRow, len(upserts))
		for i, nb := range upserts {
			raw, err := nb.AccountID.Value()
			if err != nil {
				return fmt.Errorf("converting account address to bytes for upsert: %w", err)
			}
			rawBytes, ok := raw.([]byte)
			if !ok {
				return fmt.Errorf("converting account address to bytes for upsert: expected []byte, got %T", raw)
			}
			rows[i] = upsertRow{accountID: rawBytes, nb: nb}
		}
		// Upserts arrive in Go map-iteration order; sorting by the PK column
		// descends the btree and touches heap pages in key order instead of
		// scattering thousands of random probes across the relation.
		slices.SortFunc(rows, func(a, b upsertRow) int {
			return bytes.Compare(a.accountID, b.accountID)
		})

		accountIDs := make([][]byte, len(rows))
		balances := make([]int64, len(rows))
		minimumBalances := make([]int64, len(rows))
		buyingLiabilities := make([]int64, len(rows))
		sellingLiabilities := make([]int64, len(rows))
		numSubentries := make([]int32, len(rows))
		ledgerNumbers := make([]int64, len(rows))
		for i, row := range rows {
			accountIDs[i] = row.accountID
			balances[i] = row.nb.Balance
			minimumBalances[i] = row.nb.MinimumBalance
			buyingLiabilities[i] = row.nb.BuyingLiabilities
			sellingLiabilities[i] = row.nb.SellingLiabilities
			numSubentries[i] = int32(row.nb.NumSubEntries)
			ledgerNumbers[i] = int64(row.nb.LedgerNumber)
		}

		// The WHERE clause turns updates that would rewrite an identical row
		// into no-ops: no new tuple version, no dead tuple, no WAL, no index
		// churn — the update fires only when some column actually differs.
		const upsertQuery = `
			INSERT INTO native_balances (account_id, balance, minimum_balance, buying_liabilities, selling_liabilities, num_subentries, last_modified_ledger)
			SELECT * FROM UNNEST($1::bytea[], $2::bigint[], $3::bigint[], $4::bigint[], $5::bigint[], $6::int[], $7::bigint[])
			ON CONFLICT (account_id) DO UPDATE SET
				balance = EXCLUDED.balance,
				minimum_balance = EXCLUDED.minimum_balance,
				buying_liabilities = EXCLUDED.buying_liabilities,
				selling_liabilities = EXCLUDED.selling_liabilities,
				num_subentries = EXCLUDED.num_subentries,
				last_modified_ledger = EXCLUDED.last_modified_ledger
			WHERE (native_balances.balance, native_balances.minimum_balance, native_balances.buying_liabilities,
			       native_balances.selling_liabilities, native_balances.num_subentries, native_balances.last_modified_ledger)
			      IS DISTINCT FROM
			      (EXCLUDED.balance, EXCLUDED.minimum_balance, EXCLUDED.buying_liabilities,
			       EXCLUDED.selling_liabilities, EXCLUDED.num_subentries, EXCLUDED.last_modified_ledger)`

		if _, err := dbTx.Exec(ctx, upsertQuery, accountIDs, balances, minimumBalances, buyingLiabilities, sellingLiabilities, numSubentries, ledgerNumbers); err != nil {
			m.Metrics.QueryDuration.WithLabelValues("BatchUpsert", "native_balances").Observe(time.Since(start).Seconds())
			m.Metrics.QueriesTotal.WithLabelValues("BatchUpsert", "native_balances").Inc()
			m.Metrics.QueryErrors.WithLabelValues("BatchUpsert", "native_balances", utils.GetDBErrorType(err)).Inc()
			return fmt.Errorf("upserting native balances: %w", err)
		}
	}

	if len(deletes) > 0 {
		deleteIDs := make([][]byte, len(deletes))
		for i, addr := range deletes {
			raw, err := addr.Value()
			if err != nil {
				return fmt.Errorf("converting account address to bytes for delete: %w", err)
			}
			rawBytes, ok := raw.([]byte)
			if !ok {
				return fmt.Errorf("converting account address to bytes for delete: expected []byte, got %T", raw)
			}
			deleteIDs[i] = rawBytes
		}

		const deleteQuery = `DELETE FROM native_balances WHERE account_id = ANY($1::bytea[])`

		if _, err := dbTx.Exec(ctx, deleteQuery, deleteIDs); err != nil {
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

	copyCount, err := dbTx.CopyFrom(
		ctx,
		pgx.Identifier{"native_balances"},
		[]string{"account_id", "balance", "minimum_balance", "buying_liabilities", "selling_liabilities", "num_subentries", "last_modified_ledger"},
		pgx.CopyFromSlice(len(balances), func(i int) ([]any, error) {
			nb := balances[i]
			accountIDBytes, err := nb.AccountID.Value()
			if err != nil {
				return nil, fmt.Errorf("converting account address to bytes: %w", err)
			}
			return []any{accountIDBytes, nb.Balance, nb.MinimumBalance, nb.BuyingLiabilities, nb.SellingLiabilities, int32(nb.NumSubEntries), nb.LedgerNumber}, nil
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
