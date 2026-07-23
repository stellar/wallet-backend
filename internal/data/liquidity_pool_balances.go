// Package data provides data access layer for liquidity pool balance operations.
// This file handles PostgreSQL storage of account liquidity-pool share balances.
package data

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/stellar/wallet-backend/internal/db"
	"github.com/stellar/wallet-backend/internal/indexer/types"
	"github.com/stellar/wallet-backend/internal/metrics"
	"github.com/stellar/wallet-backend/internal/utils"
)

// LiquidityPoolBalance holds an account's pool-share balance together with the pool's reserves
// (constituent assets + amounts) from the JOIN with liquidity_pools.
type LiquidityPoolBalance struct {
	AccountID    types.AddressBytea `db:"account_id"`
	PoolID       string             `db:"pool_id"`
	Shares       int64              `db:"shares"`
	LedgerNumber uint32             `db:"last_modified_ledger"`
	// Pool reserves from JOIN with liquidity_pools:
	AssetA  string `db:"asset_a"`  // Canonical asset A ("native" or "CODE:ISSUER")
	AmountA int64  `db:"amount_a"` // Reserve of asset A
	AssetB  string `db:"asset_b"`  // Canonical asset B ("native" or "CODE:ISSUER")
	AmountB int64  `db:"amount_b"` // Reserve of asset B
}

// LiquidityPoolBalanceModelInterface defines the interface for liquidity pool balance operations.
type LiquidityPoolBalanceModelInterface interface {
	// GetByAccount returns an account's pool-share balances ordered by pool_id, JOINed with
	// liquidity_pools for reserves. Pass nil limit/cursor to fetch all rows, or provide them
	// for keyset pagination (the cursor is the last seen pool_id from the previous page).
	GetByAccount(ctx context.Context, accountAddress string, limit *int32, cursor *string, sortOrder SortOrder) ([]LiquidityPoolBalance, error)

	// Write operations (for live ingestion)
	BatchUpsert(ctx context.Context, dbTx pgx.Tx, upserts []LiquidityPoolBalance, deletes []LiquidityPoolBalance) error

	// Batch operations (for initial population)
	BatchCopy(ctx context.Context, dbTx pgx.Tx, balances []LiquidityPoolBalance) error
}

// LiquidityPoolBalanceModel implements LiquidityPoolBalanceModelInterface.
type LiquidityPoolBalanceModel struct {
	DB      *pgxpool.Pool
	Metrics *metrics.DBMetrics
}

var _ LiquidityPoolBalanceModelInterface = (*LiquidityPoolBalanceModel)(nil)

// GetByAccount retrieves an account's pool-share balances ordered by pool_id, JOINed with
// liquidity_pools for reserves. Pass nil limit/cursor to fetch all rows; provide them for
// keyset pagination (the cursor is the last seen pool_id from the previous page).
func (m *LiquidityPoolBalanceModel) GetByAccount(ctx context.Context, accountAddress string, limit *int32, cursor *string, sortOrder SortOrder) ([]LiquidityPoolBalance, error) {
	if accountAddress == "" {
		return nil, fmt.Errorf("empty account address")
	}

	query := `
		SELECT lpb.account_id, lpb.pool_id, lpb.shares, lpb.last_modified_ledger,
		       lp.asset_a, lp.amount_a, lp.asset_b, lp.amount_b
		FROM liquidity_pool_balances lpb
		INNER JOIN liquidity_pools lp ON lp.pool_id = lpb.pool_id
		WHERE lpb.account_id = $1`
	args := []interface{}{types.AddressBytea(accountAddress)}
	argIndex := 2

	if cursor != nil {
		// Cursor comparisons implement keyset pagination over the primary key.
		op := ">"
		if sortOrder == DESC {
			op = "<"
		}
		query += fmt.Sprintf(" AND lpb.pool_id %s $%d", op, argIndex)
		args = append(args, *cursor)
		argIndex++
	}

	if sortOrder == DESC {
		query += " ORDER BY lpb.pool_id DESC"
	} else {
		query += " ORDER BY lpb.pool_id ASC"
	}

	if limit != nil {
		query += fmt.Sprintf(" LIMIT $%d", argIndex)
		args = append(args, *limit)
	}

	start := time.Now()
	balances, err := db.QueryMany[LiquidityPoolBalance](ctx, m.DB, query, args...)
	duration := time.Since(start).Seconds()
	m.Metrics.QueryDuration.WithLabelValues("GetByAccount", "liquidity_pool_balances").Observe(duration)
	m.Metrics.QueriesTotal.WithLabelValues("GetByAccount", "liquidity_pool_balances").Inc()
	if err != nil {
		m.Metrics.QueryErrors.WithLabelValues("GetByAccount", "liquidity_pool_balances", utils.GetDBErrorType(err)).Inc()
		return nil, fmt.Errorf("querying paginated liquidity pool balances for %s: %w", accountAddress, err)
	}
	return balances, nil
}

// BatchUpsert performs upserts and deletes using UNNEST for efficiency.
// For upserts (ADD/UPDATE): inserts or updates the share balance.
// For deletes (REMOVE): removes the balance row.
func (m *LiquidityPoolBalanceModel) BatchUpsert(ctx context.Context, dbTx pgx.Tx, upserts []LiquidityPoolBalance, deletes []LiquidityPoolBalance) error {
	if len(upserts) == 0 && len(deletes) == 0 {
		return nil
	}

	start := time.Now()

	if len(upserts) > 0 {
		accountIDs := make([][]byte, len(upserts))
		poolIDs := make([]string, len(upserts))
		shares := make([]int64, len(upserts))
		ledgerNumbers := make([]int64, len(upserts))

		for i, lpb := range upserts {
			raw, err := lpb.AccountID.Value()
			if err != nil {
				return fmt.Errorf("converting account address to bytes for upsert: %w", err)
			}
			rawBytes, ok := raw.([]byte)
			if !ok {
				return fmt.Errorf("converting account address to bytes for upsert: expected []byte, got %T", raw)
			}
			accountIDs[i] = rawBytes
			poolIDs[i] = lpb.PoolID
			shares[i] = lpb.Shares
			ledgerNumbers[i] = int64(lpb.LedgerNumber)
		}

		const upsertQuery = `
			INSERT INTO liquidity_pool_balances (
				account_id, pool_id, shares, last_modified_ledger
			)
			SELECT * FROM UNNEST($1::bytea[], $2::text[], $3::bigint[], $4::bigint[])
			ON CONFLICT (account_id, pool_id) DO UPDATE SET
				shares = EXCLUDED.shares,
				last_modified_ledger = EXCLUDED.last_modified_ledger`

		if _, err := dbTx.Exec(ctx, upsertQuery, accountIDs, poolIDs, shares, ledgerNumbers); err != nil {
			m.Metrics.QueryDuration.WithLabelValues("BatchUpsert", "liquidity_pool_balances").Observe(time.Since(start).Seconds())
			m.Metrics.QueriesTotal.WithLabelValues("BatchUpsert", "liquidity_pool_balances").Inc()
			m.Metrics.QueryErrors.WithLabelValues("BatchUpsert", "liquidity_pool_balances", utils.GetDBErrorType(err)).Inc()
			return fmt.Errorf("upserting liquidity pool balances: %w", err)
		}
	}

	if len(deletes) > 0 {
		deleteAccountIDs := make([][]byte, len(deletes))
		deletePoolIDs := make([]string, len(deletes))

		for i, lpb := range deletes {
			raw, err := lpb.AccountID.Value()
			if err != nil {
				return fmt.Errorf("converting account address to bytes for delete: %w", err)
			}
			rawBytes, ok := raw.([]byte)
			if !ok {
				return fmt.Errorf("converting account address to bytes for delete: expected []byte, got %T", raw)
			}
			deleteAccountIDs[i] = rawBytes
			deletePoolIDs[i] = lpb.PoolID
		}

		const deleteQuery = `
			DELETE FROM liquidity_pool_balances
			WHERE (account_id, pool_id) IN (
				SELECT * FROM UNNEST($1::bytea[], $2::text[])
			)`

		if _, err := dbTx.Exec(ctx, deleteQuery, deleteAccountIDs, deletePoolIDs); err != nil {
			m.Metrics.QueryDuration.WithLabelValues("BatchUpsert", "liquidity_pool_balances").Observe(time.Since(start).Seconds())
			m.Metrics.QueriesTotal.WithLabelValues("BatchUpsert", "liquidity_pool_balances").Inc()
			m.Metrics.QueryErrors.WithLabelValues("BatchUpsert", "liquidity_pool_balances", utils.GetDBErrorType(err)).Inc()
			return fmt.Errorf("deleting liquidity pool balances: %w", err)
		}
	}

	m.Metrics.QueryDuration.WithLabelValues("BatchUpsert", "liquidity_pool_balances").Observe(time.Since(start).Seconds())
	m.Metrics.QueriesTotal.WithLabelValues("BatchUpsert", "liquidity_pool_balances").Inc()
	return nil
}

// BatchCopy performs bulk insert using COPY protocol for speed during checkpoint population.
func (m *LiquidityPoolBalanceModel) BatchCopy(ctx context.Context, dbTx pgx.Tx, balances []LiquidityPoolBalance) error {
	if len(balances) == 0 {
		return nil
	}

	start := time.Now()

	copyCount, err := dbTx.CopyFrom(
		ctx,
		pgx.Identifier{"liquidity_pool_balances"},
		[]string{
			"account_id",
			"pool_id",
			"shares",
			"last_modified_ledger",
		},
		pgx.CopyFromSlice(len(balances), func(i int) ([]any, error) {
			lpb := balances[i]
			accountIDBytes, err := lpb.AccountID.Value()
			if err != nil {
				return nil, fmt.Errorf("converting account address to bytes: %w", err)
			}
			return []any{
				accountIDBytes,
				lpb.PoolID,
				lpb.Shares,
				lpb.LedgerNumber,
			}, nil
		}),
	)
	if err != nil {
		m.Metrics.QueryDuration.WithLabelValues("BatchCopy", "liquidity_pool_balances").Observe(time.Since(start).Seconds())
		m.Metrics.QueriesTotal.WithLabelValues("BatchCopy", "liquidity_pool_balances").Inc()
		m.Metrics.QueryErrors.WithLabelValues("BatchCopy", "liquidity_pool_balances", utils.GetDBErrorType(err)).Inc()
		return fmt.Errorf("batch inserting liquidity pool balances via COPY: %w", err)
	}

	if int(copyCount) != len(balances) {
		m.Metrics.QueryDuration.WithLabelValues("BatchCopy", "liquidity_pool_balances").Observe(time.Since(start).Seconds())
		m.Metrics.QueriesTotal.WithLabelValues("BatchCopy", "liquidity_pool_balances").Inc()
		m.Metrics.QueryErrors.WithLabelValues("BatchCopy", "liquidity_pool_balances", "row_count_mismatch").Inc()
		return fmt.Errorf("expected %d rows copied, got %d", len(balances), copyCount)
	}

	m.Metrics.QueryDuration.WithLabelValues("BatchCopy", "liquidity_pool_balances").Observe(time.Since(start).Seconds())
	m.Metrics.QueriesTotal.WithLabelValues("BatchCopy", "liquidity_pool_balances").Inc()
	return nil
}
