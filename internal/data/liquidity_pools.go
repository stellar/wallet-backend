// Package data provides data access layer for liquidity pool operations.
// This file handles PostgreSQL storage of constant-product liquidity pool reserves.
package data

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/stellar/wallet-backend/internal/metrics"
	"github.com/stellar/wallet-backend/internal/utils"
)

// LiquidityPool holds a constant-product pool's constituent assets and reserve amounts.
// Assets are stored canonically ("native" or "CODE:ISSUER"). Joined with liquidity_pool_balances
// at query time to expose a holder's shares alongside the pool's reserves.
type LiquidityPool struct {
	PoolID       string `db:"pool_id"`
	AssetA       string `db:"asset_a"`
	AmountA      int64  `db:"amount_a"`
	AssetB       string `db:"asset_b"`
	AmountB      int64  `db:"amount_b"`
	LedgerNumber uint32 `db:"last_modified_ledger"`
}

// LiquidityPoolModelInterface defines the interface for liquidity pool operations.
type LiquidityPoolModelInterface interface {
	// Write operations (for live ingestion)
	BatchUpsert(ctx context.Context, dbTx pgx.Tx, upserts []LiquidityPool, deletes []LiquidityPool) error

	// Batch operations (for initial population)
	BatchCopy(ctx context.Context, dbTx pgx.Tx, pools []LiquidityPool) error
}

// LiquidityPoolModel implements LiquidityPoolModelInterface.
type LiquidityPoolModel struct {
	DB      *pgxpool.Pool
	Metrics *metrics.DBMetrics
}

var _ LiquidityPoolModelInterface = (*LiquidityPoolModel)(nil)

// BatchUpsert performs upserts and deletes using UNNEST for efficiency.
// For upserts (ADD/UPDATE): inserts or updates the pool reserves.
// For deletes (REMOVE): removes the pool row.
func (m *LiquidityPoolModel) BatchUpsert(ctx context.Context, dbTx pgx.Tx, upserts []LiquidityPool, deletes []LiquidityPool) error {
	if len(upserts) == 0 && len(deletes) == 0 {
		return nil
	}

	start := time.Now()

	if len(upserts) > 0 {
		poolIDs := make([]string, len(upserts))
		assetsA := make([]string, len(upserts))
		amountsA := make([]int64, len(upserts))
		assetsB := make([]string, len(upserts))
		amountsB := make([]int64, len(upserts))
		ledgerNumbers := make([]int64, len(upserts))

		for i, lp := range upserts {
			poolIDs[i] = lp.PoolID
			assetsA[i] = lp.AssetA
			amountsA[i] = lp.AmountA
			assetsB[i] = lp.AssetB
			amountsB[i] = lp.AmountB
			ledgerNumbers[i] = int64(lp.LedgerNumber)
		}

		const upsertQuery = `
			INSERT INTO liquidity_pools (
				pool_id, asset_a, amount_a, asset_b, amount_b, last_modified_ledger
			)
			SELECT * FROM UNNEST($1::text[], $2::text[], $3::bigint[], $4::text[], $5::bigint[], $6::bigint[])
			ON CONFLICT (pool_id) DO UPDATE SET
				asset_a = EXCLUDED.asset_a,
				amount_a = EXCLUDED.amount_a,
				asset_b = EXCLUDED.asset_b,
				amount_b = EXCLUDED.amount_b,
				last_modified_ledger = EXCLUDED.last_modified_ledger`

		if _, err := dbTx.Exec(ctx, upsertQuery, poolIDs, assetsA, amountsA, assetsB, amountsB, ledgerNumbers); err != nil {
			m.Metrics.QueryDuration.WithLabelValues("BatchUpsert", "liquidity_pools").Observe(time.Since(start).Seconds())
			m.Metrics.QueriesTotal.WithLabelValues("BatchUpsert", "liquidity_pools").Inc()
			m.Metrics.QueryErrors.WithLabelValues("BatchUpsert", "liquidity_pools", utils.GetDBErrorType(err)).Inc()
			return fmt.Errorf("upserting liquidity pools: %w", err)
		}
	}

	if len(deletes) > 0 {
		deletePoolIDs := make([]string, len(deletes))
		for i, lp := range deletes {
			deletePoolIDs[i] = lp.PoolID
		}

		const deleteQuery = `
			DELETE FROM liquidity_pools
			WHERE pool_id = ANY($1::text[])`

		if _, err := dbTx.Exec(ctx, deleteQuery, deletePoolIDs); err != nil {
			m.Metrics.QueryDuration.WithLabelValues("BatchUpsert", "liquidity_pools").Observe(time.Since(start).Seconds())
			m.Metrics.QueriesTotal.WithLabelValues("BatchUpsert", "liquidity_pools").Inc()
			m.Metrics.QueryErrors.WithLabelValues("BatchUpsert", "liquidity_pools", utils.GetDBErrorType(err)).Inc()
			return fmt.Errorf("deleting liquidity pools: %w", err)
		}
	}

	m.Metrics.QueryDuration.WithLabelValues("BatchUpsert", "liquidity_pools").Observe(time.Since(start).Seconds())
	m.Metrics.QueriesTotal.WithLabelValues("BatchUpsert", "liquidity_pools").Inc()
	return nil
}

// BatchCopy performs bulk insert using COPY protocol for speed during checkpoint population.
func (m *LiquidityPoolModel) BatchCopy(ctx context.Context, dbTx pgx.Tx, pools []LiquidityPool) error {
	if len(pools) == 0 {
		return nil
	}

	start := time.Now()

	copyCount, err := dbTx.CopyFrom(
		ctx,
		pgx.Identifier{"liquidity_pools"},
		[]string{
			"pool_id",
			"asset_a",
			"amount_a",
			"asset_b",
			"amount_b",
			"last_modified_ledger",
		},
		pgx.CopyFromSlice(len(pools), func(i int) ([]any, error) {
			lp := pools[i]
			return []any{
				lp.PoolID,
				lp.AssetA,
				lp.AmountA,
				lp.AssetB,
				lp.AmountB,
				lp.LedgerNumber,
			}, nil
		}),
	)
	if err != nil {
		m.Metrics.QueryDuration.WithLabelValues("BatchCopy", "liquidity_pools").Observe(time.Since(start).Seconds())
		m.Metrics.QueriesTotal.WithLabelValues("BatchCopy", "liquidity_pools").Inc()
		m.Metrics.QueryErrors.WithLabelValues("BatchCopy", "liquidity_pools", utils.GetDBErrorType(err)).Inc()
		return fmt.Errorf("batch inserting liquidity pools via COPY: %w", err)
	}

	if int(copyCount) != len(pools) {
		m.Metrics.QueryDuration.WithLabelValues("BatchCopy", "liquidity_pools").Observe(time.Since(start).Seconds())
		m.Metrics.QueriesTotal.WithLabelValues("BatchCopy", "liquidity_pools").Inc()
		m.Metrics.QueryErrors.WithLabelValues("BatchCopy", "liquidity_pools", "row_count_mismatch").Inc()
		return fmt.Errorf("expected %d rows copied, got %d", len(pools), copyCount)
	}

	m.Metrics.QueryDuration.WithLabelValues("BatchCopy", "liquidity_pools").Observe(time.Since(start).Seconds())
	m.Metrics.QueriesTotal.WithLabelValues("BatchCopy", "liquidity_pools").Inc()
	return nil
}
