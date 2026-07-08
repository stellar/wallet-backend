// Pool-level config/metadata storage for Blend v2 (blend_pools).
package blend

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/stellar/wallet-backend/internal/indexer/types"
	"github.com/stellar/wallet-backend/internal/metrics"
	"github.com/stellar/wallet-backend/internal/utils"
)

// poolsTable is the metrics label for blend_pools queries.
const poolsTable = "blend_pools"

// Pool is a row of blend_pools: pool-level config parsed from PoolConfig
// instance storage plus the pool's display name. Config fields are pointers
// because a single event often reveals only some of them (e.g. a name comes
// from a different source than PoolConfig); BatchUpsert preserves whichever
// fields aren't supplied.
type Pool struct {
	PoolContractID types.AddressBytea
	Name           *string
	// OracleContractID is stored as SQL NULL when empty (no oracle wired for
	// this pool yet, or not known from the current event).
	OracleContractID   types.AddressBytea
	BackstopRate       *int32
	Status             *int32
	MaxPositions       *int32
	MinCollateral      *string
	LastModifiedLedger uint32
}

// PoolModelInterface exposes Blend v2 pool storage operations.
type PoolModelInterface interface {
	// BatchUpsert inserts or partially updates pool config rows. Every nullable
	// column uses COALESCE(EXCLUDED.col, blend_pools.col) so a nil field (not
	// known/changed by the event that produced this row) never clobbers a
	// previously known value. last_modified_ledger is always taken from EXCLUDED.
	BatchUpsert(ctx context.Context, dbTx pgx.Tx, rows []Pool) error
}

// PoolModel implements PoolModelInterface against blend_pools.
type PoolModel struct {
	DB      *pgxpool.Pool
	Metrics *metrics.DBMetrics
}

var _ PoolModelInterface = (*PoolModel)(nil)

// BatchUpsert inserts or partially updates blend_pools rows. See PoolModelInterface.
func (m *PoolModel) BatchUpsert(ctx context.Context, dbTx pgx.Tx, rows []Pool) error {
	if len(rows) == 0 {
		return nil
	}

	start := time.Now()

	poolIDs := make([][]byte, len(rows))
	names := make([]*string, len(rows))
	oracleIDs := make([][]byte, len(rows))
	backstopRates := make([]*int32, len(rows))
	statuses := make([]*int32, len(rows))
	maxPositions := make([]*int32, len(rows))
	minCollaterals := make([]*string, len(rows))
	ledgers := make([]int32, len(rows))
	for i, r := range rows {
		poolBytes, err := addressToBytes(string(r.PoolContractID))
		if err != nil {
			return fmt.Errorf("converting pool address for pool upsert: %w", err)
		}
		poolIDs[i] = poolBytes

		var oracleBytes []byte
		if r.OracleContractID != "" {
			oracleBytes, err = addressToBytes(string(r.OracleContractID))
			if err != nil {
				return fmt.Errorf("converting oracle address for pool upsert: %w", err)
			}
		}
		oracleIDs[i] = oracleBytes

		names[i] = r.Name
		backstopRates[i] = r.BackstopRate
		statuses[i] = r.Status
		maxPositions[i] = r.MaxPositions
		minCollaterals[i] = r.MinCollateral
		ledgers[i] = int32(r.LastModifiedLedger)
	}

	const upsertQuery = `
		INSERT INTO blend_pools (
			pool_contract_id, name, oracle_contract_id, backstop_rate,
			status, max_positions, min_collateral, last_modified_ledger
		)
		SELECT * FROM UNNEST(
			$1::bytea[], $2::text[], $3::bytea[], $4::integer[],
			$5::integer[], $6::integer[], $7::text[], $8::integer[]
		)
		ON CONFLICT (pool_contract_id) DO UPDATE SET
			name                 = COALESCE(EXCLUDED.name, blend_pools.name),
			oracle_contract_id   = COALESCE(EXCLUDED.oracle_contract_id, blend_pools.oracle_contract_id),
			backstop_rate        = COALESCE(EXCLUDED.backstop_rate, blend_pools.backstop_rate),
			status               = COALESCE(EXCLUDED.status, blend_pools.status),
			max_positions        = COALESCE(EXCLUDED.max_positions, blend_pools.max_positions),
			min_collateral       = COALESCE(EXCLUDED.min_collateral, blend_pools.min_collateral),
			last_modified_ledger = EXCLUDED.last_modified_ledger`
	if _, err := dbTx.Exec(ctx, upsertQuery,
		poolIDs, names, oracleIDs, backstopRates, statuses, maxPositions, minCollaterals, ledgers,
	); err != nil {
		m.Metrics.QueryErrors.WithLabelValues("BatchUpsert", poolsTable, utils.GetDBErrorType(err)).Inc()
		return fmt.Errorf("upserting blend pools: %w", err)
	}

	duration := time.Since(start).Seconds()
	m.Metrics.QueryDuration.WithLabelValues("BatchUpsert", poolsTable).Observe(duration)
	m.Metrics.QueriesTotal.WithLabelValues("BatchUpsert", poolsTable).Inc()
	m.Metrics.BatchSize.WithLabelValues("BatchUpsert", poolsTable).Observe(float64(len(rows)))
	return nil
}
