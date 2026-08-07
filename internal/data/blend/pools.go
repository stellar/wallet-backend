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
	OracleContractID types.AddressBytea
	BackstopRate     *int32
	Status           *int32
	MaxPositions     *int32
	MinCollateral    *string
	// Admin is stored as SQL NULL when empty, identically to OracleContractID.
	Admin types.AddressBytea
	// InRewardZone mirrors blend_pools.in_reward_zone: membership in the
	// backstop's reward zone (BLND emissions eligibility), maintained by
	// SetRewardZone rather than BatchUpsert.
	InRewardZone       bool
	LastModifiedLedger uint32
}

// PoolModelInterface exposes Blend v2 pool storage operations.
type PoolModelInterface interface {
	// BatchUpsert inserts or partially updates pool config rows. Every nullable
	// column uses COALESCE(EXCLUDED.col, blend_pools.col) so a nil field (not
	// known/changed by the event that produced this row) never clobbers a
	// previously known value. last_modified_ledger only moves forward
	// (GREATEST), so validator enrichment writing ledger 0 never regresses a
	// ledger recorded by the processor.
	BatchUpsert(ctx context.Context, dbTx pgx.Tx, rows []Pool) error
	// SetRewardZone makes poolIDs the exact reward-zone membership set: listed
	// pools become members, all other rows non-members. Rows whose membership
	// already matches are left untouched (their last_modified_ledger keeps its
	// value). An empty poolIDs is valid and clears membership everywhere.
	SetRewardZone(ctx context.Context, dbTx pgx.Tx, poolIDs []types.AddressBytea, ledger int32) error
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
	admins := make([][]byte, len(rows))
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

		var adminBytes []byte
		if r.Admin != "" {
			adminBytes, err = addressToBytes(string(r.Admin))
			if err != nil {
				return fmt.Errorf("converting admin address for pool upsert: %w", err)
			}
		}
		admins[i] = adminBytes

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
			status, max_positions, min_collateral, admin, last_modified_ledger
		)
		SELECT * FROM UNNEST(
			$1::bytea[], $2::text[], $3::bytea[], $4::integer[],
			$5::integer[], $6::integer[], $7::text[], $8::bytea[], $9::integer[]
		)
		ON CONFLICT (pool_contract_id) DO UPDATE SET
			name                 = COALESCE(EXCLUDED.name, blend_pools.name),
			oracle_contract_id   = COALESCE(EXCLUDED.oracle_contract_id, blend_pools.oracle_contract_id),
			backstop_rate        = COALESCE(EXCLUDED.backstop_rate, blend_pools.backstop_rate),
			status               = COALESCE(EXCLUDED.status, blend_pools.status),
			max_positions        = COALESCE(EXCLUDED.max_positions, blend_pools.max_positions),
			min_collateral       = COALESCE(EXCLUDED.min_collateral, blend_pools.min_collateral),
			admin                = COALESCE(EXCLUDED.admin, blend_pools.admin),
			last_modified_ledger = GREATEST(blend_pools.last_modified_ledger, EXCLUDED.last_modified_ledger)`
	if _, err := dbTx.Exec(ctx, upsertQuery,
		poolIDs, names, oracleIDs, backstopRates, statuses, maxPositions, minCollaterals, admins, ledgers,
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

// SetRewardZone makes poolIDs the exact reward-zone membership set. See
// PoolModelInterface.
func (m *PoolModel) SetRewardZone(ctx context.Context, dbTx pgx.Tx, poolIDs []types.AddressBytea, ledger int32) error {
	start := time.Now()

	pools := make([][]byte, len(poolIDs))
	for i, p := range poolIDs {
		poolBytes, err := addressToBytes(string(p))
		if err != nil {
			return fmt.Errorf("converting pool address for reward-zone set: %w", err)
		}
		pools[i] = poolBytes
	}

	const setQuery = `
		UPDATE blend_pools SET
			in_reward_zone = (pool_contract_id = ANY($1)),
			last_modified_ledger = GREATEST(last_modified_ledger, $2)
		WHERE in_reward_zone <> (pool_contract_id = ANY($1))`
	if _, err := dbTx.Exec(ctx, setQuery, pools, ledger); err != nil {
		m.Metrics.QueryErrors.WithLabelValues("SetRewardZone", poolsTable, utils.GetDBErrorType(err)).Inc()
		return fmt.Errorf("setting blend pool reward zone: %w", err)
	}

	duration := time.Since(start).Seconds()
	m.Metrics.QueryDuration.WithLabelValues("SetRewardZone", poolsTable).Observe(duration)
	m.Metrics.QueriesTotal.WithLabelValues("SetRewardZone", poolsTable).Inc()
	m.Metrics.BatchSize.WithLabelValues("SetRewardZone", poolsTable).Observe(float64(len(poolIDs)))
	return nil
}
