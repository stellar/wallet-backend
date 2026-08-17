// Lifetime claimed-emissions accumulators for Blend v2 (blend_pool_claimed,
// blend_backstop_claimed). Unlike blend_emissions (which mirrors the current
// on-chain accrued/index snapshot, i.e. claimable-now), these tables hold the
// cumulative amount a user has already claimed. The contracts store no running
// claimed total — claims zero out accrued — so the figure exists only as the
// sequence of `claim` events and is folded additively here during current-state
// indexing, exactly like blend_positions.net_supplied/net_borrowed.
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

// poolClaimedTable is the metrics label for blend_pool_claimed queries.
const poolClaimedTable = "blend_pool_claimed"

// backstopClaimedTable is the metrics label for blend_backstop_claimed queries.
const backstopClaimedTable = "blend_backstop_claimed"

// PoolClaimed is a full row of blend_pool_claimed: one user's lifetime pool-source
// BLND claims for a single pool. Writers use PoolClaimedDelta; this type exists for
// readers to scan a full row into.
type PoolClaimed struct {
	PoolContractID     types.AddressBytea
	UserAccountID      types.AddressBytea
	ClaimedBlnd        string
	LastModifiedLedger uint32
}

// BackstopClaimed is a full row of blend_backstop_claimed: one user's account-wide
// lifetime backstop-source claims, denominated in Comet LP tokens.
type BackstopClaimed struct {
	UserAccountID      types.AddressBytea
	ClaimedLp          string
	LastModifiedLedger uint32
}

// PoolClaimedDelta is an additive claim amount for one (pool, user), summed into
// blend_pool_claimed.claimed_blnd.
type PoolClaimedDelta struct {
	Pool, User   string // Pool: C-address, User: G-address
	ClaimedBlnd  string
	LedgerNumber uint32
}

// BackstopClaimedDelta is an additive claim amount for one user, summed into
// blend_backstop_claimed.claimed_lp.
type BackstopClaimedDelta struct {
	User         string // G-address
	ClaimedLp    string
	LedgerNumber uint32
}

// PoolClaimedModelInterface exposes Blend v2 pool-source claimed-total storage.
type PoolClaimedModelInterface interface {
	// BatchApplyDeltas adds each delta's claimed_blnd into the (pool, user) row,
	// inserting it at 0 on first claim.
	BatchApplyDeltas(ctx context.Context, dbTx pgx.Tx, deltas []PoolClaimedDelta) error
}

// BackstopClaimedModelInterface exposes Blend v2 backstop-source claimed-total storage.
type BackstopClaimedModelInterface interface {
	// BatchApplyDeltas adds each delta's claimed_lp into the user's row, inserting
	// it at 0 on first claim.
	BatchApplyDeltas(ctx context.Context, dbTx pgx.Tx, deltas []BackstopClaimedDelta) error
}

// PoolClaimedModel implements PoolClaimedModelInterface against blend_pool_claimed.
type PoolClaimedModel struct {
	DB      *pgxpool.Pool
	Metrics *metrics.DBMetrics
}

// BackstopClaimedModel implements BackstopClaimedModelInterface against blend_backstop_claimed.
type BackstopClaimedModel struct {
	DB      *pgxpool.Pool
	Metrics *metrics.DBMetrics
}

var (
	_ PoolClaimedModelInterface     = (*PoolClaimedModel)(nil)
	_ BackstopClaimedModelInterface = (*BackstopClaimedModel)(nil)
)

// BatchApplyDeltas accumulates pool-source claim amounts server-side
// (claimed_blnd := existing + delta), inserting the row at the delta value on
// first claim.
//
// Each (Pool, User) key must appear at most once per batch: ON CONFLICT applies
// only ONE conflicting row per statement, so a duplicate key would silently drop
// a claim. The caller pre-aggregates (the processor's staged fold does); duplicates
// are rejected with an error.
func (m *PoolClaimedModel) BatchApplyDeltas(ctx context.Context, dbTx pgx.Tx, deltas []PoolClaimedDelta) error {
	if len(deltas) == 0 {
		return nil
	}

	start := time.Now()

	pools := make([][]byte, len(deltas))
	users := make([][]byte, len(deltas))
	claimed := make([]string, len(deltas))
	ledgers := make([]int32, len(deltas))
	seen := make(map[PoolUserKey]struct{}, len(deltas))
	for i, d := range deltas {
		poolBytes, err := addressToBytes(d.Pool)
		if err != nil {
			return fmt.Errorf("converting pool address for pool-claimed apply: %w", err)
		}
		userBytes, err := addressToBytes(d.User)
		if err != nil {
			return fmt.Errorf("converting user address for pool-claimed apply: %w", err)
		}
		key := PoolUserKey{Pool: d.Pool, User: d.User}
		if _, dup := seen[key]; dup {
			return fmt.Errorf("duplicate pool-claimed delta for pool=%s user=%s: deltas must be pre-aggregated per key", d.Pool, d.User)
		}
		seen[key] = struct{}{}
		pools[i] = poolBytes
		users[i] = userBytes
		claimed[i] = d.ClaimedBlnd
		ledgers[i] = int32(d.LedgerNumber)
	}

	const applyQuery = `
		INSERT INTO blend_pool_claimed (pool_contract_id, user_account_id, claimed_blnd, last_modified_ledger)
		SELECT * FROM UNNEST($1::bytea[], $2::bytea[], $3::text[], $4::integer[])
		ON CONFLICT (pool_contract_id, user_account_id) DO UPDATE SET
			claimed_blnd         = (blend_pool_claimed.claimed_blnd::numeric + EXCLUDED.claimed_blnd::numeric)::text,
			last_modified_ledger = GREATEST(blend_pool_claimed.last_modified_ledger, EXCLUDED.last_modified_ledger)`
	if _, err := dbTx.Exec(ctx, applyQuery, pools, users, claimed, ledgers); err != nil {
		m.Metrics.QueryErrors.WithLabelValues("BatchApplyDeltas", poolClaimedTable, utils.GetDBErrorType(err)).Inc()
		return fmt.Errorf("applying blend pool claimed deltas: %w", err)
	}

	duration := time.Since(start).Seconds()
	m.Metrics.QueryDuration.WithLabelValues("BatchApplyDeltas", poolClaimedTable).Observe(duration)
	m.Metrics.QueriesTotal.WithLabelValues("BatchApplyDeltas", poolClaimedTable).Inc()
	m.Metrics.BatchSize.WithLabelValues("BatchApplyDeltas", poolClaimedTable).Observe(float64(len(deltas)))
	return nil
}

// BatchApplyDeltas accumulates backstop-source claim amounts server-side
// (claimed_lp := existing + delta), inserting the row at the delta value on first
// claim. Each User key must appear at most once per batch (see PoolClaimedModel).
func (m *BackstopClaimedModel) BatchApplyDeltas(ctx context.Context, dbTx pgx.Tx, deltas []BackstopClaimedDelta) error {
	if len(deltas) == 0 {
		return nil
	}

	start := time.Now()

	users := make([][]byte, len(deltas))
	claimed := make([]string, len(deltas))
	ledgers := make([]int32, len(deltas))
	seen := make(map[string]struct{}, len(deltas))
	for i, d := range deltas {
		userBytes, err := addressToBytes(d.User)
		if err != nil {
			return fmt.Errorf("converting user address for backstop-claimed apply: %w", err)
		}
		if _, dup := seen[d.User]; dup {
			return fmt.Errorf("duplicate backstop-claimed delta for user=%s: deltas must be pre-aggregated per key", d.User)
		}
		seen[d.User] = struct{}{}
		users[i] = userBytes
		claimed[i] = d.ClaimedLp
		ledgers[i] = int32(d.LedgerNumber)
	}

	const applyQuery = `
		INSERT INTO blend_backstop_claimed (user_account_id, claimed_lp, last_modified_ledger)
		SELECT * FROM UNNEST($1::bytea[], $2::text[], $3::integer[])
		ON CONFLICT (user_account_id) DO UPDATE SET
			claimed_lp           = (blend_backstop_claimed.claimed_lp::numeric + EXCLUDED.claimed_lp::numeric)::text,
			last_modified_ledger = GREATEST(blend_backstop_claimed.last_modified_ledger, EXCLUDED.last_modified_ledger)`
	if _, err := dbTx.Exec(ctx, applyQuery, users, claimed, ledgers); err != nil {
		m.Metrics.QueryErrors.WithLabelValues("BatchApplyDeltas", backstopClaimedTable, utils.GetDBErrorType(err)).Inc()
		return fmt.Errorf("applying blend backstop claimed deltas: %w", err)
	}

	duration := time.Since(start).Seconds()
	m.Metrics.QueryDuration.WithLabelValues("BatchApplyDeltas", backstopClaimedTable).Observe(duration)
	m.Metrics.QueriesTotal.WithLabelValues("BatchApplyDeltas", backstopClaimedTable).Inc()
	m.Metrics.BatchSize.WithLabelValues("BatchApplyDeltas", backstopClaimedTable).Observe(float64(len(deltas)))
	return nil
}
