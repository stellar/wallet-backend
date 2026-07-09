// Package blend provides data access for Blend v2 lending protocol state:
// per-pool-per-user positions, reserve config/rates, backstop positions, and
// emissions. This file covers the positions model.
package blend

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/stellar/wallet-backend/internal/indexer/types"
	"github.com/stellar/wallet-backend/internal/metrics"
	"github.com/stellar/wallet-backend/internal/utils"
)

// positionsTable is the metrics label for blend_positions queries.
const positionsTable = "blend_positions"

// rateScalar is Blend v2's 12-decimal fixed-point base for b_rate/d_rate,
// inlined into SQL that converts protocol-token amounts to underlying.
// Pinned against blend-contracts-v2 in Task 3.0.
const rateScalar = "1000000000000"

// PoolUserKey identifies a single (pool, user) position group for deletion.
type PoolUserKey struct {
	Pool, User string // Pool: C-address, User: G-address
}

// Position is a full row of blend_positions, mirroring every column. Writers
// operate on the narrower structs below instead; this type exists for PR5
// readers to scan a full row into.
type Position struct {
	PoolContractID     types.AddressBytea
	UserAccountID      types.AddressBytea
	ReserveIndex       int32
	SupplyBTokens      string
	CollateralBTokens  string
	LiabilityDTokens   string
	NetSupplied        string
	NetBorrowed        string
	LastModifiedLedger uint32
}

// PositionPresence carries the set of reserve indexes still present in a user's
// Positions entry for a pool, so absent indexes can be zeroed (a partial exit
// keeps the row for cost-basis history instead of deleting it).
type PositionPresence struct {
	Pool, User     string
	PresentIndexes []int32
	LedgerNumber   uint32
}

// PositionSnapshot is a last-write-wins snapshot of current token holdings for
// one (pool, user, reserve_index). It never touches the net_supplied/net_borrowed
// cost-basis columns.
type PositionSnapshot struct {
	Pool, User        string
	ReserveIndex      int32
	SupplyBTokens     string
	CollateralBTokens string
	LiabilityDTokens  string
	LedgerNumber      uint32
}

// PositionNetDelta is a signed, additive delta against the cost-basis columns
// for one (pool, user, asset). ZeroBorrowed, when true, replaces net_borrowed
// with NetBorrowedDelta instead of adding to it (used to fold a bad_debt event,
// which resets liability cost basis rather than accumulating against it).
type PositionNetDelta struct {
	Pool, User, Asset string
	NetSuppliedDelta  string
	NetBorrowedDelta  string
	ZeroBorrowed      bool
	LedgerNumber      uint32
}

// PositionAuctionAdjustment values a protocol-token delta (lot/bid amounts from
// an auction fill) at the reserve's current b_rate/d_rate and applies the
// resulting underlying-asset amount to the cost-basis columns.
type PositionAuctionAdjustment struct {
	Pool, User, Asset string
	LotBTokensDelta   string // protocol-token units, signed
	BidDTokensDelta   string // protocol-token units, signed
	LedgerNumber      uint32
}

// PositionModelInterface exposes Blend v2 position storage operations.
type PositionModelInterface interface {
	// DeleteByPoolUser removes every reserve row for the given (pool, user) pairs.
	// Used when a Positions entry is removed entirely (full exit).
	DeleteByPoolUser(ctx context.Context, dbTx pgx.Tx, keys []PoolUserKey) error
	// ZeroAbsentReserves zeroes the token columns (supply/collateral/liability) of
	// any existing row whose reserve_index is not present in PresentIndexes. Cost
	// basis (net_supplied/net_borrowed) is left untouched.
	ZeroAbsentReserves(ctx context.Context, dbTx pgx.Tx, presences []PositionPresence) error
	// BatchUpsertSnapshots upserts current token holdings. Cost-basis columns are
	// left at their existing (or default) value.
	BatchUpsertSnapshots(ctx context.Context, dbTx pgx.Tx, rows []PositionSnapshot) error
	// BatchApplyNetDeltas accumulates signed cost-basis deltas server-side, resolving
	// each delta's asset to a reserve_index via blend_reserves.
	BatchApplyNetDeltas(ctx context.Context, dbTx pgx.Tx, deltas []PositionNetDelta) error
	// ApplyAuctionAdjustments values protocol-token auction fills at the reserve's
	// current rates and applies the resulting underlying amounts to cost basis.
	ApplyAuctionAdjustments(ctx context.Context, dbTx pgx.Tx, adjs []PositionAuctionAdjustment) error
}

// PositionModel implements PositionModelInterface against blend_positions.
type PositionModel struct {
	DB      *pgxpool.Pool
	Metrics *metrics.DBMetrics
}

var _ PositionModelInterface = (*PositionModel)(nil)

// addressToBytes converts a strkey address (G... or C...) to the 33-byte BYTEA
// representation used by the address columns, identically to how sep41 models
// convert AddressBytea at the write boundary.
func addressToBytes(addr string) ([]byte, error) {
	raw, err := types.AddressBytea(addr).Value()
	if err != nil {
		return nil, fmt.Errorf("converting address %s to bytes: %w", addr, err)
	}
	b, ok := raw.([]byte)
	if !ok {
		return nil, fmt.Errorf("converting address %s to bytes: expected []byte, got %T", addr, raw)
	}
	return b, nil
}

// presentIndexesCSV joins reserve indexes into a comma-separated string for the
// ZeroAbsentReserves query, which unpacks it server-side via string_to_array.
// An empty slice yields "", and string_to_array(”, ',') yields an empty array
// — i.e. every reserve_index is considered absent.
func presentIndexesCSV(indexes []int32) string {
	if len(indexes) == 0 {
		return ""
	}
	parts := make([]string, len(indexes))
	for i, idx := range indexes {
		parts[i] = strconv.FormatInt(int64(idx), 10)
	}
	return strings.Join(parts, ",")
}

// DeleteByPoolUser removes every blend_positions row for the given (pool, user) pairs.
func (m *PositionModel) DeleteByPoolUser(ctx context.Context, dbTx pgx.Tx, keys []PoolUserKey) error {
	if len(keys) == 0 {
		return nil
	}

	start := time.Now()

	pools := make([][]byte, len(keys))
	users := make([][]byte, len(keys))
	for i, k := range keys {
		poolBytes, err := addressToBytes(k.Pool)
		if err != nil {
			return fmt.Errorf("converting pool address for delete: %w", err)
		}
		userBytes, err := addressToBytes(k.User)
		if err != nil {
			return fmt.Errorf("converting user address for delete: %w", err)
		}
		pools[i] = poolBytes
		users[i] = userBytes
	}

	const deleteQuery = `
		DELETE FROM blend_positions
		WHERE (pool_contract_id, user_account_id) IN (SELECT * FROM UNNEST($1::bytea[], $2::bytea[]))`
	if _, err := dbTx.Exec(ctx, deleteQuery, pools, users); err != nil {
		m.Metrics.QueryErrors.WithLabelValues("DeleteByPoolUser", positionsTable, utils.GetDBErrorType(err)).Inc()
		return fmt.Errorf("deleting blend positions: %w", err)
	}

	duration := time.Since(start).Seconds()
	m.Metrics.QueryDuration.WithLabelValues("DeleteByPoolUser", positionsTable).Observe(duration)
	m.Metrics.QueriesTotal.WithLabelValues("DeleteByPoolUser", positionsTable).Inc()
	m.Metrics.BatchSize.WithLabelValues("DeleteByPoolUser", positionsTable).Observe(float64(len(keys)))
	return nil
}

// ZeroAbsentReserves zeroes token columns for any reserve_index not present in
// each presence's PresentIndexes list. Cost-basis columns are untouched.
func (m *PositionModel) ZeroAbsentReserves(ctx context.Context, dbTx pgx.Tx, presences []PositionPresence) error {
	if len(presences) == 0 {
		return nil
	}

	start := time.Now()

	pools := make([][]byte, len(presences))
	users := make([][]byte, len(presences))
	presentCSVs := make([]string, len(presences))
	ledgers := make([]int32, len(presences))
	for i, p := range presences {
		poolBytes, err := addressToBytes(p.Pool)
		if err != nil {
			return fmt.Errorf("converting pool address for zero-absent-reserves: %w", err)
		}
		userBytes, err := addressToBytes(p.User)
		if err != nil {
			return fmt.Errorf("converting user address for zero-absent-reserves: %w", err)
		}
		pools[i] = poolBytes
		users[i] = userBytes
		presentCSVs[i] = presentIndexesCSV(p.PresentIndexes)
		ledgers[i] = int32(p.LedgerNumber)
	}

	const zeroQuery = `
		UPDATE blend_positions p SET
			supply_b_tokens = '0', collateral_b_tokens = '0', liability_d_tokens = '0',
			last_modified_ledger = u.ledger
		FROM UNNEST($1::bytea[], $2::bytea[], $3::text[], $4::integer[]) AS u(pool, usr, present_csv, ledger)
		WHERE p.pool_contract_id = u.pool AND p.user_account_id = u.usr
		  AND NOT (p.reserve_index = ANY (string_to_array(u.present_csv, ',')::integer[]))`
	if _, err := dbTx.Exec(ctx, zeroQuery, pools, users, presentCSVs, ledgers); err != nil {
		m.Metrics.QueryErrors.WithLabelValues("ZeroAbsentReserves", positionsTable, utils.GetDBErrorType(err)).Inc()
		return fmt.Errorf("zeroing absent blend reserves: %w", err)
	}

	duration := time.Since(start).Seconds()
	m.Metrics.QueryDuration.WithLabelValues("ZeroAbsentReserves", positionsTable).Observe(duration)
	m.Metrics.QueriesTotal.WithLabelValues("ZeroAbsentReserves", positionsTable).Inc()
	m.Metrics.BatchSize.WithLabelValues("ZeroAbsentReserves", positionsTable).Observe(float64(len(presences)))
	return nil
}

// BatchUpsertSnapshots upserts current token holdings (supply/collateral/liability)
// as absolute, last-write-wins values. Cost-basis columns (net_supplied/net_borrowed)
// are left at their existing or default value.
func (m *PositionModel) BatchUpsertSnapshots(ctx context.Context, dbTx pgx.Tx, rows []PositionSnapshot) error {
	if len(rows) == 0 {
		return nil
	}

	start := time.Now()

	pools := make([][]byte, len(rows))
	users := make([][]byte, len(rows))
	indexes := make([]int32, len(rows))
	supplyBTokens := make([]string, len(rows))
	collateralBTokens := make([]string, len(rows))
	liabilityDTokens := make([]string, len(rows))
	ledgers := make([]int32, len(rows))
	for i, r := range rows {
		poolBytes, err := addressToBytes(r.Pool)
		if err != nil {
			return fmt.Errorf("converting pool address for snapshot upsert: %w", err)
		}
		userBytes, err := addressToBytes(r.User)
		if err != nil {
			return fmt.Errorf("converting user address for snapshot upsert: %w", err)
		}
		pools[i] = poolBytes
		users[i] = userBytes
		indexes[i] = r.ReserveIndex
		supplyBTokens[i] = r.SupplyBTokens
		collateralBTokens[i] = r.CollateralBTokens
		liabilityDTokens[i] = r.LiabilityDTokens
		ledgers[i] = int32(r.LedgerNumber)
	}

	const upsertQuery = `
		INSERT INTO blend_positions (pool_contract_id, user_account_id, reserve_index,
			supply_b_tokens, collateral_b_tokens, liability_d_tokens, last_modified_ledger)
		SELECT * FROM UNNEST($1::bytea[], $2::bytea[], $3::integer[], $4::text[], $5::text[], $6::text[], $7::integer[])
		ON CONFLICT (pool_contract_id, user_account_id, reserve_index) DO UPDATE SET
			supply_b_tokens = EXCLUDED.supply_b_tokens,
			collateral_b_tokens = EXCLUDED.collateral_b_tokens,
			liability_d_tokens = EXCLUDED.liability_d_tokens,
			last_modified_ledger = EXCLUDED.last_modified_ledger`
	if _, err := dbTx.Exec(ctx, upsertQuery, pools, users, indexes, supplyBTokens, collateralBTokens, liabilityDTokens, ledgers); err != nil {
		m.Metrics.QueryErrors.WithLabelValues("BatchUpsertSnapshots", positionsTable, utils.GetDBErrorType(err)).Inc()
		return fmt.Errorf("upserting blend position snapshots: %w", err)
	}

	duration := time.Since(start).Seconds()
	m.Metrics.QueryDuration.WithLabelValues("BatchUpsertSnapshots", positionsTable).Observe(duration)
	m.Metrics.QueriesTotal.WithLabelValues("BatchUpsertSnapshots", positionsTable).Inc()
	m.Metrics.BatchSize.WithLabelValues("BatchUpsertSnapshots", positionsTable).Observe(float64(len(rows)))
	return nil
}

// BatchApplyNetDeltas accumulates signed cost-basis deltas server-side
// (net_supplied/net_borrowed := existing + delta), resolving each delta's asset
// to a reserve_index via blend_reserves. Deltas whose asset has no matching
// blend_reserves row for the pool are silently skipped (0 rows affected).
//
// ZeroBorrowed, when true, replaces net_borrowed with NetBorrowedDelta instead
// of adding to it — used to fold a bad_debt event, which resets the borrower's
// liability cost basis rather than accumulating against it.
//
// Each (Pool, User, Asset) key must appear at most once per batch: Postgres's
// UPDATE ... FROM applies only ONE matching source row per target row, so a
// duplicate key would silently drop deltas. Duplicates can't be merged here
// either — ZeroBorrowed makes combination order-dependent, and the batch
// carries no ordering — so the caller must pre-aggregate (the processor's
// staged fold does) and duplicates are rejected with an error.
func (m *PositionModel) BatchApplyNetDeltas(ctx context.Context, dbTx pgx.Tx, deltas []PositionNetDelta) error {
	if len(deltas) == 0 {
		return nil
	}

	start := time.Now()

	pools := make([][]byte, len(deltas))
	users := make([][]byte, len(deltas))
	assets := make([][]byte, len(deltas))
	netSuppliedDeltas := make([]string, len(deltas))
	netBorrowedDeltas := make([]string, len(deltas))
	zeroBorrowedFlags := make([]bool, len(deltas))
	ledgers := make([]int32, len(deltas))
	seen := make(map[PoolUserKey]map[string]struct{}, len(deltas))
	for i, d := range deltas {
		poolBytes, err := addressToBytes(d.Pool)
		if err != nil {
			return fmt.Errorf("converting pool address for net-delta apply: %w", err)
		}
		userBytes, err := addressToBytes(d.User)
		if err != nil {
			return fmt.Errorf("converting user address for net-delta apply: %w", err)
		}
		assetBytes, err := addressToBytes(d.Asset)
		if err != nil {
			return fmt.Errorf("converting asset address for net-delta apply: %w", err)
		}
		groupKey := PoolUserKey{Pool: d.Pool, User: d.User}
		if _, dup := seen[groupKey][d.Asset]; dup {
			return fmt.Errorf("duplicate net delta for pool=%s user=%s asset=%s: deltas must be pre-aggregated per key", d.Pool, d.User, d.Asset)
		}
		if seen[groupKey] == nil {
			seen[groupKey] = make(map[string]struct{})
		}
		seen[groupKey][d.Asset] = struct{}{}
		pools[i] = poolBytes
		users[i] = userBytes
		assets[i] = assetBytes
		netSuppliedDeltas[i] = d.NetSuppliedDelta
		netBorrowedDeltas[i] = d.NetBorrowedDelta
		zeroBorrowedFlags[i] = d.ZeroBorrowed
		ledgers[i] = int32(d.LedgerNumber)
	}

	const applyQuery = `
		UPDATE blend_positions p SET
			net_supplied = (p.net_supplied::numeric + u.ns_delta::numeric)::text,
			net_borrowed = CASE WHEN u.zero_borrowed THEN u.nb_delta
				ELSE (p.net_borrowed::numeric + u.nb_delta::numeric)::text END,
			last_modified_ledger = GREATEST(p.last_modified_ledger, u.ledger)
		FROM UNNEST($1::bytea[], $2::bytea[], $3::bytea[], $4::text[], $5::text[], $6::boolean[], $7::integer[])
			AS u(pool, usr, asset, ns_delta, nb_delta, zero_borrowed, ledger)
		JOIN blend_reserves r ON r.pool_contract_id = u.pool AND r.asset_contract_id = u.asset
		WHERE p.pool_contract_id = u.pool AND p.user_account_id = u.usr AND p.reserve_index = r.reserve_index`
	if _, err := dbTx.Exec(ctx, applyQuery, pools, users, assets, netSuppliedDeltas, netBorrowedDeltas, zeroBorrowedFlags, ledgers); err != nil {
		m.Metrics.QueryErrors.WithLabelValues("BatchApplyNetDeltas", positionsTable, utils.GetDBErrorType(err)).Inc()
		return fmt.Errorf("applying blend position net deltas: %w", err)
	}

	duration := time.Since(start).Seconds()
	m.Metrics.QueryDuration.WithLabelValues("BatchApplyNetDeltas", positionsTable).Observe(duration)
	m.Metrics.QueriesTotal.WithLabelValues("BatchApplyNetDeltas", positionsTable).Inc()
	m.Metrics.BatchSize.WithLabelValues("BatchApplyNetDeltas", positionsTable).Observe(float64(len(deltas)))
	return nil
}

// applyAuctionAdjustmentsSQL divides by rateScalar to convert protocol-token
// amounts (lot/bid, scaled by b_rate/d_rate) to underlying-asset amounts.
//
// The UNNEST rows are pre-aggregated per (pool, user, asset): Postgres's
// UPDATE ... FROM applies only ONE matching source row per target row, so two
// fills against the same position in one batch would otherwise silently drop
// an adjustment. Auction deltas are purely additive, so summing is exact.
var applyAuctionAdjustmentsSQL = fmt.Sprintf(`
	UPDATE blend_positions p SET
		net_supplied = (p.net_supplied::numeric + u.lot_b * r.b_rate::numeric / %[1]s)::text,
		net_borrowed = (p.net_borrowed::numeric + u.bid_d * r.d_rate::numeric / %[1]s)::text,
		last_modified_ledger = GREATEST(p.last_modified_ledger, u.ledger)
	FROM (
		SELECT raw.pool, raw.usr, raw.asset,
			SUM(raw.lot_b::numeric) AS lot_b, SUM(raw.bid_d::numeric) AS bid_d,
			MAX(raw.ledger) AS ledger
		FROM UNNEST($1::bytea[], $2::bytea[], $3::bytea[], $4::text[], $5::text[], $6::integer[])
			AS raw(pool, usr, asset, lot_b, bid_d, ledger)
		GROUP BY raw.pool, raw.usr, raw.asset
	) u
	JOIN blend_reserves r ON r.pool_contract_id = u.pool AND r.asset_contract_id = u.asset
	WHERE p.pool_contract_id = u.pool AND p.user_account_id = u.usr AND p.reserve_index = r.reserve_index`,
	rateScalar,
)

// ApplyAuctionAdjustments values protocol-token auction fills (lot/bid amounts)
// at the reserve's current b_rate/d_rate and applies the resulting underlying
// amount to cost basis. Adjustments whose asset has no matching blend_reserves
// row for the pool are silently skipped (0 rows affected). Multiple
// adjustments for the same (pool, user, asset) in one batch are summed
// server-side before applying.
func (m *PositionModel) ApplyAuctionAdjustments(ctx context.Context, dbTx pgx.Tx, adjs []PositionAuctionAdjustment) error {
	if len(adjs) == 0 {
		return nil
	}

	start := time.Now()

	pools := make([][]byte, len(adjs))
	users := make([][]byte, len(adjs))
	assets := make([][]byte, len(adjs))
	lotBTokensDeltas := make([]string, len(adjs))
	bidDTokensDeltas := make([]string, len(adjs))
	ledgers := make([]int32, len(adjs))
	for i, a := range adjs {
		poolBytes, err := addressToBytes(a.Pool)
		if err != nil {
			return fmt.Errorf("converting pool address for auction adjustment: %w", err)
		}
		userBytes, err := addressToBytes(a.User)
		if err != nil {
			return fmt.Errorf("converting user address for auction adjustment: %w", err)
		}
		assetBytes, err := addressToBytes(a.Asset)
		if err != nil {
			return fmt.Errorf("converting asset address for auction adjustment: %w", err)
		}
		pools[i] = poolBytes
		users[i] = userBytes
		assets[i] = assetBytes
		lotBTokensDeltas[i] = a.LotBTokensDelta
		bidDTokensDeltas[i] = a.BidDTokensDelta
		ledgers[i] = int32(a.LedgerNumber)
	}

	if _, err := dbTx.Exec(ctx, applyAuctionAdjustmentsSQL, pools, users, assets, lotBTokensDeltas, bidDTokensDeltas, ledgers); err != nil {
		m.Metrics.QueryErrors.WithLabelValues("ApplyAuctionAdjustments", positionsTable, utils.GetDBErrorType(err)).Inc()
		return fmt.Errorf("applying blend position auction adjustments: %w", err)
	}

	duration := time.Since(start).Seconds()
	m.Metrics.QueryDuration.WithLabelValues("ApplyAuctionAdjustments", positionsTable).Observe(duration)
	m.Metrics.QueriesTotal.WithLabelValues("ApplyAuctionAdjustments", positionsTable).Inc()
	m.Metrics.BatchSize.WithLabelValues("ApplyAuctionAdjustments", positionsTable).Observe(float64(len(adjs)))
	return nil
}
