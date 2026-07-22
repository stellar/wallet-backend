// Read-side data access for Blend v2 GraphQL resolvers: per-account
// position/backstop-position/emission lookups, and pool/reserve/backstop-pool
// lookups scoped to a pool-ID list or spanning every pool. All queries are
// plain reads against m.DB (no caller transaction) with a deterministic
// ORDER BY so paging/rendering is stable across calls.
package blend

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/stellar/wallet-backend/internal/utils"
)

// addressesToBytes converts a slice of strkey addresses to their BYTEA
// representations, preserving order. Used by list-scoped readers that filter
// with `= ANY($1::bytea[])`.
func addressesToBytes(addrs []string) ([][]byte, error) {
	out := make([][]byte, len(addrs))
	for i, a := range addrs {
		b, err := addressToBytes(a)
		if err != nil {
			return nil, err
		}
		out[i] = b
	}
	return out, nil
}

// GetByAccount returns every blend_positions row held by account, ordered by
// (pool_contract_id, reserve_index). Returns an empty, non-nil slice if the
// account holds no positions.
func (m *PositionModel) GetByAccount(ctx context.Context, account string) ([]Position, error) {
	accountBytes, err := addressToBytes(account)
	if err != nil {
		return nil, fmt.Errorf("converting account address for position lookup: %w", err)
	}

	start := time.Now()
	const query = `
		SELECT pool_contract_id, user_account_id, reserve_index,
			supply_b_tokens, collateral_b_tokens, liability_d_tokens,
			net_supplied, net_borrowed, last_modified_ledger
		FROM blend_positions
		WHERE user_account_id = $1
		ORDER BY pool_contract_id, reserve_index`
	rows, err := m.DB.Query(ctx, query, accountBytes)
	if err != nil {
		m.Metrics.QueryErrors.WithLabelValues("GetByAccount", positionsTable, utils.GetDBErrorType(err)).Inc()
		return nil, fmt.Errorf("querying blend positions for account: %w", err)
	}
	defer rows.Close()

	positions := []Position{}
	for rows.Next() {
		var p Position
		if err := rows.Scan(
			&p.PoolContractID, &p.UserAccountID, &p.ReserveIndex,
			&p.SupplyBTokens, &p.CollateralBTokens, &p.LiabilityDTokens,
			&p.NetSupplied, &p.NetBorrowed, &p.LastModifiedLedger,
		); err != nil {
			m.Metrics.QueryErrors.WithLabelValues("GetByAccount", positionsTable, utils.GetDBErrorType(err)).Inc()
			return nil, fmt.Errorf("scanning blend position row: %w", err)
		}
		positions = append(positions, p)
	}
	if err := rows.Err(); err != nil {
		m.Metrics.QueryErrors.WithLabelValues("GetByAccount", positionsTable, utils.GetDBErrorType(err)).Inc()
		return nil, fmt.Errorf("iterating blend position rows: %w", err)
	}

	duration := time.Since(start).Seconds()
	m.Metrics.QueryDuration.WithLabelValues("GetByAccount", positionsTable).Observe(duration)
	m.Metrics.QueriesTotal.WithLabelValues("GetByAccount", positionsTable).Inc()
	return positions, nil
}

// GetByAccount returns every blend_pool_claimed row for account (one per pool it
// has claimed pool-reserve emissions from), ordered by pool_contract_id. Empty,
// non-nil slice if the account has never claimed.
func (m *PoolClaimedModel) GetByAccount(ctx context.Context, account string) ([]PoolClaimed, error) {
	accountBytes, err := addressToBytes(account)
	if err != nil {
		return nil, fmt.Errorf("converting account address for pool-claimed lookup: %w", err)
	}

	start := time.Now()
	const query = `
		SELECT pool_contract_id, user_account_id, claimed_blnd, last_modified_ledger
		FROM blend_pool_claimed
		WHERE user_account_id = $1
		ORDER BY pool_contract_id`
	rows, err := m.DB.Query(ctx, query, accountBytes)
	if err != nil {
		m.Metrics.QueryErrors.WithLabelValues("GetByAccount", poolClaimedTable, utils.GetDBErrorType(err)).Inc()
		return nil, fmt.Errorf("querying blend pool claimed totals for account: %w", err)
	}
	defer rows.Close()

	claimed := []PoolClaimed{}
	for rows.Next() {
		var c PoolClaimed
		if scanErr := rows.Scan(&c.PoolContractID, &c.UserAccountID, &c.ClaimedBlnd, &c.LastModifiedLedger); scanErr != nil {
			m.Metrics.QueryErrors.WithLabelValues("GetByAccount", poolClaimedTable, utils.GetDBErrorType(scanErr)).Inc()
			return nil, fmt.Errorf("scanning blend pool claimed row: %w", scanErr)
		}
		claimed = append(claimed, c)
	}
	if err := rows.Err(); err != nil {
		m.Metrics.QueryErrors.WithLabelValues("GetByAccount", poolClaimedTable, utils.GetDBErrorType(err)).Inc()
		return nil, fmt.Errorf("iterating blend pool claimed rows: %w", err)
	}

	duration := time.Since(start).Seconds()
	m.Metrics.QueryDuration.WithLabelValues("GetByAccount", poolClaimedTable).Observe(duration)
	m.Metrics.QueriesTotal.WithLabelValues("GetByAccount", poolClaimedTable).Inc()
	return claimed, nil
}

// GetByAccount returns account's single account-wide blend_backstop_claimed row,
// or nil if the account has never claimed backstop emissions.
func (m *BackstopClaimedModel) GetByAccount(ctx context.Context, account string) (*BackstopClaimed, error) {
	accountBytes, err := addressToBytes(account)
	if err != nil {
		return nil, fmt.Errorf("converting account address for backstop-claimed lookup: %w", err)
	}

	start := time.Now()
	const query = `
		SELECT user_account_id, claimed_lp, last_modified_ledger
		FROM blend_backstop_claimed
		WHERE user_account_id = $1`
	var c BackstopClaimed
	err = m.DB.QueryRow(ctx, query, accountBytes).Scan(&c.UserAccountID, &c.ClaimedLp, &c.LastModifiedLedger)
	duration := time.Since(start).Seconds()
	m.Metrics.QueryDuration.WithLabelValues("GetByAccount", backstopClaimedTable).Observe(duration)
	m.Metrics.QueriesTotal.WithLabelValues("GetByAccount", backstopClaimedTable).Inc()
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		m.Metrics.QueryErrors.WithLabelValues("GetByAccount", backstopClaimedTable, utils.GetDBErrorType(err)).Inc()
		return nil, fmt.Errorf("querying blend backstop claimed total for account: %w", err)
	}
	return &c, nil
}

// decodeQ4W unmarshals a blend_backstop_positions.q4w JSONB column. A NULL
// column (raw == nil) decodes to an empty, non-nil slice, mirroring how
// BatchUpsert's marshalQ4W always writes "[]" instead of SQL NULL.
func decodeQ4W(raw []byte) ([]Q4W, error) {
	if len(raw) == 0 {
		return []Q4W{}, nil
	}
	var q4w []Q4W
	if err := json.Unmarshal(raw, &q4w); err != nil {
		return nil, fmt.Errorf("unmarshalling q4w: %w", err)
	}
	if q4w == nil {
		q4w = []Q4W{}
	}
	return q4w, nil
}

// GetByAccount returns every blend_backstop_positions row held by account,
// ordered by pool_contract_id. Returns an empty, non-nil slice if the account
// holds no backstop positions.
func (m *BackstopPositionModel) GetByAccount(ctx context.Context, account string) ([]BackstopPosition, error) {
	accountBytes, err := addressToBytes(account)
	if err != nil {
		return nil, fmt.Errorf("converting account address for backstop position lookup: %w", err)
	}

	start := time.Now()
	const query = `
		SELECT pool_contract_id, user_account_id, shares, q4w, last_modified_ledger
		FROM blend_backstop_positions
		WHERE user_account_id = $1
		ORDER BY pool_contract_id`
	rows, err := m.DB.Query(ctx, query, accountBytes)
	if err != nil {
		m.Metrics.QueryErrors.WithLabelValues("GetByAccount", backstopPositionsTable, utils.GetDBErrorType(err)).Inc()
		return nil, fmt.Errorf("querying blend backstop positions for account: %w", err)
	}
	defer rows.Close()

	positions := []BackstopPosition{}
	for rows.Next() {
		var bp BackstopPosition
		var q4wRaw []byte
		if scanErr := rows.Scan(&bp.PoolContractID, &bp.UserAccountID, &bp.Shares, &q4wRaw, &bp.LastModifiedLedger); scanErr != nil {
			m.Metrics.QueryErrors.WithLabelValues("GetByAccount", backstopPositionsTable, utils.GetDBErrorType(scanErr)).Inc()
			return nil, fmt.Errorf("scanning blend backstop position row: %w", scanErr)
		}
		q4w, decodeErr := decodeQ4W(q4wRaw)
		if decodeErr != nil {
			return nil, decodeErr
		}
		bp.Q4W = q4w
		positions = append(positions, bp)
	}
	if err := rows.Err(); err != nil {
		m.Metrics.QueryErrors.WithLabelValues("GetByAccount", backstopPositionsTable, utils.GetDBErrorType(err)).Inc()
		return nil, fmt.Errorf("iterating blend backstop position rows: %w", err)
	}

	duration := time.Since(start).Seconds()
	m.Metrics.QueryDuration.WithLabelValues("GetByAccount", backstopPositionsTable).Observe(duration)
	m.Metrics.QueriesTotal.WithLabelValues("GetByAccount", backstopPositionsTable).Inc()
	return positions, nil
}

// scanPools scans every row of rows into a Pool slice, matching blend_pools'
// column order. Shared by GetByIDs and GetAll.
func scanPools(rows pgx.Rows) ([]Pool, error) {
	pools := []Pool{}
	for rows.Next() {
		var p Pool
		if err := rows.Scan(
			&p.PoolContractID, &p.Name, &p.OracleContractID, &p.BackstopRate,
			&p.Status, &p.MaxPositions, &p.MinCollateral, &p.Admin, &p.InRewardZone, &p.LastModifiedLedger,
		); err != nil {
			return nil, fmt.Errorf("scanning blend pool row: %w", err)
		}
		pools = append(pools, p)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating blend pool rows: %w", err)
	}
	return pools, nil
}

// GetByIDs returns the blend_pools rows for the given pool contract IDs,
// ordered by pool_contract_id. Unknown pool IDs are silently excluded. Returns
// an empty, non-nil slice without querying when poolIDs is empty.
func (m *PoolModel) GetByIDs(ctx context.Context, poolIDs []string) ([]Pool, error) {
	if len(poolIDs) == 0 {
		return []Pool{}, nil
	}
	pools, err := addressesToBytes(poolIDs)
	if err != nil {
		return nil, fmt.Errorf("converting pool addresses for pool lookup: %w", err)
	}

	start := time.Now()
	const query = `
		SELECT pool_contract_id, name, oracle_contract_id, backstop_rate,
			status, max_positions, min_collateral, admin, in_reward_zone, last_modified_ledger
		FROM blend_pools
		WHERE pool_contract_id = ANY($1::bytea[])
		ORDER BY pool_contract_id`
	rows, err := m.DB.Query(ctx, query, pools)
	if err != nil {
		m.Metrics.QueryErrors.WithLabelValues("GetByIDs", poolsTable, utils.GetDBErrorType(err)).Inc()
		return nil, fmt.Errorf("querying blend pools by IDs: %w", err)
	}
	defer rows.Close()

	result, err := scanPools(rows)
	if err != nil {
		m.Metrics.QueryErrors.WithLabelValues("GetByIDs", poolsTable, utils.GetDBErrorType(err)).Inc()
		return nil, err
	}

	duration := time.Since(start).Seconds()
	m.Metrics.QueryDuration.WithLabelValues("GetByIDs", poolsTable).Observe(duration)
	m.Metrics.QueriesTotal.WithLabelValues("GetByIDs", poolsTable).Inc()
	m.Metrics.BatchSize.WithLabelValues("GetByIDs", poolsTable).Observe(float64(len(poolIDs)))
	return result, nil
}

// GetAll returns every blend_pools row, ordered by pool_contract_id.
func (m *PoolModel) GetAll(ctx context.Context) ([]Pool, error) {
	start := time.Now()
	const query = `
		SELECT pool_contract_id, name, oracle_contract_id, backstop_rate,
			status, max_positions, min_collateral, admin, in_reward_zone, last_modified_ledger
		FROM blend_pools
		ORDER BY pool_contract_id`
	rows, err := m.DB.Query(ctx, query)
	if err != nil {
		m.Metrics.QueryErrors.WithLabelValues("GetAll", poolsTable, utils.GetDBErrorType(err)).Inc()
		return nil, fmt.Errorf("querying all blend pools: %w", err)
	}
	defer rows.Close()

	result, err := scanPools(rows)
	if err != nil {
		m.Metrics.QueryErrors.WithLabelValues("GetAll", poolsTable, utils.GetDBErrorType(err)).Inc()
		return nil, err
	}

	duration := time.Since(start).Seconds()
	m.Metrics.QueryDuration.WithLabelValues("GetAll", poolsTable).Observe(duration)
	m.Metrics.QueriesTotal.WithLabelValues("GetAll", poolsTable).Inc()
	return result, nil
}

// scanReserves scans every row of rows into a Reserve slice, matching
// blend_reserves' column order.
func scanReserves(rows pgx.Rows) ([]Reserve, error) {
	reserves := []Reserve{}
	for rows.Next() {
		var r Reserve
		if err := rows.Scan(
			&r.PoolContractID, &r.ReserveIndex, &r.AssetContractID,
			&r.BRate, &r.DRate, &r.BSupply, &r.DSupply, &r.IRMod, &r.BackstopCredit, &r.LastTime,
			&r.Decimals, &r.CFactor, &r.LFactor, &r.Util, &r.MaxUtil,
			&r.RBase, &r.ROne, &r.RTwo, &r.RThree, &r.Reactivity, &r.SupplyCap, &r.Enabled,
			&r.LastModifiedLedger,
		); err != nil {
			return nil, fmt.Errorf("scanning blend reserve row: %w", err)
		}
		reserves = append(reserves, r)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating blend reserve rows: %w", err)
	}
	return reserves, nil
}

const reservesSelectColumns = `
	pool_contract_id, reserve_index, asset_contract_id,
	b_rate, d_rate, b_supply, d_supply, ir_mod, backstop_credit, last_time,
	decimals, c_factor, l_factor, util, max_util,
	r_base, r_one, r_two, r_three, reactivity, supply_cap, enabled,
	last_modified_ledger`

// GetByPools returns the blend_reserves rows for the given pool contract IDs,
// ordered by (pool_contract_id, reserve_index). Returns an empty, non-nil
// slice without querying when poolIDs is empty.
func (m *ReserveModel) GetByPools(ctx context.Context, poolIDs []string) ([]Reserve, error) {
	if len(poolIDs) == 0 {
		return []Reserve{}, nil
	}
	pools, err := addressesToBytes(poolIDs)
	if err != nil {
		return nil, fmt.Errorf("converting pool addresses for reserve lookup: %w", err)
	}

	start := time.Now()
	query := fmt.Sprintf(`
		SELECT %s
		FROM blend_reserves
		WHERE pool_contract_id = ANY($1::bytea[])
		ORDER BY pool_contract_id, reserve_index`, reservesSelectColumns)
	rows, err := m.DB.Query(ctx, query, pools)
	if err != nil {
		m.Metrics.QueryErrors.WithLabelValues("GetByPools", reservesTable, utils.GetDBErrorType(err)).Inc()
		return nil, fmt.Errorf("querying blend reserves by pools: %w", err)
	}
	defer rows.Close()

	result, err := scanReserves(rows)
	if err != nil {
		m.Metrics.QueryErrors.WithLabelValues("GetByPools", reservesTable, utils.GetDBErrorType(err)).Inc()
		return nil, err
	}

	duration := time.Since(start).Seconds()
	m.Metrics.QueryDuration.WithLabelValues("GetByPools", reservesTable).Observe(duration)
	m.Metrics.QueriesTotal.WithLabelValues("GetByPools", reservesTable).Inc()
	m.Metrics.BatchSize.WithLabelValues("GetByPools", reservesTable).Observe(float64(len(poolIDs)))
	return result, nil
}

// GetByPools returns the blend_reserve_emissions rows for the given pool
// contract IDs, ordered by (pool_contract_id, reserve_token_id). Returns an
// empty, non-nil slice without querying when poolIDs is empty.
func (m *ReserveEmissionModel) GetByPools(ctx context.Context, poolIDs []string) ([]ReserveEmission, error) {
	if len(poolIDs) == 0 {
		return []ReserveEmission{}, nil
	}
	pools, err := addressesToBytes(poolIDs)
	if err != nil {
		return nil, fmt.Errorf("converting pool addresses for reserve emission lookup: %w", err)
	}

	start := time.Now()
	const query = `
		SELECT pool_contract_id, reserve_token_id, eps, emission_index, expiration, last_time, last_modified_ledger
		FROM blend_reserve_emissions
		WHERE pool_contract_id = ANY($1::bytea[])
		ORDER BY pool_contract_id, reserve_token_id`
	rows, err := m.DB.Query(ctx, query, pools)
	if err != nil {
		m.Metrics.QueryErrors.WithLabelValues("GetByPools", reserveEmissionsTable, utils.GetDBErrorType(err)).Inc()
		return nil, fmt.Errorf("querying blend reserve emissions by pools: %w", err)
	}
	defer rows.Close()

	emissions := []ReserveEmission{}
	for rows.Next() {
		var e ReserveEmission
		if err := rows.Scan(
			&e.PoolContractID, &e.ReserveTokenID, &e.Eps, &e.EmissionIndex, &e.Expiration, &e.LastTime, &e.LastModifiedLedger,
		); err != nil {
			m.Metrics.QueryErrors.WithLabelValues("GetByPools", reserveEmissionsTable, utils.GetDBErrorType(err)).Inc()
			return nil, fmt.Errorf("scanning blend reserve emission row: %w", err)
		}
		emissions = append(emissions, e)
	}
	if err := rows.Err(); err != nil {
		m.Metrics.QueryErrors.WithLabelValues("GetByPools", reserveEmissionsTable, utils.GetDBErrorType(err)).Inc()
		return nil, fmt.Errorf("iterating blend reserve emission rows: %w", err)
	}

	duration := time.Since(start).Seconds()
	m.Metrics.QueryDuration.WithLabelValues("GetByPools", reserveEmissionsTable).Observe(duration)
	m.Metrics.QueriesTotal.WithLabelValues("GetByPools", reserveEmissionsTable).Inc()
	m.Metrics.BatchSize.WithLabelValues("GetByPools", reserveEmissionsTable).Observe(float64(len(poolIDs)))
	return emissions, nil
}

// GetByIDs returns the blend_backstop_pools rows for the given pool contract
// IDs, ordered by pool_contract_id. Returns an empty, non-nil slice without
// querying when poolIDs is empty.
func (m *BackstopPoolModel) GetByIDs(ctx context.Context, poolIDs []string) ([]BackstopPool, error) {
	if len(poolIDs) == 0 {
		return []BackstopPool{}, nil
	}
	pools, err := addressesToBytes(poolIDs)
	if err != nil {
		return nil, fmt.Errorf("converting pool addresses for backstop pool lookup: %w", err)
	}

	start := time.Now()
	const query = `
		SELECT pool_contract_id, shares, tokens, q4w, emis_eps, emis_index, emis_expiration, emis_last_time, last_modified_ledger
		FROM blend_backstop_pools
		WHERE pool_contract_id = ANY($1::bytea[])
		ORDER BY pool_contract_id`
	rows, err := m.DB.Query(ctx, query, pools)
	if err != nil {
		m.Metrics.QueryErrors.WithLabelValues("GetByIDs", backstopPoolsTable, utils.GetDBErrorType(err)).Inc()
		return nil, fmt.Errorf("querying blend backstop pools by IDs: %w", err)
	}
	defer rows.Close()

	backstopPools := []BackstopPool{}
	for rows.Next() {
		var bp BackstopPool
		if err := rows.Scan(
			&bp.PoolContractID, &bp.Shares, &bp.Tokens, &bp.Q4W,
			&bp.EmisEps, &bp.EmisIndex, &bp.EmisExpiration, &bp.EmisLastTime, &bp.LastModifiedLedger,
		); err != nil {
			m.Metrics.QueryErrors.WithLabelValues("GetByIDs", backstopPoolsTable, utils.GetDBErrorType(err)).Inc()
			return nil, fmt.Errorf("scanning blend backstop pool row: %w", err)
		}
		backstopPools = append(backstopPools, bp)
	}
	if err := rows.Err(); err != nil {
		m.Metrics.QueryErrors.WithLabelValues("GetByIDs", backstopPoolsTable, utils.GetDBErrorType(err)).Inc()
		return nil, fmt.Errorf("iterating blend backstop pool rows: %w", err)
	}

	duration := time.Since(start).Seconds()
	m.Metrics.QueryDuration.WithLabelValues("GetByIDs", backstopPoolsTable).Observe(duration)
	m.Metrics.QueriesTotal.WithLabelValues("GetByIDs", backstopPoolsTable).Inc()
	m.Metrics.BatchSize.WithLabelValues("GetByIDs", backstopPoolsTable).Observe(float64(len(poolIDs)))
	return backstopPools, nil
}

// decodeAuctionAmounts unmarshals a blend_auctions.bid/lot JSONB column.
// marshalAuctionAmounts always writes "{}" rather than SQL NULL (see its
// doc), so a NOT NULL column is never actually empty, but this treats a
// zero-length read the same as "{}" defensively.
func decodeAuctionAmounts(raw []byte) (map[string]string, error) {
	if len(raw) == 0 {
		return map[string]string{}, nil
	}
	var amounts map[string]string
	if err := json.Unmarshal(raw, &amounts); err != nil {
		return nil, fmt.Errorf("unmarshalling auction amounts: %w", err)
	}
	if amounts == nil {
		amounts = map[string]string{}
	}
	return amounts, nil
}

// GetByAccount returns every blend_auctions row where account is the auction
// owner — the liquidated user for USER_LIQUIDATION, the backstop address for
// BAD_DEBT/INTEREST — ordered by (pool_contract_id, auction_type). Returns an
// empty, non-nil slice if the account owns no active auctions.
func (m *AuctionModel) GetByAccount(ctx context.Context, account string) ([]Auction, error) {
	accountBytes, err := addressToBytes(account)
	if err != nil {
		return nil, fmt.Errorf("converting account address for auction lookup: %w", err)
	}

	start := time.Now()
	const query = `
		SELECT pool_contract_id, user_account_id, auction_type, bid, lot, start_block, last_modified_ledger
		FROM blend_auctions
		WHERE user_account_id = $1
		ORDER BY pool_contract_id, auction_type`
	rows, err := m.DB.Query(ctx, query, accountBytes)
	if err != nil {
		m.Metrics.QueryErrors.WithLabelValues("GetByAccount", auctionsTable, utils.GetDBErrorType(err)).Inc()
		return nil, fmt.Errorf("querying blend auctions for account: %w", err)
	}
	defer rows.Close()

	auctions := []Auction{}
	for rows.Next() {
		var a Auction
		var bidRaw, lotRaw []byte
		if scanErr := rows.Scan(&a.Pool, &a.User, &a.AuctionType, &bidRaw, &lotRaw, &a.StartBlock, &a.LastModifiedLedger); scanErr != nil {
			m.Metrics.QueryErrors.WithLabelValues("GetByAccount", auctionsTable, utils.GetDBErrorType(scanErr)).Inc()
			return nil, fmt.Errorf("scanning blend auction row: %w", scanErr)
		}
		bid, decodeErr := decodeAuctionAmounts(bidRaw)
		if decodeErr != nil {
			return nil, decodeErr
		}
		lot, decodeErr := decodeAuctionAmounts(lotRaw)
		if decodeErr != nil {
			return nil, decodeErr
		}
		a.Bid = bid
		a.Lot = lot
		auctions = append(auctions, a)
	}
	if err := rows.Err(); err != nil {
		m.Metrics.QueryErrors.WithLabelValues("GetByAccount", auctionsTable, utils.GetDBErrorType(err)).Inc()
		return nil, fmt.Errorf("iterating blend auction rows: %w", err)
	}

	duration := time.Since(start).Seconds()
	m.Metrics.QueryDuration.WithLabelValues("GetByAccount", auctionsTable).Observe(duration)
	m.Metrics.QueriesTotal.WithLabelValues("GetByAccount", auctionsTable).Inc()
	return auctions, nil
}

// GetByAccount returns every blend_emissions row held by account (both
// reserve-emission streams and the backstop stream), ordered by
// (source_contract_id, token_id). Returns an empty, non-nil slice if the
// account has no emission accrual rows.
func (m *EmissionModel) GetByAccount(ctx context.Context, account string) ([]Emission, error) {
	accountBytes, err := addressToBytes(account)
	if err != nil {
		return nil, fmt.Errorf("converting account address for emission lookup: %w", err)
	}

	start := time.Now()
	const query = `
		SELECT source_contract_id, user_account_id, token_id, emission_index, accrued, last_modified_ledger
		FROM blend_emissions
		WHERE user_account_id = $1
		ORDER BY source_contract_id, token_id`
	rows, err := m.DB.Query(ctx, query, accountBytes)
	if err != nil {
		m.Metrics.QueryErrors.WithLabelValues("GetByAccount", emissionsTable, utils.GetDBErrorType(err)).Inc()
		return nil, fmt.Errorf("querying blend emissions for account: %w", err)
	}
	defer rows.Close()

	emissions := []Emission{}
	for rows.Next() {
		var e Emission
		if err := rows.Scan(
			&e.SourceContractID, &e.UserAccountID, &e.TokenID, &e.EmissionIndex, &e.Accrued, &e.LastModifiedLedger,
		); err != nil {
			m.Metrics.QueryErrors.WithLabelValues("GetByAccount", emissionsTable, utils.GetDBErrorType(err)).Inc()
			return nil, fmt.Errorf("scanning blend emission row: %w", err)
		}
		emissions = append(emissions, e)
	}
	if err := rows.Err(); err != nil {
		m.Metrics.QueryErrors.WithLabelValues("GetByAccount", emissionsTable, utils.GetDBErrorType(err)).Inc()
		return nil, fmt.Errorf("iterating blend emission rows: %w", err)
	}

	duration := time.Since(start).Seconds()
	m.Metrics.QueryDuration.WithLabelValues("GetByAccount", emissionsTable).Observe(duration)
	m.Metrics.QueriesTotal.WithLabelValues("GetByAccount", emissionsTable).Inc()
	return emissions, nil
}
