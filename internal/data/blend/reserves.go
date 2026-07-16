// Per-pool reserve config/rate storage for Blend v2 (blend_reserves).
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

// reservesTable is the metrics label for blend_reserves queries.
const reservesTable = "blend_reserves"

// Reserve is a full row of blend_reserves (ResConfig + ResData + the reserve's
// asset/index identity), mirroring every column.
type Reserve struct {
	PoolContractID     types.AddressBytea
	ReserveIndex       int32
	AssetContractID    types.AddressBytea
	BRate              string
	DRate              string
	BSupply            string
	DSupply            string
	IRMod              string
	BackstopCredit     string
	LastTime           int64
	Decimals           int32
	CFactor            int32
	LFactor            int32
	Util               int32
	MaxUtil            int32
	RBase              int32
	ROne               int32
	RTwo               int32
	RThree             int32
	Reactivity         int32
	SupplyCap          string
	Enabled            bool
	LastModifiedLedger uint32
}

// ReserveDataUpdate carries only the ResData columns (live rates/accrual state
// that change on every pool interaction), keyed by (Pool, Asset). It is
// narrower than the full-row Reserve so per-interaction rate refreshes don't
// need to know or preserve ResConfig (decimals/factors/caps), which only
// changes on admin config updates.
type ReserveDataUpdate struct {
	Pool, Asset    string
	BRate          string
	DRate          string
	IRMod          string
	BSupply        string
	DSupply        string
	BackstopCredit string
	LastTime       int64
	LedgerNumber   uint32
}

// ReserveModelInterface exposes Blend v2 reserve storage operations.
type ReserveModelInterface interface {
	// BatchUpsert inserts or fully replaces reserve rows keyed by
	// (pool_contract_id, reserve_index). Every column is overwritten — callers
	// must supply the reserve's full current ResConfig+ResData together.
	BatchUpsert(ctx context.Context, dbTx pgx.Tx, rows []Reserve) error
	// BatchUpdateData updates only the ResData columns of existing rows, keyed
	// by (pool_contract_id, asset_contract_id). Rows with no matching (pool,
	// asset) are silently skipped (0 rows affected, no error).
	BatchUpdateData(ctx context.Context, dbTx pgx.Tx, rows []ReserveDataUpdate) error
}

// ReserveModel implements ReserveModelInterface against blend_reserves.
type ReserveModel struct {
	DB      *pgxpool.Pool
	Metrics *metrics.DBMetrics
}

var _ ReserveModelInterface = (*ReserveModel)(nil)

// BatchUpsert inserts or fully replaces blend_reserves rows. See ReserveModelInterface.
func (m *ReserveModel) BatchUpsert(ctx context.Context, dbTx pgx.Tx, rows []Reserve) error {
	if len(rows) == 0 {
		return nil
	}

	start := time.Now()

	pools := make([][]byte, len(rows))
	indexes := make([]int32, len(rows))
	assets := make([][]byte, len(rows))
	bRates := make([]string, len(rows))
	dRates := make([]string, len(rows))
	bSupplies := make([]string, len(rows))
	dSupplies := make([]string, len(rows))
	irMods := make([]string, len(rows))
	backstopCredits := make([]string, len(rows))
	lastTimes := make([]int64, len(rows))
	decimals := make([]int32, len(rows))
	cFactors := make([]int32, len(rows))
	lFactors := make([]int32, len(rows))
	targetUtils := make([]int32, len(rows))
	maxUtils := make([]int32, len(rows))
	rBases := make([]int32, len(rows))
	rOnes := make([]int32, len(rows))
	rTwos := make([]int32, len(rows))
	rThrees := make([]int32, len(rows))
	reactivities := make([]int32, len(rows))
	supplyCaps := make([]string, len(rows))
	enableds := make([]bool, len(rows))
	ledgers := make([]int32, len(rows))
	for i, r := range rows {
		poolBytes, err := addressToBytes(string(r.PoolContractID))
		if err != nil {
			return fmt.Errorf("converting pool address for reserve upsert: %w", err)
		}
		assetBytes, err := addressToBytes(string(r.AssetContractID))
		if err != nil {
			return fmt.Errorf("converting asset address for reserve upsert: %w", err)
		}
		pools[i] = poolBytes
		indexes[i] = r.ReserveIndex
		assets[i] = assetBytes
		bRates[i] = r.BRate
		dRates[i] = r.DRate
		bSupplies[i] = r.BSupply
		dSupplies[i] = r.DSupply
		irMods[i] = r.IRMod
		backstopCredits[i] = r.BackstopCredit
		lastTimes[i] = r.LastTime
		decimals[i] = r.Decimals
		cFactors[i] = r.CFactor
		lFactors[i] = r.LFactor
		targetUtils[i] = r.Util
		maxUtils[i] = r.MaxUtil
		rBases[i] = r.RBase
		rOnes[i] = r.ROne
		rTwos[i] = r.RTwo
		rThrees[i] = r.RThree
		reactivities[i] = r.Reactivity
		supplyCaps[i] = r.SupplyCap
		enableds[i] = r.Enabled
		ledgers[i] = int32(r.LastModifiedLedger)
	}

	const upsertQuery = `
		INSERT INTO blend_reserves (
			pool_contract_id, reserve_index, asset_contract_id,
			b_rate, d_rate, b_supply, d_supply, ir_mod, backstop_credit, last_time,
			decimals, c_factor, l_factor, util, max_util,
			r_base, r_one, r_two, r_three, reactivity, supply_cap, enabled,
			last_modified_ledger
		)
		SELECT * FROM UNNEST(
			$1::bytea[], $2::integer[], $3::bytea[],
			$4::text[], $5::text[], $6::text[], $7::text[], $8::text[], $9::text[], $10::bigint[],
			$11::integer[], $12::integer[], $13::integer[], $14::integer[], $15::integer[],
			$16::integer[], $17::integer[], $18::integer[], $19::integer[], $20::integer[], $21::text[], $22::boolean[],
			$23::integer[]
		)
		ON CONFLICT (pool_contract_id, reserve_index) DO UPDATE SET
			asset_contract_id    = EXCLUDED.asset_contract_id,
			b_rate               = EXCLUDED.b_rate,
			d_rate               = EXCLUDED.d_rate,
			b_supply             = EXCLUDED.b_supply,
			d_supply             = EXCLUDED.d_supply,
			ir_mod               = EXCLUDED.ir_mod,
			backstop_credit      = EXCLUDED.backstop_credit,
			last_time            = EXCLUDED.last_time,
			decimals             = EXCLUDED.decimals,
			c_factor             = EXCLUDED.c_factor,
			l_factor             = EXCLUDED.l_factor,
			util                 = EXCLUDED.util,
			max_util             = EXCLUDED.max_util,
			r_base               = EXCLUDED.r_base,
			r_one                = EXCLUDED.r_one,
			r_two                = EXCLUDED.r_two,
			r_three              = EXCLUDED.r_three,
			reactivity           = EXCLUDED.reactivity,
			supply_cap           = EXCLUDED.supply_cap,
			enabled              = EXCLUDED.enabled,
			last_modified_ledger = GREATEST(blend_reserves.last_modified_ledger, EXCLUDED.last_modified_ledger)`
	if _, err := dbTx.Exec(ctx, upsertQuery,
		pools, indexes, assets,
		bRates, dRates, bSupplies, dSupplies, irMods, backstopCredits, lastTimes,
		decimals, cFactors, lFactors, targetUtils, maxUtils,
		rBases, rOnes, rTwos, rThrees, reactivities, supplyCaps, enableds,
		ledgers,
	); err != nil {
		m.Metrics.QueryErrors.WithLabelValues("BatchUpsert", reservesTable, utils.GetDBErrorType(err)).Inc()
		return fmt.Errorf("upserting blend reserves: %w", err)
	}

	duration := time.Since(start).Seconds()
	m.Metrics.QueryDuration.WithLabelValues("BatchUpsert", reservesTable).Observe(duration)
	m.Metrics.QueriesTotal.WithLabelValues("BatchUpsert", reservesTable).Inc()
	m.Metrics.BatchSize.WithLabelValues("BatchUpsert", reservesTable).Observe(float64(len(rows)))
	return nil
}

// BatchUpdateData updates only the ResData columns of existing blend_reserves
// rows, keyed by (pool_contract_id, asset_contract_id). See ReserveModelInterface.
func (m *ReserveModel) BatchUpdateData(ctx context.Context, dbTx pgx.Tx, rows []ReserveDataUpdate) error {
	if len(rows) == 0 {
		return nil
	}

	start := time.Now()

	pools := make([][]byte, len(rows))
	assets := make([][]byte, len(rows))
	bRates := make([]string, len(rows))
	dRates := make([]string, len(rows))
	irMods := make([]string, len(rows))
	bSupplies := make([]string, len(rows))
	dSupplies := make([]string, len(rows))
	backstopCredits := make([]string, len(rows))
	lastTimes := make([]int64, len(rows))
	ledgers := make([]int32, len(rows))
	for i, r := range rows {
		poolBytes, err := addressToBytes(r.Pool)
		if err != nil {
			return fmt.Errorf("converting pool address for reserve data update: %w", err)
		}
		assetBytes, err := addressToBytes(r.Asset)
		if err != nil {
			return fmt.Errorf("converting asset address for reserve data update: %w", err)
		}
		pools[i] = poolBytes
		assets[i] = assetBytes
		bRates[i] = r.BRate
		dRates[i] = r.DRate
		irMods[i] = r.IRMod
		bSupplies[i] = r.BSupply
		dSupplies[i] = r.DSupply
		backstopCredits[i] = r.BackstopCredit
		lastTimes[i] = r.LastTime
		ledgers[i] = int32(r.LedgerNumber)
	}

	const updateQuery = `
		UPDATE blend_reserves r SET
			b_rate               = u.b_rate,
			d_rate               = u.d_rate,
			ir_mod               = u.ir_mod,
			b_supply             = u.b_supply,
			d_supply             = u.d_supply,
			backstop_credit      = u.backstop_credit,
			last_time            = u.last_time,
			last_modified_ledger = GREATEST(r.last_modified_ledger, u.ledger)
		FROM UNNEST(
			$1::bytea[], $2::bytea[], $3::text[], $4::text[], $5::text[],
			$6::text[], $7::text[], $8::text[], $9::bigint[], $10::integer[]
		) AS u(pool, asset, b_rate, d_rate, ir_mod, b_supply, d_supply, backstop_credit, last_time, ledger)
		WHERE r.pool_contract_id = u.pool AND r.asset_contract_id = u.asset`
	if _, err := dbTx.Exec(ctx, updateQuery,
		pools, assets, bRates, dRates, irMods, bSupplies, dSupplies, backstopCredits, lastTimes, ledgers,
	); err != nil {
		m.Metrics.QueryErrors.WithLabelValues("BatchUpdateData", reservesTable, utils.GetDBErrorType(err)).Inc()
		return fmt.Errorf("updating blend reserve data: %w", err)
	}

	duration := time.Since(start).Seconds()
	m.Metrics.QueryDuration.WithLabelValues("BatchUpdateData", reservesTable).Observe(duration)
	m.Metrics.QueriesTotal.WithLabelValues("BatchUpdateData", reservesTable).Inc()
	m.Metrics.BatchSize.WithLabelValues("BatchUpdateData", reservesTable).Observe(float64(len(rows)))
	return nil
}
