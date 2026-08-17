// Oracle price snapshot storage for Blend v2 (blend_oracle_prices).
package blend

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/stellar/wallet-backend/internal/indexer/types"
	"github.com/stellar/wallet-backend/internal/metrics"
	"github.com/stellar/wallet-backend/internal/utils"
)

// oraclePricesTable is the metrics label for blend_oracle_prices queries.
const oraclePricesTable = "blend_oracle_prices"

// PriceTarget identifies one (oracle, asset) pair that some pool's reserve
// prices against — the input list the SEP-40 lastprice snapshot task queries
// on each tick.
type PriceTarget struct {
	OracleContractID types.AddressBytea
	AssetContractID  types.AddressBytea
}

// OraclePrice is a full row of blend_oracle_prices: the current-only (not
// historical) latest known SEP-40 price for one (oracle, asset) pair.
type OraclePrice struct {
	OracleContractID types.AddressBytea
	AssetContractID  types.AddressBytea
	Price            string // raw i128 fixed-point value at PriceDecimals
	PriceDecimals    int32
	PriceTimestamp   int64 // oracle-reported timestamp
	// UpdatedAt is populated when scanning an existing row. BatchUpsert ignores
	// it on write and always sets updated_at to NOW() itself.
	UpdatedAt time.Time
}

// OraclePriceModelInterface exposes Blend v2 oracle price snapshot storage
// operations.
//
// Unlike sibling Blend models, both methods here operate directly on the
// pgxpool rather than a caller-supplied pgx.Tx: the price snapshot task runs
// on its own schedule, independent of ledger ingestion, so there is no
// enclosing ingest transaction for it to join.
type OraclePriceModelInterface interface {
	// GetPriceTargets returns the deduplicated set of (oracle, asset) pairs
	// that some pool's reserve prices against — every blend_reserves row
	// joined to its pool's oracle. Pools with no oracle wired yet (NULL
	// oracle_contract_id) are excluded.
	GetPriceTargets(ctx context.Context) ([]PriceTarget, error)
	// BatchUpsert inserts or fully replaces price rows keyed by
	// (oracle_contract_id, asset_contract_id). updated_at is always set to
	// NOW(), whether the row is freshly inserted or replaced.
	BatchUpsert(ctx context.Context, rows []OraclePrice) error
}

// OraclePriceModel implements OraclePriceModelInterface against blend_oracle_prices.
type OraclePriceModel struct {
	DB      *pgxpool.Pool
	Metrics *metrics.DBMetrics
}

var _ OraclePriceModelInterface = (*OraclePriceModel)(nil)

// GetPriceTargets returns the deduplicated (oracle, asset) pairs derived from
// blend_reserves joined to their pool's oracle. See OraclePriceModelInterface.
func (m *OraclePriceModel) GetPriceTargets(ctx context.Context) ([]PriceTarget, error) {
	start := time.Now()

	const query = `
		SELECT DISTINCT p.oracle_contract_id, r.asset_contract_id
		FROM blend_pools p
		JOIN blend_reserves r ON r.pool_contract_id = p.pool_contract_id
		WHERE p.oracle_contract_id IS NOT NULL
		ORDER BY p.oracle_contract_id, r.asset_contract_id`
	rows, err := m.DB.Query(ctx, query)
	if err != nil {
		m.Metrics.QueryErrors.WithLabelValues("GetPriceTargets", oraclePricesTable, utils.GetDBErrorType(err)).Inc()
		return nil, fmt.Errorf("querying blend oracle price targets: %w", err)
	}
	defer rows.Close()

	var targets []PriceTarget
	for rows.Next() {
		var t PriceTarget
		if err := rows.Scan(&t.OracleContractID, &t.AssetContractID); err != nil {
			m.Metrics.QueryErrors.WithLabelValues("GetPriceTargets", oraclePricesTable, utils.GetDBErrorType(err)).Inc()
			return nil, fmt.Errorf("scanning blend oracle price target row: %w", err)
		}
		targets = append(targets, t)
	}
	if err := rows.Err(); err != nil {
		m.Metrics.QueryErrors.WithLabelValues("GetPriceTargets", oraclePricesTable, utils.GetDBErrorType(err)).Inc()
		return nil, fmt.Errorf("iterating blend oracle price target rows: %w", err)
	}

	duration := time.Since(start).Seconds()
	m.Metrics.QueryDuration.WithLabelValues("GetPriceTargets", oraclePricesTable).Observe(duration)
	m.Metrics.QueriesTotal.WithLabelValues("GetPriceTargets", oraclePricesTable).Inc()
	return targets, nil
}

// BatchUpsert inserts or fully replaces blend_oracle_prices rows. See OraclePriceModelInterface.
func (m *OraclePriceModel) BatchUpsert(ctx context.Context, rows []OraclePrice) error {
	if len(rows) == 0 {
		return nil
	}

	start := time.Now()

	oracles := make([][]byte, len(rows))
	assets := make([][]byte, len(rows))
	prices := make([]string, len(rows))
	decimals := make([]int32, len(rows))
	timestamps := make([]int64, len(rows))
	for i, r := range rows {
		oracleBytes, err := addressToBytes(string(r.OracleContractID))
		if err != nil {
			return fmt.Errorf("converting oracle address for price upsert: %w", err)
		}
		assetBytes, err := addressToBytes(string(r.AssetContractID))
		if err != nil {
			return fmt.Errorf("converting asset address for price upsert: %w", err)
		}
		oracles[i] = oracleBytes
		assets[i] = assetBytes
		prices[i] = r.Price
		decimals[i] = r.PriceDecimals
		timestamps[i] = r.PriceTimestamp
	}

	const upsertQuery = `
		INSERT INTO blend_oracle_prices (oracle_contract_id, asset_contract_id, price, price_decimals, price_timestamp, updated_at)
		SELECT u.*, NOW() FROM UNNEST($1::bytea[], $2::bytea[], $3::text[], $4::integer[], $5::bigint[])
			AS u(oracle_contract_id, asset_contract_id, price, price_decimals, price_timestamp)
		ON CONFLICT (oracle_contract_id, asset_contract_id) DO UPDATE SET
			price = EXCLUDED.price, price_decimals = EXCLUDED.price_decimals,
			price_timestamp = EXCLUDED.price_timestamp, updated_at = NOW()`
	if _, err := m.DB.Exec(ctx, upsertQuery, oracles, assets, prices, decimals, timestamps); err != nil {
		m.Metrics.QueryErrors.WithLabelValues("BatchUpsert", oraclePricesTable, utils.GetDBErrorType(err)).Inc()
		return fmt.Errorf("upserting blend oracle prices: %w", err)
	}

	duration := time.Since(start).Seconds()
	m.Metrics.QueryDuration.WithLabelValues("BatchUpsert", oraclePricesTable).Observe(duration)
	m.Metrics.QueriesTotal.WithLabelValues("BatchUpsert", oraclePricesTable).Inc()
	m.Metrics.BatchSize.WithLabelValues("BatchUpsert", oraclePricesTable).Observe(float64(len(rows)))
	return nil
}
