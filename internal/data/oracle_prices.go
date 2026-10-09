package data

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/stellar/wallet-backend/internal/indexer/types"
	"github.com/stellar/wallet-backend/internal/metrics"
	"github.com/stellar/wallet-backend/internal/utils"
)

// OraclePrice is the latest oracle reading for one asset, in USD.
type OraclePrice struct {
	Asset          string
	PriceUSD       float64
	PriceTimestamp int64
	UpdatedAt      time.Time
}

// OraclePricesModel stores the latest oracle reading per asset.
type OraclePricesModel struct {
	DB      *pgxpool.Pool
	Metrics *metrics.DBMetrics
}

// Upsert replaces each asset's reading.
func (m *OraclePricesModel) Upsert(ctx context.Context, prices []OraclePrice) error {
	if len(prices) == 0 {
		return nil
	}
	assets := make([][]byte, len(prices))
	usd := make([]float64, len(prices))
	ts := make([]int64, len(prices))
	for i, p := range prices {
		b, err := addressBytea(p.Asset)
		if err != nil {
			return err
		}
		assets[i] = b
		usd[i] = p.PriceUSD
		ts[i] = p.PriceTimestamp
	}
	const q = `
		INSERT INTO oracle_prices (asset, price_usd, price_timestamp, updated_at)
		SELECT u.*, NOW() FROM UNNEST($1::bytea[], $2::float8[], $3::bigint[]) AS u
		ON CONFLICT (asset) DO UPDATE SET
			price_usd = EXCLUDED.price_usd,
			price_timestamp = EXCLUDED.price_timestamp,
			updated_at = EXCLUDED.updated_at`
	start := time.Now()
	_, err := m.DB.Exec(ctx, q, assets, usd, ts)
	m.Metrics.QueryDuration.WithLabelValues("Upsert", "oracle_prices").Observe(time.Since(start).Seconds())
	m.Metrics.QueriesTotal.WithLabelValues("Upsert", "oracle_prices").Inc()
	if err != nil {
		m.Metrics.QueryErrors.WithLabelValues("Upsert", "oracle_prices", utils.GetDBErrorType(err)).Inc()
		return fmt.Errorf("upserting oracle_prices: %w", err)
	}
	return nil
}

// GetAll returns every asset's latest reading.
func (m *OraclePricesModel) GetAll(ctx context.Context) ([]OraclePrice, error) {
	start := time.Now()
	rows, err := m.DB.Query(ctx, `SELECT asset, price_usd, price_timestamp, updated_at FROM oracle_prices`)
	m.Metrics.QueryDuration.WithLabelValues("GetAll", "oracle_prices").Observe(time.Since(start).Seconds())
	m.Metrics.QueriesTotal.WithLabelValues("GetAll", "oracle_prices").Inc()
	if err != nil {
		m.Metrics.QueryErrors.WithLabelValues("GetAll", "oracle_prices", utils.GetDBErrorType(err)).Inc()
		return nil, fmt.Errorf("querying oracle_prices: %w", err)
	}
	defer rows.Close()
	var out []OraclePrice
	for rows.Next() {
		var p OraclePrice
		var asset types.AddressBytea
		if err := rows.Scan(&asset, &p.PriceUSD, &p.PriceTimestamp, &p.UpdatedAt); err != nil {
			return nil, fmt.Errorf("scanning oracle_prices: %w", err)
		}
		p.Asset = string(asset)
		out = append(out, p)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating oracle_prices: %w", err)
	}
	return out, nil
}
