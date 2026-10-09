package data

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

// AMMPoolsModel stores the Soroban AMM pools whose swap events the trades processor trusts.
type AMMPoolsModel struct {
	DB      *pgxpool.Pool
	Metrics *metrics.DBMetrics
}

// BatchUpsert inserts pools, keeping the first registration of a pool.
func (m *AMMPoolsModel) BatchUpsert(ctx context.Context, dbTx pgx.Tx, pools []types.AMMPool) error {
	if len(pools) == 0 {
		return nil
	}
	ids := make([][]byte, len(pools))
	venues := make([]int16, len(pools))
	t0 := make([][]byte, len(pools))
	t1 := make([][]byte, len(pools))
	ledgers := make([]int32, len(pools))
	for i, p := range pools {
		var err error
		if ids[i], err = addressBytea(p.Pool); err != nil {
			return err
		}
		if t0[i], err = addressBytea(p.Token0); err != nil {
			return err
		}
		if t1[i], err = addressBytea(p.Token1); err != nil {
			return err
		}
		venues[i] = int16(p.Venue)
		ledgers[i] = int32(p.CreatedLedger)
	}
	const q = `
		INSERT INTO amm_pools (pool, venue, token_0, token_1, created_ledger)
		SELECT * FROM UNNEST($1::bytea[], $2::smallint[], $3::bytea[], $4::bytea[], $5::integer[])
		ON CONFLICT (pool) DO NOTHING`
	start := time.Now()
	_, err := dbTx.Exec(ctx, q, ids, venues, t0, t1, ledgers)
	m.Metrics.QueryDuration.WithLabelValues("BatchUpsert", "amm_pools").Observe(time.Since(start).Seconds())
	m.Metrics.QueriesTotal.WithLabelValues("BatchUpsert", "amm_pools").Inc()
	if err != nil {
		m.Metrics.QueryErrors.WithLabelValues("BatchUpsert", "amm_pools", utils.GetDBErrorType(err)).Inc()
		return fmt.Errorf("upserting amm_pools: %w", err)
	}
	return nil
}

// GetAll returns every registered pool.
func (m *AMMPoolsModel) GetAll(ctx context.Context) ([]types.AMMPool, error) {
	start := time.Now()
	rows, err := m.DB.Query(ctx, `SELECT pool, venue, token_0, token_1, created_ledger FROM amm_pools`)
	m.Metrics.QueryDuration.WithLabelValues("GetAll", "amm_pools").Observe(time.Since(start).Seconds())
	m.Metrics.QueriesTotal.WithLabelValues("GetAll", "amm_pools").Inc()
	if err != nil {
		m.Metrics.QueryErrors.WithLabelValues("GetAll", "amm_pools", utils.GetDBErrorType(err)).Inc()
		return nil, fmt.Errorf("querying amm_pools: %w", err)
	}
	defer rows.Close()
	var out []types.AMMPool
	for rows.Next() {
		var p types.AMMPool
		var pool, t0, t1 types.AddressBytea
		var venue int16
		var ledger int32
		if err := rows.Scan(&pool, &venue, &t0, &t1, &ledger); err != nil {
			return nil, fmt.Errorf("scanning amm_pools: %w", err)
		}
		p.Pool, p.Token0, p.Token1 = string(pool), string(t0), string(t1)
		p.Venue = types.TradeVenue(venue)
		p.CreatedLedger = uint32(ledger)
		out = append(out, p)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating amm_pools: %w", err)
	}
	return out, nil
}
