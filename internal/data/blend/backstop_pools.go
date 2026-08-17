// Pool-level backstop totals + emission accrual storage for Blend v2 (blend_backstop_pools).
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

// backstopPoolsTable is the metrics label for blend_backstop_pools queries.
const backstopPoolsTable = "blend_backstop_pools"

// BackstopPool is a row of blend_backstop_pools: pool-level backstop totals
// (PoolBalance(pool) on the backstop contract) plus that pool's backstop
// emission accrual (BEmisData(pool)). The emis_* fields are pointers because
// BatchUpsertBalances and BatchUpsertEmissions each only ever know one half of
// the row.
type BackstopPool struct {
	PoolContractID types.AddressBytea
	Shares         string
	Tokens         string
	Q4W            string
	EmisEps        *int64
	EmisIndex      *string
	EmisExpiration *int64
	EmisLastTime   *int64
	// LedgerNumber is written to last_modified_ledger.
	LastModifiedLedger uint32
}

// BackstopPoolEmission carries only the emis_* columns of blend_backstop_pools,
// keyed by Pool. It is narrower than BackstopPool so a backstop emission update
// doesn't need to know or preserve the pool's shares/tokens/q4w balances.
type BackstopPoolEmission struct {
	Pool           string
	EmisEps        *int64
	EmisIndex      *string
	EmisExpiration *int64
	EmisLastTime   *int64
	LedgerNumber   uint32
}

// BackstopPoolModelInterface exposes Blend v2 backstop pool storage operations.
type BackstopPoolModelInterface interface {
	// BatchUpsertBalances inserts or updates only the shares/tokens/q4w columns.
	// On a fresh insert the emis_* columns are left NULL.
	BatchUpsertBalances(ctx context.Context, dbTx pgx.Tx, rows []BackstopPool) error
	// BatchUpsertEmissions inserts or updates only the emis_* columns. On a
	// fresh insert the shares/tokens/q4w columns fall to their table DEFAULT '0'.
	BatchUpsertEmissions(ctx context.Context, dbTx pgx.Tx, rows []BackstopPoolEmission) error
}

// BackstopPoolModel implements BackstopPoolModelInterface against blend_backstop_pools.
type BackstopPoolModel struct {
	DB      *pgxpool.Pool
	Metrics *metrics.DBMetrics
}

var _ BackstopPoolModelInterface = (*BackstopPoolModel)(nil)

// BatchUpsertBalances inserts or updates only shares/tokens/q4w. See
// BackstopPoolModelInterface.
func (m *BackstopPoolModel) BatchUpsertBalances(ctx context.Context, dbTx pgx.Tx, rows []BackstopPool) error {
	if len(rows) == 0 {
		return nil
	}

	start := time.Now()

	pools := make([][]byte, len(rows))
	shares := make([]string, len(rows))
	tokens := make([]string, len(rows))
	q4ws := make([]string, len(rows))
	ledgers := make([]int32, len(rows))
	for i, r := range rows {
		poolBytes, err := addressToBytes(string(r.PoolContractID))
		if err != nil {
			return fmt.Errorf("converting pool address for backstop pool balance upsert: %w", err)
		}
		pools[i] = poolBytes
		shares[i] = r.Shares
		tokens[i] = r.Tokens
		q4ws[i] = r.Q4W
		ledgers[i] = int32(r.LastModifiedLedger)
	}

	const upsertQuery = `
		INSERT INTO blend_backstop_pools (pool_contract_id, shares, tokens, q4w, last_modified_ledger)
		SELECT * FROM UNNEST($1::bytea[], $2::text[], $3::text[], $4::text[], $5::integer[])
		ON CONFLICT (pool_contract_id) DO UPDATE SET
			shares               = EXCLUDED.shares,
			tokens               = EXCLUDED.tokens,
			q4w                  = EXCLUDED.q4w,
			last_modified_ledger = GREATEST(blend_backstop_pools.last_modified_ledger, EXCLUDED.last_modified_ledger)`
	if _, err := dbTx.Exec(ctx, upsertQuery, pools, shares, tokens, q4ws, ledgers); err != nil {
		m.Metrics.QueryErrors.WithLabelValues("BatchUpsertBalances", backstopPoolsTable, utils.GetDBErrorType(err)).Inc()
		return fmt.Errorf("upserting blend backstop pool balances: %w", err)
	}

	duration := time.Since(start).Seconds()
	m.Metrics.QueryDuration.WithLabelValues("BatchUpsertBalances", backstopPoolsTable).Observe(duration)
	m.Metrics.QueriesTotal.WithLabelValues("BatchUpsertBalances", backstopPoolsTable).Inc()
	m.Metrics.BatchSize.WithLabelValues("BatchUpsertBalances", backstopPoolsTable).Observe(float64(len(rows)))
	return nil
}

// BatchUpsertEmissions inserts or updates only the emis_* columns. See
// BackstopPoolModelInterface.
func (m *BackstopPoolModel) BatchUpsertEmissions(ctx context.Context, dbTx pgx.Tx, rows []BackstopPoolEmission) error {
	if len(rows) == 0 {
		return nil
	}

	start := time.Now()

	pools := make([][]byte, len(rows))
	emisEps := make([]*int64, len(rows))
	emisIndexes := make([]*string, len(rows))
	emisExpirations := make([]*int64, len(rows))
	emisLastTimes := make([]*int64, len(rows))
	ledgers := make([]int32, len(rows))
	for i, r := range rows {
		poolBytes, err := addressToBytes(r.Pool)
		if err != nil {
			return fmt.Errorf("converting pool address for backstop pool emission upsert: %w", err)
		}
		pools[i] = poolBytes
		emisEps[i] = r.EmisEps
		emisIndexes[i] = r.EmisIndex
		emisExpirations[i] = r.EmisExpiration
		emisLastTimes[i] = r.EmisLastTime
		ledgers[i] = int32(r.LedgerNumber)
	}

	const upsertQuery = `
		INSERT INTO blend_backstop_pools (pool_contract_id, emis_eps, emis_index, emis_expiration, emis_last_time, last_modified_ledger)
		SELECT * FROM UNNEST($1::bytea[], $2::bigint[], $3::text[], $4::bigint[], $5::bigint[], $6::integer[])
		ON CONFLICT (pool_contract_id) DO UPDATE SET
			emis_eps             = EXCLUDED.emis_eps,
			emis_index           = EXCLUDED.emis_index,
			emis_expiration      = EXCLUDED.emis_expiration,
			emis_last_time       = EXCLUDED.emis_last_time,
			last_modified_ledger = GREATEST(blend_backstop_pools.last_modified_ledger, EXCLUDED.last_modified_ledger)`
	if _, err := dbTx.Exec(ctx, upsertQuery, pools, emisEps, emisIndexes, emisExpirations, emisLastTimes, ledgers); err != nil {
		m.Metrics.QueryErrors.WithLabelValues("BatchUpsertEmissions", backstopPoolsTable, utils.GetDBErrorType(err)).Inc()
		return fmt.Errorf("upserting blend backstop pool emissions: %w", err)
	}

	duration := time.Since(start).Seconds()
	m.Metrics.QueryDuration.WithLabelValues("BatchUpsertEmissions", backstopPoolsTable).Observe(duration)
	m.Metrics.QueriesTotal.WithLabelValues("BatchUpsertEmissions", backstopPoolsTable).Inc()
	m.Metrics.BatchSize.WithLabelValues("BatchUpsertEmissions", backstopPoolsTable).Observe(float64(len(rows)))
	return nil
}
