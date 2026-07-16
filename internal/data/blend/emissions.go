// Reserve-level emission config and raw user emission accrual storage for
// Blend v2 (blend_reserve_emissions, blend_emissions).
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

// reserveEmissionsTable is the metrics label for blend_reserve_emissions queries.
const reserveEmissionsTable = "blend_reserve_emissions"

// emissionsTable is the metrics label for blend_emissions queries.
const emissionsTable = "blend_emissions"

// BackstopEmissionTokenID is the blend_emissions.token_id sentinel for a user's
// backstop emission stream; those rows carry the backstopped POOL as
// source_contract_id. Reserve-emission rows use token_id = reserve_index*2
// (+1 for bToken) >= 0, so the key spaces stay disjoint.
const BackstopEmissionTokenID int32 = -1

// ReserveEmission is a row of blend_reserve_emissions: a pool's per-reserve-token
// emission config/accrual (EmisData(reserve_token_id) on the pool contract).
type ReserveEmission struct {
	PoolContractID     types.AddressBytea
	ReserveTokenID     int32
	Eps                int64
	EmissionIndex      string
	Expiration         int64
	LastTime           int64
	LastModifiedLedger uint32
}

// ReserveEmissionModelInterface exposes Blend v2 reserve emission storage operations.
type ReserveEmissionModelInterface interface {
	// BatchUpsert inserts or fully replaces reserve emission rows keyed by
	// (pool_contract_id, reserve_token_id).
	BatchUpsert(ctx context.Context, dbTx pgx.Tx, rows []ReserveEmission) error
}

// ReserveEmissionModel implements ReserveEmissionModelInterface against blend_reserve_emissions.
type ReserveEmissionModel struct {
	DB      *pgxpool.Pool
	Metrics *metrics.DBMetrics
}

var _ ReserveEmissionModelInterface = (*ReserveEmissionModel)(nil)

// BatchUpsert inserts or fully replaces blend_reserve_emissions rows. See
// ReserveEmissionModelInterface.
func (m *ReserveEmissionModel) BatchUpsert(ctx context.Context, dbTx pgx.Tx, rows []ReserveEmission) error {
	if len(rows) == 0 {
		return nil
	}

	start := time.Now()

	pools := make([][]byte, len(rows))
	tokenIDs := make([]int32, len(rows))
	epsValues := make([]int64, len(rows))
	emissionIndexes := make([]string, len(rows))
	expirations := make([]int64, len(rows))
	lastTimes := make([]int64, len(rows))
	ledgers := make([]int32, len(rows))
	for i, r := range rows {
		poolBytes, err := addressToBytes(string(r.PoolContractID))
		if err != nil {
			return fmt.Errorf("converting pool address for reserve emission upsert: %w", err)
		}
		pools[i] = poolBytes
		tokenIDs[i] = r.ReserveTokenID
		epsValues[i] = r.Eps
		emissionIndexes[i] = r.EmissionIndex
		expirations[i] = r.Expiration
		lastTimes[i] = r.LastTime
		ledgers[i] = int32(r.LastModifiedLedger)
	}

	const upsertQuery = `
		INSERT INTO blend_reserve_emissions (
			pool_contract_id, reserve_token_id, eps, emission_index, expiration, last_time, last_modified_ledger
		)
		SELECT * FROM UNNEST(
			$1::bytea[], $2::integer[], $3::bigint[], $4::text[], $5::bigint[], $6::bigint[], $7::integer[]
		)
		ON CONFLICT (pool_contract_id, reserve_token_id) DO UPDATE SET
			eps                  = EXCLUDED.eps,
			emission_index       = EXCLUDED.emission_index,
			expiration           = EXCLUDED.expiration,
			last_time            = EXCLUDED.last_time,
			last_modified_ledger = GREATEST(blend_reserve_emissions.last_modified_ledger, EXCLUDED.last_modified_ledger)`
	if _, err := dbTx.Exec(ctx, upsertQuery, pools, tokenIDs, epsValues, emissionIndexes, expirations, lastTimes, ledgers); err != nil {
		m.Metrics.QueryErrors.WithLabelValues("BatchUpsert", reserveEmissionsTable, utils.GetDBErrorType(err)).Inc()
		return fmt.Errorf("upserting blend reserve emissions: %w", err)
	}

	duration := time.Since(start).Seconds()
	m.Metrics.QueryDuration.WithLabelValues("BatchUpsert", reserveEmissionsTable).Observe(duration)
	m.Metrics.QueriesTotal.WithLabelValues("BatchUpsert", reserveEmissionsTable).Inc()
	m.Metrics.BatchSize.WithLabelValues("BatchUpsert", reserveEmissionsTable).Observe(float64(len(rows)))
	return nil
}

// Emission is a row of blend_emissions: a user's raw emission accrual for one
// token stream, either reserve emissions on a pool (TokenID >= 0) or backstop
// emissions on a backstop (TokenID == BackstopEmissionTokenID).
type Emission struct {
	SourceContractID   types.AddressBytea
	UserAccountID      types.AddressBytea
	TokenID            int32
	EmissionIndex      string
	Accrued            string
	LastModifiedLedger uint32
}

// EmissionModelInterface exposes Blend v2 user emission storage operations.
type EmissionModelInterface interface {
	// BatchUpsert inserts or fully replaces emission rows keyed by
	// (source_contract_id, user_account_id, token_id).
	BatchUpsert(ctx context.Context, dbTx pgx.Tx, rows []Emission) error
}

// EmissionModel implements EmissionModelInterface against blend_emissions.
type EmissionModel struct {
	DB      *pgxpool.Pool
	Metrics *metrics.DBMetrics
}

var _ EmissionModelInterface = (*EmissionModel)(nil)

// BatchUpsert inserts or fully replaces blend_emissions rows. See
// EmissionModelInterface.
func (m *EmissionModel) BatchUpsert(ctx context.Context, dbTx pgx.Tx, rows []Emission) error {
	if len(rows) == 0 {
		return nil
	}

	start := time.Now()

	sources := make([][]byte, len(rows))
	users := make([][]byte, len(rows))
	tokenIDs := make([]int32, len(rows))
	emissionIndexes := make([]string, len(rows))
	accrueds := make([]string, len(rows))
	ledgers := make([]int32, len(rows))
	for i, r := range rows {
		sourceBytes, err := addressToBytes(string(r.SourceContractID))
		if err != nil {
			return fmt.Errorf("converting source address for emission upsert: %w", err)
		}
		userBytes, err := addressToBytes(string(r.UserAccountID))
		if err != nil {
			return fmt.Errorf("converting user address for emission upsert: %w", err)
		}
		sources[i] = sourceBytes
		users[i] = userBytes
		tokenIDs[i] = r.TokenID
		emissionIndexes[i] = r.EmissionIndex
		accrueds[i] = r.Accrued
		ledgers[i] = int32(r.LastModifiedLedger)
	}

	const upsertQuery = `
		INSERT INTO blend_emissions (
			source_contract_id, user_account_id, token_id, emission_index, accrued, last_modified_ledger
		)
		SELECT * FROM UNNEST(
			$1::bytea[], $2::bytea[], $3::integer[], $4::text[], $5::text[], $6::integer[]
		)
		ON CONFLICT (source_contract_id, user_account_id, token_id) DO UPDATE SET
			emission_index       = EXCLUDED.emission_index,
			accrued              = EXCLUDED.accrued,
			last_modified_ledger = GREATEST(blend_emissions.last_modified_ledger, EXCLUDED.last_modified_ledger)`
	if _, err := dbTx.Exec(ctx, upsertQuery, sources, users, tokenIDs, emissionIndexes, accrueds, ledgers); err != nil {
		m.Metrics.QueryErrors.WithLabelValues("BatchUpsert", emissionsTable, utils.GetDBErrorType(err)).Inc()
		return fmt.Errorf("upserting blend emissions: %w", err)
	}

	duration := time.Since(start).Seconds()
	m.Metrics.QueryDuration.WithLabelValues("BatchUpsert", emissionsTable).Observe(duration)
	m.Metrics.QueriesTotal.WithLabelValues("BatchUpsert", emissionsTable).Inc()
	m.Metrics.BatchSize.WithLabelValues("BatchUpsert", emissionsTable).Observe(float64(len(rows)))
	return nil
}
