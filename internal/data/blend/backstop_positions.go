// Per-user backstop position storage for Blend v2 (blend_backstop_positions).
package blend

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/stellar/wallet-backend/internal/indexer/types"
	"github.com/stellar/wallet-backend/internal/metrics"
	"github.com/stellar/wallet-backend/internal/utils"
)

// backstopPositionsTable is the metrics label for blend_backstop_positions queries.
const backstopPositionsTable = "blend_backstop_positions"

// Q4W is one queued withdrawal inside blend_backstop_positions.q4w (JSONB).
type Q4W struct {
	Amount     string `json:"amount"`
	Expiration int64  `json:"expiration"`
}

// BackstopPosition is a row of blend_backstop_positions: a user's backstop
// shares and queued-withdrawal list for one pool (UserBalance(pool, user)).
type BackstopPosition struct {
	PoolContractID     types.AddressBytea
	UserAccountID      types.AddressBytea
	Shares             string
	Q4W                []Q4W
	LastModifiedLedger uint32
}

// BackstopPositionModelInterface exposes Blend v2 backstop position storage operations.
type BackstopPositionModelInterface interface {
	// BatchUpsert inserts or fully replaces backstop position rows keyed by
	// (pool_contract_id, user_account_id).
	BatchUpsert(ctx context.Context, dbTx pgx.Tx, rows []BackstopPosition) error
	// DeleteByPoolUser removes every backstop position row for the given
	// (pool, user) pairs (full exit).
	DeleteByPoolUser(ctx context.Context, dbTx pgx.Tx, keys []PoolUserKey) error
}

// BackstopPositionModel implements BackstopPositionModelInterface against blend_backstop_positions.
type BackstopPositionModel struct {
	DB      *pgxpool.Pool
	Metrics *metrics.DBMetrics
}

var _ BackstopPositionModelInterface = (*BackstopPositionModel)(nil)

// marshalQ4W serializes a queued-withdrawal list for storage in the q4w JSONB
// column. A nil/empty slice serializes as "[]" (not SQL NULL or JSON null) so
// readers can always unmarshal the column into []Q4W without a null check.
func marshalQ4W(q4w []Q4W) (string, error) {
	if len(q4w) == 0 {
		return "[]", nil
	}
	b, err := json.Marshal(q4w)
	if err != nil {
		return "", fmt.Errorf("marshalling q4w: %w", err)
	}
	return string(b), nil
}

// BatchUpsert inserts or fully replaces blend_backstop_positions rows. See
// BackstopPositionModelInterface.
func (m *BackstopPositionModel) BatchUpsert(ctx context.Context, dbTx pgx.Tx, rows []BackstopPosition) error {
	if len(rows) == 0 {
		return nil
	}

	start := time.Now()

	pools := make([][]byte, len(rows))
	users := make([][]byte, len(rows))
	shares := make([]string, len(rows))
	q4ws := make([]string, len(rows))
	ledgers := make([]int32, len(rows))
	for i, r := range rows {
		poolBytes, err := addressToBytes(string(r.PoolContractID))
		if err != nil {
			return fmt.Errorf("converting pool address for backstop position upsert: %w", err)
		}
		userBytes, err := addressToBytes(string(r.UserAccountID))
		if err != nil {
			return fmt.Errorf("converting user address for backstop position upsert: %w", err)
		}
		q4wJSON, err := marshalQ4W(r.Q4W)
		if err != nil {
			return fmt.Errorf("marshalling q4w for backstop position upsert: %w", err)
		}
		pools[i] = poolBytes
		users[i] = userBytes
		shares[i] = r.Shares
		q4ws[i] = q4wJSON
		ledgers[i] = int32(r.LastModifiedLedger)
	}

	const upsertQuery = `
		INSERT INTO blend_backstop_positions (pool_contract_id, user_account_id, shares, q4w, last_modified_ledger)
		SELECT u.pool, u.usr, u.shares::numeric, u.q4w, u.ledger
		FROM UNNEST($1::bytea[], $2::bytea[], $3::text[], $4::text[]::jsonb[], $5::integer[])
			AS u(pool, usr, shares, q4w, ledger)
		ON CONFLICT (pool_contract_id, user_account_id) DO UPDATE SET
			shares               = EXCLUDED.shares,
			q4w                  = EXCLUDED.q4w,
			last_modified_ledger = EXCLUDED.last_modified_ledger`
	if _, err := dbTx.Exec(ctx, upsertQuery, pools, users, shares, q4ws, ledgers); err != nil {
		m.Metrics.QueryErrors.WithLabelValues("BatchUpsert", backstopPositionsTable, utils.GetDBErrorType(err)).Inc()
		return fmt.Errorf("upserting blend backstop positions: %w", err)
	}

	duration := time.Since(start).Seconds()
	m.Metrics.QueryDuration.WithLabelValues("BatchUpsert", backstopPositionsTable).Observe(duration)
	m.Metrics.QueriesTotal.WithLabelValues("BatchUpsert", backstopPositionsTable).Inc()
	m.Metrics.BatchSize.WithLabelValues("BatchUpsert", backstopPositionsTable).Observe(float64(len(rows)))
	return nil
}

// DeleteByPoolUser removes every blend_backstop_positions row for the given
// (pool, user) pairs.
func (m *BackstopPositionModel) DeleteByPoolUser(ctx context.Context, dbTx pgx.Tx, keys []PoolUserKey) error {
	if len(keys) == 0 {
		return nil
	}

	start := time.Now()

	pools := make([][]byte, len(keys))
	users := make([][]byte, len(keys))
	for i, k := range keys {
		poolBytes, err := addressToBytes(k.Pool)
		if err != nil {
			return fmt.Errorf("converting pool address for backstop position delete: %w", err)
		}
		userBytes, err := addressToBytes(k.User)
		if err != nil {
			return fmt.Errorf("converting user address for backstop position delete: %w", err)
		}
		pools[i] = poolBytes
		users[i] = userBytes
	}

	const deleteQuery = `
		DELETE FROM blend_backstop_positions
		WHERE (pool_contract_id, user_account_id) IN (SELECT * FROM UNNEST($1::bytea[], $2::bytea[]))`
	if _, err := dbTx.Exec(ctx, deleteQuery, pools, users); err != nil {
		m.Metrics.QueryErrors.WithLabelValues("DeleteByPoolUser", backstopPositionsTable, utils.GetDBErrorType(err)).Inc()
		return fmt.Errorf("deleting blend backstop positions: %w", err)
	}

	duration := time.Since(start).Seconds()
	m.Metrics.QueryDuration.WithLabelValues("DeleteByPoolUser", backstopPositionsTable).Observe(duration)
	m.Metrics.QueriesTotal.WithLabelValues("DeleteByPoolUser", backstopPositionsTable).Inc()
	m.Metrics.BatchSize.WithLabelValues("DeleteByPoolUser", backstopPositionsTable).Observe(float64(len(keys)))
	return nil
}
