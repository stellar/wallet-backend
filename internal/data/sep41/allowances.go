package sep41

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/stellar/wallet-backend/internal/indexer/types"
	"github.com/stellar/wallet-backend/internal/metrics"
	"github.com/stellar/wallet-backend/internal/utils"
)

const expiredAllowanceSweepLimit = 1000

// latestIngestLedgerCursor mirrors data.LatestLedgerCursorName. It is duplicated
// here because internal/data depends on this package, so an import in the
// opposite direction would be circular. The string must stay in sync.
const latestIngestLedgerCursor = "latest_ingest_ledger"

// Allowance represents a SEP-41 approve() grant: owner allows spender to move up to amount
// until expiration_ledger. The pair (owner, spender, contract_id) is unique.
type Allowance struct {
	OwnerID          types.AddressBytea
	SpenderID        types.AddressBytea
	ContractID       uuid.UUID
	Amount           string // i128 decimal string
	ExpirationLedger uint32
	LedgerNumber     uint32
	TokenID          string // C... address populated on read
}

// AllowanceCursor is a keyset cursor into a single owner's allowance list,
// ordered by (spender_id, contract_id). Those two columns together with
// owner_id form the primary key, so the pair is unique per page.
type AllowanceCursor struct {
	SpenderID  types.AddressBytea
	ContractID uuid.UUID
}

// AllowanceModelInterface exposes SEP-41 allowance storage operations.
type AllowanceModelInterface interface {
	// GetByOwner returns non-expired allowances owned by the given account.
	// The expiration threshold is read atomically from ingest_store inside the
	// same SQL statement (statement-level MVCC), so the filter is always
	// consistent with the live high-watermark — there is no separate ledger
	// parameter. If ingest_store has no row yet, the threshold defaults to 0
	// and no rows are filtered. Results are keyset-paginated by
	// (spender_address, contract_id); pass a nil cursor for the first page.
	GetByOwner(ctx context.Context, ownerAddress string, limit int32, cursor *AllowanceCursor, sortOrder SortOrder) ([]Allowance, error)
	BatchUpsert(ctx context.Context, dbTx pgx.Tx, upserts []Allowance, deletes []Allowance) error
	// DeleteExpiredBefore removes a bounded batch of allowances whose expiration_ledger is
	// strictly below currentLedger. Callers should invoke it from the current-state
	// transaction after applying the ledger's own allowance writes so refreshed grants
	// survive the sweep.
	DeleteExpiredBefore(ctx context.Context, dbTx pgx.Tx, currentLedger uint32) error
}

type AllowanceModel struct {
	DB      *pgxpool.Pool
	Metrics *metrics.DBMetrics
}

var _ AllowanceModelInterface = (*AllowanceModel)(nil)

// GetByOwner returns active allowances (expiration_ledger >= ingest watermark),
// keyset-paginated by (spender_id, contract_id). The PK on
// (owner_id, spender_id, contract_id) backs the ordering + keyset predicate
// without an extra index. The expiration threshold is read inline via a
// subquery against ingest_store so both reads share a single statement-level
// MVCC snapshot.
func (m *AllowanceModel) GetByOwner(ctx context.Context, ownerAddress string, limit int32, cursor *AllowanceCursor, sortOrder SortOrder) ([]Allowance, error) {
	if ownerAddress == "" {
		return nil, fmt.Errorf("empty owner address")
	}
	if limit <= 0 {
		return nil, fmt.Errorf("limit must be positive, got %d", limit)
	}

	query := `
		SELECT
			a.spender_id, a.contract_id, a.amount::text, a.expiration_ledger, a.last_modified_ledger,
			ct.contract_id AS token_id
		FROM sep41_allowances a
		INNER JOIN contract_tokens ct ON ct.id = a.contract_id
		WHERE a.owner_id = $1
		  AND a.expiration_ledger >= COALESCE(
		      (SELECT value::bigint FROM ingest_store WHERE key = $2),
		      0
		  )`
	args := []interface{}{types.AddressBytea(ownerAddress), latestIngestLedgerCursor}
	argIndex := 3

	if cursor != nil {
		op := ">"
		if sortOrder == SortDESC {
			op = "<"
		}
		query += fmt.Sprintf(" AND (a.spender_id, a.contract_id) %s ($%d, $%d)", op, argIndex, argIndex+1)
		args = append(args, cursor.SpenderID, cursor.ContractID)
		argIndex += 2
	}

	if sortOrder == SortDESC {
		query += " ORDER BY a.spender_id DESC, a.contract_id DESC"
	} else {
		query += " ORDER BY a.spender_id ASC, a.contract_id ASC"
	}

	query += fmt.Sprintf(" LIMIT $%d", argIndex)
	args = append(args, limit)

	start := time.Now()
	rows, err := m.DB.Query(ctx, query, args...)
	duration := time.Since(start).Seconds()
	m.Metrics.QueryDuration.WithLabelValues("GetByOwner", "sep41_allowances").Observe(duration)
	m.Metrics.QueriesTotal.WithLabelValues("GetByOwner", "sep41_allowances").Inc()
	if err != nil {
		m.Metrics.QueryErrors.WithLabelValues("GetByOwner", "sep41_allowances", utils.GetDBErrorType(err)).Inc()
		return nil, fmt.Errorf("querying SEP-41 allowances for %s: %w", ownerAddress, err)
	}
	defer rows.Close()

	var allowances []Allowance
	for rows.Next() {
		var a Allowance
		if err := rows.Scan(
			&a.SpenderID, &a.ContractID, &a.Amount, &a.ExpirationLedger, &a.LedgerNumber,
			&a.TokenID,
		); err != nil {
			return nil, fmt.Errorf("scanning SEP-41 allowance: %w", err)
		}
		a.OwnerID = types.AddressBytea(ownerAddress)
		allowances = append(allowances, a)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating SEP-41 allowances: %w", err)
	}

	return allowances, nil
}

// BatchUpsert applies approve-style writes and deletes inside the given transaction.
//
// Callers must supply key-disjoint inputs: each (owner_id, spender_id, contract_id)
// tuple may appear at most once across upserts and deletes combined.
// Today this holds because the upstream SEP-41 processor builds both slices from
// a single Go map keyed by that tuple (see internal/services/sep41/processor.go).
// The UNNEST upsert below relies on the precondition — a duplicate key inside
// `upserts` would trip Postgres's "ON CONFLICT DO UPDATE command cannot affect
// row a second time" guard.
func (m *AllowanceModel) BatchUpsert(ctx context.Context, dbTx pgx.Tx, upserts []Allowance, deletes []Allowance) error {
	if len(upserts) == 0 && len(deletes) == 0 {
		return nil
	}

	start := time.Now()

	if len(upserts) > 0 {
		owners := make([][]byte, len(upserts))
		spenders := make([][]byte, len(upserts))
		contractIDs := make([]uuid.UUID, len(upserts))
		amounts := make([]string, len(upserts))
		expirations := make([]int32, len(upserts))
		ledgers := make([]int32, len(upserts))
		for i, a := range upserts {
			ownerRaw, err := a.OwnerID.Value()
			if err != nil {
				return fmt.Errorf("converting owner id to bytes for upsert: %w", err)
			}
			ownerBytes, ok := ownerRaw.([]byte)
			if !ok {
				return fmt.Errorf("converting owner id to bytes for upsert: expected []byte, got %T", ownerRaw)
			}
			spenderRaw, err := a.SpenderID.Value()
			if err != nil {
				return fmt.Errorf("converting spender id to bytes for upsert: %w", err)
			}
			spenderBytes, ok := spenderRaw.([]byte)
			if !ok {
				return fmt.Errorf("converting spender id to bytes for upsert: expected []byte, got %T", spenderRaw)
			}
			owners[i] = ownerBytes
			spenders[i] = spenderBytes
			contractIDs[i] = a.ContractID
			amounts[i] = a.Amount
			expirations[i] = int32(a.ExpirationLedger)
			ledgers[i] = int32(a.LedgerNumber)
		}

		const upsertQuery = `
			INSERT INTO sep41_allowances (
				owner_id, spender_id, contract_id,
				amount, expiration_ledger, last_modified_ledger
			)
			SELECT u.owner_id, u.spender_id, u.contract_id,
				u.amount::numeric, u.expiration_ledger, u.last_modified_ledger
			FROM UNNEST(
				$1::bytea[], $2::bytea[], $3::uuid[],
				$4::text[], $5::integer[], $6::integer[]
			) AS u(owner_id, spender_id, contract_id, amount, expiration_ledger, last_modified_ledger)
			ON CONFLICT (owner_id, spender_id, contract_id) DO UPDATE SET
				amount               = EXCLUDED.amount,
				expiration_ledger    = EXCLUDED.expiration_ledger,
				last_modified_ledger = EXCLUDED.last_modified_ledger`
		if _, err := dbTx.Exec(ctx, upsertQuery, owners, spenders, contractIDs, amounts, expirations, ledgers); err != nil {
			m.Metrics.QueryErrors.WithLabelValues("BatchUpsert", "sep41_allowances", utils.GetDBErrorType(err)).Inc()
			return fmt.Errorf("upserting SEP-41 allowances: %w", err)
		}
	}

	if len(deletes) > 0 {
		owners := make([][]byte, len(deletes))
		spenders := make([][]byte, len(deletes))
		contractIDs := make([]uuid.UUID, len(deletes))
		for i, a := range deletes {
			ownerRaw, err := a.OwnerID.Value()
			if err != nil {
				return fmt.Errorf("converting owner id to bytes for delete: %w", err)
			}
			ownerBytes, ok := ownerRaw.([]byte)
			if !ok {
				return fmt.Errorf("converting owner id to bytes for delete: expected []byte, got %T", ownerRaw)
			}
			spenderRaw, err := a.SpenderID.Value()
			if err != nil {
				return fmt.Errorf("converting spender id to bytes for delete: %w", err)
			}
			spenderBytes, ok := spenderRaw.([]byte)
			if !ok {
				return fmt.Errorf("converting spender id to bytes for delete: expected []byte, got %T", spenderRaw)
			}
			owners[i] = ownerBytes
			spenders[i] = spenderBytes
			contractIDs[i] = a.ContractID
		}

		const deleteQuery = `
			DELETE FROM sep41_allowances
			WHERE (owner_id, spender_id, contract_id) IN (
				SELECT * FROM UNNEST($1::bytea[], $2::bytea[], $3::uuid[])
			)`
		if _, err := dbTx.Exec(ctx, deleteQuery, owners, spenders, contractIDs); err != nil {
			m.Metrics.QueryErrors.WithLabelValues("BatchUpsert", "sep41_allowances", utils.GetDBErrorType(err)).Inc()
			return fmt.Errorf("deleting SEP-41 allowances: %w", err)
		}
	}

	duration := time.Since(start).Seconds()
	m.Metrics.QueryDuration.WithLabelValues("BatchUpsert", "sep41_allowances").Observe(duration)
	m.Metrics.QueriesTotal.WithLabelValues("BatchUpsert", "sep41_allowances").Inc()
	m.Metrics.BatchSize.WithLabelValues("BatchUpsert", "sep41_allowances").Observe(float64(len(upserts) + len(deletes)))
	return nil
}

// DeleteExpiredBefore removes a bounded batch of expired current-state rows.
// Rows whose expiration_ledger equals currentLedger remain active for that ledger
// and are not deleted until the next successful current-state write.
func (m *AllowanceModel) DeleteExpiredBefore(ctx context.Context, dbTx pgx.Tx, currentLedger uint32) error {
	if currentLedger == 0 {
		return nil
	}

	start := time.Now()
	_, err := dbTx.Exec(ctx, `
		WITH expired AS (
			SELECT ctid
			FROM sep41_allowances
			WHERE expiration_ledger < $1
			ORDER BY expiration_ledger ASC
			LIMIT $2
		)
		DELETE FROM sep41_allowances a
		USING expired
		WHERE a.ctid = expired.ctid
	`, currentLedger, expiredAllowanceSweepLimit)
	duration := time.Since(start).Seconds()
	m.Metrics.QueryDuration.WithLabelValues("DeleteExpiredBefore", "sep41_allowances").Observe(duration)
	m.Metrics.QueriesTotal.WithLabelValues("DeleteExpiredBefore", "sep41_allowances").Inc()
	if err != nil {
		m.Metrics.QueryErrors.WithLabelValues("DeleteExpiredBefore", "sep41_allowances", utils.GetDBErrorType(err)).Inc()
		return fmt.Errorf("deleting expired SEP-41 allowances before ledger %d: %w", currentLedger, err)
	}
	return nil
}
