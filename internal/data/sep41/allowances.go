package sep41

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/stellar/wallet-backend/internal/metrics"
	"github.com/stellar/wallet-backend/internal/utils"
)

// Allowance represents a SEP-41 approve() grant: owner allows spender to move up to amount
// until expiration_ledger. The pair (owner, spender, contract_id) is unique.
type Allowance struct {
	OwnerAddress     string
	SpenderAddress   string
	ContractID       uuid.UUID
	Amount           string // i128 decimal string
	ExpirationLedger uint32
	LedgerNumber     uint32
	TokenID          string // C... address populated on read
}

// AllowanceCursor is a keyset cursor into a single owner's allowance list,
// ordered by (spender_address, contract_id). Those two columns together with
// owner_address form the primary key, so the pair is unique per page.
type AllowanceCursor struct {
	SpenderAddress string
	ContractID     uuid.UUID
}

// AllowanceModelInterface exposes SEP-41 allowance storage operations.
type AllowanceModelInterface interface {
	// GetByOwner returns non-expired allowances owned by the given account, filtered by ledger.
	// An allowance is considered active while expiration_ledger >= currentLedger. Results are
	// keyset-paginated by (spender_address, contract_id); pass a nil cursor for the first page.
	GetByOwner(ctx context.Context, ownerAddress string, currentLedger uint32, limit int32, cursor *AllowanceCursor, sortOrder SortOrder) ([]Allowance, error)
	BatchUpsert(ctx context.Context, dbTx pgx.Tx, upserts []Allowance, deletes []Allowance) error
}

type AllowanceModel struct {
	DB      *pgxpool.Pool
	Metrics *metrics.DBMetrics
}

var _ AllowanceModelInterface = (*AllowanceModel)(nil)

// GetByOwner returns active allowances (expiration_ledger >= currentLedger), keyset-paginated
// by (spender_address, contract_id). The PK on (owner_address, spender_address, contract_id)
// backs the ordering + keyset predicate without an extra index.
func (m *AllowanceModel) GetByOwner(ctx context.Context, ownerAddress string, currentLedger uint32, limit int32, cursor *AllowanceCursor, sortOrder SortOrder) ([]Allowance, error) {
	if ownerAddress == "" {
		return nil, fmt.Errorf("empty owner address")
	}
	if limit <= 0 {
		return nil, fmt.Errorf("limit must be positive, got %d", limit)
	}

	query := `
		SELECT
			a.spender_address, a.contract_id, a.amount, a.expiration_ledger, a.last_modified_ledger,
			ct.contract_id
		FROM sep41_allowances a
		INNER JOIN contract_tokens ct ON ct.id = a.contract_id
		WHERE a.owner_address = $1 AND a.expiration_ledger >= $2`
	args := []interface{}{ownerAddress, currentLedger}
	argIndex := 3

	if cursor != nil {
		op := ">"
		if sortOrder == SortDESC {
			op = "<"
		}
		query += fmt.Sprintf(" AND (a.spender_address, a.contract_id) %s ($%d, $%d)", op, argIndex, argIndex+1)
		args = append(args, cursor.SpenderAddress, cursor.ContractID)
		argIndex += 2
	}

	if sortOrder == SortDESC {
		query += " ORDER BY a.spender_address DESC, a.contract_id DESC"
	} else {
		query += " ORDER BY a.spender_address ASC, a.contract_id ASC"
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
			&a.SpenderAddress, &a.ContractID, &a.Amount, &a.ExpirationLedger, &a.LedgerNumber,
			&a.TokenID,
		); err != nil {
			return nil, fmt.Errorf("scanning SEP-41 allowance: %w", err)
		}
		a.OwnerAddress = ownerAddress
		allowances = append(allowances, a)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating SEP-41 allowances: %w", err)
	}

	return allowances, nil
}

// BatchUpsert applies approve-style writes and deletes inside the given transaction.
func (m *AllowanceModel) BatchUpsert(ctx context.Context, dbTx pgx.Tx, upserts []Allowance, deletes []Allowance) error {
	if len(upserts) == 0 && len(deletes) == 0 {
		return nil
	}

	start := time.Now()
	batch := &pgx.Batch{}

	const upsertQuery = `
		INSERT INTO sep41_allowances (
			owner_address, spender_address, contract_id, amount, expiration_ledger, last_modified_ledger
		) VALUES ($1, $2, $3, $4, $5, $6)
		ON CONFLICT (owner_address, spender_address, contract_id) DO UPDATE SET
			amount = EXCLUDED.amount,
			expiration_ledger = EXCLUDED.expiration_ledger,
			last_modified_ledger = EXCLUDED.last_modified_ledger`

	for _, a := range upserts {
		batch.Queue(upsertQuery,
			a.OwnerAddress, a.SpenderAddress, a.ContractID,
			a.Amount, a.ExpirationLedger, a.LedgerNumber,
		)
	}

	const deleteQuery = `
		DELETE FROM sep41_allowances
		WHERE owner_address = $1 AND spender_address = $2 AND contract_id = $3`
	for _, a := range deletes {
		batch.Queue(deleteQuery, a.OwnerAddress, a.SpenderAddress, a.ContractID)
	}

	if batch.Len() == 0 {
		return nil
	}

	br := dbTx.SendBatch(ctx, batch)
	for i := 0; i < batch.Len(); i++ {
		if _, err := br.Exec(); err != nil {
			_ = br.Close() //nolint:errcheck // cleanup on error path
			m.Metrics.QueryErrors.WithLabelValues("BatchUpsert", "sep41_allowances", utils.GetDBErrorType(err)).Inc()
			return fmt.Errorf("upserting SEP-41 allowances: %w", err)
		}
	}
	if err := br.Close(); err != nil {
		return fmt.Errorf("closing SEP-41 allowance batch: %w", err)
	}

	duration := time.Since(start).Seconds()
	m.Metrics.QueryDuration.WithLabelValues("BatchUpsert", "sep41_allowances").Observe(duration)
	m.Metrics.QueriesTotal.WithLabelValues("BatchUpsert", "sep41_allowances").Inc()
	m.Metrics.BatchSize.WithLabelValues("BatchUpsert", "sep41_allowances").Observe(float64(len(upserts) + len(deletes)))
	return nil
}
