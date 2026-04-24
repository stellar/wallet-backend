// Package sep41 provides data access for SEP-41 token balances and allowances.
// SAC balances are tracked separately in internal/data (sac_balances); this package
// covers pure SEP-41 (non-SAC) token contracts classified by the protocol-setup pipeline.
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

// SortOrder is the local alias of the repo-wide data.SortOrder so this package
// does not have to import its parent (which would create a cycle). Values are the
// same string literals ("ASC"/"DESC").
type SortOrder string

const (
	SortASC  SortOrder = "ASC"
	SortDESC SortOrder = "DESC"
)

// Balance contains a holder balance for a SEP-41 token contract.
// Metadata (code, name, symbol, decimals) is read from contract_tokens on demand.
type Balance struct {
	AccountAddress string
	ContractID     uuid.UUID
	Balance        string // i128 stored as decimal string
	LedgerNumber   uint32

	// Metadata populated by GetByAccount JOIN with contract_tokens.
	TokenID  string  // C... address
	Name     *string // may be nil until metadata fetch backfills
	Symbol   *string
	Decimals uint32
}

// BalanceModelInterface exposes SEP-41 balance storage operations.
type BalanceModelInterface interface {
	// GetByAccount returns SEP-41 balances held by the account, ordered by contract_id.
	// Pass nil limit/cursor to fetch every row; provide them for keyset pagination (the
	// cursor is the last contract_id seen from the previous page).
	GetByAccount(ctx context.Context, accountAddress string, limit *int32, cursor *uuid.UUID, sortOrder SortOrder) ([]Balance, error)
	// BatchApplyDeltas applies signed balance deltas server-side
	// (balance := existing + delta) and sweeps any rows that settle to zero.
	// Each input Balance.Balance is interpreted as the delta to add, NOT as
	// the absolute new balance. This avoids needing to preload state from DB
	// into memory at the cost of per-ledger SQL arithmetic on TEXT→numeric.
	BatchApplyDeltas(ctx context.Context, dbTx pgx.Tx, deltas []Balance) error
	BatchCopy(ctx context.Context, dbTx pgx.Tx, balances []Balance) error
}

type BalanceModel struct {
	DB      *pgxpool.Pool
	Metrics *metrics.DBMetrics
}

var _ BalanceModelInterface = (*BalanceModel)(nil)

// GetByAccount returns SEP-41 balances held by an account, ordered by contract_id and
// joined with contract_tokens metadata. The optional cursor carries the last contract_id
// seen by the previous page so GraphQL can page deterministically.
func (m *BalanceModel) GetByAccount(ctx context.Context, accountAddress string, limit *int32, cursor *uuid.UUID, sortOrder SortOrder) ([]Balance, error) {
	if accountAddress == "" {
		return nil, fmt.Errorf("empty account address")
	}

	query := `
		SELECT
			b.contract_id, b.balance, b.last_modified_ledger,
			ct.contract_id, ct.name, ct.symbol, ct.decimals
		FROM sep41_balances b
		INNER JOIN contract_tokens ct ON ct.id = b.contract_id
		WHERE b.account_address = $1`
	args := []interface{}{accountAddress}
	argIndex := 2

	if cursor != nil {
		op := ">"
		if sortOrder == SortDESC {
			op = "<"
		}
		query += fmt.Sprintf(" AND b.contract_id %s $%d", op, argIndex)
		args = append(args, *cursor)
		argIndex++
	}

	if sortOrder == SortDESC {
		query += " ORDER BY b.contract_id DESC"
	} else {
		query += " ORDER BY b.contract_id ASC"
	}

	if limit != nil {
		query += fmt.Sprintf(" LIMIT $%d", argIndex)
		args = append(args, *limit)
	}

	start := time.Now()
	rows, err := m.DB.Query(ctx, query, args...)
	duration := time.Since(start).Seconds()
	m.Metrics.QueryDuration.WithLabelValues("GetByAccount", "sep41_balances").Observe(duration)
	m.Metrics.QueriesTotal.WithLabelValues("GetByAccount", "sep41_balances").Inc()
	if err != nil {
		m.Metrics.QueryErrors.WithLabelValues("GetByAccount", "sep41_balances", utils.GetDBErrorType(err)).Inc()
		return nil, fmt.Errorf("querying SEP-41 balances for %s: %w", accountAddress, err)
	}
	defer rows.Close()

	var balances []Balance
	for rows.Next() {
		var bal Balance
		if err := rows.Scan(
			&bal.ContractID, &bal.Balance, &bal.LedgerNumber,
			&bal.TokenID, &bal.Name, &bal.Symbol, &bal.Decimals,
		); err != nil {
			return nil, fmt.Errorf("scanning SEP-41 balance: %w", err)
		}
		bal.AccountAddress = accountAddress
		balances = append(balances, bal)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating SEP-41 balances: %w", err)
	}

	return balances, nil
}

// BatchApplyDeltas accumulates signed balance deltas server-side (balance := existing + delta)
// and removes rows that settle to zero. Each input Balance.Balance is a decimal string delta.
//
// The per-delta upsert runs within the caller's transaction so retries re-apply the same
// ledger's deltas exactly once (guarded by the CAS cursor). Zero-balance cleanup is a single
// DELETE sweep at the end, scoped to the (account, contract) pairs we just touched.
func (m *BalanceModel) BatchApplyDeltas(ctx context.Context, dbTx pgx.Tx, deltas []Balance) error {
	if len(deltas) == 0 {
		return nil
	}

	start := time.Now()
	batch := &pgx.Batch{}

	// Sum in SQL: existing + delta. The TEXT columns are cast to numeric for arithmetic
	// and back to text for storage so the column type stays TEXT (preserves full i128 range).
	const upsertQuery = `
		INSERT INTO sep41_balances (account_address, contract_id, balance, last_modified_ledger)
		VALUES ($1, $2, $3, $4)
		ON CONFLICT (account_address, contract_id) DO UPDATE SET
			balance = (sep41_balances.balance::numeric + EXCLUDED.balance::numeric)::text,
			last_modified_ledger = EXCLUDED.last_modified_ledger`

	accountAddresses := make([]string, 0, len(deltas))
	contractIDs := make([]uuid.UUID, 0, len(deltas))
	for _, d := range deltas {
		batch.Queue(upsertQuery, d.AccountAddress, d.ContractID, d.Balance, d.LedgerNumber)
		accountAddresses = append(accountAddresses, d.AccountAddress)
		contractIDs = append(contractIDs, d.ContractID)
	}

	// Single DELETE sweep for any (account, contract) that settled to zero this batch.
	const deleteZeroesQuery = `
		DELETE FROM sep41_balances
		WHERE balance::numeric = 0
		  AND (account_address, contract_id) IN (
		      SELECT UNNEST($1::text[]), UNNEST($2::uuid[])
		  )`
	batch.Queue(deleteZeroesQuery, accountAddresses, contractIDs)

	br := dbTx.SendBatch(ctx, batch)
	for i := 0; i < batch.Len(); i++ {
		if _, err := br.Exec(); err != nil {
			_ = br.Close() //nolint:errcheck // cleanup on error path
			m.Metrics.QueryErrors.WithLabelValues("BatchApplyDeltas", "sep41_balances", utils.GetDBErrorType(err)).Inc()
			return fmt.Errorf("applying SEP-41 balance deltas: %w", err)
		}
	}
	if err := br.Close(); err != nil {
		return fmt.Errorf("closing SEP-41 balance batch: %w", err)
	}

	duration := time.Since(start).Seconds()
	m.Metrics.QueryDuration.WithLabelValues("BatchApplyDeltas", "sep41_balances").Observe(duration)
	m.Metrics.QueriesTotal.WithLabelValues("BatchApplyDeltas", "sep41_balances").Inc()
	m.Metrics.BatchSize.WithLabelValues("BatchApplyDeltas", "sep41_balances").Observe(float64(len(deltas)))
	return nil
}

// BatchCopy bulk-loads balances via the COPY protocol. Intended for checkpoint/bootstrap paths.
func (m *BalanceModel) BatchCopy(ctx context.Context, dbTx pgx.Tx, balances []Balance) error {
	if len(balances) == 0 {
		return nil
	}

	start := time.Now()

	copyCount, err := dbTx.CopyFrom(
		ctx,
		pgx.Identifier{"sep41_balances"},
		[]string{"account_address", "contract_id", "balance", "last_modified_ledger"},
		pgx.CopyFromSlice(len(balances), func(i int) ([]any, error) {
			bal := balances[i]
			return []any{bal.AccountAddress, bal.ContractID, bal.Balance, bal.LedgerNumber}, nil
		}),
	)
	if err != nil {
		m.Metrics.QueryErrors.WithLabelValues("BatchCopy", "sep41_balances", utils.GetDBErrorType(err)).Inc()
		return fmt.Errorf("batch inserting SEP-41 balances via COPY: %w", err)
	}
	if int(copyCount) != len(balances) {
		return fmt.Errorf("expected %d rows copied, got %d", len(balances), copyCount)
	}

	duration := time.Since(start).Seconds()
	m.Metrics.QueryDuration.WithLabelValues("BatchCopy", "sep41_balances").Observe(duration)
	m.Metrics.QueriesTotal.WithLabelValues("BatchCopy", "sep41_balances").Inc()
	m.Metrics.BatchSize.WithLabelValues("BatchCopy", "sep41_balances").Observe(float64(len(balances)))
	return nil
}
