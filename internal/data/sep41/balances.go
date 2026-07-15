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

	"github.com/stellar/wallet-backend/internal/indexer/types"
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
	AccountID    types.AddressBytea
	ContractID   uuid.UUID
	Balance      string // i128 stored as decimal string
	LedgerNumber uint32

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
			b.contract_id, b.balance::text, b.last_modified_ledger,
			ct.contract_id AS token_id, ct.name, ct.symbol, ct.decimals
		FROM sep41_balances b
		INNER JOIN contract_tokens ct ON ct.id = b.contract_id
		WHERE b.account_id = $1`
	args := []interface{}{types.AddressBytea(accountAddress)}
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
		bal.AccountID = types.AddressBytea(accountAddress)
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
// Callers must supply key-disjoint deltas: each (account_id, contract_id) tuple may
// appear at most once per call. Today this holds because the upstream SEP-41 processor
// sums into a single Go map keyed by that tuple (stagedBalanceDelta in
// internal/services/sep41/processor.go) before materializing the slice. The UNNEST upsert
// below relies on the precondition — a duplicate key would trip Postgres's "ON CONFLICT
// DO UPDATE command cannot affect row a second time" guard.
//
// The upsert and zero-sweep run within the caller's transaction so retries re-apply the
// same ledger's deltas exactly once (guarded by the CAS cursor). The DELETE is scoped to
// the (account, contract) pairs we just touched.
func (m *BalanceModel) BatchApplyDeltas(ctx context.Context, dbTx pgx.Tx, deltas []Balance) error {
	if len(deltas) == 0 {
		return nil
	}

	start := time.Now()

	accountIDs := make([][]byte, len(deltas))
	contractIDs := make([]uuid.UUID, len(deltas))
	balances := make([]string, len(deltas))
	ledgers := make([]int32, len(deltas))
	for i, d := range deltas {
		raw, err := d.AccountID.Value()
		if err != nil {
			return fmt.Errorf("converting account id to bytes for upsert: %w", err)
		}
		rawBytes, ok := raw.([]byte)
		if !ok {
			return fmt.Errorf("converting account id to bytes for upsert: expected []byte, got %T", raw)
		}
		accountIDs[i] = rawBytes
		contractIDs[i] = d.ContractID
		balances[i] = d.Balance
		ledgers[i] = int32(d.LedgerNumber)
	}

	// Sum in SQL: existing + delta. balance is stored as NUMERIC so this is native
	// arithmetic; the delta arrives as text (i128 decimal string) and is cast once at
	// the boundary, since Postgres has no implicit text->numeric cast.
	const upsertQuery = `
		INSERT INTO sep41_balances (account_id, contract_id, balance, last_modified_ledger)
		SELECT u.account_id, u.contract_id, u.balance::numeric, u.last_modified_ledger
		FROM UNNEST($1::bytea[], $2::uuid[], $3::text[], $4::integer[])
			AS u(account_id, contract_id, balance, last_modified_ledger)
		ON CONFLICT (account_id, contract_id) DO UPDATE SET
			balance              = sep41_balances.balance + EXCLUDED.balance,
			last_modified_ledger = EXCLUDED.last_modified_ledger`
	if _, err := dbTx.Exec(ctx, upsertQuery, accountIDs, contractIDs, balances, ledgers); err != nil {
		m.Metrics.QueryErrors.WithLabelValues("BatchApplyDeltas", "sep41_balances", utils.GetDBErrorType(err)).Inc()
		return fmt.Errorf("applying SEP-41 balance deltas: %w", err)
	}

	const deleteZeroesQuery = `
		DELETE FROM sep41_balances
		WHERE balance = 0
		  AND (account_id, contract_id) IN (
		      SELECT * FROM UNNEST($1::bytea[], $2::uuid[])
		  )`
	if _, err := dbTx.Exec(ctx, deleteZeroesQuery, accountIDs, contractIDs); err != nil {
		m.Metrics.QueryErrors.WithLabelValues("BatchApplyDeltas", "sep41_balances", utils.GetDBErrorType(err)).Inc()
		return fmt.Errorf("sweeping zero SEP-41 balances: %w", err)
	}

	duration := time.Since(start).Seconds()
	m.Metrics.QueryDuration.WithLabelValues("BatchApplyDeltas", "sep41_balances").Observe(duration)
	m.Metrics.QueriesTotal.WithLabelValues("BatchApplyDeltas", "sep41_balances").Inc()
	m.Metrics.BatchSize.WithLabelValues("BatchApplyDeltas", "sep41_balances").Observe(float64(len(deltas)))
	return nil
}
