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

// Balance contains a holder balance for a SEP-41 token contract. Balance is
// absolute — the authoritative value reported by `balance(addr)` on the
// contract — not a delta. SEP-41 only mandates the interface, so emitted
// transfer/mint/burn amounts are not guaranteed to equal the real balance
// change (fee-on-transfer, rebasing, interest-bearing tokens diverge).
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

// BalancePair identifies one (account, contract) row that the LoadCurrentState
// bootstrap should refresh via RPC. ContractAddress is the C... strkey form
// needed to issue a `balance(addr)` simulation call.
type BalancePair struct {
	AccountAddress  string
	ContractID      uuid.UUID
	ContractAddress string
}

// BalanceModelInterface exposes SEP-41 balance storage operations.
type BalanceModelInterface interface {
	// GetByAccount returns SEP-41 balances held by the account, ordered by contract_id.
	// Pass nil limit/cursor to fetch every row; provide them for keyset pagination (the
	// cursor is the last contract_id seen from the previous page).
	GetByAccount(ctx context.Context, accountAddress string, limit *int32, cursor *uuid.UUID, sortOrder SortOrder) ([]Balance, error)
	// BatchUpsertAbsolute writes authoritative balances fetched from
	// `balance(addr)` on the contract. Each input Balance.Balance is the
	// absolute value, not a delta — the upsert replaces existing rows. Rows
	// whose new balance is zero are swept in the same batch.
	BatchUpsertAbsolute(ctx context.Context, dbTx pgx.Tx, balances []Balance) error
	BatchCopy(ctx context.Context, dbTx pgx.Tx, balances []Balance) error
	// GetAllSEP41Pairs returns every (account, contract) pair that has ever
	// touched a SEP-41 token, sourced from `state_changes` (the
	// history-migration's authoritative output). Used once at handoff to
	// fetch authoritative balances for every known holder via RPC, including
	// quiet accounts that don't have a fresh event in live mode.
	GetAllSEP41Pairs(ctx context.Context, dbTx pgx.Tx) ([]BalancePair, error)
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

// BatchUpsertAbsolute writes authoritative balances fetched from `balance(addr)`
// on the contract. Each input Balance.Balance is the absolute value of the
// account's holding at LedgerNumber; the upsert replaces any existing row.
// Rows whose new balance is zero are deleted in the same batch (scoped to the
// touched pairs so the sweep stays cheap).
func (m *BalanceModel) BatchUpsertAbsolute(ctx context.Context, dbTx pgx.Tx, balances []Balance) error {
	if len(balances) == 0 {
		return nil
	}

	start := time.Now()
	batch := &pgx.Batch{}

	const upsertQuery = `
		INSERT INTO sep41_balances (account_address, contract_id, balance, last_modified_ledger)
		VALUES ($1, $2, $3, $4)
		ON CONFLICT (account_address, contract_id) DO UPDATE SET
			balance = EXCLUDED.balance,
			last_modified_ledger = EXCLUDED.last_modified_ledger`

	accountAddresses := make([]string, 0, len(balances))
	contractIDs := make([]uuid.UUID, 0, len(balances))
	for _, b := range balances {
		batch.Queue(upsertQuery, b.AccountAddress, b.ContractID, b.Balance, b.LedgerNumber)
		accountAddresses = append(accountAddresses, b.AccountAddress)
		contractIDs = append(contractIDs, b.ContractID)
	}

	const deleteZeroesQuery = `
		DELETE FROM sep41_balances
		WHERE balance::numeric = 0
		  AND (account_address, contract_id) IN (
		      SELECT * FROM UNNEST($1::text[], $2::uuid[])
		  )`
	batch.Queue(deleteZeroesQuery, accountAddresses, contractIDs)

	br := dbTx.SendBatch(ctx, batch)
	for i := 0; i < batch.Len(); i++ {
		if _, err := br.Exec(); err != nil {
			_ = br.Close() //nolint:errcheck // cleanup on error path
			m.Metrics.QueryErrors.WithLabelValues("BatchUpsertAbsolute", "sep41_balances", utils.GetDBErrorType(err)).Inc()
			return fmt.Errorf("upserting SEP-41 balances: %w", err)
		}
	}
	if err := br.Close(); err != nil {
		return fmt.Errorf("closing SEP-41 balance batch: %w", err)
	}

	duration := time.Since(start).Seconds()
	m.Metrics.QueryDuration.WithLabelValues("BatchUpsertAbsolute", "sep41_balances").Observe(duration)
	m.Metrics.QueriesTotal.WithLabelValues("BatchUpsertAbsolute", "sep41_balances").Inc()
	m.Metrics.BatchSize.WithLabelValues("BatchUpsertAbsolute", "sep41_balances").Observe(float64(len(balances)))
	return nil
}

// GetAllSEP41Pairs enumerates every (account, contract) pair that has ever
// touched a SEP-41 token according to `state_changes` — i.e., the canonical
// output of `protocol-migrate history` plus all live SEP-41 events ingested
// since. The handoff bootstrap uses this to refresh authoritative balances
// for every known holder via RPC.
//
// Implementation is a two-pass join in Go because state_changes uses BYTEA
// for account_id/token_id while contract_tokens uses C-strkey TEXT, and we
// don't have a Postgres-side strkey UDF to bridge them in SQL.
//
//  1. Scan state_changes for distinct (account_id, token_id) pairs in
//     state_change_category='BALANCE'. types.AddressBytea.Scan handles the
//     BYTEA-to-strkey decode automatically.
//  2. Load every SEP-41 contract_tokens row into a map keyed by C-strkey.
//  3. Filter the pairs by membership in the SEP-41 map, dropping tokens
//     that turn out to be SAC or some other type.
//
// This runs once per deploy at handoff and may scan a large hypertable;
// callers should expect a long-ish transaction for very busy networks.
func (m *BalanceModel) GetAllSEP41Pairs(ctx context.Context, dbTx pgx.Tx) ([]BalancePair, error) {
	type rawPair struct {
		Account types.AddressBytea
		Token   types.AddressBytea
	}

	// Pass 1: distinct (account, token) BYTEA pairs from balance-category state changes.
	const pairsQuery = `
		SELECT DISTINCT account_id, token_id
		FROM state_changes
		WHERE token_id IS NOT NULL
		  AND state_change_category = 'BALANCE'`

	start := time.Now()
	rows, err := dbTx.Query(ctx, pairsQuery)
	duration := time.Since(start).Seconds()
	m.Metrics.QueryDuration.WithLabelValues("GetAllSEP41Pairs", "state_changes").Observe(duration)
	m.Metrics.QueriesTotal.WithLabelValues("GetAllSEP41Pairs", "state_changes").Inc()
	if err != nil {
		m.Metrics.QueryErrors.WithLabelValues("GetAllSEP41Pairs", "state_changes", utils.GetDBErrorType(err)).Inc()
		return nil, fmt.Errorf("enumerating SEP-41 balance pairs from state_changes: %w", err)
	}

	var raws []rawPair
	for rows.Next() {
		var p rawPair
		if err := rows.Scan(&p.Account, &p.Token); err != nil {
			rows.Close()
			return nil, fmt.Errorf("scanning state_changes pair: %w", err)
		}
		raws = append(raws, p)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating state_changes pairs: %w", err)
	}
	if len(raws) == 0 {
		return nil, nil
	}

	// Pass 2: load SEP-41 contract_tokens into a strkey→UUID map.
	const sep41Query = `SELECT id, contract_id FROM contract_tokens WHERE type = 'sep41'`

	ctRows, err := dbTx.Query(ctx, sep41Query)
	if err != nil {
		m.Metrics.QueryErrors.WithLabelValues("GetAllSEP41Pairs", "contract_tokens", utils.GetDBErrorType(err)).Inc()
		return nil, fmt.Errorf("loading SEP-41 contract_tokens: %w", err)
	}
	sep41 := map[string]uuid.UUID{}
	for ctRows.Next() {
		var (
			id      uuid.UUID
			strkey1 string
		)
		if err := ctRows.Scan(&id, &strkey1); err != nil {
			ctRows.Close()
			return nil, fmt.Errorf("scanning contract_tokens row: %w", err)
		}
		sep41[strkey1] = id
	}
	ctRows.Close()
	if err := ctRows.Err(); err != nil {
		return nil, fmt.Errorf("iterating contract_tokens rows: %w", err)
	}

	// Filter the BYTEA pairs by SEP-41 membership.
	pairs := make([]BalancePair, 0, len(raws))
	for _, r := range raws {
		tokenStrkey := r.Token.String()
		id, ok := sep41[tokenStrkey]
		if !ok {
			continue // not SEP-41 (likely SAC or unknown)
		}
		pairs = append(pairs, BalancePair{
			AccountAddress:  r.Account.String(),
			ContractID:      id,
			ContractAddress: tokenStrkey,
		})
	}
	return pairs, nil
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
