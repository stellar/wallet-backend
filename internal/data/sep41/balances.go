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
	GetByAccount(ctx context.Context, accountAddress string) ([]Balance, error)
	BatchUpsert(ctx context.Context, dbTx pgx.Tx, upserts []Balance, deletes []Balance) error
	BatchCopy(ctx context.Context, dbTx pgx.Tx, balances []Balance) error
}

type BalanceModel struct {
	DB      *pgxpool.Pool
	Metrics *metrics.DBMetrics
}

var _ BalanceModelInterface = (*BalanceModel)(nil)

// GetByAccount returns all SEP-41 balances held by an account, joined with contract_tokens metadata.
func (m *BalanceModel) GetByAccount(ctx context.Context, accountAddress string) ([]Balance, error) {
	if accountAddress == "" {
		return nil, fmt.Errorf("empty account address")
	}

	const query = `
		SELECT
			b.contract_id, b.balance, b.last_modified_ledger,
			ct.contract_id, ct.name, ct.symbol, ct.decimals
		FROM sep41_balances b
		INNER JOIN contract_tokens ct ON ct.id = b.contract_id
		WHERE b.account_address = $1`

	start := time.Now()
	rows, err := m.DB.Query(ctx, query, accountAddress)
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

// BatchUpsert applies a set of balance inserts/updates and deletes inside the given transaction.
func (m *BalanceModel) BatchUpsert(ctx context.Context, dbTx pgx.Tx, upserts []Balance, deletes []Balance) error {
	if len(upserts) == 0 && len(deletes) == 0 {
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

	for _, bal := range upserts {
		batch.Queue(upsertQuery, bal.AccountAddress, bal.ContractID, bal.Balance, bal.LedgerNumber)
	}

	const deleteQuery = `DELETE FROM sep41_balances WHERE account_address = $1 AND contract_id = $2`
	for _, bal := range deletes {
		batch.Queue(deleteQuery, bal.AccountAddress, bal.ContractID)
	}

	if batch.Len() == 0 {
		return nil
	}

	br := dbTx.SendBatch(ctx, batch)
	for i := 0; i < batch.Len(); i++ {
		if _, err := br.Exec(); err != nil {
			_ = br.Close() //nolint:errcheck // cleanup on error path
			m.Metrics.QueryErrors.WithLabelValues("BatchUpsert", "sep41_balances", utils.GetDBErrorType(err)).Inc()
			return fmt.Errorf("upserting SEP-41 balances: %w", err)
		}
	}
	if err := br.Close(); err != nil {
		return fmt.Errorf("closing SEP-41 balance batch: %w", err)
	}

	duration := time.Since(start).Seconds()
	m.Metrics.QueryDuration.WithLabelValues("BatchUpsert", "sep41_balances").Observe(duration)
	m.Metrics.QueriesTotal.WithLabelValues("BatchUpsert", "sep41_balances").Inc()
	m.Metrics.BatchSize.WithLabelValues("BatchUpsert", "sep41_balances").Observe(float64(len(upserts) + len(deletes)))
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
