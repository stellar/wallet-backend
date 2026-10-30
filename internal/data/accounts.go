// AccountModel provides data access methods for account-related queries
// including fee bump eligibility checks and batch lookups for dataloaders.
package data

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/stellar/wallet-backend/internal/db"
	"github.com/stellar/wallet-backend/internal/indexer/types"
	"github.com/stellar/wallet-backend/internal/metrics"
	"github.com/stellar/wallet-backend/internal/utils"
)

type AccountModel struct {
	DB      *pgxpool.Pool
	Metrics *metrics.DBMetrics
}

// BatchGetByToIDs gets the accounts that are associated with the given transaction ToIDs.
func (m *AccountModel) BatchGetByToIDs(ctx context.Context, toIDs []int64, columns string) ([]*types.AccountWithToID, error) {
	query := `
		SELECT account_id AS stellar_address, tx_to_id
		FROM transactions_accounts
		WHERE tx_to_id = ANY($1)`
	start := time.Now()
	accounts, err := db.QueryManyPtrs[types.AccountWithToID](ctx, m.DB, query, toIDs)
	duration := time.Since(start).Seconds()
	m.Metrics.QueryDuration.WithLabelValues("BatchGetByToIDs", "transactions_accounts").Observe(duration)
	m.Metrics.BatchSize.WithLabelValues("BatchGetByToIDs", "transactions_accounts").Observe(float64(len(toIDs)))
	m.Metrics.QueriesTotal.WithLabelValues("BatchGetByToIDs", "transactions_accounts").Inc()
	if err != nil {
		m.Metrics.QueryErrors.WithLabelValues("BatchGetByToIDs", "transactions_accounts", utils.GetDBErrorType(err)).Inc()
		return nil, fmt.Errorf("getting accounts by transaction ToIDs: %w", err)
	}
	return accounts, nil
}

// BatchGetByOperationIDs gets the accounts that are associated with the given operation IDs.
func (m *AccountModel) BatchGetByOperationIDs(ctx context.Context, operationIDs []int64, columns string) ([]*types.AccountWithOperationID, error) {
	query := `
		SELECT account_id AS stellar_address, operation_id
		FROM operations_accounts
		WHERE operation_id = ANY($1)`
	start := time.Now()
	accounts, err := db.QueryManyPtrs[types.AccountWithOperationID](ctx, m.DB, query, operationIDs)
	duration := time.Since(start).Seconds()
	m.Metrics.QueryDuration.WithLabelValues("BatchGetByOperationIDs", "operations_accounts").Observe(duration)
	m.Metrics.BatchSize.WithLabelValues("BatchGetByOperationIDs", "operations_accounts").Observe(float64(len(operationIDs)))
	m.Metrics.QueriesTotal.WithLabelValues("BatchGetByOperationIDs", "operations_accounts").Inc()
	if err != nil {
		m.Metrics.QueryErrors.WithLabelValues("BatchGetByOperationIDs", "operations_accounts", utils.GetDBErrorType(err)).Inc()
		return nil, fmt.Errorf("getting accounts by operation IDs: %w", err)
	}
	return accounts, nil
}

// accountLinkCopyColumns is the COPY column order for an account-link table
// (transactions_accounts, operations_accounts), matching the tuple order
// BuildAccountLinkCopyRows produces. idColumn is the table's parent-ID column.
func accountLinkCopyColumns(idColumn string) []string {
	return []string{"ledger_created_at", idColumn, "account_id"}
}

// BuildAccountLinkCopyRows returns one accountLinkCopyColumns-shaped tuple per
// (parent, participant address) link in addressesByID. parents supplies each link's
// ledger_created_at — the link table's partition column — through idAndCreatedAt, keyed
// by the same ID that keys addressesByID; both arguments are read only. idColumn names
// the link table's parent-ID column in the error for a participant without a parent.
// Upstream participants handling ensures that account address is not NULL here. It
// touches no database and no metrics, so callers can build rows on any goroutine.
func BuildAccountLinkCopyRows[T any](
	parents []T,
	idAndCreatedAt func(T) (int64, time.Time),
	addressesByID map[int64]map[string]struct{},
	idColumn string,
) ([][]any, error) {
	if len(addressesByID) == 0 {
		return nil, nil
	}

	// Build ID -> LedgerCreatedAt lookup from the parent rows
	ledgerCreatedAtByID := make(map[int64]time.Time, len(parents))
	for _, parent := range parents {
		id, ledgerCreatedAt := idAndCreatedAt(parent)
		ledgerCreatedAtByID[id] = ledgerCreatedAt
	}

	var rows [][]any
	for id, addresses := range addressesByID {
		ledgerCreatedAt, ok := ledgerCreatedAtByID[id]
		if !ok {
			// A silent miss would COPY a zero timestamp — a year-0001 chunk in
			// the hypertable — and means the caller's parent rows and
			// participants disagree about which IDs exist.
			return nil, fmt.Errorf("no row supplies ledger_created_at for %s %d", idColumn, id)
		}
		ledgerCreatedAtPgtype := pgtype.Timestamptz{Time: ledgerCreatedAt, Valid: true}
		idPgtype := pgtype.Int8{Int64: id, Valid: true}
		for addr := range addresses {
			addrBytes, addrErr := types.AddressBytea(addr).Value()
			if addrErr != nil {
				return nil, fmt.Errorf("converting address %s to bytes: %w", addr, addrErr)
			}
			rows = append(rows, []any{
				ledgerCreatedAtPgtype,
				idPgtype,
				addrBytes,
			})
		}
	}
	return rows, nil
}

// copyAccountLinkRows streams pre-built account-link tuples (see
// BuildAccountLinkCopyRows) into table with pgx's binary COPY protocol and returns the
// number of rows written. idColumn names the link table's parent-ID column.
//
// IMPORTANT: like the parent table's BatchCopy, this FAILS on duplicates — COPY has no
// conflict handling.
func copyAccountLinkRows(
	ctx context.Context,
	pgxTx pgx.Tx,
	dbMetrics *metrics.DBMetrics,
	table string,
	idColumn string,
	rows [][]any,
) (int64, error) {
	if len(rows) == 0 {
		return 0, nil
	}

	start := time.Now()
	copyCount, err := pgxTx.CopyFrom(ctx, pgx.Identifier{table}, accountLinkCopyColumns(idColumn), pgx.CopyFromRows(rows))
	duration := time.Since(start).Seconds()
	dbMetrics.QueryDuration.WithLabelValues("BatchCopyAccounts", table).Observe(duration)
	dbMetrics.BatchSize.WithLabelValues("BatchCopyAccounts", table).Observe(float64(len(rows)))
	dbMetrics.QueriesTotal.WithLabelValues("BatchCopyAccounts", table).Inc()
	if err != nil {
		dbMetrics.QueryErrors.WithLabelValues("BatchCopyAccounts", table, utils.GetDBErrorType(err)).Inc()
		return 0, fmt.Errorf("pgx CopyFrom %s: %w", table, err)
	}

	return copyCount, nil
}
