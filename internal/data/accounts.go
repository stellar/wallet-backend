// AccountModel provides data access methods for account-related queries
// including fee bump eligibility checks and batch lookups for dataloaders. It
// also holds the COPY helper shared by the transactions_accounts and
// operations_accounts link tables.
package data

import (
	"bytes"
	"cmp"
	"context"
	"fmt"
	"slices"
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

// batchCopyAccounts inserts the rows of an account-link table (transactions_accounts,
// operations_accounts) using pgx's binary COPY protocol: it materializes every link up
// front with BuildAccountLinkCopyRows, then streams them with CopyAccountLinkRows.
// idColumn names the link table's parent-ID column.
func batchCopyAccounts[T any](
	ctx context.Context,
	pgxTx pgx.Tx,
	dbMetrics *metrics.DBMetrics,
	table string,
	idColumn string,
	parents []T,
	idAndCreatedAt func(T) (int64, time.Time),
	addressesByID map[int64]map[string]struct{},
) error {
	rows, err := BuildAccountLinkCopyRows(nil, parents, idAndCreatedAt, addressesByID, make(types.AddressByteaMemo))
	if err != nil {
		return fmt.Errorf("building %s COPY rows: %w", table, err)
	}
	if _, err := CopyAccountLinkRows(ctx, pgxTx, dbMetrics, table, idColumn, rows); err != nil {
		return err
	}
	return nil
}

// accountLinkCopyColumns is the COPY column order for an account-link table; idColumn is
// that table's parent-ID column (tx_to_id, operation_id). The order returned here is the
// tuple order BuildAccountLinkCopyRows appends below — the two must be read together.
func accountLinkCopyColumns(idColumn string) []string {
	return []string{"ledger_created_at", idColumn, "account_id"}
}

// BuildAccountLinkCopyRows appends one accountLinkCopyColumns-shaped tuple per
// (parent, participant address) link to rows and returns the extended slice, so callers
// can reuse one backing array across batches. parents supplies each link's
// ledger_created_at — the link table's partition column — through idAndCreatedAt, keyed by
// the same ID that keys addressesByID; both arguments are read only.
//
// memo caches strkey→BYTEA conversions across the appended rows; it is a plain map, so one
// call must not share it with another goroutine. Participants are deduplicated per parent
// row upstream, so a busy account repeats once per transaction/operation here and the memo
// collapses that to one decode per unique address per build. Upstream participants
// handling ensures that account address is not NULL here.
func BuildAccountLinkCopyRows[T any](
	rows [][]any,
	parents []T,
	idAndCreatedAt func(T) (int64, time.Time),
	addressesByID map[int64]map[string]struct{},
	memo types.AddressByteaMemo,
) ([][]any, error) {
	if len(addressesByID) == 0 {
		return rows, nil
	}

	// Build ID -> LedgerCreatedAt lookup from the parent rows
	ledgerCreatedAtByID := make(map[int64]time.Time, len(parents))
	for _, parent := range parents {
		id, ledgerCreatedAt := idAndCreatedAt(parent)
		ledgerCreatedAtByID[id] = ledgerCreatedAt
	}

	type linkRow struct {
		createdAt pgtype.Timestamptz
		id        int64
		addr      []byte
	}
	links := make([]linkRow, 0, len(addressesByID))
	for id, addresses := range addressesByID {
		ledgerCreatedAt, ok := ledgerCreatedAtByID[id]
		if !ok {
			// A silent miss would COPY a zero timestamp — a year-0001 chunk in
			// the hypertable — and means the caller's parent rows and
			// participants disagree about which IDs exist.
			return nil, fmt.Errorf("no row supplies ledger_created_at for %d", id)
		}
		ledgerCreatedAtPgtype := pgtype.Timestamptz{Time: ledgerCreatedAt, Valid: true}
		for addr := range addresses {
			addrBytes, addrErr := memo.Bytes(types.AddressBytea(addr))
			if addrErr != nil {
				return nil, fmt.Errorf("converting address %s to bytes: %w", addr, addrErr)
			}
			links = append(links, linkRow{createdAt: ledgerCreatedAtPgtype, id: id, addr: addrBytes})
		}
	}
	// The maps above iterate in randomized order, but the link table's primary
	// key leads with account_id: COPYing in (account_id, id) order walks the
	// index left-to-right, revisiting each btree page once, instead of paying a
	// random descent — and, on a cold cache, a random page read — per row.
	slices.SortFunc(links, func(a, b linkRow) int {
		if c := bytes.Compare(a.addr, b.addr); c != 0 {
			return c
		}
		return cmp.Compare(a.id, b.id)
	})
	for _, link := range links {
		rows = append(rows, []any{link.createdAt, pgtype.Int8{Int64: link.id, Valid: true}, link.addr})
	}
	return rows, nil
}

// CopyAccountLinkRows streams pre-built account-link tuples (see BuildAccountLinkCopyRows)
// into table with pgx's binary COPY protocol and returns the number of rows the server
// accepted. idColumn names the link table's parent-ID column.
//
// IMPORTANT: like the parent table's BatchCopy, this FAILS on duplicates — COPY has no
// conflict handling.
func CopyAccountLinkRows(
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
