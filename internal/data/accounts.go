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
// operations_accounts) using pgx's binary COPY protocol. parents supplies each link's
// ledger_created_at — the link table's partition column — through idAndCreatedAt, keyed
// by the same ID that keys addressesByID; both arguments come from the same buffer and
// are read only. idColumn names the link table's ID column.
//
// IMPORTANT: like the parent table's BatchCopy, this FAILS on duplicates — COPY has no
// conflict handling.
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
	if len(addressesByID) == 0 {
		return nil
	}

	start := time.Now()

	// Build ID -> LedgerCreatedAt lookup from the parent rows
	ledgerCreatedAtByID := make(map[int64]time.Time, len(parents))
	for _, parent := range parents {
		id, ledgerCreatedAt := idAndCreatedAt(parent)
		ledgerCreatedAtByID[id] = ledgerCreatedAt
	}

	// COPY the link table using pgx binary format with native pgtype types. Upstream
	// participants handling ensures that account address is not NULL here.
	// Participants are deduplicated per parent row upstream, so a busy account
	// repeats once per transaction/operation here; the memo collapses that to
	// one decode per unique address per batch.
	memo := make(types.AddressByteaMemo)
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
			return fmt.Errorf("no row supplies ledger_created_at for %s %d", idColumn, id)
		}
		ledgerCreatedAtPgtype := pgtype.Timestamptz{Time: ledgerCreatedAt, Valid: true}
		for addr := range addresses {
			addrBytes, addrErr := memo.Bytes(types.AddressBytea(addr))
			if addrErr != nil {
				return fmt.Errorf("converting address %s to bytes: %w", addr, addrErr)
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
	rows := make([][]any, len(links))
	for i, link := range links {
		rows[i] = []any{link.createdAt, pgtype.Int8{Int64: link.id, Valid: true}, link.addr}
	}

	_, err := pgxTx.CopyFrom(
		ctx,
		pgx.Identifier{table},
		[]string{"ledger_created_at", idColumn, "account_id"},
		pgx.CopyFromRows(rows),
	)
	duration := time.Since(start).Seconds()
	dbMetrics.QueryDuration.WithLabelValues("BatchCopyAccounts", table).Observe(duration)
	dbMetrics.BatchSize.WithLabelValues("BatchCopyAccounts", table).Observe(float64(len(rows)))
	dbMetrics.QueriesTotal.WithLabelValues("BatchCopyAccounts", table).Inc()
	if err != nil {
		dbMetrics.QueryErrors.WithLabelValues("BatchCopyAccounts", table, utils.GetDBErrorType(err)).Inc()
		return fmt.Errorf("pgx CopyFrom %s: %w", table, err)
	}

	return nil
}
