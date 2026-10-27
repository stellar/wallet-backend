package data

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/stellar/wallet-backend/internal/db"
	"github.com/stellar/wallet-backend/internal/indexer/types"
	"github.com/stellar/wallet-backend/internal/utils"
)

const (
	// TransactionHashesTable maps a transaction hash to the ledger close time of its transactions
	// row. See migration 2026-10-27.0-transaction-hashes.sql for the layout.
	TransactionHashesTable = "transaction_hashes"
)

// sqlHashPrefix and sqlHashBucket compute types.HashBytea.LookupKey in SQL, from a BYTEA hash
// column; the one-off fill in docs/deploy/upgrading.md uses the same expressions.
const (
	sqlHashPrefix = `('x' || encode(substr(hash, 1, 5), 'hex'))::bit(40)::bigint`
	sqlHashBucket = `((('x' || encode(substr(hash, 1, 5), 'hex'))::bit(40)::bigint) >> 28)::smallint`
)

// hashLookupArgs returns the lookup key for a hex hash: its bucket, its prefix, and the 32 raw
// bytes the full-hash check compares against.
func hashLookupArgs(hash string) (bucket int16, prefix int64, hashBytes any, err error) {
	bucket, prefix, err = types.HashBytea(hash).LookupKey()
	if err != nil {
		return 0, 0, nil, fmt.Errorf("computing lookup key: %w", err)
	}
	hashBytes, err = types.HashBytea(hash).Value()
	if err != nil {
		return 0, 0, nil, fmt.Errorf("encoding hash: %w", err)
	}
	return bucket, prefix, hashBytes, nil
}

// lookupCloseTimes returns the distinct ledger close times stored in transaction_hashes for a
// (bucket, prefix), newest first. Usually one; more when a second hash shares the prefix, and
// none for a hash that is not indexed. A re-ingested ledger can store the same row twice, so the
// times are distinct.
func lookupCloseTimes(ctx context.Context, conn db.Querier, bucket int16, prefix int64) ([]int64, error) {
	rows, err := conn.Query(ctx,
		`SELECT DISTINCT ledger_close_epoch FROM transaction_hashes WHERE hash_bucket = $1 AND hash_prefix = $2 ORDER BY ledger_close_epoch DESC`,
		bucket, prefix)
	if err != nil {
		return nil, fmt.Errorf("reading %s: %w", TransactionHashesTable, err)
	}
	closeTimes, err := pgx.CollectRows(rows, pgx.RowTo[int64])
	if err != nil {
		return nil, fmt.Errorf("reading %s rows: %w", TransactionHashesTable, err)
	}
	return closeTimes, nil
}

// transactionInSecondQuery returns the statement that reads the transaction with hash $1 whose
// close time falls in the one second starting at closeTime. The second is written into the
// statement as a number, not bound as a parameter, so the planner excludes every other chunk of
// transactions before planning it; bound as a parameter, the time would be unknown at planning
// time and every chunk would be planned, which costs about 0.15 ms per chunk on every lookup.
// Ledger close times are whole seconds; matching the whole second keeps the lookup correct even
// if one were not. columns is the projection, prefixed with "t.".
func transactionInSecondQuery(columns string, closeTime int64) string {
	var qb strings.Builder
	fmt.Fprintf(&qb, `SELECT %s FROM transactions t
		WHERE t.ledger_created_at >= to_timestamp(%d) AND t.ledger_created_at < to_timestamp(%d)
		  AND t.hash = $1`, columns, closeTime, closeTime+1)
	appendIngestCursorBound(&qb, "t.to_id")
	qb.WriteString(" LIMIT 1")
	return qb.String()
}

// transactionKeyByHash returns the primary key (to_id, ledger_created_at) of the committed
// transaction with the given hash. found is false when there is none. Hashes are unique on the
// network; if a data bug stored the same hash twice, the newest transaction wins.
func transactionKeyByHash(ctx context.Context, conn db.Querier, hash string) (toID int64, ledgerCreatedAt time.Time, found bool, err error) {
	bucket, prefix, hashBytes, err := hashLookupArgs(hash)
	if err != nil {
		return 0, time.Time{}, false, err
	}
	closeTimes, err := lookupCloseTimes(ctx, conn, bucket, prefix)
	if err != nil {
		return 0, time.Time{}, false, err
	}
	for _, closeTime := range closeTimes {
		err = conn.QueryRow(ctx, transactionInSecondQuery("t.to_id, t.ledger_created_at", closeTime), hashBytes).Scan(&toID, &ledgerCreatedAt)
		if errors.Is(err, pgx.ErrNoRows) {
			continue // a different hash shares the prefix
		}
		if err != nil {
			return 0, time.Time{}, false, fmt.Errorf("looking up transaction by hash: %w", err)
		}
		return toID, ledgerCreatedAt, true, nil
	}
	return 0, time.Time{}, false, nil
}

// BatchCopyHashes writes the transaction_hashes rows for txs using pgx's binary COPY protocol. It
// runs in the same database transaction as BatchCopy, so a transaction and its lookup row commit
// together. transaction_hashes has no unique constraint: re-ingesting a ledger after a crash
// writes the same rows again, and lookups tolerate the duplicates.
func (m *TransactionModel) BatchCopyHashes(ctx context.Context, pgxTx pgx.Tx, txs []*types.Transaction) (int, error) {
	if len(txs) == 0 {
		return 0, nil
	}

	start := time.Now()
	copyCount, err := pgxTx.CopyFrom(
		ctx,
		pgx.Identifier{TransactionHashesTable},
		[]string{"hash_bucket", "hash_prefix", "ledger_close_epoch"},
		pgx.CopyFromSlice(len(txs), func(i int) ([]any, error) {
			tx := txs[i]
			bucket, prefix, err := tx.Hash.LookupKey()
			if err != nil {
				return nil, fmt.Errorf("computing lookup key for hash %s: %w", tx.Hash, err)
			}
			return []any{
				pgtype.Int2{Int16: bucket, Valid: true},
				pgtype.Int8{Int64: prefix, Valid: true},
				pgtype.Int8{Int64: tx.LedgerCreatedAt.Unix(), Valid: true},
			}, nil
		}),
	)
	m.Metrics.QueryDuration.WithLabelValues("BatchCopyHashes", TransactionHashesTable).Observe(time.Since(start).Seconds())
	m.Metrics.BatchSize.WithLabelValues("BatchCopyHashes", TransactionHashesTable).Observe(float64(len(txs)))
	m.Metrics.QueriesTotal.WithLabelValues("BatchCopyHashes", TransactionHashesTable).Inc()
	if err != nil {
		m.Metrics.QueryErrors.WithLabelValues("BatchCopyHashes", TransactionHashesTable, utils.GetDBErrorType(err)).Inc()
		return 0, fmt.Errorf("pgx CopyFrom %s: %w", TransactionHashesTable, err)
	}
	if int(copyCount) != len(txs) {
		m.Metrics.QueryErrors.WithLabelValues("BatchCopyHashes", TransactionHashesTable, "row_count_mismatch").Inc()
		return 0, fmt.Errorf("expected %d rows copied, got %d", len(txs), copyCount)
	}
	return len(txs), nil
}
