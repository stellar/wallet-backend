// lookup_helpers.go provides helper functions for testing the transaction_hashes lookup table.
package infrastructure

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/stellar/wallet-backend/internal/db"
)

// lookupBucketSQL and lookupPrefixSQL compute a transactions row's lookup key from t.hash, the
// same expressions as the fill script in docs/deploy/upgrading.md.
const (
	lookupPrefixSQL = `('x' || encode(substr(t.hash, 1, 5), 'hex'))::bit(40)::bigint`
	lookupBucketSQL = `((('x' || encode(substr(t.hash, 1, 5), 'hex'))::bit(40)::bigint) >> 20)::integer`
)

// openWalletDBPool opens a connection pool to the wallet-backend database.
func (s *SharedContainers) openWalletDBPool(ctx context.Context) (*pgxpool.Pool, error) {
	dbURL, err := s.GetWalletDBConnectionString(ctx)
	if err != nil {
		return nil, fmt.Errorf("getting database connection string: %w", err)
	}
	dbPool, err := db.OpenDBConnectionPool(ctx, dbURL)
	if err != nil {
		return nil, fmt.Errorf("opening database connection: %w", err)
	}
	return dbPool, nil
}

// RunLookupMaintenance runs transaction_hashes_maintenance once as worker `worker` of `workers`.
// The procedure commits after each chunk, so it runs outside a transaction block, as the only
// statement, through the simple protocol, the way a TimescaleDB job calls it. A non-zero jobID
// also deletes the lookup rows of periods that closed before the oldest transactions row.
func (s *SharedContainers) RunLookupMaintenance(ctx context.Context, jobID int, workers, worker int) error {
	dbPool, err := s.openWalletDBPool(ctx)
	if err != nil {
		return err
	}
	defer dbPool.Close()

	config := fmt.Sprintf(`{"workers": %d, "worker": %d}`, workers, worker)
	if _, err := dbPool.Exec(ctx, `CALL transaction_hashes_maintenance($1, $2)`, pgx.QueryExecModeSimpleProtocol, jobID, config); err != nil {
		return fmt.Errorf("running transaction_hashes_maintenance(%d, %s): %w", jobID, config, err)
	}
	return nil
}

// LookupChunkCounts returns the number of transaction_hashes chunks, how many of them are not
// compressed, and how many are partially compressed: compressed, with rows written since in their
// rowstore part.
func (s *SharedContainers) LookupChunkCounts(ctx context.Context) (total, uncompressed, partial int, err error) {
	dbPool, err := s.openWalletDBPool(ctx)
	if err != nil {
		return 0, 0, 0, err
	}
	defer dbPool.Close()

	// The catalog's chunk row carries the status bits (8 = partial) and the hypertable id on every
	// TimescaleDB version the tests run against; the information view gives the compressed flag.
	query := `
		SELECT
			(SELECT count(*) FROM timescaledb_information.chunks WHERE hypertable_name = 'transaction_hashes'),
			(SELECT count(*) FROM timescaledb_information.chunks WHERE hypertable_name = 'transaction_hashes' AND NOT is_compressed),
			(SELECT count(*) FROM _timescaledb_catalog.chunk c
			   JOIN _timescaledb_catalog.hypertable h ON h.id = c.hypertable_id
			  WHERE h.table_name = 'transaction_hashes' AND c.status & 8 <> 0)
	`
	if err := dbPool.QueryRow(ctx, query).Scan(&total, &uncompressed, &partial); err != nil {
		return 0, 0, 0, fmt.Errorf("counting transaction_hashes chunks: %w", err)
	}
	return total, uncompressed, partial, nil
}

// TransactionsWithoutLookupRow counts the transactions rows in [startLedger, endLedger] that have
// no transaction_hashes row with their bucket, prefix and close second. startLedger and endLedger
// both 0 counts over all transactions.
func (s *SharedContainers) TransactionsWithoutLookupRow(ctx context.Context, startLedger, endLedger uint32) (int, error) {
	dbPool, err := s.openWalletDBPool(ctx)
	if err != nil {
		return 0, err
	}
	defer dbPool.Close()

	query := fmt.Sprintf(`
		SELECT count(*)
		FROM transactions t
		LEFT JOIN transaction_hashes l
			ON l.hash_bucket = %s
			AND l.hash_prefix = %s
			AND l.ledger_close_epoch = floor(extract(epoch FROM t.ledger_created_at))::bigint
		WHERE l.hash_prefix IS NULL
		AND (($1::int = 0 AND $2::int = 0) OR t.ledger_number BETWEEN $1 AND $2)
	`, lookupBucketSQL, lookupPrefixSQL)
	var count int
	if err := dbPool.QueryRow(ctx, query, int64(startLedger), int64(endLedger)).Scan(&count); err != nil {
		return 0, fmt.Errorf("counting transactions without a lookup row in ledgers %d-%d: %w", startLedger, endLedger, err)
	}
	return count, nil
}

// TransactionHashesInLedgerRange returns up to limit transaction hashes in [startLedger, endLedger],
// as the lowercase hex the API takes.
func (s *SharedContainers) TransactionHashesInLedgerRange(ctx context.Context, startLedger, endLedger uint32, limit int) ([]string, error) {
	dbPool, err := s.openWalletDBPool(ctx)
	if err != nil {
		return nil, err
	}
	defer dbPool.Close()

	rows, err := dbPool.Query(ctx, `
		SELECT encode(hash, 'hex')
		FROM transactions
		WHERE ledger_number BETWEEN $1 AND $2
		ORDER BY ledger_number, to_id
		LIMIT $3
	`, int64(startLedger), int64(endLedger), limit)
	if err != nil {
		return nil, fmt.Errorf("querying transaction hashes in ledgers %d-%d: %w", startLedger, endLedger, err)
	}
	hashes, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		return nil, fmt.Errorf("collecting transaction hashes in ledgers %d-%d: %w", startLedger, endLedger, err)
	}
	return hashes, nil
}
