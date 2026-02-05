// backfill_helpers.go provides helper functions for backfill integration testing.
package infrastructure

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"strconv"
	"time"

	"github.com/stellar/go-stellar-sdk/support/log"

	"github.com/stellar/wallet-backend/internal/indexer/types"
)

// GetIngestCursor retrieves a cursor value from the ingest_store table.
func (s *SharedContainers) GetIngestCursor(ctx context.Context, cursorName string) (uint32, error) {
	dbURL, err := s.GetWalletDBConnectionString(ctx)
	if err != nil {
		return 0, fmt.Errorf("getting database connection string: %w", err)
	}
	db, err := sql.Open("postgres", dbURL)
	if err != nil {
		return 0, fmt.Errorf("opening database connection: %w", err)
	}
	defer db.Close() //nolint:errcheck

	var valueStr string
	query := `SELECT value FROM ingest_store WHERE key = $1`
	err = db.QueryRowContext(ctx, query, cursorName).Scan(&valueStr)
	if err != nil {
		return 0, fmt.Errorf("querying ingest_store for %s: %w", cursorName, err)
	}

	value, err := strconv.ParseUint(valueStr, 10, 32)
	if err != nil {
		return 0, fmt.Errorf("parsing cursor value %s: %w", valueStr, err)
	}

	return uint32(value), nil
}

// GetTransactionCountForAccount counts transactions involving a specific account in a ledger range.
func (s *SharedContainers) GetTransactionCountForAccount(ctx context.Context, accountAddr string, startLedger, endLedger uint32) (int, error) {
	dbURL, err := s.GetWalletDBConnectionString(ctx)
	if err != nil {
		return 0, fmt.Errorf("getting database connection string: %w", err)
	}
	db, err := sql.Open("postgres", dbURL)
	if err != nil {
		return 0, fmt.Errorf("opening database connection: %w", err)
	}
	defer db.Close() //nolint:errcheck

	var count int
	query := `
		SELECT COUNT(DISTINCT t.hash)
		FROM transactions t
		INNER JOIN transactions_accounts ta ON t.to_id = ta.tx_to_id
		WHERE ta.account_id = $1
		AND t.ledger_number BETWEEN $2 AND $3
	`
	err = db.QueryRowContext(ctx, query, types.AddressBytea(accountAddr), startLedger, endLedger).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("counting transactions for account %s: %w", accountAddr, err)
	}

	return count, nil
}

// HasOperationForAccount checks if an operation of a specific type exists for an account in a ledger range.
func (s *SharedContainers) HasOperationForAccount(ctx context.Context, accountAddr, opType string, startLedger, endLedger uint32) (bool, error) {
	dbURL, err := s.GetWalletDBConnectionString(ctx)
	if err != nil {
		return false, fmt.Errorf("getting database connection string: %w", err)
	}
	db, err := sql.Open("postgres", dbURL)
	if err != nil {
		return false, fmt.Errorf("opening database connection: %w", err)
	}
	defer db.Close() //nolint:errcheck

	var exists bool
	query := `
		SELECT EXISTS (
			SELECT 1 FROM operations o
			INNER JOIN operations_accounts oa ON o.id = oa.operation_id
			WHERE oa.account_id = $1
			AND o.operation_type = $2
			AND o.ledger_number BETWEEN $3 AND $4
		)
	`
	err = db.QueryRowContext(ctx, query, types.AddressBytea(accountAddr), opType, startLedger, endLedger).Scan(&exists)
	if err != nil {
		return false, fmt.Errorf("checking operation for account %s: %w", accountAddr, err)
	}

	return exists, nil
}

// GetTransactionAccountLinkCount counts transaction-account links for an account in a ledger range.
func (s *SharedContainers) GetTransactionAccountLinkCount(ctx context.Context, accountAddr string, startLedger, endLedger uint32) (int, error) {
	dbURL, err := s.GetWalletDBConnectionString(ctx)
	if err != nil {
		return 0, fmt.Errorf("getting database connection string: %w", err)
	}
	db, err := sql.Open("postgres", dbURL)
	if err != nil {
		return 0, fmt.Errorf("opening database connection: %w", err)
	}
	defer db.Close() //nolint:errcheck

	var count int
	query := `
		SELECT COUNT(*)
		FROM transactions_accounts ta
		INNER JOIN transactions t ON ta.tx_to_id = t.to_id
		WHERE ta.account_id = $1
		AND t.ledger_number BETWEEN $2 AND $3
	`
	err = db.QueryRowContext(ctx, query, accountAddr, startLedger, endLedger).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("counting transaction-account links for %s: %w", accountAddr, err)
	}

	return count, nil
}

// GetStateChangeCountForLedgerRange counts state changes in a ledger range.
func (s *SharedContainers) GetStateChangeCountForLedgerRange(ctx context.Context, startLedger, endLedger uint32) (int, error) {
	dbURL, err := s.GetWalletDBConnectionString(ctx)
	if err != nil {
		return 0, fmt.Errorf("getting database connection string: %w", err)
	}
	db, err := sql.Open("postgres", dbURL)
	if err != nil {
		return 0, fmt.Errorf("opening database connection: %w", err)
	}
	defer db.Close() //nolint:errcheck

	var count int
	query := `SELECT COUNT(*) FROM state_changes WHERE ledger_number BETWEEN $1 AND $2`
	err = db.QueryRowContext(ctx, query, startLedger, endLedger).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("counting state changes: %w", err)
	}

	return count, nil
}

// GetLedgerGapCount counts the number of missing ledgers (gaps) in the transactions table
// within the specified ledger range. Returns 0 if there are no gaps.
func (s *SharedContainers) GetLedgerGapCount(ctx context.Context, startLedger, endLedger uint32) (int, error) {
	dbURL, err := s.GetWalletDBConnectionString(ctx)
	if err != nil {
		return 0, fmt.Errorf("getting database connection string: %w", err)
	}
	db, err := sql.Open("postgres", dbURL)
	if err != nil {
		return 0, fmt.Errorf("opening database connection: %w", err)
	}
	defer db.Close() //nolint:errcheck

	// Count the number of distinct ledgers that have transactions in the range
	// Then compare with expected count to find gaps
	var distinctLedgers int
	query := `
		SELECT COUNT(DISTINCT ledger_number)
		FROM transactions
		WHERE ledger_number BETWEEN $1 AND $2
	`
	err = db.QueryRowContext(ctx, query, startLedger, endLedger).Scan(&distinctLedgers)
	if err != nil {
		return 0, fmt.Errorf("counting distinct ledgers: %w", err)
	}

	// Note: Not all ledgers will have transactions, so we can't simply compare
	// against expected range. Instead, use window function to find actual gaps.
	var gapCount int
	gapQuery := `
		WITH ledger_sequence AS (
			SELECT DISTINCT ledger_number
			FROM transactions
			WHERE ledger_number BETWEEN $1 AND $2
			ORDER BY ledger_number
		),
		gaps AS (
			SELECT
				ledger_number,
				LEAD(ledger_number) OVER (ORDER BY ledger_number) AS next_ledger,
				LEAD(ledger_number) OVER (ORDER BY ledger_number) - ledger_number - 1 AS gap_size
			FROM ledger_sequence
		)
		SELECT COALESCE(SUM(gap_size), 0) FROM gaps WHERE gap_size > 0
	`
	err = db.QueryRowContext(ctx, gapQuery, startLedger, endLedger).Scan(&gapCount)
	if err != nil {
		return 0, fmt.Errorf("counting ledger gaps: %w", err)
	}

	return gapCount, nil
}

// WaitForBackfillCompletion polls until the oldest_ingest_ledger cursor reaches the expected value.
func (s *SharedContainers) WaitForBackfillCompletion(ctx context.Context, expectedOldestLedger uint32, timeout time.Duration) error {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	timeoutChan := time.After(timeout)

	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("context cancelled: %w", ctx.Err())
		case <-timeoutChan:
			return fmt.Errorf("backfill did not complete within %v", timeout)
		case <-ticker.C:
			oldest, err := s.GetIngestCursor(ctx, "oldest_ingest_ledger")
			if err != nil {
				log.Ctx(ctx).Warnf("Error getting oldest cursor during backfill wait: %v", err)
				continue
			}

			log.Ctx(ctx).Infof("Backfill progress: oldest_ingest_ledger=%d, expected=%d", oldest, expectedOldestLedger)

			if oldest <= expectedOldestLedger {
				log.Ctx(ctx).Infof("Backfill completed: oldest_ingest_ledger=%d reached expected=%d", oldest, expectedOldestLedger)
				return nil
			}

			// Check if backfill container has exited (it exits on completion or error)
			if s.BackfillContainer != nil {
				state, stateErr := s.BackfillContainer.Container.State(ctx)
				if stateErr == nil && !state.Running {
					if state.ExitCode != 0 {
						// Log container output for debugging
						logs, logErr := s.BackfillContainer.Container.Logs(ctx)
						if logErr == nil {
							logBytes, _ := io.ReadAll(logs) //nolint:errcheck
							logs.Close()                    //nolint:errcheck
							log.Ctx(ctx).Errorf("Backfill container logs:\n%s", string(logBytes))
						}
						return fmt.Errorf("backfill container exited with code %d", state.ExitCode)
					}
					// Container exited successfully, check cursor one more time
					oldest, err = s.GetIngestCursor(ctx, "oldest_ingest_ledger")
					if err == nil {
						if oldest <= expectedOldestLedger {
							return nil
						} else {
							return fmt.Errorf("backfill container exited successfully but cursor %d did not reach expected %d", oldest, expectedOldestLedger)
						}
					}
				}
			}
		}
	}
}
