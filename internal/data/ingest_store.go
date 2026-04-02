package data

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/stellar/wallet-backend/internal/db"
	"github.com/stellar/wallet-backend/internal/metrics"
	"github.com/stellar/wallet-backend/internal/utils"
)

type LedgerRange struct {
	GapStart uint32 `db:"gap_start"`
	GapEnd   uint32 `db:"gap_end"`
}

type IngestStoreModel struct {
	DB      *pgxpool.Pool
	Metrics *metrics.DBMetrics
}

func (m *IngestStoreModel) Get(ctx context.Context, cursorName string) (uint32, error) {
	start := time.Now()
	valueStr, err := db.QueryOne[string](ctx, m.DB, `SELECT value FROM ingest_store WHERE key = $1`, cursorName)
	duration := time.Since(start).Seconds()
	m.Metrics.QueryDuration.WithLabelValues("Get", "ingest_store").Observe(duration)
	m.Metrics.QueriesTotal.WithLabelValues("Get", "ingest_store").Inc()
	// First run, key does not exist yet
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, nil
	}
	if err != nil {
		m.Metrics.QueryErrors.WithLabelValues("Get", "ingest_store", utils.GetDBErrorType(err)).Inc()
		return 0, fmt.Errorf("getting latest ledger synced for cursor %s: %w", cursorName, err)
	}

	v, err := strconv.ParseUint(valueStr, 10, 32)
	if err != nil {
		return 0, fmt.Errorf("parsing ingest_store value %q for cursor %s: %w", valueStr, cursorName, err)
	}
	return uint32(v), nil
}

func (m *IngestStoreModel) Update(ctx context.Context, dbTx pgx.Tx, cursorName string, ledger uint32) error {
	const query = `
		INSERT INTO ingest_store (key, value) VALUES ($1, $2)
		ON CONFLICT (key) DO UPDATE SET value = excluded.value
	`
	start := time.Now()
	_, err := dbTx.Exec(ctx, query, cursorName, strconv.FormatUint(uint64(ledger), 10))
	duration := time.Since(start).Seconds()
	m.Metrics.QueryDuration.WithLabelValues("Update", "ingest_store").Observe(duration)
	m.Metrics.QueriesTotal.WithLabelValues("Update", "ingest_store").Inc()
	if err != nil {
		m.Metrics.QueryErrors.WithLabelValues("Update", "ingest_store", utils.GetDBErrorType(err)).Inc()
		return fmt.Errorf("updating last synced ledger to %d: %w", ledger, err)
	}
	return nil
}

func (m *IngestStoreModel) CompareAndSwap(ctx context.Context, dbTx pgx.Tx, cursorName string, expectedValue string, newValue string) (bool, error) {
	const query = `UPDATE ingest_store SET value = $1 WHERE key = $2 AND value = $3`
	start := time.Now()
	result, err := dbTx.Exec(ctx, query, newValue, cursorName, expectedValue)
	duration := time.Since(start).Seconds()
	m.Metrics.QueryDuration.WithLabelValues("CompareAndSwap", "ingest_store").Observe(duration)
	m.Metrics.QueriesTotal.WithLabelValues("CompareAndSwap", "ingest_store").Inc()
	if err != nil {
		m.Metrics.QueryErrors.WithLabelValues("CompareAndSwap", "ingest_store", utils.GetDBErrorType(err)).Inc()
		return false, fmt.Errorf("compare-and-swap for cursor %s: %w", cursorName, err)
	}
	return result.RowsAffected() == 1, nil
}

func (m *IngestStoreModel) UpdateMin(ctx context.Context, dbTx pgx.Tx, cursorName string, ledger uint32) error {
	const query = `
		UPDATE ingest_store
		SET value = LEAST(value::integer, $2)::text
		WHERE key = $1
	`
	start := time.Now()
	_, err := dbTx.Exec(ctx, query, cursorName, strconv.FormatUint(uint64(ledger), 10))
	duration := time.Since(start).Seconds()
	m.Metrics.QueryDuration.WithLabelValues("UpdateMin", "ingest_store").Observe(duration)
	m.Metrics.QueriesTotal.WithLabelValues("UpdateMin", "ingest_store").Inc()
	if err != nil {
		m.Metrics.QueryErrors.WithLabelValues("UpdateMin", "ingest_store", utils.GetDBErrorType(err)).Inc()
		return fmt.Errorf("updating minimum ledger for cursor %s: %w", cursorName, err)
	}
	return nil
}

func (m *IngestStoreModel) GetLedgerGaps(ctx context.Context) ([]LedgerRange, error) {
	const query = `
		SELECT gap_start, gap_end FROM (
			SELECT
				ledger_number + 1 AS gap_start,
				LEAD(ledger_number) OVER (ORDER BY ledger_number) - 1 AS gap_end
			FROM (SELECT DISTINCT ledger_number FROM transactions) t
		) gaps
		WHERE gap_start <= gap_end
		ORDER BY gap_start
	`
	start := time.Now()
	ledgerGaps, err := db.QueryMany[LedgerRange](ctx, m.DB, query)
	duration := time.Since(start).Seconds()
	m.Metrics.QueryDuration.WithLabelValues("GetLedgerGaps", "transactions").Observe(duration)
	m.Metrics.QueriesTotal.WithLabelValues("GetLedgerGaps", "transactions").Inc()
	if err != nil {
		m.Metrics.QueryErrors.WithLabelValues("GetLedgerGaps", "transactions", utils.GetDBErrorType(err)).Inc()
		return nil, fmt.Errorf("getting ledger gaps: %w", err)
	}
	return ledgerGaps, nil
}

func (m *IngestStoreModel) GetOldestLedger(ctx context.Context) (uint32, error) {
	start := time.Now()
	oldest, err := db.QueryOne[uint32](ctx, m.DB,
		`SELECT ledger_number FROM transactions ORDER BY ledger_created_at ASC, to_id ASC LIMIT 1`)
	duration := time.Since(start).Seconds()
	m.Metrics.QueryDuration.WithLabelValues("GetOldestLedger", "transactions").Observe(duration)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		m.Metrics.QueryErrors.WithLabelValues("GetOldestLedger", "transactions", utils.GetDBErrorType(err)).Inc()
		return 0, fmt.Errorf("getting actual oldest ledger from transactions: %w", err)
	}
	m.Metrics.QueriesTotal.WithLabelValues("GetOldestLedger", "transactions").Inc()
	return oldest, nil
}
