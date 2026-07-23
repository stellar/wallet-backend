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

const (
	LatestLedgerCursorName = "latest_ingest_ledger"
	OldestLedgerCursorName = "oldest_ingest_ledger"
)

// ErrCASCursorMissing is returned by CompareAndSwap when the cursor row does
// not exist in ingest_store. The model layer keeps this distinct from the
// ordinary value-mismatch race so callers can decide what to do — a missing
// row may be operationally normal (cursor not yet initialized by protocol-setup
// / protocol-migrate) or a real incident (dropped row, bad restore). Live
// ingestion (see ingest_live.go's casProtocolCursor) only ever calls
// CompareAndSwap for a cursor its protocolCursorSnapshot believes exists, so
// from that caller this error always means the genuine incident case: the
// `cursor_missing` query-error metric recorded below fires accordingly,
// rather than on every not-yet-initialized ledger.
var ErrCASCursorMissing = errors.New("ingest_store cursor row missing")

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

func (m *IngestStoreModel) GetMany(ctx context.Context, keys []string) (map[string]uint32, error) {
	if len(keys) == 0 {
		return nil, nil
	}

	const query = `SELECT key, value FROM ingest_store WHERE key = ANY($1)`

	start := time.Now()
	rows, err := m.DB.Query(ctx, query, keys)
	duration := time.Since(start).Seconds()
	m.Metrics.QueryDuration.WithLabelValues("GetMany", "ingest_store").Observe(duration)
	m.Metrics.QueriesTotal.WithLabelValues("GetMany", "ingest_store").Inc()
	if err != nil {
		m.Metrics.QueryErrors.WithLabelValues("GetMany", "ingest_store", utils.GetDBErrorType(err)).Inc()
		return nil, fmt.Errorf("getting values for keys: %w", err)
	}
	defer rows.Close()

	result := make(map[string]uint32, len(keys))
	for rows.Next() {
		var key, value string
		if err := rows.Scan(&key, &value); err != nil {
			m.Metrics.QueryErrors.WithLabelValues("GetMany", "ingest_store", utils.GetDBErrorType(err)).Inc()
			return nil, fmt.Errorf("scanning ingest_store row: %w", err)
		}
		parsed, err := strconv.ParseUint(value, 10, 32)
		if err != nil {
			m.Metrics.QueryErrors.WithLabelValues("GetMany", "ingest_store", utils.GetDBErrorType(err)).Inc()
			return nil, fmt.Errorf("parsing value for key %s: %w", key, err)
		}
		result[key] = uint32(parsed)
	}
	if err := rows.Err(); err != nil {
		m.Metrics.QueryErrors.WithLabelValues("GetMany", "ingest_store", utils.GetDBErrorType(err)).Inc()
		return nil, fmt.Errorf("iterating ingest_store rows: %w", err)
	}
	return result, nil
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

// ErrCursorGuardFailed is returned by UpdateGuarded when the cursor's current
// value is neither ledger-1 nor ledger (see UpdateGuarded), or the row does
// not exist. Either way, a writer other than the one calling UpdateGuarded
// has moved the cursor past what it expected — most commonly a second live
// ingestion instance that acquired the advisory lock after this session's
// Postgres session died in a failover (see startLiveIngestion's
// checkLockSession, which is the primary defense; this guard is the backstop
// for the race window before that probe observes the dead session).
var ErrCursorGuardFailed = errors.New("ingest_store guarded cursor update refused: cursor value not owned by this writer")

// UpdateGuarded advances cursorName to ledger only if its current value is ledger-1 (the normal
// sequential case: this writer is the sole owner and is advancing by exactly one ledger) or
// ledger itself (the self-value case: the first ledger processed immediately after
// startLiveIngestion's initializeCursors already set the cursor to this same starting ledger).
// Any other current value — including a missing row — means a writer other than the caller has
// moved the cursor, so the swap is refused with ErrCursorGuardFailed instead of silently
// overwriting a value another instance already advanced (which a blind Update would do).
func (m *IngestStoreModel) UpdateGuarded(ctx context.Context, dbTx pgx.Tx, cursorName string, ledger uint32) error {
	const query = `
		UPDATE ingest_store
		SET value = $1
		WHERE key = $2 AND value IN ($3, $4)
	`
	newValue := strconv.FormatUint(uint64(ledger), 10)
	prevValue := strconv.FormatUint(uint64(ledger-1), 10)

	start := time.Now()
	tag, err := dbTx.Exec(ctx, query, newValue, cursorName, prevValue, newValue)
	duration := time.Since(start).Seconds()
	m.Metrics.QueryDuration.WithLabelValues("UpdateGuarded", "ingest_store").Observe(duration)
	m.Metrics.QueriesTotal.WithLabelValues("UpdateGuarded", "ingest_store").Inc()
	if err != nil {
		m.Metrics.QueryErrors.WithLabelValues("UpdateGuarded", "ingest_store", utils.GetDBErrorType(err)).Inc()
		return fmt.Errorf("guarded update for cursor %s to %d: %w", cursorName, ledger, err)
	}
	if tag.RowsAffected() == 0 {
		m.Metrics.QueryErrors.WithLabelValues("UpdateGuarded", "ingest_store", "cursor_guard_failed").Inc()
		return fmt.Errorf("guarded update for cursor %s to %d: %w", cursorName, ledger, ErrCursorGuardFailed)
	}
	return nil
}

func (m *IngestStoreModel) CompareAndSwap(ctx context.Context, dbTx pgx.Tx, cursorName string, expectedValue string, newValue string) (bool, error) {
	// A plain UPDATE returns RowsAffected=0 for both "value mismatch" and
	// "row missing" — callers treat false as a race loss and mark the
	// migration as handed off, which silently succeeds if the cursor row
	// was dropped. Distinguish the two cases in a single round-trip: EXISTS
	// runs against the same snapshot as the UPDATE and, since UPDATE can't
	// delete rows, correctly reports whether the cursor exists at all.
	const query = `
		WITH cas AS (
			UPDATE ingest_store SET value = $1 WHERE key = $2 AND value = $3 RETURNING 1
		)
		SELECT
			EXISTS(SELECT 1 FROM ingest_store WHERE key = $2) AS row_exists,
			(SELECT COUNT(*) FROM cas) AS updated_count
	`
	start := time.Now()
	var rowExists bool
	var updatedCount int
	err := dbTx.QueryRow(ctx, query, newValue, cursorName, expectedValue).Scan(&rowExists, &updatedCount)
	duration := time.Since(start).Seconds()
	m.Metrics.QueryDuration.WithLabelValues("CompareAndSwap", "ingest_store").Observe(duration)
	m.Metrics.QueriesTotal.WithLabelValues("CompareAndSwap", "ingest_store").Inc()
	if err != nil {
		m.Metrics.QueryErrors.WithLabelValues("CompareAndSwap", "ingest_store", utils.GetDBErrorType(err)).Inc()
		return false, fmt.Errorf("compare-and-swap for cursor %s: %w", cursorName, err)
	}
	if !rowExists {
		m.Metrics.QueryErrors.WithLabelValues("CompareAndSwap", "ingest_store", "cursor_missing").Inc()
		return false, fmt.Errorf("compare-and-swap for cursor %s: %w", cursorName, ErrCASCursorMissing)
	}
	return updatedCount == 1, nil
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

// GetLedgerGaps returns gaps between consecutive present ledger_number values within
// [startLedger, endLedger]. The window bounds the cost two ways: chunks whose recorded
// ledger_number range (enable_chunk_skipping on transactions.ledger_number; ranges are
// captured when a chunk is compressed) doesn't overlap the window are excluded at plan
// time, and any remaining out-of-window batches are dropped by the columnstore's batch
// min/max metadata without decompression.
//
// Edge semantics: callers must pass a startLedger that is itself a present ledger_number (e.g.
// the oldest ingested ledger) — the left edge is NOT synthesized. If startLedger fell strictly
// inside an already-open gap, this function would not report the portion before the first
// present row in-window; this mirrors the original unwindowed behavior, which never reported a
// gap before the very first row in the whole table. The right edge IS handled: COALESCE falls
// back to endLedger+1 when LEAD finds no next row within the window (the next present ledger
// lies beyond endLedger, or doesn't exist yet), so a gap still open at the window's boundary is
// reported clipped to endLedger instead of silently dropped (plain LEAD would return NULL there,
// failing the gap_start <= gap_end filter and losing the trailing partial gap).
func (m *IngestStoreModel) GetLedgerGaps(ctx context.Context, startLedger, endLedger uint32) ([]LedgerRange, error) {
	const query = `
		SELECT gap_start, gap_end FROM (
			SELECT
				ledger_number + 1 AS gap_start,
				COALESCE(LEAD(ledger_number) OVER (ORDER BY ledger_number), $2 + 1) - 1 AS gap_end
			FROM (SELECT DISTINCT ledger_number FROM transactions WHERE ledger_number BETWEEN $1 AND $2) t
		) gaps
		WHERE gap_start <= gap_end
		ORDER BY gap_start
	`
	start := time.Now()
	ledgerGaps, err := db.QueryMany[LedgerRange](ctx, m.DB, query, int32(startLedger), int32(endLedger))
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
