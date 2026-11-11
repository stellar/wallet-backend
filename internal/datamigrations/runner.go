package datamigrations

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stellar/go-stellar-sdk/ingest/ledgerbackend"
	"github.com/stellar/go-stellar-sdk/support/log"

	"github.com/stellar/wallet-backend/internal/data"
	"github.com/stellar/wallet-backend/internal/db"
	"github.com/stellar/wallet-backend/internal/metrics"
	"github.com/stellar/wallet-backend/internal/utils"
)

var (
	sliceLedgers  uint32 = 10_000
	flushLedgers  uint32 = 1_000
	sliceAttempts        = 3
	retryWait            = 10 * time.Second
)

// ErrNotRecorded means the live ingester has not recorded the migration yet, so its
// boundary ledger is unknown.
var ErrNotRecorded = errors.New("data migration is not recorded yet: start the upgraded ingester first, then run it again")

// lockID is the advisory lock every data migration run takes. Changing the key stops
// excluding runs of older builds.
func lockID() int {
	h := fnv.New64a()
	h.Write([]byte("wallet-backend-data-migrate"))
	return int(h.Sum64())
}

// Runner walks one migration over the ledgers below its boundary.
//
// The walk is newest-first in slices, so recent history is rewritten first. Each slice
// reads one bounded range through a fresh backend (the SDK backends accept a single
// PrepareRange), applies its rows every flushLedgers ledgers to bound memory, and moves
// the cursor in the transaction of the slice's last apply. An interrupted run therefore
// redoes at most one slice. A compressed chunk is recompressed once the walk has passed
// it, so at most about one chunk per table holds decompressed rows at a time.
//
// Live ingestion only writes ledgers from the boundary on and the walk only writes
// ledgers below it, so the two need no coordination.
type Runner struct {
	Deps
	// BackendFactory returns a new ledger backend for one bounded range.
	BackendFactory func(ctx context.Context) (ledgerbackend.LedgerBackend, error)
	Metrics        *metrics.DataMigrationMetrics
}

// Run walks the named migration to completion. It returns nil when the migration is done
// (including when it already was) and an error when a slice kept failing or ctx was
// cancelled; in both error cases the next Run resumes from the recorded cursor.
func (r *Runner) Run(ctx context.Context, name string) error {
	migration, ok := Find(name)
	if !ok {
		return fmt.Errorf("unknown data migration %q", name)
	}

	// One data migration at a time, whatever its name. The lock lives as long as this
	// connection's session, which is closed on return.
	conn, err := r.Models.DB.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("acquiring the lock connection: %w", err)
	}
	defer func() {
		closeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
		defer cancel()
		if closeErr := conn.Hijack().Close(closeCtx); closeErr != nil {
			log.Ctx(ctx).Warnf("closing the data migration lock connection: %v", closeErr)
		}
	}()
	locked, err := db.AcquireAdvisoryLock(ctx, conn, lockID())
	if err != nil {
		return fmt.Errorf("acquiring the data migration lock: %w", err)
	}
	if !locked {
		return errors.New("another data migration is running")
	}

	row, err := r.Models.DataMigrations.Get(ctx, name)
	if err != nil {
		return fmt.Errorf("loading the migration's row: %w", err)
	}
	if row == nil {
		return ErrNotRecorded
	}
	if row.Status == data.DataMigrationDone {
		log.Ctx(ctx).Infof("data migration %s is already done", name)
		return nil
	}

	floor, err := r.Models.IngestStore.Get(ctx, data.OldestLedgerCursorName)
	if err != nil {
		return fmt.Errorf("reading oldest ingested ledger: %w", err)
	}
	cursor := row.CursorLedger
	if cursor > floor {
		// The oldest ledger is the hardest for the source to serve; without it the walk
		// could not finish.
		if _, err = r.readLedgers(ctx, migration, floor, floor, nil); err != nil {
			err = fmt.Errorf("the ledger backend cannot serve the oldest ingested ledger %d (history older than the RPC retention window needs --ledger-backend-type=datastore): %w", floor, err)
			if setErr := r.Models.DataMigrations.SetError(ctx, name, err.Error()); setErr != nil {
				log.Ctx(ctx).Errorf("%v", setErr)
			}
			return err
		}
	}
	if err = r.Models.DataMigrations.Start(ctx, name); err != nil {
		return fmt.Errorf("marking the run in progress: %w", err)
	}
	r.Metrics.Cursor.WithLabelValues(name).Set(float64(cursor))

	// Chunks compressed before the walk; each is recompressed once the walk has passed its
	// start. The live chunk is never compressed, so it is never in this list.
	compressed, err := data.ListCompressedChunks(ctx, r.Models.DB, migration.Tables())
	if err != nil {
		return fmt.Errorf("listing chunks to recompress: %w", err)
	}
	log.Ctx(ctx).Infof("data migration %s: rewriting ledgers [%d, %d] newest-first, %d compressed chunks to recompress", name, floor, cursor-1, len(compressed))

	for cursor > floor {
		hi := cursor - 1
		lo := floor
		if cursor-floor > sliceLedgers {
			lo = cursor - sliceLedgers
		}

		start := time.Now()
		loClosedAt, err := r.runSlice(ctx, migration, lo, hi)
		if err != nil {
			return err
		}
		r.Metrics.Cursor.WithLabelValues(name).Set(float64(lo))
		log.Ctx(ctx).Infof("data migration %s: ledgers [%d, %d] done in %s", name, lo, hi, time.Since(start).Round(time.Millisecond))

		for len(compressed) > 0 && !compressed[0].RangeStart.Before(loClosedAt) {
			if err := data.CompressChunk(ctx, r.Models.DB, compressed[0].Name); err != nil {
				return fmt.Errorf("recompressing a chunk the walk has passed: %w", err)
			}
			compressed = compressed[1:]
		}

		cursor = lo
		// Retention raises the floor and a gap backfill lowers it while the walk runs.
		floor, err = r.Models.IngestStore.Get(ctx, data.OldestLedgerCursorName)
		if err != nil {
			return fmt.Errorf("reading oldest ingested ledger: %w", err)
		}
	}

	// The chunk holding the oldest ledger starts before any slice's lowest close time, so
	// it is recompressed here.
	for _, c := range compressed {
		if err := data.CompressChunk(ctx, r.Models.DB, c.Name); err != nil {
			return fmt.Errorf("recompressing the remaining chunks: %w", err)
		}
	}
	if err := r.Models.DataMigrations.MarkDone(ctx, name); err != nil {
		return fmt.Errorf("recording completion: %w", err)
	}
	log.Ctx(ctx).Infof("data migration %s: done down to ledger %d", name, cursor)
	return nil
}

// runSlice rewrites ledgers [lo, hi] and moves the cursor to lo, trying up to
// sliceAttempts times. After the last failure it records the error on the migration's
// row. It returns lo's close time, the lowest ledger_created_at written.
func (r *Runner) runSlice(ctx context.Context, migration Migration, lo, hi uint32) (time.Time, error) {
	name := migration.Name()
	var err error
	for attempt := 1; attempt <= sliceAttempts; attempt++ {
		var loClosedAt time.Time
		loClosedAt, err = r.readLedgers(ctx, migration, lo, hi, func(rows []any, last bool) error {
			return db.RunInTransaction(ctx, r.Models.DB, func(dbTx pgx.Tx) error {
				if applyErr := migration.Apply(ctx, r.Deps, dbTx, rows); applyErr != nil {
					return fmt.Errorf("applying rows: %w", applyErr)
				}
				if !last {
					return nil
				}
				return r.Models.DataMigrations.Advance(ctx, dbTx, name, lo)
			})
		})
		if err == nil {
			return loClosedAt, nil
		}
		if ctx.Err() != nil {
			return time.Time{}, fmt.Errorf("data migration %s interrupted at ledgers [%d, %d]: %w", name, lo, hi, ctx.Err())
		}
		r.Metrics.ErrorsTotal.WithLabelValues(name).Inc()
		log.Ctx(ctx).Errorf("data migration %s: ledgers [%d, %d], attempt %d of %d: %v", name, lo, hi, attempt, sliceAttempts, err)
		if attempt < sliceAttempts {
			select {
			case <-ctx.Done():
				return time.Time{}, fmt.Errorf("data migration %s interrupted at ledgers [%d, %d]: %w", name, lo, hi, ctx.Err())
			case <-time.After(retryWait):
			}
		}
	}
	err = fmt.Errorf("data migration %s: ledgers [%d, %d] failed %d times: %w", name, lo, hi, sliceAttempts, err)
	if setErr := r.Models.DataMigrations.SetError(ctx, name, err.Error()); setErr != nil {
		log.Ctx(ctx).Errorf("%v", setErr)
	}
	return time.Time{}, err
}

// readLedgers reads ledgers [lo, hi] through a fresh bounded backend and hands the
// extracted rows to flush every flushLedgers ledgers; last is true on the final call. A
// nil flush only checks that the range can be read. It returns lo's close time.
func (r *Runner) readLedgers(ctx context.Context, migration Migration, lo, hi uint32, flush func(rows []any, last bool) error) (time.Time, error) {
	backend, err := r.BackendFactory(ctx)
	if err != nil {
		return time.Time{}, fmt.Errorf("creating ledger backend: %w", err)
	}
	defer utils.DeferredClose(ctx, backend, "closing data migration ledger backend")
	if err := backend.PrepareRange(ctx, ledgerbackend.BoundedRange(lo, hi)); err != nil {
		return time.Time{}, fmt.Errorf("preparing ledger range: %w", err)
	}

	var (
		loClosedAt time.Time
		rows       []any
		flushFrom  = lo
	)
	// seq stops at hi by an explicit break: seq <= hi never ends when hi is MaxUint32.
	for seq := lo; ; seq++ {
		meta, err := backend.GetLedger(ctx, seq)
		if err != nil {
			return time.Time{}, fmt.Errorf("fetching ledger %d: %w", seq, err)
		}
		if seq == lo {
			loClosedAt = meta.ClosedAt()
		}
		if flush == nil {
			break
		}
		extracted, err := migration.Extract(ctx, r.Deps, meta)
		if err != nil {
			return time.Time{}, fmt.Errorf("extracting ledger %d: %w", seq, err)
		}
		rows = append(rows, extracted...)

		last := seq == hi
		if last || seq-flushFrom+1 >= flushLedgers {
			if err := flush(rows, last); err != nil {
				return time.Time{}, fmt.Errorf("writing ledgers [%d, %d]: %w", flushFrom, seq, err)
			}
			rows = rows[:0]
			flushFrom = seq + 1
		}
		if last {
			break
		}
	}
	return loClosedAt, nil
}
