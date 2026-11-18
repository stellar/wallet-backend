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
	"golang.org/x/sync/errgroup"

	"github.com/stellar/wallet-backend/internal/data"
	"github.com/stellar/wallet-backend/internal/db"
	"github.com/stellar/wallet-backend/internal/metrics"
	"github.com/stellar/wallet-backend/internal/utils"
)

const sliceAttempts = 3

var (
	sliceLedgers uint32 = 10_000
	flushLedgers uint32 = 1_000
	retryWait           = 10 * time.Second
	// reindexWait is how long the last recompressed chunks wait for their index rebuild.
	reindexWait = 30 * time.Second
)

// errNotRecorded means the live ingester has not recorded the migration yet, so its
// boundary ledger is unknown.
var errNotRecorded = errors.New("data migration is not recorded yet: start the upgraded ingester first, then run it again")

// lockID is the advisory lock every data migration run takes. Changing the key stops
// excluding runs of older builds.
func lockID() int {
	h := fnv.New64a()
	h.Write([]byte("wallet-backend-data-migrate"))
	return int(h.Sum64())
}

// Runner walks one migration over the ledgers below its boundary.
//
// The walk is newest-first in slices, so recent history is rewritten first. A slice is
// split into Workers lanes, each rewritten on its own. A lane reads one bounded range
// through a fresh backend (the SDK backends accept a single PrepareRange) and applies its
// rows every flushLedgers ledgers to bound memory, reading the next batch while one is
// written. The cursor moves once the whole slice has committed, so an interrupted run
// redoes at most one slice. A compressed chunk is recompressed once the walk has passed
// it, while the walk goes on, so only the chunks of a few slices hold decompressed rows
// at a time.
//
// Live ingestion only writes ledgers from the boundary on and the walk only writes
// ledgers below it, so the two need no coordination.
type Runner struct {
	Deps
	// BackendFactory returns a new ledger backend for one bounded range.
	BackendFactory func(ctx context.Context) (ledgerbackend.LedgerBackend, error)
	Metrics        *metrics.DataMigrationMetrics
	// Workers is how many lanes rewrite a slice at once; below 1 means 1. Each lane
	// reads and writes its own part of the slice, so the load on the ledger source and
	// on the database grows with it.
	Workers int
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
		return errNotRecorded
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
		if err = r.checkSource(ctx, floor); err != nil {
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

	// Chunks the walk has passed are recompressed off the walk, so the next slice does
	// not wait for them.
	recompress := r.startRecompressor(ctx)
	defer recompress.stop()

	for cursor > floor {
		hi := cursor - 1
		lo := floor
		// A slice gives every lane sliceLedgers ledgers.
		if span := sliceLedgers * uint32(r.lanes()); cursor-floor > span {
			lo = cursor - span
		}

		start := time.Now()
		loClosedAt, err := r.runSlice(ctx, migration, lo, hi)
		if err != nil {
			return err
		}
		r.Metrics.Cursor.WithLabelValues(name).Set(float64(lo))
		log.Ctx(ctx).Infof("data migration %s: ledgers [%d, %d] done in %s", name, lo, hi, time.Since(start).Round(time.Millisecond))

		for len(compressed) > 0 && !compressed[0].RangeStart.Before(loClosedAt) {
			if err := recompress.add(compressed[0].Name); err != nil {
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
	// it is recompressed here. The migration is done once every chunk is recompressed.
	for _, c := range compressed {
		if err := recompress.add(c.Name); err != nil {
			return fmt.Errorf("recompressing the remaining chunks: %w", err)
		}
	}
	if err := recompress.finish(); err != nil {
		return fmt.Errorf("recompressing the remaining chunks: %w", err)
	}
	if err := r.Models.DataMigrations.MarkDone(ctx, name); err != nil {
		return fmt.Errorf("recording completion: %w", err)
	}
	log.Ctx(ctx).Infof("data migration %s: done down to ledger %d", name, cursor)
	return nil
}

// recompressor recompresses the chunks the walk has passed, one at a time, while the walk
// goes on. A chunk is only handed to it once the cursor is below the whole chunk, so no
// lane writes to it again.
type recompressor struct {
	// chunks is the backlog. Its capacity bounds how many passed chunks hold decompressed
	// rows: a full backlog holds the walk until a chunk is recompressed.
	chunks chan string
	cancel context.CancelFunc
	done   chan struct{}
	err    error // set before done closes
}

func (r *Runner) startRecompressor(ctx context.Context) *recompressor {
	ctx, cancel := context.WithCancel(ctx)
	rc := &recompressor{chunks: make(chan string, 4*r.lanes()), cancel: cancel, done: make(chan struct{})}
	go func() {
		defer close(rc.done)
		// A chunk's indexes are rebuilt after the next chunk is recompressed: a rebuild
		// still indexes rows deleted after the oldest snapshot anyone holds, and a
		// standby reports its snapshot only every few seconds, so a rebuild straight
		// after recompression keeps most of what it should drop.
		previous := ""
		for open := true; open; {
			var chunk string
			select {
			case <-ctx.Done():
				rc.err = ctx.Err()
				return
			case chunk, open = <-rc.chunks:
			}
			if !open {
				break
			}
			if err := data.CompressChunk(ctx, r.Models.DB, chunk); err != nil {
				rc.err = err
				return
			}
			if previous != "" {
				r.reindex(ctx, previous)
			}
			previous = chunk
		}
		if previous != "" {
			select {
			case <-ctx.Done():
				rc.err = ctx.Err()
				return
			case <-time.After(reindexWait):
			}
			r.reindex(ctx, previous)
		}
	}()
	return rc
}

// add queues a chunk, waiting while the backlog is full. It fails once a recompression
// has failed.
func (rc *recompressor) add(chunk string) error {
	select {
	case rc.chunks <- chunk:
		return nil
	case <-rc.done:
		return rc.err
	}
}

// finish waits for the backlog to drain and returns the first recompression error.
func (rc *recompressor) finish() error {
	close(rc.chunks)
	<-rc.done
	return rc.err
}

// stop abandons the backlog: a recompression in flight is cancelled. The chunks left
// partly decompressed are recompressed by the next run, which starts with every chunk
// above its cursor.
func (rc *recompressor) stop() {
	rc.cancel()
	<-rc.done
}

// reindex rebuilds the indexes of a chunk the walk rewrote and recompressed. The rewritten
// rows grew them and recompression does not shrink them. A rebuild that fails, for
// example because it cannot take its lock, is logged and skipped: it only costs disk.
func (r *Runner) reindex(ctx context.Context, chunk string) {
	if err := data.ReindexChunk(ctx, r.Models.DB, chunk); err != nil {
		log.Ctx(ctx).Warnf("data migration: %v; its indexes keep their grown size", err)
	}
}

// lanes is the number of lanes a slice is split into.
func (r *Runner) lanes() int {
	return max(r.Workers, 1)
}

// runSlice rewrites ledgers [lo, hi] and moves the cursor to lo, trying up to
// sliceAttempts times. After the last failure it records the error on the migration's
// row. It returns lo's close time, the lowest ledger_created_at written.
func (r *Runner) runSlice(ctx context.Context, migration Migration, lo, hi uint32) (time.Time, error) {
	name := migration.Name()
	var err error
	for attempt := 1; attempt <= sliceAttempts; attempt++ {
		var loClosedAt time.Time
		loClosedAt, err = r.rewriteSlice(ctx, migration, lo, hi)
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

// rewriteSlice splits ledgers [lo, hi] into contiguous lanes, rewrites the lanes at the
// same time, and moves the cursor to lo once every lane has committed. A lane that fails
// stops the others; the cursor stays, so the next attempt redoes the whole slice. It
// returns lo's close time.
func (r *Runner) rewriteSlice(ctx context.Context, migration Migration, lo, hi uint32) (time.Time, error) {
	apply := func(rows []any) error {
		return db.RunInTransaction(ctx, r.Models.DB, func(dbTx pgx.Tx) error {
			if err := migration.Apply(ctx, r.Deps, dbTx, rows); err != nil {
				return fmt.Errorf("applying rows: %w", err)
			}
			return nil
		})
	}

	// cursor_ledger is an INTEGER column, so ledger numbers here stay below 2^31 and
	// this arithmetic cannot overflow uint32.
	ledgers := hi - lo + 1
	lanes := min(uint32(r.lanes()), ledgers)
	perLane := (ledgers + lanes - 1) / lanes

	var loClosedAt time.Time
	group, groupCtx := errgroup.WithContext(ctx)
	for laneLo := lo; laneLo <= hi; laneLo += perLane {
		from, to := laneLo, min(laneLo+perLane-1, hi)
		group.Go(func() error {
			closedAt, err := r.readLedgers(groupCtx, migration, from, to, apply)
			if from == lo {
				loClosedAt = closedAt
			}
			return err
		})
	}
	if err := group.Wait(); err != nil {
		return time.Time{}, err //nolint:wrapcheck // readLedgers names the lane's range
	}

	if err := r.Models.DataMigrations.Advance(ctx, migration.Name(), lo); err != nil {
		return time.Time{}, fmt.Errorf("moving the cursor: %w", err)
	}
	return loClosedAt, nil
}

// checkSource reads one ledger through a fresh backend, to prove the source can serve it.
func (r *Runner) checkSource(ctx context.Context, seq uint32) error {
	backend, err := r.BackendFactory(ctx)
	if err != nil {
		return fmt.Errorf("creating ledger backend: %w", err)
	}
	defer utils.DeferredClose(ctx, backend, "closing data migration ledger backend")
	if err := backend.PrepareRange(ctx, ledgerbackend.BoundedRange(seq, seq)); err != nil {
		return fmt.Errorf("preparing ledger range: %w", err)
	}
	if _, err := backend.GetLedger(ctx, seq); err != nil {
		return fmt.Errorf("fetching ledger %d: %w", seq, err)
	}
	return nil
}

// readLedgers reads ledgers [lo, hi] through a fresh bounded backend and hands the
// extracted rows to flush every flushLedgers ledgers. It returns lo's close time.
//
// Reading and writing overlap: while flush writes one batch, the next is read and
// extracted. flush calls never overlap each other, so the database still sees one
// writer. At most three batches are in memory: one being written, one waiting, one
// being read.
func (r *Runner) readLedgers(ctx context.Context, migration Migration, lo, hi uint32, flush func(rows []any) error) (time.Time, error) {
	backend, err := r.BackendFactory(ctx)
	if err != nil {
		return time.Time{}, fmt.Errorf("creating ledger backend: %w", err)
	}
	defer utils.DeferredClose(ctx, backend, "closing data migration ledger backend")
	if err := backend.PrepareRange(ctx, ledgerbackend.BoundedRange(lo, hi)); err != nil {
		return time.Time{}, fmt.Errorf("preparing ledger range: %w", err)
	}

	type batch struct {
		rows     []any
		from, to uint32
	}
	batches := make(chan batch, 1)
	group, groupCtx := errgroup.WithContext(ctx)

	var loClosedAt time.Time
	group.Go(func() error {
		defer close(batches)
		var rows []any
		flushFrom := lo
		for seq := lo; seq <= hi; seq++ {
			meta, err := backend.GetLedger(groupCtx, seq)
			if err != nil {
				return fmt.Errorf("fetching ledger %d: %w", seq, err)
			}
			if seq == lo {
				loClosedAt = meta.ClosedAt()
			}
			extracted, err := migration.Extract(groupCtx, r.Deps, meta)
			if err != nil {
				return fmt.Errorf("extracting ledger %d: %w", seq, err)
			}
			rows = append(rows, extracted...)

			if seq == hi || seq-flushFrom+1 >= flushLedgers {
				select {
				case batches <- batch{rows: rows, from: flushFrom, to: seq}:
				case <-groupCtx.Done():
					return groupCtx.Err()
				}
				rows = nil
				flushFrom = seq + 1
			}
		}
		return nil
	})
	group.Go(func() error {
		for b := range batches {
			// A failed read ends the slice; nothing more of it is written.
			if groupCtx.Err() != nil {
				return groupCtx.Err()
			}
			if err := flush(b.rows); err != nil {
				return fmt.Errorf("writing ledgers [%d, %d]: %w", b.from, b.to, err)
			}
		}
		return nil
	})
	if err := group.Wait(); err != nil {
		return time.Time{}, fmt.Errorf("reading and writing ledgers [%d, %d]: %w", lo, hi, err)
	}
	return loClosedAt, nil
}
