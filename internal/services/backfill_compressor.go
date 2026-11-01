package services

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stellar/go-stellar-sdk/support/log"
	"golang.org/x/sync/errgroup"

	"github.com/stellar/wallet-backend/internal/data"
	"github.com/stellar/wallet-backend/internal/db"
	"github.com/stellar/wallet-backend/internal/metrics"
)

// lookupRecompressRows is how many transaction_hashes rows the run writes
// between two recompressions of the lookup table. The table is partitioned
// by hash, so a window's lookup rows land in the rowstore part of every one
// of its chunks and only a recompression folds them into the columnstore.
// 500 million rows are about 45 GB of rowstore with the index at the measured
// 90 bytes per row, the fill script's pass size; counting rows rather than
// windows keeps that bound at any transaction rate.
const lookupRecompressRows int64 = 500_000_000

// lookupMaintenanceCall runs transaction_hashes_maintenance in
// recompression-only mode (job id 0) over one share of the lookup table's
// chunks: those whose prefix range index modulo the share count is the share.
// The procedure commits per chunk, so the call runs through the simple
// protocol outside any transaction.
const lookupMaintenanceCall = `CALL transaction_hashes_maintenance(0, $1)`

// sqlstateDuplicateObject is what compress_chunk raises for a chunk that is
// already compressed. The live policy and a rerun can both reach a chunk the
// backfill also compresses, so the collision is expected and harmless.
const sqlstateDuplicateObject = "42710"

// backfillCompressor compresses the chunks the backfill has finished writing.
// It owns a fixed number of connections, each running one compress_chunk at
// a time, and a window budget: the backfill may leave at most
// maxWindowsInFlight windows uncompressed before it must wait, which bounds
// the uncompressed disk a run can hold.
//
// Only chunks that end at or before the run's top close time are compressed
// here. The chunk that also holds live rows, and any gap filled above the
// cursor, stay with the live policies, which resume when the run ends.
type backfillCompressor struct {
	pool    db.Querier
	metrics *metrics.IngestionMetrics
	// runTop is the close time of the first live-owned ledger: chunks ending
	// after it hold live rows and are left alone.
	runTop time.Time

	jobs  chan compressJob
	group *errgroup.Group
	gctx  context.Context
	// workers is the connection count; a lookup recompression is split into
	// that many share jobs, one per connection.
	workers int

	mu       sync.Mutex
	enqueued map[string]struct{}
	// lookupRows counts the transaction_hashes rows written since the last
	// recompression was queued; lookupPending counts the share jobs of a
	// recompression still queued or running, so a second one is not queued on
	// top of it.
	lookupRows    int64
	lookupPending int
	// slots bounds windows in flight: acquired before a window fills, released
	// when its chunks are compressed (or at close for a window this
	// compressor does not own).
	slots chan struct{}
}

type compressJob struct {
	chunk string
	// lookup marks a job that recompresses share worker of transaction_hashes
	// instead of compressing one chunk.
	lookup bool
	worker int
	// done is called when the job finishes, successfully or not; windows use
	// it to release their slot once every chunk is compressed.
	done func()
}

// maxWindowsInFlight derives the uncompressed-window budget from the
// compressor's connection count: one window's chunks can occupy up to five
// connections, so K connections keep ceil(K/5) windows compressing while one
// more fills.
func maxWindowsInFlight(concurrency int) int {
	return (concurrency+len(data.BulkCopyTableNames())-1)/len(data.BulkCopyTableNames()) + 1
}

// newBackfillCompressor starts concurrency workers that drain the job queue.
// Errors other than the already-compressed collision fail the run.
func newBackfillCompressor(ctx context.Context, pool db.Querier, ingestionMetrics *metrics.IngestionMetrics, concurrency int, runTop time.Time) (*backfillCompressor, context.Context) {
	group, gctx := errgroup.WithContext(ctx)
	c := &backfillCompressor{
		pool:     pool,
		metrics:  ingestionMetrics,
		runTop:   runTop,
		jobs:     make(chan compressJob, 4*concurrency),
		group:    group,
		gctx:     gctx,
		workers:  concurrency,
		enqueued: make(map[string]struct{}),
		slots:    make(chan struct{}, maxWindowsInFlight(concurrency)),
	}
	for range concurrency {
		group.Go(func() error { return c.worker(gctx) })
	}
	return c, gctx
}

func (c *backfillCompressor) worker(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("compressor cancelled: %w", ctx.Err())
		case job, ok := <-c.jobs:
			if !ok {
				return nil
			}
			var err error
			if job.lookup {
				err = c.recompressLookupTable(ctx, job.worker)
			} else {
				err = c.compress(ctx, job.chunk)
			}
			if job.done != nil {
				job.done()
			}
			if err != nil {
				return err
			}
		}
	}
}

func (c *backfillCompressor) compress(ctx context.Context, chunk string) error {
	start := time.Now()
	_, err := c.pool.Exec(ctx, `SELECT compress_chunk($1::regclass)`, chunk)
	c.metrics.PhaseDuration.WithLabelValues("compress").Observe(time.Since(start).Seconds())
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == sqlstateDuplicateObject {
			log.Ctx(ctx).Debugf("chunk %s is already compressed", chunk)
			return nil
		}
		if ctx.Err() == nil {
			c.metrics.ErrorsTotal.WithLabelValues("compress").Inc()
		}
		return fmt.Errorf("compressing chunk %s: %w", chunk, err)
	}
	log.Ctx(ctx).Infof("Compressed chunk %s in %s", chunk, time.Since(start).Round(time.Millisecond))
	return nil
}

// recompressLookupTable runs the lookup table's maintenance procedure over
// share worker of its chunks, which folds the lookup rows written since the
// last pass from each chunk's rowstore part into its columnstore.
func (c *backfillCompressor) recompressLookupTable(ctx context.Context, worker int) error {
	start := time.Now()
	config := fmt.Sprintf(`{"workers": %d, "worker": %d}`, c.workers, worker)
	_, err := c.pool.Exec(ctx, lookupMaintenanceCall, pgx.QueryExecModeSimpleProtocol, config)
	c.metrics.PhaseDuration.WithLabelValues("lookup_recompress").Observe(time.Since(start).Seconds())
	if err != nil {
		if ctx.Err() == nil {
			c.metrics.ErrorsTotal.WithLabelValues("compress").Inc()
		}
		return fmt.Errorf("recompressing share %d of %d of %s: %w", worker, c.workers, data.TransactionHashesTable, err)
	}
	log.Ctx(ctx).Infof("Recompressed share %d of %d of %s in %s", worker, c.workers, data.TransactionHashesTable, time.Since(start).Round(time.Second))
	return nil
}

// AcquireSlot blocks until the backfill may fill another window.
func (c *backfillCompressor) AcquireSlot(ctx context.Context) error {
	select {
	case c.slots <- struct{}{}:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("waiting for a window slot: %w", ctx.Err())
	}
}

func (c *backfillCompressor) releaseSlot() { <-c.slots }

// Owns reports whether this compressor compresses the window's chunks: a
// window entirely below the run's top close time, which the live ingester
// never writes to.
func (c *backfillCompressor) Owns(w backfillWindow) bool {
	return w.BelowCursor && !w.RangeEnd.After(c.runTop)
}

// WindowClosed is called once a window's rows are committed (plain) or
// attached. For a window this compressor owns it queues the window's chunks
// and releases the slot when the last one is compressed; otherwise it
// releases the slot at once.
func (c *backfillCompressor) WindowClosed(ctx context.Context, w backfillWindow) error {
	if !c.Owns(w) {
		c.releaseSlot()
		return nil
	}
	chunks, err := c.windowChunks(ctx, w)
	if err != nil {
		c.releaseSlot()
		return err
	}
	if len(chunks) == 0 {
		c.releaseSlot()
		return nil
	}
	remaining := len(chunks)
	var mu sync.Mutex
	done := func() {
		mu.Lock()
		defer mu.Unlock()
		remaining--
		if remaining == 0 {
			c.releaseSlot()
		}
	}
	for _, chunk := range chunks {
		if err := c.enqueue(ctx, compressJob{chunk: chunk, done: done}); err != nil {
			return err
		}
	}
	return nil
}

// windowChunks lists the bulk-table chunks whose range is exactly the
// window's range. Every chunk window is one chunk per table by construction.
func (c *backfillCompressor) windowChunks(ctx context.Context, w backfillWindow) ([]string, error) {
	rows, err := c.pool.Query(ctx,
		`SELECT chunk_schema || '.' || chunk_name
		   FROM timescaledb_information.chunks
		  WHERE hypertable_name = ANY($1) AND range_start = $2 AND range_end = $3`,
		data.BulkCopyTableNames(), w.RangeStart, w.RangeEnd)
	if err != nil {
		return nil, fmt.Errorf("listing chunks of %s: %w", w, err)
	}
	defer rows.Close()
	var chunks []string
	for rows.Next() {
		var chunk string
		if err := rows.Scan(&chunk); err != nil {
			return nil, fmt.Errorf("scanning chunk of %s: %w", w, err)
		}
		chunks = append(chunks, chunk)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("listing chunks of %s: %w", w, err)
	}
	return chunks, nil
}

// LookupRows adds the transaction_hashes rows a window wrote and queues a
// recompression of the lookup table, one share job per connection, once
// lookupRecompressRows are pending. A recompression still queued or running
// is left to finish; the rows keep counting and the next window queues the
// following one.
func (c *backfillCompressor) LookupRows(ctx context.Context, rows int64) error {
	c.mu.Lock()
	c.lookupRows += rows
	if c.lookupRows < lookupRecompressRows || c.lookupPending > 0 {
		c.mu.Unlock()
		return nil
	}
	c.lookupRows = 0
	c.lookupPending += c.workers
	c.mu.Unlock()
	return c.queueLookupRecompress(ctx)
}

// queueLookupRecompress queues one share job per connection. The caller has
// added the share count to lookupPending under the lock; each job's done
// takes one off, as does a share not queued because the context ended.
func (c *backfillCompressor) queueLookupRecompress(ctx context.Context) error {
	done := func() {
		c.mu.Lock()
		c.lookupPending--
		c.mu.Unlock()
	}
	for worker := range c.workers {
		select {
		case c.jobs <- compressJob{lookup: true, worker: worker, done: done}:
		case <-ctx.Done():
			for ; worker < c.workers; worker++ {
				done()
			}
			return fmt.Errorf("queueing recompression of %s: %w", data.TransactionHashesTable, ctx.Err())
		}
	}
	return nil
}

func (c *backfillCompressor) enqueue(ctx context.Context, job compressJob) error {
	c.mu.Lock()
	if _, seen := c.enqueued[job.chunk]; seen {
		c.mu.Unlock()
		if job.done != nil {
			job.done()
		}
		return nil
	}
	c.enqueued[job.chunk] = struct{}{}
	c.mu.Unlock()
	select {
	case c.jobs <- job:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("queueing compression of %s: %w", job.chunk, ctx.Err())
	}
}

// Close queues a last recompression of transaction_hashes when windows have
// closed since the previous one, stops accepting jobs, waits for the queued
// ones, and returns the first worker error.
func (c *backfillCompressor) Close() error {
	c.mu.Lock()
	final := c.lookupRows > 0
	c.lookupRows = 0
	if final {
		c.lookupPending += c.workers
	}
	c.mu.Unlock()
	if final {
		if err := c.queueLookupRecompress(c.gctx); err != nil {
			log.Ctx(c.gctx).Warnf("Skipping the final recompression of %s: %v", data.TransactionHashesTable, err)
		}
	}
	close(c.jobs)
	return c.group.Wait() //nolint:wrapcheck // worker errors are already wrapped
}

// pauseCompressionPolicies turns off scheduling of every columnstore policy
// and of the lookup table's maintenance jobs, and returns the job ids it
// paused. A scheduled policy compresses any chunk older than compress_after,
// which is every chunk a backfill writes, so it would compress half-filled
// windows under the run; a maintenance job would recompress chunks the
// compressor is recompressing. Live mode schedules all of them again at every
// start, so a crash here leaves nothing to repair.
func pauseCompressionPolicies(ctx context.Context, pool db.Querier) ([]int, error) {
	rows, err := pool.Query(ctx,
		`SELECT job_id FROM timescaledb_information.jobs
		  WHERE proc_name IN ('policy_compression', 'transaction_hashes_maintenance') AND scheduled`)
	if err != nil {
		return nil, fmt.Errorf("listing compression policies: %w", err)
	}
	defer rows.Close()
	var jobIDs []int
	for rows.Next() {
		var id int
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("scanning compression policy: %w", err)
		}
		jobIDs = append(jobIDs, id)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("listing compression policies: %w", err)
	}
	for _, id := range jobIDs {
		if _, err := pool.Exec(ctx, `SELECT alter_job($1, scheduled => false)`, id); err != nil {
			return jobIDs, fmt.Errorf("pausing compression policy %d: %w", id, err)
		}
	}
	return jobIDs, nil
}

// resumeCompressionPolicies schedules the given policies again.
func resumeCompressionPolicies(ctx context.Context, pool db.Querier, jobIDs []int) error {
	var errs []error
	for _, id := range jobIDs {
		if _, err := pool.Exec(ctx, `SELECT alter_job($1, scheduled => true)`, id); err != nil {
			errs = append(errs, fmt.Errorf("resuming compression policy %d: %w", id, err))
		}
	}
	return errors.Join(errs...)
}
