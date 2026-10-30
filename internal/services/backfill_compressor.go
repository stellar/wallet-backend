package services

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stellar/go-stellar-sdk/support/log"
	"golang.org/x/sync/errgroup"

	"github.com/stellar/wallet-backend/internal/data"
	"github.com/stellar/wallet-backend/internal/db"
	"github.com/stellar/wallet-backend/internal/metrics"
)

// transactionHashesTable is the lookup hypertable the backfill writes next to
// transactions. It is partitioned on an integer close epoch, so its chunks are
// selected through range_start_integer rather than range_start.
const transactionHashesTable = "transaction_hashes"

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

	mu       sync.Mutex
	enqueued map[string]struct{}
	// slots bounds windows in flight: acquired before a window fills, released
	// when its chunks are compressed (or at close for a window this
	// compressor does not own).
	slots chan struct{}
}

type compressJob struct {
	chunk string
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
			err := c.compress(ctx, job.chunk)
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

// EnqueueLookupChunks queues every uncompressed transaction_hashes chunk that
// the run has finished writing: its range starts at or after the fill
// frontier (the oldest closed window's start) and ends at or before the run's
// top. A lookup chunk spans several windows, so this runs after each window
// closes and queues a chunk once.
func (c *backfillCompressor) EnqueueLookupChunks(ctx context.Context, frontier time.Time) error {
	rows, err := c.pool.Query(ctx,
		`SELECT chunk_schema || '.' || chunk_name
		   FROM timescaledb_information.chunks
		  WHERE hypertable_name = $1 AND NOT is_compressed
		    AND range_start_integer >= $2 AND range_end_integer <= $3`,
		transactionHashesTable, frontier.Unix(), c.runTop.Unix())
	if err != nil {
		return fmt.Errorf("listing %s chunks: %w", transactionHashesTable, err)
	}
	defer rows.Close()
	var chunks []string
	for rows.Next() {
		var chunk string
		if err := rows.Scan(&chunk); err != nil {
			return fmt.Errorf("scanning %s chunk: %w", transactionHashesTable, err)
		}
		chunks = append(chunks, chunk)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("listing %s chunks: %w", transactionHashesTable, err)
	}
	for _, chunk := range chunks {
		if err := c.enqueue(ctx, compressJob{chunk: chunk}); err != nil {
			return err
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

// Close stops accepting jobs, waits for the queued ones, and returns the
// first worker error.
func (c *backfillCompressor) Close() error {
	close(c.jobs)
	return c.group.Wait() //nolint:wrapcheck // worker errors are already wrapped
}

// pauseCompressionPolicies turns off scheduling of every columnstore policy
// and returns the job ids it paused. A scheduled policy compresses any chunk
// older than compress_after, which is every chunk a backfill writes, so it
// would compress half-filled windows under the run. Live mode schedules the
// policies again at every start, so a crash here leaves nothing to repair.
func pauseCompressionPolicies(ctx context.Context, pool db.Querier) ([]int, error) {
	rows, err := pool.Query(ctx,
		`SELECT job_id FROM timescaledb_information.jobs WHERE proc_name = 'policy_compression' AND scheduled`)
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
