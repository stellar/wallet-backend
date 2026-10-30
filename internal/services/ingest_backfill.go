package services

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stellar/go-stellar-sdk/ingest/ledgerbackend"
	"github.com/stellar/go-stellar-sdk/support/log"
	"github.com/stellar/go-stellar-sdk/xdr"
	"golang.org/x/sync/errgroup"

	"github.com/stellar/wallet-backend/internal/data"
	"github.com/stellar/wallet-backend/internal/indexer"
	"github.com/stellar/wallet-backend/internal/utils"
)

const (
	// maxWindowAttempts bounds how often a window is redone after a transient
	// failure. The window is the unit of retry: its rows commit per flush on
	// five connections, so a failed flush cannot be replayed on its own
	// without duplicating what the other connections already committed. A
	// rerun of the window deletes or drops what the failed attempt left.
	maxWindowAttempts     = 3
	maxWindowRetryBackoff = 30 * time.Second

	// decodedLedgerBytes is the working-set estimate for one decoded pubnet
	// ledger, used only to tell the operator what the configured buffers
	// cost. Measured on 2026-10 pubnet ledgers.
	decodedLedgerBytes = 30 << 20
	// uncompressedWindowBytes is the disk one uncompressed day of pubnet rows
	// takes across the bulk tables after its indexes are built, for the same
	// purpose.
	uncompressedWindowBytes = 15 << 30
)

// backfillConnections is the pool floor for a backfill: one connection per
// bulk table for the flush, the compressor's connections, and one for the
// cursor, catalog reads and the advisory lock.
func backfillConnections(compressConcurrency int) int {
	return len(data.BulkCopyTableNames()) + compressConcurrency + 1
}

// generateBackfillAdvisoryLockID derives the backfill advisory lock from the
// network passphrase, distinct from the live lock so a backfill never blocks
// the live ingester and two backfills never share a cursor.
func generateBackfillAdvisoryLockID(networkPassphrase string) int {
	return advisoryLockID("wallet-backend-backfill-" + networkPassphrase)
}

// startBackfilling makes [startLedger, latest_ingest_ledger] contiguous: it
// fills any hole above oldest_ingest_ledger, then extends history below the
// cursor one chunk window at a time, newest first, lowering the cursor as
// each window is committed. The cursor is exact: everything from it to the
// tip is present. Any failure ends the run with an error; a rerun resumes
// from the cursor and the remaining gaps.
func (m *ingestService) startBackfilling(ctx context.Context, startLedger uint32) error {
	if startLedger == 0 {
		return fmt.Errorf("start ledger must be positive")
	}
	pool := m.models.DB
	if floor := backfillConnections(m.backfillCompressConcurrency); int(pool.Config().MaxConns) < floor {
		return fmt.Errorf("db-max-conns is %d, below the %d connections a backfill with compress concurrency %d requires", pool.Config().MaxConns, floor, m.backfillCompressConcurrency)
	}

	// The lock lives on one connection for the whole run.
	lockConn, err := pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("acquiring lock connection: %w", err)
	}
	defer lockConn.Release()
	var locked bool
	if err := lockConn.QueryRow(ctx, `SELECT pg_try_advisory_lock($1)`, generateBackfillAdvisoryLockID(m.networkPassphrase)).Scan(&locked); err != nil {
		return fmt.Errorf("acquiring backfill advisory lock: %w", err)
	}
	if !locked {
		return fmt.Errorf("another backfill holds the lock for this network")
	}

	cursors, err := m.models.IngestStore.GetMany(ctx, []string{data.OldestLedgerCursorName, data.LatestLedgerCursorName})
	if err != nil {
		return fmt.Errorf("reading cursors: %w", err)
	}
	oldest, latest := cursors[data.OldestLedgerCursorName], cursors[data.LatestLedgerCursorName]
	if oldest == 0 || latest == 0 {
		return fmt.Errorf("no ingestion cursors yet: run live ingestion once before backfilling")
	}
	if startLedger >= oldest {
		log.Ctx(ctx).Infof("Start ledger %d is at or above the oldest ingested ledger %d; only gaps above the cursor will be filled", startLedger, oldest)
	}

	interval, err := readBulkChunkInterval(ctx, pool)
	if err != nil {
		return err
	}
	if orphans, err := m.models.IngestStore.DropOrphanStagingTables(ctx); err != nil {
		return fmt.Errorf("dropping staging tables left by an earlier run: %w", err)
	} else if len(orphans) > 0 {
		log.Ctx(ctx).Infof("Dropped %d staging tables left by an earlier run: %v", len(orphans), orphans)
	}
	probe, closeProbe, err := newLedgerCloseTimeProbe(ctx, m.ledgerBackendFactory)
	if err != nil {
		return err
	}
	defer closeProbe()

	// The cursor ledger is the first live-owned ledger; its close time is the
	// top of the region this run compresses.
	runTop, err := probe(ctx, oldest)
	if err != nil {
		return fmt.Errorf("placing the cursor ledger %d: %w", oldest, err)
	}
	if startLedger < oldest {
		startClose, err := probe(ctx, startLedger)
		if err != nil {
			return fmt.Errorf("placing the start ledger %d: %w", startLedger, err)
		}
		if err := m.checkRetentionKeeps(ctx, startClose); err != nil {
			return err
		}
		windows := int64(runTop.Sub(startClose)/interval) + 1
		m.appMetrics.Ingestion.BackfillWindowsRemaining.Set(float64(windows))
		log.Ctx(ctx).Infof("Backfill of ledgers %d-%d: about %d windows of %s; memory ≈ %d MB for %d prefetched + %d in-process + %d buffered ledgers; disk ≈ %d GB of uncompressed windows at compress concurrency %d",
			startLedger, oldest-1, windows, interval,
			int64(m.datastoreBufferSize+m.backfillWorkers+m.backfillFlushSize)*decodedLedgerBytes>>20, m.datastoreBufferSize, m.backfillWorkers, m.backfillFlushSize,
			int64(maxWindowsInFlight(m.backfillCompressConcurrency))*uncompressedWindowBytes>>30, m.backfillCompressConcurrency)
	}

	pausedJobs, err := pauseCompressionPolicies(ctx, pool)
	if err != nil {
		return err
	}
	defer func() {
		resumeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), time.Minute)
		defer cancel()
		if err := resumeCompressionPolicies(resumeCtx, pool, pausedJobs); err != nil {
			log.Ctx(ctx).Errorf("Resuming compression policies: %v (live ingestion schedules them again at its next start)", err)
		}
	}()

	compressor, cctx := newBackfillCompressor(ctx, pool, m.appMetrics.Ingestion, m.backfillCompressConcurrency, runTop)
	runErr := m.runBackfillWindows(cctx, compressor, probe, interval, startLedger, oldest, latest)
	closeErr := compressor.Close()
	switch {
	case runErr == nil:
		return closeErr
	case closeErr == nil || errors.Is(runErr, context.Canceled):
		// A compressor failure cancels the run; its error is the cause.
		if closeErr != nil {
			return closeErr
		}
		return runErr
	default:
		return errors.Join(runErr, closeErr)
	}
}

// runBackfillWindows fills the holes above the cursor, then walks the
// windows below it. It runs under the compressor's context so a compression
// failure stops the fill.
func (m *ingestService) runBackfillWindows(ctx context.Context, compressor *backfillCompressor, probe ledgerCloseTimeProbe, interval time.Duration, startLedger, oldest, latest uint32) error {
	gaps, err := m.models.IngestStore.GetLedgerGaps(ctx, oldest, latest)
	if err != nil {
		return fmt.Errorf("finding gaps above the cursor: %w", err)
	}
	gapWins, err := gapWindows(ctx, probe, gaps, interval)
	if err != nil {
		return err
	}
	for _, w := range gapWins {
		if err := m.runWindowAndClose(ctx, compressor, w); err != nil {
			return err
		}
	}

	planner, err := newBackfillPlanner(ctx, probe, interval, startLedger, oldest)
	if err != nil {
		return err
	}
	for {
		w, ok, err := planner.Next(ctx)
		if err != nil {
			return err
		}
		if !ok {
			break
		}
		if err := m.runWindowAndClose(ctx, compressor, w); err != nil {
			return err
		}
		if err := compressor.EnqueueLookupChunks(ctx, w.RangeStart); err != nil {
			return err
		}
		m.appMetrics.Ingestion.BackfillWindowsRemaining.Dec()
	}
	m.appMetrics.Ingestion.BackfillWindowsRemaining.Set(0)
	return nil
}

// runWindowAndClose takes a window slot, fills the window (retrying the whole
// window on transient failure), lowers the cursor if the window is below it,
// and hands the window to the compressor.
func (m *ingestService) runWindowAndClose(ctx context.Context, compressor *backfillCompressor, w backfillWindow) error {
	if err := compressor.AcquireSlot(ctx); err != nil {
		return err
	}
	start := time.Now()
	if err := m.runWindowWithRetry(ctx, w); err != nil {
		compressor.releaseSlot()
		return err
	}
	if w.BelowCursor {
		if err := m.lowerOldestCursor(ctx, w.FirstLedger); err != nil {
			compressor.releaseSlot()
			return err
		}
	}
	elapsed := time.Since(start)
	log.Ctx(ctx).Infof("%s: %d ledgers in %s (%.1f ledgers/s)", w, w.ledgerCount(), elapsed.Round(time.Second), float64(w.ledgerCount())/elapsed.Seconds())
	return compressor.WindowClosed(ctx, w)
}

// lowerOldestCursor records that every ledger from firstLedger to the tip is
// present. It runs in its own transaction after the window's data is
// committed, so the cursor never claims rows that are not there.
func (m *ingestService) lowerOldestCursor(ctx context.Context, firstLedger uint32) error {
	err := pgx.BeginFunc(ctx, m.models.DB, func(tx pgx.Tx) error {
		return m.models.IngestStore.UpdateMin(ctx, tx, data.OldestLedgerCursorName, firstLedger) //nolint:wrapcheck // wrapped below
	})
	if err != nil {
		return fmt.Errorf("lowering %s to %d: %w", data.OldestLedgerCursorName, firstLedger, err)
	}
	m.appMetrics.Ingestion.OldestLedger.Set(float64(firstLedger))
	return nil
}

// runWindowWithRetry redoes a window after a transient failure. A permanent
// error (a constraint violation, a dead ledger buffer, a row that cannot be
// encoded) or a cancelled context ends the run.
func (m *ingestService) runWindowWithRetry(ctx context.Context, w backfillWindow) error {
	isPermanent := func(err error) bool {
		return isPermanentPersistError(err) || (m.isPermanentFetchError != nil && m.isPermanentFetchError(err))
	}
	_, err := utils.RetryWithBackoff(ctx, maxWindowAttempts, maxWindowRetryBackoff,
		func(ctx context.Context) (struct{}, error) {
			return struct{}{}, m.runWindow(ctx, w)
		},
		func(attempt int, err error, backoff time.Duration) {
			m.appMetrics.Ingestion.RetriesTotal.WithLabelValues("backfill_window").Inc()
			log.Ctx(ctx).Warnf("%s failed (attempt %d/%d): %v, redoing it in %v", w, attempt+1, maxWindowAttempts, err, backoff)
		},
		isPermanent,
	)
	if err == nil {
		return nil
	}
	if ctx.Err() == nil {
		m.appMetrics.Ingestion.ErrorsTotal.WithLabelValues("backfill_window").Inc()
		if errors.Is(err, utils.ErrRetriesExhausted) {
			m.appMetrics.Ingestion.RetryExhaustionsTotal.WithLabelValues("backfill_window").Inc()
		}
	}
	return fmt.Errorf("%s: %w", w, err)
}

// runWindow fetches, processes and writes one window: one fetcher streaming
// the window's ledgers in order, N workers processing them on the shared
// indexer, one flusher writing every flush-size ledgers through the five
// bulk-table connections.
//
// A window below the cursor whose range has no chunk in any bulk table is
// written into staging tables and attached as new chunks once complete:
// the rows are copied without index maintenance and the indexes are built
// once, at attach. Any other window (a gap above the cursor, a range that
// already has a chunk, or a window a failed attempt left half attached) is
// written straight into the hypertables after deleting whatever an earlier
// attempt left in its range, so every attempt converges.
func (m *ingestService) runWindow(ctx context.Context, w backfillWindow) error {
	attach := false
	if w.BelowCursor {
		ranges, err := readChunkRanges(ctx, m.models.DB, data.BulkCopyTableNames())
		if err != nil {
			return err
		}
		attach = !overlapsAnyChunk(ranges, w.RangeStart, w.RangeEnd)
	}
	targets := hypertableTargets()
	if attach {
		if err := m.models.IngestStore.CreateStagingTables(ctx, w.RangeStart); err != nil {
			return fmt.Errorf("creating staging tables for %s: %w", w, err)
		}
		targets = stagingTargets(w.RangeStart)
	} else {
		if err := m.models.IngestStore.DropStagingTables(ctx, w.RangeStart); err != nil {
			return fmt.Errorf("dropping staging tables of %s: %w", w, err)
		}
		if err := m.reconcileWindow(ctx, w); err != nil {
			return err
		}
	}
	log.Ctx(ctx).Infof("%s: filling %d ledgers (%s)", w, w.ledgerCount(), targets.mode)

	backend, err := m.ledgerBackendFactory(ctx)
	if err != nil {
		return fmt.Errorf("creating ledger backend: %w", err)
	}
	defer func() {
		if err := backend.Close(); err != nil {
			log.Ctx(ctx).Warnf("closing ledger backend for %s: %v", w, err)
		}
	}()
	if err := backend.PrepareRange(ctx, ledgerbackend.BoundedRange(w.FirstLedger, w.LastLedger)); err != nil {
		return fmt.Errorf("preparing ledgers %d-%d: %w", w.FirstLedger, w.LastLedger, err)
	}

	g, gctx := errgroup.WithContext(ctx)
	fetched := make(chan fetchedLedger, 1)
	// The process stage must keep going while a flush runs: the queue holds a
	// whole next flush, and the buffer rotation covers the flush being
	// written, the flush queued behind it and one buffer per worker. Sized
	// any tighter, the workers stall for the length of every flush.
	processed := make(chan processedLedger, m.backfillFlushSize)
	bufferCount := m.backfillWorkers + 2*m.backfillFlushSize
	freeBuffers := make(chan *indexer.IndexerBuffer, bufferCount)
	for range bufferCount {
		freeBuffers <- indexer.NewIndexerBuffer()
	}

	g.Go(func() error { return m.fetchWindow(gctx, backend, w, fetched) })
	workers, wctx := errgroup.WithContext(gctx)
	for range m.backfillWorkers {
		workers.Go(func() error { return m.processWindowLedgers(wctx, w, fetched, freeBuffers, processed) })
	}
	g.Go(func() error {
		defer close(processed)
		return workers.Wait() //nolint:wrapcheck // worker errors are wrapped
	})
	g.Go(func() error { return m.flushWindow(gctx, w, targets, processed, freeBuffers) })
	if err := g.Wait(); err != nil {
		return err //nolint:wrapcheck // stage errors are wrapped
	}
	if attach {
		return m.attachWindow(ctx, w)
	}
	return nil
}

// attachWindow turns the window's five staging tables into chunks, one
// session per table so the index builds run side by side. Each attach
// commits on its own; a failure leaves the window half attached, and the
// next attempt writes it in place.
func (m *ingestService) attachWindow(ctx context.Context, w backfillWindow) error {
	start := time.Now()
	g, gctx := errgroup.WithContext(ctx)
	for _, table := range data.BulkCopyTableNames() {
		g.Go(func() error {
			conn, err := m.models.DB.Acquire(gctx)
			if err != nil {
				return fmt.Errorf("acquiring connection to attach %s: %w", table, err)
			}
			defer conn.Release()
			return pgx.BeginFunc(gctx, conn, func(tx pgx.Tx) error {
				return data.AttachChunk(gctx, tx, table, w.RangeStart, w.RangeEnd) //nolint:wrapcheck // AttachChunk names the table
			})
		})
	}
	if err := g.Wait(); err != nil {
		return fmt.Errorf("attaching %s: %w", w, err)
	}
	m.appMetrics.Ingestion.PhaseDuration.WithLabelValues("attach").Observe(time.Since(start).Seconds())
	log.Ctx(ctx).Infof("%s: attached in %s", w, time.Since(start).Round(time.Millisecond))
	return nil
}

// windowTargets names the relation each bulk table's rows are written to for
// one window: the hypertables themselves, or the window's staging tables.
type windowTargets struct {
	mode      string
	relations map[string]pgx.Identifier
}

func hypertableTargets() windowTargets {
	t := windowTargets{mode: "in place", relations: make(map[string]pgx.Identifier, len(data.BulkCopyTableNames()))}
	for _, table := range data.BulkCopyTableNames() {
		t.relations[table] = pgx.Identifier{table}
	}
	return t
}

func stagingTargets(rangeStart time.Time) windowTargets {
	t := windowTargets{mode: "staging, attach at close", relations: make(map[string]pgx.Identifier, len(data.BulkCopyTableNames()))}
	for _, table := range data.BulkCopyTableNames() {
		t.relations[table] = data.StagingRelation(table, rangeStart)
	}
	return t
}

// reconcileWindow deletes the window's ledgers from the bulk tables before a
// plain fill, bounded to the window's chunks. It is a no-op on a first
// attempt and makes a rerun converge without ON CONFLICT.
func (m *ingestService) reconcileWindow(ctx context.Context, w backfillWindow) error {
	var deleted int64
	err := pgx.BeginFunc(ctx, m.models.DB, func(tx pgx.Tx) error {
		var err error
		deleted, err = m.models.IngestStore.DeleteRowsInLedgerRange(ctx, tx, w.FirstLedger, w.LastLedger, w.RangeStart, w.RangeEnd)
		return err //nolint:wrapcheck // wrapped below
	})
	if err != nil {
		return fmt.Errorf("deleting rows of %s before refilling: %w", w, err)
	}
	if deleted > 0 {
		log.Ctx(ctx).Infof("%s: deleted %d rows left by an earlier attempt", w, deleted)
	}
	return nil
}

func (m *ingestService) fetchWindow(ctx context.Context, backend ledgerbackend.LedgerBackend, w backfillWindow, fetched chan<- fetchedLedger) error {
	defer close(fetched)
	for seq := w.FirstLedger; ; seq++ {
		fetchStart := time.Now()
		meta, err := utils.RetryWithBackoff(ctx, maxLedgerFetchRetries, maxRetryBackoff,
			func(ctx context.Context) (xdr.LedgerCloseMeta, error) {
				return backend.GetLedger(ctx, seq)
			},
			func(attempt int, err error, backoff time.Duration) {
				m.appMetrics.Ingestion.RetriesTotal.WithLabelValues("ledger_fetch").Inc()
				log.Ctx(ctx).Warnf("Error fetching ledger %d (attempt %d/%d): %v, retrying in %v...", seq, attempt+1, maxLedgerFetchRetries, err, backoff)
			},
			m.isPermanentFetchError,
		)
		if err != nil {
			return fmt.Errorf("fetching ledger %d: %w", seq, err)
		}
		m.appMetrics.Ingestion.PhaseDuration.WithLabelValues("fetch").Observe(time.Since(fetchStart).Seconds())
		select {
		case fetched <- fetchedLedger{seq: seq, meta: meta}:
		case <-ctx.Done():
			return fmt.Errorf("window cancelled: %w", ctx.Err())
		}
		if seq == w.LastLedger {
			return nil
		}
	}
}

func (m *ingestService) processWindowLedgers(ctx context.Context, w backfillWindow, fetched <-chan fetchedLedger, freeBuffers <-chan *indexer.IndexerBuffer, processed chan<- processedLedger) error {
	for {
		var fl fetchedLedger
		select {
		case f, ok := <-fetched:
			if !ok {
				return nil
			}
			fl = f
		case <-ctx.Done():
			return fmt.Errorf("window cancelled: %w", ctx.Err())
		}
		closeTime := time.Unix(fl.meta.LedgerCloseTime(), 0).UTC()
		if w.BelowCursor && (closeTime.Before(w.RangeStart) || !closeTime.Before(w.RangeEnd)) {
			return fmt.Errorf("ledger %d closed at %s, outside %s", fl.seq, closeTime.Format(time.RFC3339), w)
		}

		var buffer *indexer.IndexerBuffer
		select {
		case buffer = <-freeBuffers:
		case <-ctx.Done():
			return fmt.Errorf("window cancelled: %w", ctx.Err())
		}
		buffer.Clear()

		processStart := time.Now()
		if _, err := m.processLedger(ctx, fl.meta, buffer); err != nil {
			return fmt.Errorf("processing ledger %d: %w", fl.seq, err)
		}
		if err := buffer.BuildCopyRows(); err != nil {
			return fmt.Errorf("building rows of ledger %d: %w", fl.seq, err)
		}
		m.appMetrics.Ingestion.PhaseDuration.WithLabelValues("process_ledger").Observe(time.Since(processStart).Seconds())

		select {
		case processed <- processedLedger{seq: fl.seq, closeTime: fl.meta.LedgerCloseTime(), buffer: buffer}:
		case <-ctx.Done():
			return fmt.Errorf("window cancelled: %w", ctx.Err())
		}
	}
}

// flushWindow streams processed ledgers to the database every flush-size
// ledgers and once more at the end, then checks that the window is complete.
func (m *ingestService) flushWindow(ctx context.Context, w backfillWindow, targets windowTargets, processed <-chan processedLedger, freeBuffers chan<- *indexer.IndexerBuffer) error {
	var pending []processedLedger
	var written uint32
	flush := func() error {
		if len(pending) == 0 {
			return nil
		}
		if err := m.flushLedgers(ctx, targets, pending); err != nil {
			return err
		}
		written += uint32(len(pending))
		for _, item := range pending {
			freeBuffers <- item.buffer
		}
		pending = pending[:0]
		return nil
	}
	for {
		select {
		case item, ok := <-processed:
			if !ok {
				if err := flush(); err != nil {
					return err
				}
				if written != w.ledgerCount() {
					return fmt.Errorf("%s: wrote %d of %d ledgers", w, written, w.ledgerCount())
				}
				return nil
			}
			pending = append(pending, item)
			if len(pending) >= m.backfillFlushSize {
				if err := flush(); err != nil {
					return err
				}
			}
		case <-ctx.Done():
			return fmt.Errorf("window cancelled: %w", ctx.Err())
		}
	}
}

// flushLedgers writes a batch of processed ledgers: one transaction per bulk
// table on its own connection, all committing with synchronous_commit off.
// The transactions connection also writes the lookup rows. Each connection
// writes a different table, so no two of this process's transactions insert
// into the same hypertable (two of them creating the same chunk could wait on
// each other in Go, out of sight of the deadlock detector).
func (m *ingestService) flushLedgers(ctx context.Context, targets windowTargets, items []processedLedger) error {
	start := time.Now()
	g, gctx := errgroup.WithContext(ctx)
	for _, sibling := range m.backfillSiblings(targets) {
		g.Go(func() error {
			return m.runBackfillSibling(gctx, sibling, items)
		})
	}
	if err := g.Wait(); err != nil {
		return err //nolint:wrapcheck // sibling errors are wrapped
	}
	m.appMetrics.Ingestion.PhaseDuration.WithLabelValues("insert_into_db").Observe(time.Since(start).Seconds())
	return nil
}

// backfillSibling is one bulk table's write: the rows a processed ledger
// holds for it and the model call that streams them.
type backfillSibling struct {
	table string
	write func(ctx context.Context, tx pgx.Tx, item processedLedger) error
}

func (m *ingestService) backfillSiblings(targets windowTargets) []backfillSibling {
	rel := targets.relations
	return []backfillSibling{
		{"transactions", func(ctx context.Context, tx pgx.Tx, it processedLedger) error {
			if _, err := m.models.Transactions.CopyRowsInto(ctx, tx, rel["transactions"], it.buffer.GetTransactionCopyRows()); err != nil {
				return err //nolint:wrapcheck // caller wraps with table and ledger
			}
			// Lookup rows always go to the hypertable: its 7-day chunks span
			// several windows, so they are never staged.
			_, err := m.models.Transactions.CopyHashRows(ctx, tx, it.buffer.GetTransactionHashCopyRows())
			return err //nolint:wrapcheck // caller wraps with table and ledger
		}},
		{"transactions_accounts", func(ctx context.Context, tx pgx.Tx, it processedLedger) error {
			_, err := m.models.Transactions.CopyAccountRowsInto(ctx, tx, rel["transactions_accounts"], it.buffer.GetTransactionAccountCopyRows())
			return err //nolint:wrapcheck // caller wraps with table and ledger
		}},
		{"operations", func(ctx context.Context, tx pgx.Tx, it processedLedger) error {
			_, err := m.models.Operations.CopyRowsInto(ctx, tx, rel["operations"], it.buffer.GetOperationCopyRows())
			return err //nolint:wrapcheck // caller wraps with table and ledger
		}},
		{"operations_accounts", func(ctx context.Context, tx pgx.Tx, it processedLedger) error {
			_, err := m.models.Operations.CopyAccountRowsInto(ctx, tx, rel["operations_accounts"], it.buffer.GetOperationAccountCopyRows())
			return err //nolint:wrapcheck // caller wraps with table and ledger
		}},
		{"state_changes", func(ctx context.Context, tx pgx.Tx, it processedLedger) error {
			_, err := m.models.StateChanges.CopyRowsInto(ctx, tx, rel["state_changes"], it.buffer.GetStateChangeCopyRows())
			return err //nolint:wrapcheck // caller wraps with table and ledger
		}},
	}
}

func (m *ingestService) runBackfillSibling(ctx context.Context, sibling backfillSibling, items []processedLedger) error {
	conn, err := m.models.DB.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("acquiring %s connection: %w", sibling.table, err)
	}
	defer conn.Release()
	err = pgx.BeginFunc(ctx, conn, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `SET LOCAL synchronous_commit = off`); err != nil {
			return fmt.Errorf("setting synchronous_commit: %w", err)
		}
		for _, item := range items {
			if err := sibling.write(ctx, tx, item); err != nil {
				return fmt.Errorf("ledger %d: %w", item.seq, err)
			}
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("writing %s: %w", sibling.table, err)
	}
	return nil
}

// checkRetentionKeeps refuses a run whose oldest window the retention policy
// would drop: the rows would be written and removed by the next policy run.
func (m *ingestService) checkRetentionKeeps(ctx context.Context, startClose time.Time) error {
	var earliestKept *time.Time
	err := m.models.DB.QueryRow(ctx,
		`SELECT min(now() - (config->>'drop_after')::interval)
		   FROM timescaledb_information.jobs
		  WHERE proc_name = 'policy_retention' AND hypertable_name = ANY($1) AND config ? 'drop_after'`,
		data.BulkCopyTableNames()).Scan(&earliestKept)
	if err != nil {
		return fmt.Errorf("reading retention policy: %w", err)
	}
	if earliestKept != nil && startClose.Before(*earliestKept) {
		return fmt.Errorf("start ledger closed at %s, before the retention policy's earliest kept time %s; raise retention before backfilling that far",
			startClose.Format(time.RFC3339), earliestKept.UTC().Format(time.RFC3339))
	}
	return nil
}
