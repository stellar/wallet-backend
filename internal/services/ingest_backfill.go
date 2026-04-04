package services

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stellar/go-stellar-sdk/ingest/ledgerbackend"
	"github.com/stellar/go-stellar-sdk/support/log"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/wallet-backend/internal/data"
	"github.com/stellar/wallet-backend/internal/db"
	"github.com/stellar/wallet-backend/internal/indexer"
)

// backfillBufferPool reuses IndexerBuffers across flush cycles to avoid
// re-allocating 11 maps + 2 slices per batch. Clear() preserves capacity.
var backfillBufferPool = sync.Pool{
	New: func() any { return indexer.NewIndexerBuffer() },
}

// startBackfilling identifies gaps in the ledger range and fills them
// sequentially via a 3-stage pipeline (dispatcher → process → flush).
func (m *ingestService) startBackfilling(ctx context.Context, startLedger, endLedger uint32) error {
	if startLedger > endLedger {
		return fmt.Errorf("start ledger cannot be greater than end ledger")
	}

	latestIngestedLedger, err := m.models.IngestStore.Get(ctx, m.latestLedgerCursorName)
	if err != nil {
		return fmt.Errorf("getting latest ledger cursor: %w", err)
	}

	if endLedger > latestIngestedLedger {
		return fmt.Errorf("end ledger %d cannot be greater than latest ingested ledger %d for backfilling", endLedger, latestIngestedLedger)
	}

	gaps, err := m.calculateBackfillGaps(ctx, startLedger, endLedger)
	if err != nil {
		return fmt.Errorf("calculating backfill gaps: %w", err)
	}
	if len(gaps) == 0 {
		log.Ctx(ctx).Infof("No gaps to backfill in range [%d - %d]", startLedger, endLedger)
		return nil
	}

	// Must match ingest.hypertables — duplicated here to avoid import cycle
	// (ingest imports services).
	tables := []string{
		"transactions", "transactions_accounts", "operations",
		"operations_accounts", "state_changes",
	}

	// Fetch boundary timestamps for chunk pre-creation.
	rangeStartTime, err := m.fetchLedgerCloseTime(ctx, gaps[0].GapStart)
	if err != nil {
		return fmt.Errorf("fetching start ledger %d timestamp: %w", gaps[0].GapStart, err)
	}
	rangeEndTime, err := m.fetchLedgerCloseTime(ctx, gaps[len(gaps)-1].GapEnd)
	if err != nil {
		return fmt.Errorf("fetching end ledger %d timestamp: %w", gaps[len(gaps)-1].GapEnd, err)
	}

	// Suppress insert-triggered autovacuum on parent hypertables during backfill.
	if err := db.DisableInsertAutovacuum(ctx, m.models.DB, tables); err != nil {
		return fmt.Errorf("disabling insert autovacuum: %w", err)
	}
	defer func() {
		if restoreErr := db.RestoreInsertAutovacuum(ctx, m.models.DB, tables); restoreErr != nil {
			log.Ctx(ctx).Warnf("Failed to restore insert autovacuum: %v", restoreErr)
		}
	}()

	// Pre-create chunks with indexes dropped, UNLOGGED, and per-chunk autovacuum disabled.
	// Discard []*Chunk — progressive compression will be added separately.
	if _, err := db.PreCreateChunks(ctx, m.models.DB, tables, rangeStartTime, rangeEndTime); err != nil {
		return fmt.Errorf("pre-creating chunks: %w", err)
	}

	overallStart := time.Now()
	for i, gap := range gaps {
		log.Ctx(ctx).Infof("Processing gap %d/%d [%d - %d]", i+1, len(gaps), gap.GapStart, gap.GapEnd)
		if err := m.processGap(ctx, gap); err != nil {
			log.Ctx(ctx).Errorf("Gap %d/%d [%d - %d] failed: %v", i+1, len(gaps), gap.GapStart, gap.GapEnd, err)
			continue
		}
	}

	log.Ctx(ctx).Infof("Backfilling completed in %v: %d gaps", time.Since(overallStart), len(gaps))
	return nil
}

// calculateBackfillGaps determines which ledger ranges need to be backfilled based on
// the requested range, oldest ingested ledger, and any existing gaps in the data.
func (m *ingestService) calculateBackfillGaps(ctx context.Context, startLedger, endLedger uint32) ([]data.LedgerRange, error) {
	oldestIngestedLedger, err := m.models.IngestStore.GetOldestLedger(ctx)
	if err != nil {
		return nil, fmt.Errorf("getting oldest ingest ledger: %w", err)
	}

	currentGaps, err := m.models.IngestStore.GetLedgerGaps(ctx)
	if err != nil {
		return nil, fmt.Errorf("calculating gaps in ledger range: %w", err)
	}

	newGaps := make([]data.LedgerRange, 0)
	switch {
	case endLedger <= oldestIngestedLedger:
		// Case 1: End ledger matches/less than oldest - backfill [start, min(end, oldest-1)]
		if oldestIngestedLedger > 0 {
			newGaps = append(newGaps, data.LedgerRange{
				GapStart: startLedger,
				GapEnd:   min(endLedger, oldestIngestedLedger-1),
			})
		}

	case startLedger < oldestIngestedLedger:
		// Case 2: Overlaps with existing range - backfill before oldest + internal gaps
		if oldestIngestedLedger > 0 {
			newGaps = append(newGaps, data.LedgerRange{
				GapStart: startLedger,
				GapEnd:   oldestIngestedLedger - 1,
			})
		}
		for _, gap := range currentGaps {
			if gap.GapStart > endLedger {
				break
			}
			newGaps = append(newGaps, data.LedgerRange{
				GapStart: gap.GapStart,
				GapEnd:   min(gap.GapEnd, endLedger),
			})
		}

	default:
		// Case 3: Entirely within existing range - only fill internal gaps
		for _, gap := range currentGaps {
			if gap.GapEnd < startLedger {
				continue
			}
			if gap.GapStart > endLedger {
				break
			}
			newGaps = append(newGaps, data.LedgerRange{
				GapStart: max(gap.GapStart, startLedger),
				GapEnd:   min(gap.GapEnd, endLedger),
			})
		}
	}

	return newGaps, nil
}

// processGap runs the 3-stage pipeline for a single contiguous gap:
//  1. Fetcher: parallel S3 downloads → ledgerCh
//  2. Process workers: N goroutines process ledgers into buffers → flushCh
//  3. Flush workers: M goroutines write buffers to DB via parallel COPYs
func (m *ingestService) processGap(ctx context.Context, gap data.LedgerRange) error {
	gapStart := time.Now()
	gapCtx, gapCancel := context.WithCancelCause(ctx)
	defer gapCancel(nil)

	ledgerCh := make(chan xdr.LedgerCloseMeta, m.backfillLedgerChanSize)
	flushCh := make(chan flushItem, m.backfillFlushChanSize)
	watermark := newBackfillWatermark(gap.GapStart, gap.GapEnd)

	// Set gap boundary gauges
	m.appMetrics.Ingestion.BackfillGapStartLedger.Set(float64(gap.GapStart))
	m.appMetrics.Ingestion.BackfillGapEndLedger.Set(float64(gap.GapEnd))
	m.appMetrics.Ingestion.BackfillGapProgress.Set(0)
	defer func() {
		m.appMetrics.Ingestion.BackfillGapStartLedger.Set(0)
		m.appMetrics.Ingestion.BackfillGapEndLedger.Set(0)
	}()

	var pipelineWg sync.WaitGroup
	samplerDone := make(chan struct{})

	// Channel utilization sampler — snapshots fill ratios every second.
	// Not part of pipelineWg: its lifecycle is controlled by samplerDone,
	// which is closed after the pipeline finishes.
	go func() {
		ticker := time.NewTicker(1 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-samplerDone:
				return
			case <-gapCtx.Done():
				return
			case <-ticker.C:
				if cap(ledgerCh) > 0 {
					m.appMetrics.Ingestion.BackfillChannelUtilization.WithLabelValues("ledger").Set(
						float64(len(ledgerCh)) / float64(cap(ledgerCh)),
					)
				}
				if cap(flushCh) > 0 {
					m.appMetrics.Ingestion.BackfillChannelUtilization.WithLabelValues("flush").Set(
						float64(len(flushCh)) / float64(cap(flushCh)),
					)
				}
			}
		}
	}()

	// Stage 3: flush workers (start first so they're ready)
	flushStatsCh := make(chan *backfillFlushWorkerStats, m.backfillFlushWorkers)
	pipelineWg.Add(1)
	go func() {
		defer pipelineWg.Done()
		m.runFlushWorkers(gapCtx, flushCh, watermark, m.backfillFlushWorkers, gap, flushStatsCh)
	}()

	// Stage 2: process workers
	processStatsCh := make(chan *backfillWorkerStats, m.backfillProcessWorkers)
	pipelineWg.Add(1)
	go func() {
		defer pipelineWg.Done()
		defer close(flushCh)
		m.runProcessWorkers(gapCtx, gapCancel, ledgerCh, flushCh, processStatsCh)
	}()

	// Stage 1: parallel S3 fetcher (replaces dispatcher + backend)
	var fetchCount int
	var fetchTotal time.Duration
	var fetchChannelWait map[string]time.Duration

	pipelineWg.Add(1)
	go func() {
		defer pipelineWg.Done()
		runner := m.backfillFetcherFactory(gap.GapStart, gap.GapEnd, ledgerCh)
		fetchCount, fetchTotal, fetchChannelWait = runner(gapCtx, gapCancel)
	}()

	// Wait for all pipeline stages to complete.
	// The fetcher closes ledgerCh → process workers drain and close flushCh →
	// flush workers drain. Then close samplerDone to stop the utilization sampler.
	pipelineWg.Wait()
	close(samplerDone)

	// Aggregate gap stats from all pipeline stages
	close(processStatsCh)
	gapStats := newBackfillGapStats()
	gapStats.fetchCount += fetchCount
	gapStats.fetchTotal += fetchTotal
	for k, v := range fetchChannelWait {
		gapStats.channelWait[k] += v
	}
	for ws := range processStatsCh {
		gapStats.mergeWorker(ws)
	}
	close(flushStatsCh)
	for fs := range flushStatsCh {
		gapStats.mergeFlushWorker(fs)
	}
	if cause := context.Cause(gapCtx); cause != nil && !errors.Is(cause, context.Canceled) {
		return fmt.Errorf("pipeline failed: %w", cause)
	}

	// Log gap summary with per-stage timing breakdown
	total := gap.GapEnd - gap.GapStart + 1
	elapsed := time.Since(gapStart)
	ledgersPerSec := float64(0)
	if elapsed > 0 {
		ledgersPerSec = float64(total) / elapsed.Seconds()
	}

	if watermark.Complete() {
		log.Ctx(ctx).Infof("Gap [%d-%d] complete (%v, %.0f ledgers/sec):\n"+
			"  fetch:   %v total, %v avg (%d calls)\n"+
			"  process: %v total, %v avg (%d calls)\n"+
			"  flush:   %v total, %v avg (%d batches)\n"+
			"  channel_wait: ledger_send=%v ledger_recv=%v flush_send=%v flush_recv=%v",
			gap.GapStart, gap.GapEnd, elapsed, ledgersPerSec,
			gapStats.fetchTotal, avgOrZero(gapStats.fetchTotal, gapStats.fetchCount), gapStats.fetchCount,
			gapStats.processTotal, avgOrZero(gapStats.processTotal, gapStats.processCount), gapStats.processCount,
			gapStats.flushTotal, avgOrZero(gapStats.flushTotal, gapStats.flushCount), gapStats.flushCount,
			gapStats.channelWait["ledger:send"],
			gapStats.channelWait["ledger:receive"],
			gapStats.channelWait["flush:send"],
			gapStats.channelWait["flush:receive"],
		)
	} else {
		log.Ctx(ctx).Warnf("Gap [%d-%d] partial: cursor at %d of %d (%v)",
			gap.GapStart, gap.GapEnd, watermark.Cursor(), gap.GapEnd, elapsed)
	}

	return nil
}

// runProcessWorkers is Stage 2: N workers pull from ledgerCh, process ledgers
// into IndexerBuffers, and send filled buffers to flushCh.
// Each worker sends its stats to statsCh on exit for gap summary aggregation.
func (m *ingestService) runProcessWorkers(
	ctx context.Context,
	cancel context.CancelCauseFunc,
	ledgerCh <-chan xdr.LedgerCloseMeta,
	flushCh chan<- flushItem,
	statsCh chan<- *backfillWorkerStats,
) {
	var wg sync.WaitGroup
	for range m.backfillProcessWorkers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			stats := &backfillWorkerStats{}
			defer func() { statsCh <- stats }()

			buffer := backfillBufferPool.Get().(*indexer.IndexerBuffer)
			buffer.Clear()
			var ledgers []uint32

			flush := func() {
				if len(ledgers) == 0 {
					return
				}
				sendStart := time.Now()
				select {
				case flushCh <- flushItem{Buffer: buffer, Ledgers: ledgers}:
					sendDur := time.Since(sendStart)
					stats.addChannelWait("flush", "send", sendDur)
					m.appMetrics.Ingestion.BackfillChannelWait.WithLabelValues("flush", "send").Observe(sendDur.Seconds())
				case <-ctx.Done():
					return
				}
				buffer = backfillBufferPool.Get().(*indexer.IndexerBuffer)
				buffer.Clear()
				ledgers = nil
			}

			for {
				recvStart := time.Now()
				lcm, ok := <-ledgerCh
				if !ok {
					break
				}
				recvDur := time.Since(recvStart)
				stats.addChannelWait("ledger", "receive", recvDur)
				m.appMetrics.Ingestion.BackfillChannelWait.WithLabelValues("ledger", "receive").Observe(recvDur.Seconds())

				if ctx.Err() != nil {
					return
				}

				processStart := time.Now()
				if err := m.processLedgerSequential(ctx, lcm, buffer); err != nil {
					cancel(fmt.Errorf("processing ledger %d: %w", lcm.LedgerSequence(), err))
					return
				}
				processDur := time.Since(processStart)
				stats.addProcess(processDur)
				m.appMetrics.Ingestion.PhaseDuration.WithLabelValues("backfill_process").Observe(processDur.Seconds())

				ledgers = append(ledgers, lcm.LedgerSequence())

				if uint32(len(ledgers)) >= m.backfillFlushBatchSize {
					flush()
				}
			}

			flush()
		}()
	}
	wg.Wait()
}

// flushItem is the unit of work sent from process workers to flush workers.
// Contains a filled IndexerBuffer and the ledger sequences it covers.
type flushItem struct {
	Buffer  *indexer.IndexerBuffer
	Ledgers []uint32
}

// runFlushWorkers starts M flush workers that read from flushCh,
// write data to DB, and report flushed ledgers to the watermark.
// Each worker sends its stats to statsCh on exit for gap summary aggregation.
func (m *ingestService) runFlushWorkers(
	ctx context.Context,
	flushCh <-chan flushItem,
	watermark *backfillWatermark,
	numWorkers int,
	gap data.LedgerRange,
	statsCh chan<- *backfillFlushWorkerStats,
) {
	var wg sync.WaitGroup
	for i := range numWorkers {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			stats := &backfillFlushWorkerStats{}
			defer func() { statsCh <- stats }()

			for {
				recvStart := time.Now()
				item, ok := <-flushCh
				if !ok {
					return
				}
				recvDur := time.Since(recvStart)
				stats.addChannelWait("flush", "receive", recvDur)
				m.appMetrics.Ingestion.BackfillChannelWait.WithLabelValues("flush", "receive").Observe(recvDur.Seconds())

				if ctx.Err() != nil {
					return
				}

				m.appMetrics.Ingestion.BackfillBatchSize.Observe(float64(len(item.Ledgers)))

				flushStart := time.Now()
				if err := m.flushBufferWithRetry(ctx, item.Buffer); err != nil {
					log.Ctx(ctx).Errorf("Flush worker %d: %d ledgers failed: %v",
						workerID, len(item.Ledgers), err)
					item.Buffer.Clear()
					backfillBufferPool.Put(item.Buffer)
					continue
				}
				flushDur := time.Since(flushStart)
				stats.addFlush(flushDur)
				m.appMetrics.Ingestion.PhaseDuration.WithLabelValues("backfill_flush").Observe(flushDur.Seconds())
				m.appMetrics.Ingestion.BackfillLedgersFlushed.Add(float64(len(item.Ledgers)))

				if advanced := watermark.MarkFlushed(item.Ledgers); advanced {
					gapSize := float64(gap.GapEnd - gap.GapStart + 1)
					progress := float64(watermark.Cursor()-gap.GapStart+1) / gapSize
					m.appMetrics.Ingestion.BackfillGapProgress.Set(progress)

					if err := m.updateOldestCursor(ctx, watermark.Cursor()); err != nil {
						log.Ctx(ctx).Warnf("Flush worker %d: cursor update failed: %v",
							workerID, err)
					}
				}

				item.Buffer.Clear()
				backfillBufferPool.Put(item.Buffer)
			}
		}(i)
	}
	wg.Wait()
}

// flushBufferWithRetry persists a buffer's data to DB via parallel COPYs
// with exponential backoff retry. A CopyResult tracks which tables have already
// committed — on retry, only failed tables are re-attempted, preventing duplicates.
func (m *ingestService) flushBufferWithRetry(ctx context.Context, buffer *indexer.IndexerBuffer) error {
	txs := buffer.GetTransactions()
	result := NewCopyResult()

	var lastErr error
	for attempt := range maxIngestProcessedDataRetries {
		select {
		case <-ctx.Done():
			return fmt.Errorf("context cancelled: %w", ctx.Err())
		default:
		}

		if _, _, err := m.insertParallel(ctx, txs, buffer, result); err != nil {
			lastErr = err
			m.appMetrics.Ingestion.RetriesTotal.WithLabelValues("batch_flush").Inc()

			backoff := min(time.Duration(1<<attempt)*time.Second, maxIngestProcessedDataRetryBackoff)
			log.Ctx(ctx).Warnf("Flush error (attempt %d/%d, done=%v): %v, retrying in %v...",
				attempt+1, maxIngestProcessedDataRetries, result.done, lastErr, backoff)

			select {
			case <-ctx.Done():
				return fmt.Errorf("context cancelled during backoff: %w", ctx.Err())
			case <-time.After(backoff):
			}
			continue
		}
		return nil
	}
	m.appMetrics.Ingestion.RetryExhaustionsTotal.WithLabelValues("batch_flush").Inc()
	return lastErr
}

// fetchLedgerCloseTime fetches the ClosedAt timestamp for a single ledger.
// Creates a temporary backend, fetches via single-ledger PrepareRange, returns the time.
func (m *ingestService) fetchLedgerCloseTime(ctx context.Context, ledger uint32) (time.Time, error) {
	backend, err := m.ledgerBackendFactory(ctx)
	if err != nil {
		return time.Time{}, fmt.Errorf("creating backend for boundary timestamps: %w", err)
	}
	defer func() {
		if closeErr := backend.Close(); closeErr != nil {
			log.Ctx(ctx).Warnf("Error closing boundary timestamp backend: %v", closeErr)
		}
	}()
	if err := backend.PrepareRange(ctx, ledgerbackend.BoundedRange(ledger, ledger)); err != nil {
		return time.Time{}, fmt.Errorf("preparing range for ledger %d: %w", ledger, err)
	}
	meta, err := backend.GetLedger(ctx, ledger)
	if err != nil {
		return time.Time{}, fmt.Errorf("getting ledger %d: %w", ledger, err)
	}
	return meta.ClosedAt(), nil
}

// updateOldestCursor updates the oldest ledger cursor to the given ledger.
func (m *ingestService) updateOldestCursor(ctx context.Context, ledgerSeq uint32) error {
	err := db.RunInTransaction(ctx, m.models.DB, func(dbTx pgx.Tx) error {
		return m.models.IngestStore.UpdateMin(ctx, dbTx, m.oldestLedgerCursorName, ledgerSeq)
	})
	if err != nil {
		return fmt.Errorf("updating oldest ledger cursor: %w", err)
	}
	return nil
}
