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
//  1. Dispatcher: fetches ledgers sequentially from backend → ledgerCh
//  2. Process workers: N goroutines process ledgers into buffers → flushCh
//  3. Flush workers: M goroutines write buffers to DB via parallel COPYs
func (m *ingestService) processGap(ctx context.Context, gap data.LedgerRange) error {
	gapStart := time.Now()
	gapCtx, gapCancel := context.WithCancelCause(ctx)
	defer gapCancel(nil)

	backend, err := m.ledgerBackendFactory(gapCtx)
	if err != nil {
		return fmt.Errorf("creating ledger backend: %w", err)
	}
	defer func() {
		if closeErr := backend.Close(); closeErr != nil {
			log.Ctx(ctx).Warnf("Error closing backend for gap [%d-%d]: %v",
				gap.GapStart, gap.GapEnd, closeErr)
		}
	}()

	if err := backend.PrepareRange(gapCtx, ledgerbackend.BoundedRange(gap.GapStart, gap.GapEnd)); err != nil {
		return fmt.Errorf("preparing backend range [%d-%d]: %w", gap.GapStart, gap.GapEnd, err)
	}

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

	// Channel utilization sampler — snapshots fill ratios every second
	pipelineWg.Add(1)
	go func() {
		defer pipelineWg.Done()
		ticker := time.NewTicker(1 * time.Second)
		defer ticker.Stop()
		for {
			select {
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

	// Stage 1: dispatcher (runs on this goroutine)
	dispatcherStats := m.runDispatcher(gapCtx, gapCancel, backend, gap, ledgerCh)

	pipelineWg.Wait()

	// Aggregate gap stats from all pipeline stages
	close(processStatsCh)
	gapStats := newBackfillGapStats()
	gapStats.mergeWorker(dispatcherStats)
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

// runDispatcher is Stage 1: fetches ledgers sequentially and sends them
// to ledgerCh. Closes ledgerCh when done or on error.
// Returns per-worker stats for the gap summary log.
func (m *ingestService) runDispatcher(
	ctx context.Context,
	cancel context.CancelCauseFunc,
	backend ledgerbackend.LedgerBackend,
	gap data.LedgerRange,
	ledgerCh chan<- xdr.LedgerCloseMeta,
) *backfillWorkerStats {
	defer close(ledgerCh)
	stats := &backfillWorkerStats{}

	for seq := gap.GapStart; seq <= gap.GapEnd; seq++ {
		if ctx.Err() != nil {
			return stats
		}

		fetchStart := time.Now()
		lcm, err := m.getLedgerWithRetry(ctx, backend, seq)
		fetchDur := time.Since(fetchStart)
		if err != nil {
			cancel(fmt.Errorf("fetching ledger %d: %w", seq, err))
			return stats
		}
		stats.addFetch(fetchDur)
		m.appMetrics.Ingestion.PhaseDuration.WithLabelValues("backfill_fetch").Observe(fetchDur.Seconds())

		sendStart := time.Now()
		select {
		case ledgerCh <- lcm:
			sendDur := time.Since(sendStart)
			stats.addChannelWait("ledger", "send", sendDur)
			m.appMetrics.Ingestion.BackfillChannelWait.WithLabelValues("ledger", "send").Observe(sendDur.Seconds())
		case <-ctx.Done():
			return stats
		}
	}
	return stats
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

			buffer := indexer.NewIndexerBuffer()
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
				buffer = indexer.NewIndexerBuffer()
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
			}
		}(i)
	}
	wg.Wait()
}

// flushBufferWithRetry persists a buffer's data to DB via parallel COPYs
// with exponential backoff retry.
func (m *ingestService) flushBufferWithRetry(ctx context.Context, buffer *indexer.IndexerBuffer) error {
	var lastErr error
	for attempt := range maxIngestProcessedDataRetries {
		select {
		case <-ctx.Done():
			return fmt.Errorf("context cancelled: %w", ctx.Err())
		default:
		}

		txs := buffer.GetTransactions()
		if _, _, err := m.insertAndUpsertParallel(ctx, txs, buffer); err != nil {
			lastErr = err
			m.appMetrics.Ingestion.RetriesTotal.WithLabelValues("batch_flush").Inc()

			backoff := min(time.Duration(1<<attempt)*time.Second, maxIngestProcessedDataRetryBackoff)
			log.Ctx(ctx).Warnf("Flush error (attempt %d/%d): %v, retrying in %v...",
				attempt+1, maxIngestProcessedDataRetries, lastErr, backoff)

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
