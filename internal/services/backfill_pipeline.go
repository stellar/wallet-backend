package services

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"golang.org/x/sync/errgroup"

	"github.com/stellar/go-stellar-sdk/ingest/ledgerbackend"
	"github.com/stellar/go-stellar-sdk/support/log"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/wallet-backend/internal/data"
	"github.com/stellar/wallet-backend/internal/db"
	"github.com/stellar/wallet-backend/internal/indexer"
)

// rawLedger is a decoded ledger handed from the fetcher to a process worker.
type rawLedger struct {
	seq  uint32
	meta xdr.LedgerCloseMeta
}

// processedLedger is the indexer output for one ledger, handed from a process worker to
// the flusher.
type processedLedger struct {
	seq       uint32
	closeTime time.Time
	buffer    *indexer.IndexerBuffer
}

// orderedBuffer turns out-of-order processed ledgers into contiguous ascending runs.
// Process workers finish in arbitrary order; orderedBuffer releases a ledger only once
// every earlier ledger has arrived, so the flusher always sees a gapless prefix.
type orderedBuffer struct {
	next    uint32
	pending map[uint32]processedLedger
}

func newOrderedBuffer(start uint32) *orderedBuffer {
	return &orderedBuffer{next: start, pending: make(map[uint32]processedLedger)}
}

// add records p and returns the contiguous run (possibly empty) now releasable.
func (o *orderedBuffer) add(p processedLedger) []processedLedger {
	o.pending[p.seq] = p
	var run []processedLedger
	for {
		next, ok := o.pending[o.next]
		if !ok {
			break
		}
		run = append(run, next)
		delete(o.pending, o.next)
		o.next++
	}
	return run
}

// fetchLedgers consumes [start,end] in ascending order from a single prepared backend,
// emitting decoded ledgers onto rawCh. It closes rawCh on return. A GetLedger error is
// fatal and returned immediately — the fork retries transient downloads internally, so a
// surfaced error is non-recoverable (fail-fast).
func (m *ingestService) fetchLedgers(ctx context.Context, backend ledgerbackend.LedgerBackend, start, end uint32, rawCh chan<- rawLedger) error {
	for seq := start; seq <= end; seq++ {
		meta, err := backend.GetLedger(ctx, seq)
		if err != nil {
			return fmt.Errorf("getting ledger %d: %w", seq, err)
		}
		select {
		case rawCh <- rawLedger{seq: seq, meta: meta}:
		case <-ctx.Done():
			return fmt.Errorf("context cancelled fetching ledger %d: %w", seq, ctx.Err())
		}
	}
	return nil
}

// processWorker reads decoded ledgers from rawCh, runs the indexer into a fresh buffer
// with its own serial indexer, and emits the result onto resultCh. Returns on rawCh close
// or context cancellation.
func (m *ingestService) processWorker(ctx context.Context, idx *indexer.Indexer, rawCh <-chan rawLedger, resultCh chan<- processedLedger) error {
	for raw := range rawCh {
		buffer := indexer.NewIndexerBuffer()
		if err := m.processLedgerWith(ctx, idx, raw.meta, buffer); err != nil {
			return err
		}
		select {
		case resultCh <- processedLedger{seq: raw.seq, closeTime: raw.meta.ClosedAt(), buffer: buffer}:
		case <-ctx.Done():
			return fmt.Errorf("context cancelled emitting ledger %d: %w", raw.seq, ctx.Err())
		}
	}
	return nil
}

// runFlusher consumes processed ledgers, reorders them into contiguous runs, flushes every
// flush-size ledgers in one transaction, advances the oldest cursor, and signals the
// compressor with the highest contiguously-persisted close time. start is the gap's first
// ledger sequence.
func (m *ingestService) runFlusher(ctx context.Context, start uint32, resultCh <-chan processedLedger, compressor *backfillCompressor) error {
	ob := newOrderedBuffer(start)
	merged := indexer.NewIndexerBuffer()
	var (
		count         uint32
		lowestSeq     uint32
		contiguousEnd time.Time
	)

	flush := func() error {
		if count == 0 {
			return nil
		}
		if err := m.flushBackfillBuffer(ctx, merged, lowestSeq); err != nil {
			return err
		}
		m.appMetrics.Ingestion.OldestLedger.Set(float64(lowestSeq))
		compressor.trigger(contiguousEnd)
		merged = indexer.NewIndexerBuffer()
		count = 0
		return nil
	}

	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("context cancelled in flusher: %w", ctx.Err())
		case p, ok := <-resultCh:
			if !ok {
				return flush() // final partial flush at stream end
			}
			for _, r := range ob.add(p) {
				if count == 0 {
					lowestSeq = r.seq
				}
				merged.Merge(r.buffer)
				contiguousEnd = r.closeTime
				count++
				if count >= m.backfillFlushSize {
					if err := flush(); err != nil {
						return err
					}
				}
			}
		}
	}
}

// flushBackfillBuffer persists one merged buffer and advances the oldest cursor in a single
// transaction, with retry. synchronous_commit is disabled for the transaction — safe for
// backfill because a crash before WAL flush just re-creates the gap, which the next run
// detects and refills.
func (m *ingestService) flushBackfillBuffer(ctx context.Context, buffer *indexer.IndexerBuffer, lowestSeq uint32) error {
	var lastErr error
	for attempt := 0; attempt < maxIngestProcessedDataRetries; attempt++ {
		select {
		case <-ctx.Done():
			return fmt.Errorf("context cancelled: %w", ctx.Err())
		default:
		}

		err := db.RunInTransaction(ctx, m.models.DB, func(dbTx pgx.Tx) error {
			if _, txErr := dbTx.Exec(ctx, "SET LOCAL synchronous_commit = off"); txErr != nil {
				return fmt.Errorf("setting synchronous_commit=off: %w", txErr)
			}
			if _, _, insErr := m.insertIntoDB(ctx, dbTx, buffer); insErr != nil {
				return fmt.Errorf("inserting processed data: %w", insErr)
			}
			if curErr := m.models.IngestStore.UpdateMin(ctx, dbTx, m.oldestLedgerCursorName, lowestSeq); curErr != nil {
				return fmt.Errorf("updating oldest cursor: %w", curErr)
			}
			return nil
		})
		if err == nil {
			return nil
		}
		lastErr = err
		m.appMetrics.Ingestion.RetriesTotal.WithLabelValues("batch_flush").Inc()
		if attempt == maxIngestProcessedDataRetries-1 {
			break
		}
		backoff := time.Duration(1<<attempt) * time.Second
		if backoff > maxIngestProcessedDataRetryBackoff {
			backoff = maxIngestProcessedDataRetryBackoff
		}
		log.Ctx(ctx).Warnf("Error flushing backfill buffer (attempt %d/%d): %v, retrying in %v...",
			attempt+1, maxIngestProcessedDataRetries, lastErr, backoff)
		select {
		case <-ctx.Done():
			return fmt.Errorf("context cancelled during backoff: %w", ctx.Err())
		case <-time.After(backoff):
		}
	}
	m.appMetrics.Ingestion.RetryExhaustionsTotal.WithLabelValues("batch_flush").Inc()
	return lastErr
}

// runGapPipeline processes one contiguous gap through fetch → process → flush. It owns a
// single backend (the fork's single-consumer contract requires exactly one sequential
// GetLedger caller) and tears the pipeline down on the first error (fail-fast).
func (m *ingestService) runGapPipeline(ctx context.Context, gap data.LedgerRange, compressor *backfillCompressor) error {
	backend, err := m.ledgerBackendFactory(ctx)
	if err != nil {
		return fmt.Errorf("creating ledger backend: %w", err)
	}
	defer func() {
		if closeErr := backend.Close(); closeErr != nil {
			log.Ctx(ctx).Warnf("Error closing backend for gap [%d-%d]: %v", gap.GapStart, gap.GapEnd, closeErr)
		}
	}()
	if err := backend.PrepareRange(ctx, ledgerbackend.BoundedRange(gap.GapStart, gap.GapEnd)); err != nil {
		return fmt.Errorf("preparing backend range [%d-%d]: %w", gap.GapStart, gap.GapEnd, err)
	}

	chanCap := max(2*int(m.backfillWorkers), 64)
	rawCh := make(chan rawLedger, chanCap)
	resultCh := make(chan processedLedger, chanCap)

	g, gctx := errgroup.WithContext(ctx)

	g.Go(func() error {
		defer close(rawCh)
		return m.fetchLedgers(gctx, backend, gap.GapStart, gap.GapEnd, rawCh)
	})

	var wg sync.WaitGroup
	for range m.backfillWorkers {
		wg.Add(1)
		idx := indexer.NewIndexer(m.networkPassphrase, nil, m.appMetrics.Ingestion)
		g.Go(func() error {
			defer wg.Done()
			return m.processWorker(gctx, idx, rawCh, resultCh)
		})
	}
	// Close resultCh once every worker (its only senders) has returned — the canonical
	// fan-in close. This goroutine ends when wg drains, which happens on completion or
	// cancellation, so it never leaks.
	go func() {
		wg.Wait()
		close(resultCh)
	}()

	g.Go(func() error {
		return m.runFlusher(gctx, gap.GapStart, resultCh, compressor)
	})

	return g.Wait()
}
