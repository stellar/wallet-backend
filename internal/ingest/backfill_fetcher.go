// backfillFetcher downloads S3 ledger files in parallel and fans out
// individual LedgerCloseMeta entries directly to an external channel.
// No ordering guarantees — designed for backfill where consumers
// handle ledgers independently and the watermark tracker handles
// out-of-order flushes.
package ingest

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/klauspost/compress/zstd"
	"github.com/stellar/go-stellar-sdk/support/datastore"
	"github.com/stellar/go-stellar-sdk/support/log"
	"github.com/stellar/go-stellar-sdk/xdr"
)

// zstdDecoderPool reuses zstd decoders across fetch workers.
// Each decoder allocates internal buffers on first use; pooling avoids
// re-allocating them on every S3 file download (~39 GB of churn per gap).
var zstdDecoderPool = sync.Pool{
	New: func() any {
		d, err := zstd.NewReader(nil, zstd.WithDecoderConcurrency(1))
		if err != nil {
			panic(fmt.Sprintf("creating zstd decoder: %v", err))
		}
		return d
	},
}

// BackfillFetcherConfig configures the parallel S3 fetcher for backfill.
type BackfillFetcherConfig struct {
	NumWorkers uint32
	RetryLimit uint32
	RetryWait  time.Duration
	GapStart   uint32
	GapEnd     uint32
	// Optional Prometheus observers (nil-safe — no metrics if nil).
	OnFetchDuration func(seconds float64)
	OnChannelWait   func(channel, direction string, seconds float64)
}

// BackfillFetchStats accumulates timing from all fetch workers.
// Returned by Run for gap summary log aggregation.
type BackfillFetchStats struct {
	FetchCount  int
	FetchTotal  time.Duration
	ChannelWait map[string]time.Duration // key: "channel:direction"
}

// backfillFetcher downloads S3 ledger files in parallel and pushes
// individual LedgerCloseMeta entries to an external channel.
type backfillFetcher struct {
	dataStore datastore.DataStore
	schema    datastore.DataStoreSchema
	config    BackfillFetcherConfig

	ledgerCh chan<- xdr.LedgerCloseMeta // external, caller-owned
}

// NewBackfillFetcher creates a fetcher that will push ledgers from the
// configured gap range to ledgerCh. Call Run to start workers.
func NewBackfillFetcher(
	config BackfillFetcherConfig,
	ds datastore.DataStore,
	schema datastore.DataStoreSchema,
	ledgerCh chan<- xdr.LedgerCloseMeta,
) *backfillFetcher {
	return &backfillFetcher{
		dataStore: ds,
		schema:    schema,
		config:    config,
		ledgerCh:  ledgerCh,
	}
}

// Run starts fetch workers, waits for them to complete, then closes ledgerCh.
// Returns aggregated fetch stats for the gap summary log.
// On error, calls cancel with the cause; the caller checks context.Cause.
func (f *backfillFetcher) Run(ctx context.Context, cancel context.CancelCauseFunc) *BackfillFetchStats {
	defer close(f.ledgerCh)

	// Compute all file-start sequences up-front (bounded range).
	startBoundary := f.schema.GetSequenceNumberStartBoundary(f.config.GapStart)
	endBoundary := f.schema.GetSequenceNumberStartBoundary(f.config.GapEnd)

	taskCh := make(chan uint32, f.config.NumWorkers*4)

	// Seed tasks in a goroutine to avoid blocking if taskCh fills.
	go func() {
		defer close(taskCh)
		for seq := startBoundary; seq <= endBoundary; seq += f.schema.LedgersPerFile {
			select {
			case taskCh <- seq:
			case <-ctx.Done():
				return
			}
		}
	}()

	// Collect per-worker stats.
	statsCh := make(chan *BackfillFetchStats, f.config.NumWorkers)

	var wg sync.WaitGroup
	for i := uint32(0); i < f.config.NumWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			stats := f.worker(ctx, cancel, taskCh)
			statsCh <- stats
		}()
	}
	wg.Wait()
	close(statsCh)

	// Aggregate stats from all workers.
	agg := &BackfillFetchStats{ChannelWait: make(map[string]time.Duration)}
	for ws := range statsCh {
		agg.FetchCount += ws.FetchCount
		agg.FetchTotal += ws.FetchTotal
		for k, v := range ws.ChannelWait {
			agg.ChannelWait[k] += v
		}
	}
	return agg
}

// worker processes file-start sequences from taskCh until the channel
// closes or the context is cancelled.
func (f *backfillFetcher) worker(ctx context.Context, cancel context.CancelCauseFunc, taskCh <-chan uint32) *BackfillFetchStats {
	stats := &BackfillFetchStats{ChannelWait: make(map[string]time.Duration)}

	for sequence := range taskCh {
		if ctx.Err() != nil {
			return stats
		}

		fetchStart := time.Now()
		batch, err := f.downloadWithRetry(ctx, cancel, sequence)
		fetchDur := time.Since(fetchStart)
		if err != nil {
			return stats // cancel already called by downloadWithRetry
		}
		stats.FetchCount++
		stats.FetchTotal += fetchDur

		if f.config.OnFetchDuration != nil {
			f.config.OnFetchDuration(fetchDur.Seconds())
		}

		// Fan out individual ledgers to ledgerCh, filtering to gap range.
		for i := range batch.LedgerCloseMetas {
			lcm := batch.LedgerCloseMetas[i]
			batch.LedgerCloseMetas[i] = xdr.LedgerCloseMeta{} // allow GC to collect
			seq := lcm.LedgerSequence()
			if seq < f.config.GapStart || seq > f.config.GapEnd {
				continue
			}

			sendStart := time.Now()
			select {
			case f.ledgerCh <- lcm:
				sendDur := time.Since(sendStart)
				stats.ChannelWait["ledger:send"] += sendDur
				if f.config.OnChannelWait != nil {
					f.config.OnChannelWait("ledger", "send", sendDur.Seconds())
				}
			case <-ctx.Done():
				return stats
			}
		}
	}
	return stats
}

// downloadWithRetry downloads and decodes a single S3 file with retry.
// On permanent failure, calls cancel and returns the error.
func (f *backfillFetcher) downloadWithRetry(ctx context.Context, cancel context.CancelCauseFunc, sequence uint32) (xdr.LedgerCloseMetaBatch, error) {
	for attempt := uint32(0); attempt <= f.config.RetryLimit; attempt++ {
		batch, err := f.downloadAndDecode(ctx, sequence)
		if err != nil {
			if ctx.Err() != nil {
				return xdr.LedgerCloseMetaBatch{}, fmt.Errorf("context cancelled for sequence %d: %w", sequence, ctx.Err())
			}
			if errors.Is(err, os.ErrNotExist) {
				cancel(fmt.Errorf("ledger file for sequence %d not found: %w", sequence, err))
				return xdr.LedgerCloseMetaBatch{}, err
			}
			if attempt == f.config.RetryLimit {
				cancel(fmt.Errorf("downloading ledger file for sequence %d: maximum retries (%d) exceeded: %w",
					sequence, f.config.RetryLimit, err))
				return xdr.LedgerCloseMetaBatch{}, err
			}
			log.WithField("sequence", sequence).WithError(err).
				Warnf("Backfill fetch error (attempt %d/%d), retrying...", attempt+1, f.config.RetryLimit)
			if !sleepWithContext(ctx, f.config.RetryWait) {
				return xdr.LedgerCloseMetaBatch{}, fmt.Errorf("context cancelled during retry wait for sequence %d: %w", sequence, ctx.Err())
			}
			continue
		}
		return batch, nil
	}
	// Unreachable, but Go requires a return.
	return xdr.LedgerCloseMetaBatch{}, fmt.Errorf("unreachable: retry loop exited for sequence %d", sequence)
}

// downloadAndDecode fetches and stream-decodes a single S3 file.
// Uses pooled zstd decoders to avoid reallocating internal buffers per file.
func (f *backfillFetcher) downloadAndDecode(ctx context.Context, sequence uint32) (xdr.LedgerCloseMetaBatch, error) {
	objectKey := f.schema.GetObjectKeyFromSequenceNumber(sequence)
	reader, err := f.dataStore.GetFile(ctx, objectKey)
	if err != nil {
		return xdr.LedgerCloseMetaBatch{}, fmt.Errorf("fetching ledger file %s: %w", objectKey, err)
	}
	defer reader.Close() //nolint:errcheck

	buffered := bufio.NewReaderSize(reader, 256*1024)

	dec := zstdDecoderPool.Get().(*zstd.Decoder)
	defer zstdDecoderPool.Put(dec)
	if err := dec.Reset(buffered); err != nil {
		return xdr.LedgerCloseMetaBatch{}, fmt.Errorf("resetting zstd decoder for %s: %w", objectKey, err)
	}

	var batch xdr.LedgerCloseMetaBatch
	if _, err := xdr.Unmarshal(dec, &batch); err != nil {
		return xdr.LedgerCloseMetaBatch{}, fmt.Errorf("decoding ledger file %s: %w", objectKey, err)
	}
	return batch, nil
}
