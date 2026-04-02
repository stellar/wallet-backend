// optimizedStorageBackend is a fork of the SDK's BufferedStorageBackend, tailored for
// wallet-backend where exactly one goroutine consumes ledgers sequentially from each
// backend instance. This single-consumer contract enables several optimizations:
//
//   - No mutexes: The SDK uses an RWMutex on every method. With a single consumer,
//     no synchronization is needed on the backend struct.
//
//   - Workers decode: The SDK workers download compressed bytes and pass them through
//     a channel; the caller thread then decompresses (zstd) and unmarshals (XDR)
//     synchronously in getFromLedgerQueue(). We move decompression + XDR decode into
//     the worker goroutines, so GetLedger receives pre-decoded LedgerCloseMetaBatch
//     with zero processing on the caller thread.
//
//   - Stream-through S3 decode: The SDK does io.ReadAll(reader) to buffer the entire
//     compressed file (~5-10MB), then feeds bytes.NewReader to the decoder. We pass
//     the S3 io.ReadCloser directly to the decoder, streaming through zstd without
//     an intermediate allocation.
//
// Callers MUST NOT share an optimizedStorageBackend instance across goroutines.
package ingest

import (
	"context"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/stellar/go-stellar-sdk/ingest/ledgerbackend"
	"github.com/stellar/go-stellar-sdk/support/collections/heap"
	"github.com/stellar/go-stellar-sdk/support/compressxdr"
	"github.com/stellar/go-stellar-sdk/support/datastore"
	"github.com/stellar/go-stellar-sdk/support/log"
	"github.com/stellar/go-stellar-sdk/xdr"
)

const (
	defaultStorageBufferSize uint32 = 100
)

// optimizedStorageBackend implements ledgerbackend.LedgerBackend with optimizations
// for sequential bulk access: workers pre-decode S3 files, GetLedger is lock-free.
type optimizedStorageBackend struct {
	config    ledgerbackend.BufferedStorageBackendConfig
	dataStore datastore.DataStore
	schema    datastore.DataStoreSchema

	// All fields below accessed by single goroutine — no mutex needed.
	buffer     *storageBuffer
	prepared   *ledgerbackend.Range
	closed     bool
	lcmBatch   xdr.LedgerCloseMetaBatch // current cached decoded batch
	nextLedger uint32
}

// decodedBatch pairs a decoded LedgerCloseMetaBatch with its file-start sequence
// for ordering in the priority queue.
type decodedBatch struct {
	batch       xdr.LedgerCloseMetaBatch
	startLedger uint32
}

// storageBuffer manages worker goroutines that download and decode ledger files
// from the datastore, delivering them in sequence via batchQueue.
type storageBuffer struct {
	config    ledgerbackend.BufferedStorageBackendConfig
	dataStore datastore.DataStore
	schema    datastore.DataStoreSchema

	ctx    context.Context
	cancel context.CancelCauseFunc
	wg     sync.WaitGroup

	taskQueue  chan uint32                   // file-start sequences for workers to download
	batchQueue chan xdr.LedgerCloseMetaBatch // decoded batches, delivered in sequential order

	// priorityQueue and currentLedger are accessed by multiple workers under lock.
	// Workers push decoded batches, then drain in-order to batchQueue.
	priorityQueue     *heap.Heap[decodedBatch]
	priorityQueueLock sync.Mutex
	currentLedger     uint32 // next expected file-start for ordering

	// nextTaskLedger is only accessed from the single consumer thread
	// (via getNextBatch → pushTaskQueue) and the constructor (before workers start).
	nextTaskLedger uint32
	ledgerRange    ledgerbackend.Range
}

// newOptimizedStorageBackend creates an optimized storage backend.
// Workers are NOT started here — they start in PrepareRange.
func newOptimizedStorageBackend(
	config ledgerbackend.BufferedStorageBackendConfig,
	dataStore datastore.DataStore,
	schema datastore.DataStoreSchema,
) (*optimizedStorageBackend, error) {
	if schema.LedgersPerFile == 0 {
		return nil, fmt.Errorf("LedgersPerFile must be > 0")
	}
	if config.BufferSize == 0 {
		config.BufferSize = defaultStorageBufferSize
	}
	if config.NumWorkers == 0 {
		config.NumWorkers = 10
	}
	if config.NumWorkers > config.BufferSize {
		return nil, fmt.Errorf("NumWorkers (%d) must be <= BufferSize (%d)", config.NumWorkers, config.BufferSize)
	}

	return &optimizedStorageBackend{
		config:    config,
		dataStore: dataStore,
		schema:    schema,
	}, nil
}

// PrepareRange initializes the backend for sequential consumption of the given range.
// If a previous range was prepared, its buffer is closed first (supporting re-preparation).
func (b *optimizedStorageBackend) PrepareRange(_ context.Context, ledgerRange ledgerbackend.Range) error {
	if b.closed {
		return fmt.Errorf("optimizedStorageBackend is closed")
	}

	// Support re-preparation by closing the old buffer.
	if b.buffer != nil {
		b.buffer.close()
		b.buffer = nil
	}

	buf := b.newStorageBuffer(ledgerRange)
	b.buffer = buf
	b.prepared = &ledgerRange
	b.nextLedger = ledgerRange.From()
	b.lcmBatch = xdr.LedgerCloseMetaBatch{} // reset cached batch

	return nil
}

func (b *optimizedStorageBackend) newStorageBuffer(ledgerRange ledgerbackend.Range) *storageBuffer {
	ctx, cancel := context.WithCancelCause(context.Background())

	startBoundary := b.schema.GetSequenceNumberStartBoundary(ledgerRange.From())

	bufferSize := b.config.BufferSize
	// For bounded ranges, don't allocate more buffer than total files.
	if ledgerRange.Bounded() {
		endBoundary := b.schema.GetSequenceNumberEndBoundary(ledgerRange.To())
		totalFiles := (endBoundary-startBoundary)/b.schema.LedgersPerFile + 1
		if totalFiles < uint32(bufferSize) {
			bufferSize = totalFiles
		}
	}

	pq := heap.New(func(a, b decodedBatch) bool {
		return a.startLedger < b.startLedger
	}, int(bufferSize))

	buf := &storageBuffer{
		config:         b.config,
		dataStore:      b.dataStore,
		schema:         b.schema,
		ctx:            ctx,
		cancel:         cancel,
		taskQueue:      make(chan uint32, bufferSize),
		batchQueue:     make(chan xdr.LedgerCloseMetaBatch, bufferSize),
		priorityQueue:  pq,
		currentLedger:  startBoundary,
		nextTaskLedger: startBoundary,
		ledgerRange:    ledgerRange,
	}

	// Seed task queue with initial tasks. The +1 matches the SDK's buffer invariant:
	// len(taskQueue) + len(batchQueue) + priorityQueue.Len() <= bufferSize,
	// and the extra task accounts for the one being actively processed by a worker.
	for i := uint32(0); i <= bufferSize; i++ {
		if !buf.pushTaskQueue() {
			break
		}
	}

	// Start workers.
	for i := uint32(0); i < b.config.NumWorkers; i++ {
		buf.wg.Add(1)
		go buf.worker()
	}

	return buf
}

// GetLedger returns the ledger metadata for the given sequence number.
//
// Optimizations vs SDK BufferedStorageBackend.GetLedger:
//   - No lock acquired. The SDK holds RLock for the entire method.
//   - 3 validation checks instead of 5 (no lastLedger/nextExpectedSequence indirection).
//   - Batch is already decoded by a worker goroutine. The SDK decodes on this thread
//     via getFromLedgerQueue → compressxdr.NewXDRDecoder.ReadFrom.
func (b *optimizedStorageBackend) GetLedger(ctx context.Context, sequence uint32) (xdr.LedgerCloseMeta, error) {
	if b.closed {
		return xdr.LedgerCloseMeta{}, fmt.Errorf("optimizedStorageBackend is closed")
	}
	if b.prepared == nil {
		return xdr.LedgerCloseMeta{}, fmt.Errorf("optimizedStorageBackend must be prepared before calling GetLedger")
	}

	r := *b.prepared
	if sequence < r.From() || (r.Bounded() && sequence > r.To()) {
		return xdr.LedgerCloseMeta{}, fmt.Errorf("requested ledger %d is outside prepared range %s", sequence, r)
	}
	if sequence != b.nextLedger {
		return xdr.LedgerCloseMeta{}, fmt.Errorf("requested ledger %d is not the expected next ledger %d", sequence, b.nextLedger)
	}

	// Check if sequence is within the currently cached batch.
	// The len check guards against the zero-value batch (StartSequence=0, EndSequence=0).
	if len(b.lcmBatch.LedgerCloseMetas) > 0 &&
		sequence >= uint32(b.lcmBatch.StartSequence) && sequence <= uint32(b.lcmBatch.EndSequence) {
		lcm, err := b.lcmBatch.GetLedger(sequence)
		if err != nil {
			return xdr.LedgerCloseMeta{}, fmt.Errorf("extracting ledger %d from cached batch: %w", sequence, err)
		}
		b.nextLedger++
		return lcm, nil
	}

	// Need the next batch from the buffer.
	batch, err := b.buffer.getNextBatch(ctx)
	if err != nil {
		return xdr.LedgerCloseMeta{}, fmt.Errorf("getting next batch: %w", err)
	}
	b.lcmBatch = batch

	lcm, err := b.lcmBatch.GetLedger(sequence)
	if err != nil {
		return xdr.LedgerCloseMeta{}, fmt.Errorf("extracting ledger %d from batch: %w", sequence, err)
	}
	b.nextLedger++
	return lcm, nil
}

// GetLatestLedgerSequence returns the upper bound of the prepared range.
// For unbounded ranges this returns 0 (the range has no upper bound).
func (b *optimizedStorageBackend) GetLatestLedgerSequence(_ context.Context) (uint32, error) {
	if b.closed {
		return 0, fmt.Errorf("optimizedStorageBackend is closed")
	}
	if b.prepared == nil {
		return 0, fmt.Errorf("optimizedStorageBackend must be prepared before calling GetLatestLedgerSequence")
	}
	return b.prepared.To(), nil
}

// IsPrepared reports whether the backend is prepared for the given range.
func (b *optimizedStorageBackend) IsPrepared(_ context.Context, ledgerRange ledgerbackend.Range) (bool, error) {
	if b.closed {
		return false, fmt.Errorf("optimizedStorageBackend is closed")
	}
	if b.prepared == nil {
		return false, nil
	}
	return b.prepared.Contains(ledgerRange), nil
}

// Close marks the backend as closed and shuts down the buffer.
// Subsequent calls to any method will return an error.
func (b *optimizedStorageBackend) Close() error {
	b.closed = true
	if b.buffer != nil {
		b.buffer.close()
		b.buffer = nil
	}
	return nil
}

// worker is a goroutine that reads file-start sequences from taskQueue,
// downloads and decodes the corresponding ledger file from the datastore,
// and delivers the decoded batch via storeDecoded.
func (buf *storageBuffer) worker() {
	defer buf.wg.Done()
	for {
		select {
		case <-buf.ctx.Done():
			return
		case sequence := <-buf.taskQueue:
			buf.downloadAndStore(sequence)
		}
	}
}

func (buf *storageBuffer) downloadAndStore(sequence uint32) {
	for attempt := uint32(0); attempt <= buf.config.RetryLimit; attempt++ {
		batch, err := buf.downloadAndDecode(sequence)
		if err != nil {
			if buf.ctx.Err() != nil {
				return // context cancelled, stop retrying
			}
			if os.IsNotExist(err) {
				if !buf.ledgerRange.Bounded() {
					// Unbounded range: file may not exist yet, wait and retry.
					if !sleepWithContext(buf.ctx, buf.config.RetryWait) {
						return
					}
					continue
				}
				// Bounded range: missing file is a hard error.
				buf.cancel(fmt.Errorf("ledger file for sequence %d not found: %w", sequence, err))
				return
			}
			if attempt == buf.config.RetryLimit {
				buf.cancel(fmt.Errorf("downloading ledger file for sequence %d: maximum retries (%d) exceeded: %w",
					sequence, buf.config.RetryLimit, err))
				return
			}
			log.WithField("sequence", sequence).WithError(err).
				Warnf("Failed to download ledger file (attempt %d/%d), retrying...", attempt+1, buf.config.RetryLimit)
			if !sleepWithContext(buf.ctx, buf.config.RetryWait) {
				return
			}
			continue
		}

		buf.storeDecoded(batch, sequence)
		return
	}
}

// downloadAndDecode fetches a ledger file from the datastore and stream-decodes it.
//
// Optimization: The SDK does io.ReadAll(reader) to buffer the entire compressed file,
// then feeds bytes.NewReader to the zstd+XDR decoder. We pass the S3 io.ReadCloser
// directly to the decoder, streaming through zstd without an intermediate allocation.
func (buf *storageBuffer) downloadAndDecode(sequence uint32) (xdr.LedgerCloseMetaBatch, error) {
	objectKey := buf.schema.GetObjectKeyFromSequenceNumber(sequence)
	reader, err := buf.dataStore.GetFile(buf.ctx, objectKey)
	if err != nil {
		return xdr.LedgerCloseMetaBatch{}, err
	}
	defer reader.Close()

	var batch xdr.LedgerCloseMetaBatch
	decoder := compressxdr.NewXDRDecoder(compressxdr.DefaultCompressor, &batch)
	if _, err = decoder.ReadFrom(reader); err != nil {
		return xdr.LedgerCloseMetaBatch{}, fmt.Errorf("decoding ledger file %s: %w", objectKey, err)
	}
	return batch, nil
}

// storeDecoded adds a decoded batch to the priority queue and drains in-order
// batches to batchQueue. Items are collected under the lock and sent outside it
// to avoid blocking a worker while holding priorityQueueLock.
func (buf *storageBuffer) storeDecoded(batch xdr.LedgerCloseMetaBatch, sequence uint32) {
	var toSend []xdr.LedgerCloseMetaBatch

	buf.priorityQueueLock.Lock()
	buf.priorityQueue.Push(decodedBatch{batch: batch, startLedger: sequence})
	// Drain in-order: transfer from priority queue while head matches expected sequence.
	// The buffer invariant is maintained because items move from priorityQueue to batchQueue
	// (same total count).
	for buf.priorityQueue.Len() > 0 && buf.currentLedger == buf.priorityQueue.Peek().startLedger {
		item := buf.priorityQueue.Pop()
		toSend = append(toSend, item.batch)
		buf.currentLedger += buf.schema.LedgersPerFile
	}
	buf.priorityQueueLock.Unlock()

	// Send outside lock to avoid blocking while holding priorityQueueLock.
	for _, b := range toSend {
		select {
		case buf.batchQueue <- b:
		case <-buf.ctx.Done():
			return
		}
	}
}

// getNextBatch receives the next decoded batch from the buffer in sequence order.
// After receiving, it enqueues a new download task to maintain the buffer invariant.
func (buf *storageBuffer) getNextBatch(ctx context.Context) (xdr.LedgerCloseMetaBatch, error) {
	select {
	case <-buf.ctx.Done():
		return xdr.LedgerCloseMetaBatch{}, context.Cause(buf.ctx)
	case <-ctx.Done():
		return xdr.LedgerCloseMetaBatch{}, ctx.Err()
	case batch := <-buf.batchQueue:
		// Replenish: enqueue next download task. pushTaskQueue only modifies
		// nextTaskLedger, which is safe because it is only called from the single
		// consumer thread (here) and the constructor (before workers start).
		buf.pushTaskQueue()
		return batch, nil
	}
}

// pushTaskQueue enqueues the next file-start sequence for download.
// Returns false if the range boundary has been reached (no task enqueued).
//
// Concurrency: only called from the single consumer thread (getNextBatch)
// and the constructor (before workers start), so no lock on nextTaskLedger.
func (buf *storageBuffer) pushTaskQueue() bool {
	if buf.ledgerRange.Bounded() {
		endBoundary := buf.schema.GetSequenceNumberStartBoundary(buf.ledgerRange.To())
		if buf.nextTaskLedger > endBoundary {
			return false
		}
	}

	select {
	case buf.taskQueue <- buf.nextTaskLedger:
		buf.nextTaskLedger += buf.schema.LedgersPerFile
		return true
	case <-buf.ctx.Done():
		return false
	}
}

// close cancels the buffer context and waits for all workers to exit.
// This prevents goroutine leaks — the caller blocks until all workers are done.
func (buf *storageBuffer) close() {
	buf.cancel(context.Canceled)
	buf.wg.Wait()
}

// sleepWithContext waits for the given duration or until the context is cancelled.
// Returns true if the sleep completed, false if cancelled.
func sleepWithContext(ctx context.Context, d time.Duration) bool {
	select {
	case <-ctx.Done():
		return false
	case <-time.After(d):
		return true
	}
}
