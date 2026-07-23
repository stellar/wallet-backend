// datastoreBackend is a fork of the SDK's BufferedStorageBackend, tailored for
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
// Callers MUST NOT share an datastoreBackend instance across goroutines.
package ingest

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/stellar/go-stellar-sdk/ingest/ledgerbackend"
	"github.com/stellar/go-stellar-sdk/support/collections/heap"
	"github.com/stellar/go-stellar-sdk/support/compressxdr"
	"github.com/stellar/go-stellar-sdk/support/datastore"
	"github.com/stellar/go-stellar-sdk/support/log"
	"github.com/stellar/go-stellar-sdk/xdr"
)

const (
	defaultBufferSize uint32 = 100
	defaultNumWorkers uint32 = 10
	defaultRetryLimit uint32 = 3
	defaultRetryWait         = 5 * time.Second
)

// ErrBufferDead marks a GetLedger failure as terminal: the ledgerBuffer's
// internal context was cancelled by the buffer itself (a worker exhausted
// its download retry budget, or hit a hard NotExist on a bounded range),
// independent of the caller's context lifecycle. The buffer never recovers
// from this — every subsequent GetLedger call on this backend instance
// returns the same error — so callers should treat it as permanent rather
// than retrying against a dead buffer.
var ErrBufferDead = errors.New("ledger buffer permanently cancelled")

var _ ledgerbackend.LedgerBackend = (*datastoreBackend)(nil)

// datastoreBackend implements ledgerbackend.LedgerBackend with optimizations
// for sequential bulk access: workers pre-decode S3 files, GetLedger is lock-free.
type datastoreBackend struct {
	config    ledgerbackend.BufferedStorageBackendConfig
	dataStore datastore.DataStore
	schema    datastore.DataStoreSchema

	// All fields below accessed by single goroutine — no mutex needed.
	buffer     *ledgerBuffer
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

// ledgerBuffer manages worker goroutines that download and decode ledger files from the
// datastore. Workers feed decoded batches (in arbitrary order) to decodedQueue; a single
// drainer goroutine reorders them and delivers in sequence via batchQueue.
type ledgerBuffer struct {
	config    ledgerbackend.BufferedStorageBackendConfig
	dataStore datastore.DataStore
	schema    datastore.DataStoreSchema

	ctx    context.Context
	cancel context.CancelCauseFunc
	wg     sync.WaitGroup

	taskQueue    chan uint32                   // file-start sequences for workers to download
	decodedQueue chan decodedBatch             // decoded batches from workers, unordered
	batchQueue   chan xdr.LedgerCloseMetaBatch // decoded batches, delivered in sequential order

	// priorityQueue and currentLedger are owned exclusively by the single drainer goroutine,
	// so they need no lock. currentLedger is atomic only because GetLatestLedgerSequence reads
	// it from the consumer goroutine to report the prefetch frontier.
	priorityQueue *heap.Heap[decodedBatch]
	currentLedger atomic.Uint32 // next expected file-start to deliver in order

	// nextTaskLedger is accessed only by the constructor's seed loop and the single consumer
	// (via getNextBatch → pushTaskQueue); these never overlap, so no lock is needed.
	nextTaskLedger uint32
	ledgerRange    ledgerbackend.Range
}

// newDatastoreBackend creates a datastore backend.
// Workers are NOT started here — they start in PrepareRange.
func newDatastoreBackend(
	config ledgerbackend.BufferedStorageBackendConfig,
	dataStore datastore.DataStore,
	schema datastore.DataStoreSchema,
) (*datastoreBackend, error) {
	if schema.LedgersPerFile == 0 {
		return nil, fmt.Errorf("LedgersPerFile must be > 0")
	}
	if config.BufferSize == 0 {
		config.BufferSize = defaultBufferSize
	}
	if config.NumWorkers == 0 {
		config.NumWorkers = defaultNumWorkers
	}
	if config.RetryLimit == 0 {
		config.RetryLimit = defaultRetryLimit
	}
	if config.RetryWait <= 0 {
		config.RetryWait = defaultRetryWait
	}
	if config.NumWorkers > config.BufferSize {
		return nil, fmt.Errorf("NumWorkers (%d) must be <= BufferSize (%d)", config.NumWorkers, config.BufferSize)
	}

	log.Infof("Using datastore backend with buffer size %d, %d workers", config.BufferSize, config.NumWorkers)
	return &datastoreBackend{
		config:    config,
		dataStore: dataStore,
		schema:    schema,
	}, nil
}

// PrepareRange initializes the backend for sequential consumption of the given range.
// If a previous range was prepared, its buffer is closed first (supporting re-preparation).
// The buffer's download/decode workers run until ctx is cancelled or Close() is called.
func (b *datastoreBackend) PrepareRange(ctx context.Context, ledgerRange ledgerbackend.Range) error {
	if b.closed {
		return fmt.Errorf("datastoreBackend is closed")
	}

	// Support re-preparation by closing the old buffer.
	if b.buffer != nil {
		b.buffer.close()
		b.buffer = nil
	}

	buf := b.newLedgerBuffer(ctx, ledgerRange)
	b.buffer = buf
	b.prepared = &ledgerRange
	b.nextLedger = ledgerRange.From()
	b.lcmBatch = xdr.LedgerCloseMetaBatch{} // reset cached batch

	return nil
}

func (b *datastoreBackend) newLedgerBuffer(ctx context.Context, ledgerRange ledgerbackend.Range) *ledgerBuffer {
	ctx, cancel := context.WithCancelCause(ctx)

	startBoundary := b.schema.GetSequenceNumberStartBoundary(ledgerRange.From())

	bufferSize := b.config.BufferSize
	// For bounded ranges, don't allocate more buffer than total files.
	if ledgerRange.Bounded() {
		endBoundary := b.schema.GetSequenceNumberEndBoundary(ledgerRange.To())
		totalFiles := (endBoundary-startBoundary)/b.schema.LedgersPerFile + 1
		if totalFiles < bufferSize {
			bufferSize = totalFiles
		}
	}

	pq := heap.New(func(a, b decodedBatch) bool {
		return a.startLedger < b.startLedger
	}, int(bufferSize))

	buf := &ledgerBuffer{
		config:         b.config,
		dataStore:      b.dataStore,
		schema:         b.schema,
		ctx:            ctx,
		cancel:         cancel,
		taskQueue:      make(chan uint32, bufferSize),
		decodedQueue:   make(chan decodedBatch, bufferSize),
		batchQueue:     make(chan xdr.LedgerCloseMetaBatch, bufferSize),
		priorityQueue:  pq,
		nextTaskLedger: startBoundary,
		ledgerRange:    ledgerRange,
	}
	buf.currentLedger.Store(startBoundary)

	// Start the single drainer + the download workers before seeding, so they drain taskQueue
	// (and decodedQueue) concurrently with the seed loop below. Seeding enqueues bufferSize+1
	// tasks into a bufferSize-capacity channel; an unbounded range has no boundary to stop the
	// final push, so it would block forever if nothing were draining.
	buf.wg.Add(1)
	go buf.drainer()
	for i := uint32(0); i < b.config.NumWorkers; i++ {
		buf.wg.Add(1)
		go buf.worker()
	}

	// Seed task queue with initial tasks. The +1 matches the SDK's buffer invariant:
	// len(taskQueue) + len(batchQueue) + priorityQueue.Len() <= bufferSize,
	// and the extra task accounts for the one being actively processed by a worker.
	for i := uint32(0); i <= bufferSize; i++ {
		if !buf.pushTaskQueue() {
			break
		}
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
func (b *datastoreBackend) GetLedger(ctx context.Context, sequence uint32) (xdr.LedgerCloseMeta, error) {
	if b.closed {
		return xdr.LedgerCloseMeta{}, fmt.Errorf("datastoreBackend is closed")
	}
	if b.prepared == nil {
		return xdr.LedgerCloseMeta{}, fmt.Errorf("datastoreBackend must be prepared before calling GetLedger")
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

// GetLatestLedgerSequence returns the highest ledger the buffer has delivered so far — its
// prefetch frontier — mirroring the SDK BufferedStorageBackend. During bulk consumption the
// frontier runs ahead of the consumer; at the live tip it stalls at the last available ledger.
// Returns 0 before any batch has been delivered. This is what lets the migration's
// flushWindowsAtTip coalesce in bulk (seq <= frontier) and flush at the tip (seq > frontier),
// instead of flushing every ledger as it would if this returned the unbounded range's zero To().
func (b *datastoreBackend) GetLatestLedgerSequence(_ context.Context) (uint32, error) {
	if b.closed {
		return 0, fmt.Errorf("datastoreBackend is closed")
	}
	if b.prepared == nil || b.buffer == nil {
		return 0, fmt.Errorf("datastoreBackend must be prepared before calling GetLatestLedgerSequence")
	}
	startBoundary := b.schema.GetSequenceNumberStartBoundary(b.prepared.From())
	frontier := b.buffer.currentLedger.Load()
	if frontier <= startBoundary {
		return 0, nil // nothing delivered yet
	}
	return frontier - 1, nil
}

// IsPrepared reports whether the backend is prepared for the given range.
func (b *datastoreBackend) IsPrepared(_ context.Context, ledgerRange ledgerbackend.Range) (bool, error) {
	if b.closed {
		return false, fmt.Errorf("datastoreBackend is closed")
	}
	if b.prepared == nil {
		return false, nil
	}
	return b.prepared.Contains(ledgerRange), nil
}

// Close marks the backend as closed and shuts down the buffer.
// Subsequent calls to any method will return an error.
func (b *datastoreBackend) Close() error {
	b.closed = true
	if b.buffer != nil {
		b.buffer.close()
		b.buffer = nil
	}
	return nil
}

// worker is a goroutine that reads file-start sequences from taskQueue,
// downloads and decodes the corresponding ledger file from the datastore,
// and hands the decoded batch to the drainer via decodedQueue.
func (buf *ledgerBuffer) worker() {
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

// drainer is the single goroutine that owns priorityQueue and currentLedger. It receives decoded
// batches from workers in arbitrary order, buffers them, and delivers to batchQueue strictly in
// ascending file-start order. Being the sole sender to batchQueue is what guarantees ordering —
// per-worker sends could race and deliver out of order. It never holds a lock across a blocking
// send, so it cannot deadlock the workers.
func (buf *ledgerBuffer) drainer() {
	defer buf.wg.Done()
	for {
		select {
		case <-buf.ctx.Done():
			return
		case db := <-buf.decodedQueue:
			buf.priorityQueue.Push(db)
			for buf.priorityQueue.Len() > 0 && buf.currentLedger.Load() == buf.priorityQueue.Peek().startLedger {
				item := buf.priorityQueue.Pop()
				select {
				case buf.batchQueue <- item.batch:
					buf.currentLedger.Add(buf.schema.LedgersPerFile)
				case <-buf.ctx.Done():
					return
				}
			}
		}
	}
}

// downloadAndStore downloads, decodes, and hands a ledger file to the drainer, retrying on
// failure. A NotExist file on an unbounded range is awaited indefinitely (the live tip may not
// have published it yet) — counting that wait against RetryLimit would strand the drainer on a
// missing start ledger once the limit is hit. A NotExist file on a bounded range is a hard
// error. Other (transient) errors are bounded by RetryLimit. The function returns only after the
// batch is enqueued, the context is cancelled, or it gives up by cancelling the buffer — it never
// returns without delivering while the buffer is still live.
func (buf *ledgerBuffer) downloadAndStore(sequence uint32) {
	for transientAttempts := uint32(0); ; {
		batch, err := buf.downloadAndDecode(sequence)
		if err == nil {
			// Hand the decoded batch to the drainer (unordered); it reorders and delivers in sequence.
			select {
			case buf.decodedQueue <- decodedBatch{batch: batch, startLedger: sequence}:
			case <-buf.ctx.Done():
			}
			return
		}
		if buf.ctx.Err() != nil {
			return // context cancelled, stop retrying
		}
		// errors.Is (not os.IsNotExist) so the NotExist sentinel is still detected after
		// downloadAndDecode wraps it with %w; os.IsNotExist does not unwrap.
		if errors.Is(err, os.ErrNotExist) {
			if buf.ledgerRange.Bounded() {
				// Bounded range: missing file is a hard error.
				buf.cancel(fmt.Errorf("ledger file for sequence %d not found: %w", sequence, err))
				return
			}
			// Unbounded range: the file may not be published yet. Wait and retry indefinitely
			// (NOT counted against RetryLimit) until it appears or the context is cancelled.
			if !sleepWithContext(buf.ctx, buf.config.RetryWait) {
				return
			}
			continue
		}
		// Transient error: bounded by RetryLimit.
		if transientAttempts >= buf.config.RetryLimit {
			buf.cancel(fmt.Errorf("downloading ledger file for sequence %d: maximum retries (%d) exceeded: %w",
				sequence, buf.config.RetryLimit, err))
			return
		}
		transientAttempts++
		log.WithField("sequence", sequence).WithError(err).
			Warnf("Failed to download ledger file (attempt %d/%d), retrying...", transientAttempts, buf.config.RetryLimit)
		if !sleepWithContext(buf.ctx, buf.config.RetryWait) {
			return
		}
	}
}

// downloadAndDecode fetches a ledger file from the datastore and stream-decodes it.
//
// Optimization: The SDK does io.ReadAll(reader) to buffer the entire compressed file,
// then feeds bytes.NewReader to the zstd+XDR decoder. We pass the S3 io.ReadCloser
// directly to the decoder, streaming through zstd without an intermediate allocation.
func (buf *ledgerBuffer) downloadAndDecode(sequence uint32) (xdr.LedgerCloseMetaBatch, error) {
	objectKey := buf.schema.GetObjectKeyFromSequenceNumber(sequence)
	reader, _, err := buf.dataStore.GetFile(buf.ctx, objectKey)
	if err != nil {
		return xdr.LedgerCloseMetaBatch{}, fmt.Errorf("fetching ledger file %s: %w", objectKey, err)
	}
	defer reader.Close() //nolint:errcheck // read-only stream; close error is not actionable

	var batch xdr.LedgerCloseMetaBatch
	decoder := compressxdr.NewXDRDecoder(compressxdr.DefaultCompressor, &batch)
	if _, err = decoder.ReadFrom(reader); err != nil {
		return xdr.LedgerCloseMetaBatch{}, fmt.Errorf("decoding ledger file %s: %w", objectKey, err)
	}
	return batch, nil
}

// getNextBatch receives the next decoded batch from the buffer in sequence order.
// After receiving, it enqueues a new download task to maintain the buffer invariant.
//
// buf.ctx is derived from the caller-supplied ctx via context.WithCancelCause, so
// buf.ctx.Done() fires both when that parent ctx is cancelled (shutdown — its cause is
// context.Canceled, matching close()'s own buf.cancel(context.Canceled)) and when
// downloadAndStore gives up on a sequence and calls buf.cancel with a real error. Only the
// latter means the buffer itself has permanently died; distinguishing on the cause lets
// callers (the ledger-fetch retry ladder) tell "will never recover" apart from "shutting down".
func (buf *ledgerBuffer) getNextBatch(ctx context.Context) (xdr.LedgerCloseMetaBatch, error) {
	select {
	case <-buf.ctx.Done():
		cause := context.Cause(buf.ctx)
		if errors.Is(cause, context.Canceled) {
			return xdr.LedgerCloseMetaBatch{}, fmt.Errorf("buffer context cancelled: %w", cause)
		}
		return xdr.LedgerCloseMetaBatch{}, fmt.Errorf("%w: %w", ErrBufferDead, cause)
	case <-ctx.Done():
		return xdr.LedgerCloseMetaBatch{}, fmt.Errorf("caller context cancelled: %w", ctx.Err())
	case batch := <-buf.batchQueue:
		// Replenish: enqueue next download task. pushTaskQueue only modifies
		// nextTaskLedger, which is safe because its only callers — the constructor's seed
		// loop and this single consumer thread — never overlap (the consumer does not run
		// until PrepareRange returns), and workers never touch nextTaskLedger.
		buf.pushTaskQueue()
		return batch, nil
	}
}

// pushTaskQueue enqueues the next file-start sequence for download.
// Returns false if the range boundary has been reached (no task enqueued).
//
// Concurrency: its only callers — the constructor's seed loop and the single consumer
// thread (getNextBatch) — never overlap (the consumer does not run until PrepareRange
// returns), and workers never touch nextTaskLedger, so no lock is needed.
func (buf *ledgerBuffer) pushTaskQueue() bool {
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
func (buf *ledgerBuffer) close() {
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
