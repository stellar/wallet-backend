package ingest

import (
	"context"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/stellar/go-stellar-sdk/ingest/ledgerbackend"
	"github.com/stellar/go-stellar-sdk/support/collections/heap"
	"github.com/stellar/go-stellar-sdk/support/compressxdr"
	"github.com/stellar/go-stellar-sdk/support/datastore"
	"github.com/stellar/go-stellar-sdk/support/log"
	"github.com/stellar/go-stellar-sdk/xdr"
)

// Ensure BackfillLedgerBackend implements LedgerBackend.
var _ ledgerbackend.LedgerBackend = (*BackfillLedgerBackend)(nil)

const (
	defaultBackfillBufferSize = 5
	defaultBackfillNumWorkers = 3
	defaultBackfillRetryLimit = 3
	defaultBackfillRetryWait  = 5 * time.Second
)

// BackfillLedgerBackendConfig configures the backfill-optimized ledger backend.
type BackfillLedgerBackendConfig struct {
	BufferSize uint32        // Max decoded batches in flight (default: 5).
	NumWorkers uint32        // S3 download+decode workers (default: 3).
	RetryLimit uint32        // S3 download retries per file (default: 3).
	RetryWait  time.Duration // Wait between retries (default: 5s).
}

func (c *BackfillLedgerBackendConfig) applyDefaults() {
	if c.BufferSize == 0 {
		c.BufferSize = defaultBackfillBufferSize
	}
	if c.NumWorkers == 0 {
		c.NumWorkers = defaultBackfillNumWorkers
	}
	if c.RetryLimit == 0 {
		c.RetryLimit = defaultBackfillRetryLimit
	}
	if c.RetryWait == 0 {
		c.RetryWait = defaultBackfillRetryWait
	}
	if c.NumWorkers > c.BufferSize {
		c.NumWorkers = c.BufferSize
	}
}

// BackfillLedgerBackend implements ledgerbackend.LedgerBackend optimised for
// sequential bulk access. Workers pre-decode files so GetLedger is lock-free.
type BackfillLedgerBackend struct {
	config    BackfillLedgerBackendConfig
	dataStore datastore.DataStore
	schema    datastore.DataStoreSchema

	buffer   *backfillBuffer
	prepared *ledgerbackend.Range
	closed   bool

	lcmBatch   xdr.LedgerCloseMetaBatch
	hasBatch   bool // true after first batch is loaded
	nextLedger uint32
	lastLedger uint32
}

// NewBackfillLedgerBackend creates a new BackfillLedgerBackend.
// Workers are not started until PrepareRange is called.
func NewBackfillLedgerBackend(config BackfillLedgerBackendConfig, dataStore datastore.DataStore, schema datastore.DataStoreSchema) (*BackfillLedgerBackend, error) {
	config.applyDefaults()

	if config.BufferSize == 0 {
		return nil, errors.New("buffer size must be > 0")
	}
	if schema.LedgersPerFile <= 0 {
		return nil, errors.New("ledgersPerFile must be > 0")
	}

	return &BackfillLedgerBackend{
		config:    config,
		dataStore: dataStore,
		schema:    schema,
	}, nil
}

// PrepareRange prepares the backend to serve ledgers in the given range.
// If the backend is already prepared for a containing range, this is a no-op.
// Otherwise it closes any existing buffer and starts fresh workers.
func (b *BackfillLedgerBackend) PrepareRange(ctx context.Context, ledgerRange ledgerbackend.Range) error {
	if b.closed {
		return errors.New("BackfillLedgerBackend is closed; cannot PrepareRange")
	}

	// Already prepared for a containing range — nothing to do.
	if b.prepared != nil && b.prepared.Contains(ledgerRange) {
		return nil
	}

	// Close existing buffer if re-preparing (e.g. fetchBoundaryTimestamps calls PrepareRange twice).
	if b.buffer != nil {
		b.buffer.close()
		b.buffer = nil
	}

	cfg := b.config
	// Clamp buffer size to not exceed total files in range.
	if ledgerRange.Bounded() {
		filesInRange := (ledgerRange.To()-ledgerRange.From())/b.schema.LedgersPerFile + 1
		if cfg.BufferSize > filesInRange {
			cfg.BufferSize = filesInRange
		}
		if cfg.NumWorkers > cfg.BufferSize {
			cfg.NumWorkers = cfg.BufferSize
		}
	}

	buf := newBackfillBuffer(cfg, b.dataStore, b.schema, ledgerRange)
	b.buffer = buf
	b.prepared = &ledgerRange
	b.nextLedger = ledgerRange.From()
	b.lastLedger = 0
	b.lcmBatch = xdr.LedgerCloseMetaBatch{}
	b.hasBatch = false

	return nil
}

// GetLedger returns the LedgerCloseMeta for the given sequence.
// No lock — single consumer guaranteed by the backfill/catchup architecture.
func (b *BackfillLedgerBackend) GetLedger(ctx context.Context, sequence uint32) (xdr.LedgerCloseMeta, error) {
	if b.closed {
		return xdr.LedgerCloseMeta{}, errors.New("BackfillLedgerBackend is closed; cannot GetLedger")
	}
	if b.prepared == nil {
		return xdr.LedgerCloseMeta{}, errors.New("session is not prepared, call PrepareRange first")
	}

	if err := b.ensureBatch(ctx, sequence); err != nil {
		return xdr.LedgerCloseMeta{}, err
	}

	lcm, err := b.lcmBatch.GetLedger(sequence)
	if err != nil {
		return xdr.LedgerCloseMeta{}, fmt.Errorf("getting ledger %d from batch: %w", sequence, err)
	}

	b.lastLedger = b.nextLedger
	b.nextLedger++
	return lcm, nil
}

// ensureBatch loads the batch containing the requested sequence if not already cached.
func (b *BackfillLedgerBackend) ensureBatch(ctx context.Context, sequence uint32) error {
	// Already in cached batch?
	if b.hasBatch && sequence >= uint32(b.lcmBatch.StartSequence) && sequence <= uint32(b.lcmBatch.EndSequence) {
		return nil
	}

	// Sequence is before current batch — not supported for sequential access.
	if b.hasBatch && sequence < uint32(b.lcmBatch.StartSequence) {
		return errors.New("requested sequence precedes current LedgerCloseMetaBatch")
	}

	batch, err := b.buffer.getNextBatch(ctx)
	if err != nil {
		return fmt.Errorf("getting next batch from buffer: %w", err)
	}
	b.lcmBatch = batch
	b.hasBatch = true
	return nil
}

// GetLatestLedgerSequence returns the latest available sequence.
func (b *BackfillLedgerBackend) GetLatestLedgerSequence(ctx context.Context) (uint32, error) {
	if b.closed {
		return 0, errors.New("BackfillLedgerBackend is closed; cannot GetLatestLedgerSequence")
	}
	if b.prepared == nil {
		return 0, errors.New("BackfillLedgerBackend must be prepared, call PrepareRange first")
	}

	b.buffer.currentLedgerLock.Lock()
	defer b.buffer.currentLedgerLock.Unlock()

	if b.buffer.currentLedger == b.prepared.From() {
		return 0, nil
	}
	return b.buffer.currentLedger - 1, nil
}

// IsPrepared returns true if the given range is fully contained by the prepared range.
func (b *BackfillLedgerBackend) IsPrepared(ctx context.Context, ledgerRange ledgerbackend.Range) (bool, error) {
	if b.closed {
		return false, errors.New("BackfillLedgerBackend is closed; cannot IsPrepared")
	}
	if b.prepared == nil {
		return false, nil
	}
	return b.prepared.Contains(ledgerRange), nil
}

// Close shuts down the backend. Once closed it cannot be reused.
func (b *BackfillLedgerBackend) Close() error {
	b.closed = true
	if b.buffer != nil {
		b.buffer.close()
		b.buffer = nil
	}
	return nil
}

// ---------------------------------------------------------------------------
// backfillBuffer — worker pool that downloads, decompresses, and decodes
// ledger files, delivering decoded LedgerCloseMetaBatch via a channel.
// ---------------------------------------------------------------------------

type decodedBatchObject struct {
	batch       xdr.LedgerCloseMetaBatch
	startLedger int
}

type backfillBuffer struct {
	config    BackfillLedgerBackendConfig
	dataStore datastore.DataStore
	schema    datastore.DataStoreSchema

	ctx    context.Context
	cancel context.CancelCauseFunc
	wg     sync.WaitGroup

	taskQueue   chan uint32
	ledgerQueue chan xdr.LedgerCloseMetaBatch

	priorityQueue     *heap.Heap[decodedBatchObject]
	priorityQueueLock sync.Mutex

	currentLedger     uint32
	nextTaskLedger    uint32
	ledgerRange       ledgerbackend.Range
	currentLedgerLock sync.Mutex
}

func newBackfillBuffer(config BackfillLedgerBackendConfig, ds datastore.DataStore, schema datastore.DataStoreSchema, ledgerRange ledgerbackend.Range) *backfillBuffer {
	ctx, cancel := context.WithCancelCause(context.Background())

	less := func(a, b decodedBatchObject) bool {
		return a.startLedger < b.startLedger
	}
	pq := heap.New(less, int(config.BufferSize))

	startSeq := schema.GetSequenceNumberStartBoundary(ledgerRange.From())

	buf := &backfillBuffer{
		config:         config,
		dataStore:      ds,
		schema:         schema,
		ctx:            ctx,
		cancel:         cancel,
		taskQueue:      make(chan uint32, config.BufferSize),
		ledgerQueue:    make(chan xdr.LedgerCloseMetaBatch, config.BufferSize),
		priorityQueue:  pq,
		currentLedger:  startSeq,
		nextTaskLedger: startSeq,
		ledgerRange:    ledgerRange,
	}

	// Start workers.
	buf.wg.Add(int(config.NumWorkers))
	for i := uint32(0); i < config.NumWorkers; i++ {
		go buf.worker(ctx)
	}

	// Seed task queue — same invariant as SDK:
	// len(taskQueue) + len(ledgerQueue) + pq.Len() <= BufferSize.
	// We push BufferSize+1 because pushTaskQueue is a no-op when past the end boundary.
	for i := 0; i <= int(config.BufferSize); i++ {
		buf.pushTaskQueue()
	}

	return buf
}

func (buf *backfillBuffer) pushTaskQueue() {
	if buf.ledgerRange.Bounded() && buf.nextTaskLedger > buf.schema.GetSequenceNumberEndBoundary(buf.ledgerRange.To()) {
		return
	}
	buf.taskQueue <- buf.nextTaskLedger
	buf.nextTaskLedger += buf.schema.LedgersPerFile
}

func (buf *backfillBuffer) worker(ctx context.Context) {
	defer buf.wg.Done()

	for {
		select {
		case <-ctx.Done():
			return
		case sequence := <-buf.taskQueue:
			for attempt := uint32(0); attempt <= buf.config.RetryLimit; {
				batch, err := buf.downloadAndDecode(ctx, sequence)
				if err != nil {
					if errors.Is(err, os.ErrNotExist) {
						if !buf.ledgerRange.Bounded() {
							if !buf.sleepWithContext(ctx, buf.config.RetryWait) {
								return
							}
							continue
						}
						buf.cancel(errors.Wrapf(err, "ledger object containing sequence %v is missing", sequence))
						return
					}
					if errors.Is(err, context.Canceled) {
						return
					}
					if attempt == buf.config.RetryLimit {
						buf.cancel(errors.Wrapf(err, "maximum retries exceeded for downloading object containing sequence %v", sequence))
						return
					}
					attempt++
					if !buf.sleepWithContext(ctx, buf.config.RetryWait) {
						return
					}
					continue
				}

				buf.storeDecoded(batch, sequence)
				break
			}
		}
	}
}

// downloadAndDecode streams a compressed XDR file from the data store through
// zstd decompression + XDR decode in a single pass — no intermediate io.ReadAll buffer.
func (buf *backfillBuffer) downloadAndDecode(ctx context.Context, sequence uint32) (xdr.LedgerCloseMetaBatch, error) {
	objectKey := buf.schema.GetObjectKeyFromSequenceNumber(sequence)

	reader, err := buf.dataStore.GetFile(ctx, objectKey)
	if err != nil {
		return xdr.LedgerCloseMetaBatch{}, errors.Wrapf(err, "unable to retrieve file: %s", objectKey)
	}
	defer func() {
		if closeErr := reader.Close(); closeErr != nil {
			log.Warnf("closing datastore reader for %s: %v", objectKey, closeErr)
		}
	}()

	var batch xdr.LedgerCloseMetaBatch
	decoder := compressxdr.NewXDRDecoder(compressxdr.DefaultCompressor, &batch)
	if _, err := decoder.ReadFrom(reader); err != nil {
		return xdr.LedgerCloseMetaBatch{}, errors.Wrapf(err, "decoding file: %s", objectKey)
	}

	return batch, nil
}

func (buf *backfillBuffer) storeDecoded(batch xdr.LedgerCloseMetaBatch, sequence uint32) {
	buf.priorityQueueLock.Lock()
	defer buf.priorityQueueLock.Unlock()

	buf.currentLedgerLock.Lock()
	defer buf.currentLedgerLock.Unlock()

	buf.priorityQueue.Push(decodedBatchObject{
		batch:       batch,
		startLedger: int(sequence),
	})

	// Drain in-order batches to the ledger queue.
	for buf.priorityQueue.Len() > 0 && buf.currentLedger == uint32(buf.priorityQueue.Peek().startLedger) {
		item := buf.priorityQueue.Pop()
		buf.ledgerQueue <- item.batch
		buf.currentLedger += buf.schema.LedgersPerFile
	}
}

func (buf *backfillBuffer) getNextBatch(ctx context.Context) (xdr.LedgerCloseMetaBatch, error) {
	select {
	case <-buf.ctx.Done():
		return xdr.LedgerCloseMetaBatch{}, fmt.Errorf("buffer context cancelled: %w", context.Cause(buf.ctx))
	case <-ctx.Done():
		return xdr.LedgerCloseMetaBatch{}, fmt.Errorf("caller context cancelled: %w", ctx.Err())
	case batch := <-buf.ledgerQueue:
		// Maintain buffer invariant: replenish task queue when consuming from ledger queue.
		buf.pushTaskQueue()
		return batch, nil
	}
}

func (buf *backfillBuffer) sleepWithContext(ctx context.Context, d time.Duration) bool {
	timer := time.NewTimer(d)
	select {
	case <-ctx.Done():
		if !timer.Stop() {
			<-timer.C
		}
		return false
	case <-timer.C:
	}
	return true
}

func (buf *backfillBuffer) close() {
	buf.cancel(context.Canceled)
	buf.wg.Wait()

	// Drain channels to unblock any goroutines.
	for {
		select {
		case <-buf.ledgerQueue:
		default:
			return
		}
	}
}

// logBufferStats logs buffer performance metrics for debugging.
func logBufferStats(config BackfillLedgerBackendConfig) {
	log.Infof("BackfillLedgerBackend: BufferSize=%d, NumWorkers=%d, RetryLimit=%d, RetryWait=%s",
		config.BufferSize, config.NumWorkers, config.RetryLimit, config.RetryWait)
}
