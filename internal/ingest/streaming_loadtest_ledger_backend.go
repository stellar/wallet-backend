package ingest

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	"github.com/stellar/go-stellar-sdk/ingest/ledgerbackend"
	"github.com/stellar/go-stellar-sdk/support/log"
	"github.com/stellar/go-stellar-sdk/xdr"
)

// StreamingLoadtestBackendConfig configures the StreamingLoadtestLedgerBackend.
type StreamingLoadtestBackendConfig struct {
	// MetaPipePaths are filesystem paths of FIFOs carrying stream-framed XDR
	// LedgerCloseMeta records, each written by a `stellar-core apply-load`
	// process via its METADATA_OUTPUT_STREAM setting. With more than one path,
	// each emitted ledger is the per-sequence merge of one frame from every
	// pipe (union of their transaction sets), so N apply-load processes with
	// different transaction profiles combine into a single mixed-traffic
	// ledger stream.
	MetaPipePaths []string
	// LedgerCloseDuration is the minimum interval between GetLedger emits.
	// 0 = uncapped: the consumer ingests as fast as the generators produce.
	// Because apply-load blocks on FIFO writes once the kernel pipe buffer
	// fills, this pacing propagates back and throttles the generators too.
	LedgerCloseDuration time.Duration
}

type pipeReadResult struct {
	lcm xdr.LedgerCloseMeta
	err error
}

// pipeLookaheadFrames is how many decoded frames each pipe's reader may run
// ahead of GetLedger. Streaming one full frame through a FIFO takes the
// writer a large fraction of the ledger interval (the kernel pipe buffer is
// tiny next to a frame), so with no lookahead every GetLedger waits for the
// slowest writer's in-flight frame; lookahead lets that streaming overlap
// the consumer's processing of earlier ledgers. Each buffered frame is a
// fully decoded LedgerCloseMeta — tens of MB at full per-ledger volume — so
// the depth stays small: the reader holds one more in flight, giving
// (1+pipeLookaheadFrames) frames per pipe, and FIFO backpressure still
// reaches the writer with that much slack.
const pipeLookaheadFrames = 2

type openPipeResult struct {
	file *os.File
	err  error
}

// metaPipe tracks one FIFO and the renumbering state of its current stream
// epoch. An epoch is one apply-load process lifetime: apply-load always starts
// a fresh chain at ledger sequence 1 (genesis emits no meta, so the first
// frame is sequence 2), so every time the writer restarts, the raw sequences
// reset and a new seqDiff must be derived from the next requested ledger.
type metaPipe struct {
	path string
	file *os.File
	// opening carries the result of an in-flight blocking FIFO open. It is
	// retained across a context cancellation so a retried GetLedger reuses
	// the pending open instead of leaking the file descriptor.
	opening chan openPipeResult
	// frames delivers decoded frames in order from the reader goroutine. The
	// channel buffers pipeLookaheadFrames so the reader decodes ahead of
	// GetLedger, hiding the writer's per-frame FIFO streaming time; an epoch's
	// terminating error is always the last element the reader sends, so
	// buffered frames before it remain valid.
	frames chan pipeReadResult
	// peeked holds a frame that was consumed from the reader but not yet
	// emitted, so that a GetLedger attempt that fails partway through a
	// multi-pipe merge (context cancelled, another pipe mid-reopen) can be
	// retried without this pipe skipping a frame.
	peeked *xdr.LedgerCloseMeta
	// seqDiff maps this epoch's raw frame sequences onto emitted ledger
	// sequences: emitted = raw + seqDiff.
	seqDiff   int64
	diffValid bool
}

// StreamingLoadtestLedgerBackend reads stream-framed XDR LedgerCloseMeta from
// one or more named pipes written by `stellar-core apply-load`, renumbers each
// stream's ledger headers onto the consumer's requested sequence, merges the
// per-sequence frames into one ledger, and stamps advancing close times. It
// implements ledgerbackend.LedgerBackend. Dev-only: it exists so a load-test
// deployment can exercise the standard ingestion path against synthetic
// traffic.
//
// Only the header sequence is renumbered; ledger-sequence references inside
// ledger entries keep their raw per-stream values (see appendLedger for why).
//
// Properties the renumbering provides:
//   - The consumer's database survives generator restarts: a restarted
//     apply-load resets to raw sequence 1, and the new epoch is simply mapped
//     onto the next requested ledger.
//   - The consumer itself can restart and resume from its cursor: whatever
//     sequence it asks for first becomes the anchor for fresh diffs.
//
// Close times are stamped with the wall clock (monotone non-decreasing)
// because apply-load emits every ledger with closeTime 0, which would collapse
// all time-partitioned storage into one partition.
//
// Shutdown contract: cancel the context passed to GetLedger before calling
// Close. GetLedger holds the backend mutex while waiting for frames, and Close
// takes the same mutex.
type StreamingLoadtestLedgerBackend struct {
	config StreamingLoadtestBackendConfig

	mu            sync.Mutex
	pipes         []*metaPipe
	prepared      bool
	preparedFrom  uint32
	latestEmitted uint32
	cached        xdr.LedgerCloseMeta
	lastCloseTime xdr.TimePoint
	lastEmitTime  time.Time
	closed        bool
	// done unblocks reader goroutines parked on a channel send when the
	// backend closes.
	done chan struct{}
}

// Verify interface implementation at compile time.
var _ ledgerbackend.LedgerBackend = (*StreamingLoadtestLedgerBackend)(nil)

func NewStreamingLoadtestLedgerBackend(cfg StreamingLoadtestBackendConfig) (*StreamingLoadtestLedgerBackend, error) {
	if len(cfg.MetaPipePaths) == 0 {
		return nil, fmt.Errorf("MetaPipePaths is required")
	}
	pipes := make([]*metaPipe, len(cfg.MetaPipePaths))
	for i, path := range cfg.MetaPipePaths {
		if path == "" {
			return nil, fmt.Errorf("MetaPipePaths[%d] is empty", i)
		}
		pipes[i] = &metaPipe{path: path}
	}
	return &StreamingLoadtestLedgerBackend{
		config: cfg,
		pipes:  pipes,
		done:   make(chan struct{}),
	}, nil
}

func (b *StreamingLoadtestLedgerBackend) PrepareRange(ctx context.Context, ledgerRange ledgerbackend.Range) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return fmt.Errorf("backend closed")
	}
	if b.prepared {
		return nil
	}
	if ledgerRange.Bounded() {
		return fmt.Errorf("streaming-loadtest backend only supports unbounded ranges")
	}
	// Pipes open lazily in GetLedger: opening a FIFO read side blocks until
	// the writer attaches, and PrepareRange should not stall startup on
	// generators that are still booting.
	b.preparedFrom = ledgerRange.From()
	b.prepared = true
	return nil
}

func (b *StreamingLoadtestLedgerBackend) GetLedger(ctx context.Context, sequence uint32) (xdr.LedgerCloseMeta, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if !b.prepared {
		return xdr.LedgerCloseMeta{}, fmt.Errorf("GetLedger called before PrepareRange")
	}
	if b.closed {
		return xdr.LedgerCloseMeta{}, fmt.Errorf("backend closed")
	}

	// Re-serving the last emitted ledger keeps a retrying consumer (its
	// GetLedger call sits inside a retry-with-backoff wrapper) from
	// desynchronizing the pipes: the frames for that ledger are already
	// consumed and must not be read twice.
	if b.latestEmitted != 0 && sequence == b.latestEmitted {
		return b.cached, nil
	}
	expected := b.preparedFrom
	if b.latestEmitted != 0 {
		expected = b.latestEmitted + 1
	}
	if sequence != expected {
		return xdr.LedgerCloseMeta{}, fmt.Errorf(
			"non-sequential ledger request: expected %d, got %d", expected, sequence)
	}

	// Pace: sleep until LedgerCloseDuration has elapsed since the last emit.
	if b.config.LedgerCloseDuration > 0 && !b.lastEmitTime.IsZero() {
		wait := time.Until(b.lastEmitTime.Add(b.config.LedgerCloseDuration))
		if wait > 0 {
			timer := time.NewTimer(wait)
			select {
			case <-timer.C:
			case <-ctx.Done():
				timer.Stop()
				return xdr.LedgerCloseMeta{}, fmt.Errorf("paced GetLedger cancelled: %w", ctx.Err())
			}
		}
	}

	var merged xdr.LedgerCloseMeta
	for i, p := range b.pipes {
		frame, err := b.nextFrame(ctx, p, sequence)
		if err != nil {
			return xdr.LedgerCloseMeta{}, fmt.Errorf("pipe %s: %w", p.path, err)
		}
		if i == 0 {
			merged = frame
			if err := setLedgerSeq(&merged, sequence); err != nil {
				return xdr.LedgerCloseMeta{}, err
			}
		} else {
			if err := appendLedger(&merged, frame); err != nil {
				return xdr.LedgerCloseMeta{}, fmt.Errorf("merging frame from pipe %s: %w", p.path, err)
			}
		}
	}
	if err := b.stampCloseTime(&merged); err != nil {
		return xdr.LedgerCloseMeta{}, err
	}

	// The merge succeeded: only now release the peeked frames so a failed
	// attempt above replays the same frames on retry.
	for _, p := range b.pipes {
		p.peeked = nil
	}
	b.cached = merged
	b.latestEmitted = sequence
	b.lastEmitTime = time.Now()
	return merged, nil
}

// nextFrame returns the frame of p that must merge into the requested
// sequence, (re)opening the pipe and deriving the epoch's sequence diff as
// needed. The returned frame stays parked in p.peeked until the caller
// completes the whole merge.
func (b *StreamingLoadtestLedgerBackend) nextFrame(ctx context.Context, p *metaPipe, sequence uint32) (xdr.LedgerCloseMeta, error) {
	if p.peeked != nil {
		return *p.peeked, nil
	}
	for {
		if p.frames == nil {
			if err := b.openPipe(ctx, p); err != nil {
				return xdr.LedgerCloseMeta{}, err
			}
		}
		select {
		case <-ctx.Done():
			return xdr.LedgerCloseMeta{}, fmt.Errorf("waiting for frame: %w", ctx.Err())
		case res := <-p.frames:
			if res.err != nil {
				// Any read error ends the stream epoch: EOF means the writer
				// exited (normal end of an apply-load run), and a decode error
				// means a killed writer left a truncated frame. Either way the
				// only recovery is a fresh stream from the restarted writer.
				log.Warnf("streaming-loadtest: pipe %s stream ended (%v); reopening and waiting for writer", p.path, res.err)
				b.resetPipe(p)
				continue
			}
			frameSeq := res.lcm.LedgerSequence()
			if !p.diffValid {
				p.seqDiff = int64(sequence) - int64(frameSeq)
				p.diffValid = true
				log.Infof("streaming-loadtest: pipe %s new epoch: raw ledger %d maps to %d (diff %d)", p.path, frameSeq, sequence, p.seqDiff)
			} else if int64(frameSeq)+p.seqDiff != int64(sequence) {
				// apply-load emits every ledger it closes, in order, with no
				// gaps. A mismatch inside an epoch means frames were lost or
				// reordered — unrecoverable, unlike a writer restart.
				return xdr.LedgerCloseMeta{}, fmt.Errorf(
					"sequence mismatch within stream epoch: raw ledger %d + diff %d != requested %d",
					frameSeq, p.seqDiff, sequence)
			}
			p.peeked = &res.lcm
			return res.lcm, nil
		}
	}
}

// openPipe blocking-opens the FIFO read side (the open(2) itself blocks until
// a writer attaches) and starts the reader goroutine. Cancellation-safe: a
// cancelled open stays pending on p.opening and is adopted by the next call.
func (b *StreamingLoadtestLedgerBackend) openPipe(ctx context.Context, p *metaPipe) error {
	if p.opening == nil {
		p.opening = make(chan openPipeResult, 1)
		go func(path string, ch chan<- openPipeResult) {
			f, err := os.OpenFile(path, os.O_RDONLY, 0)
			ch <- openPipeResult{file: f, err: err}
		}(p.path, p.opening)
	}
	select {
	case <-ctx.Done():
		return fmt.Errorf("waiting for pipe writer: %w", ctx.Err())
	case res := <-p.opening:
		p.opening = nil
		if res.err != nil {
			return fmt.Errorf("opening meta pipe: %w", res.err)
		}
		p.file = res.file
		p.frames = make(chan pipeReadResult, pipeLookaheadFrames)
		go readFrames(res.file, p.frames, b.done)
		return nil
	}
}

func (b *StreamingLoadtestLedgerBackend) resetPipe(p *metaPipe) {
	if p.file != nil {
		if err := p.file.Close(); err != nil && !errors.Is(err, os.ErrClosed) {
			log.Warnf("streaming-loadtest: closing pipe %s: %v", p.path, err)
		}
	}
	p.file = nil
	p.frames = nil
	p.diffValid = false
}

// rawFrame is one framed XDR record's payload, or the drain error that ends
// the epoch — always the last element sent, so decoded frames before it
// remain valid.
type rawFrame struct {
	payload []byte
	err     error
}

// readFrames drains and decodes frames off the pipe in two stages, in order:
// this goroutine reads each record's raw bytes at pipe-transfer speed, and a
// decode goroutine unmarshals them. apply-load's meta write is synchronous —
// core does not start generating its next ledger until the consumer has
// drained the current frame — so draining at transfer speed instead of
// decode speed takes the decoder out of every producer's ledger cycle;
// decoding overlaps the producer's next apply. It exits after forwarding a
// read error (the epoch is over) or when the backend closes.
func readFrames(f *os.File, frames chan<- pipeReadResult, done <-chan struct{}) {
	// Depth 1 is all the decoupling needs: the drain runs one frame ahead of
	// the decoder, so the producer's next write never waits on a decode.
	raw := make(chan rawFrame, 1)
	go decodeFrames(raw, frames, done)

	reader := bufio.NewReaderSize(f, 1<<20)
	for {
		payload, err := drainFrame(reader)
		select {
		case raw <- rawFrame{payload: payload, err: err}:
		case <-done:
			return
		}
		if err != nil {
			return
		}
	}
}

// drainFrame reads one framed XDR record's payload: a 4-byte length header
// (last-fragment bit always set — the SDK writes single-fragment records)
// followed by that many payload bytes.
func drainFrame(r io.Reader) ([]byte, error) {
	nbytes, err := xdr.ReadFrameLength(r)
	if err != nil {
		if errors.Is(err, io.EOF) {
			// Not wrapped: plain EOF is the normal end of a writer's epoch.
			return nil, io.EOF
		}
		return nil, fmt.Errorf("reading frame header: %w", err)
	}
	if nbytes == 0 {
		return nil, io.EOF
	}
	if nbytes > xdr.DefaultMaxXDRStreamRecordSize {
		return nil, fmt.Errorf("frame of %d bytes exceeds the %d-byte record cap", nbytes, xdr.DefaultMaxXDRStreamRecordSize)
	}
	payload := make([]byte, nbytes)
	if _, err := io.ReadFull(r, payload); err != nil {
		return nil, fmt.Errorf("reading %d-byte frame payload: %w", nbytes, err)
	}
	return payload, nil
}

// decodeFrames unmarshals drained frames in order and hands them over,
// running up to the frames channel's buffer ahead of the consumer. A drain
// error arrives as the raw channel's last element and is forwarded as the
// frames channel's last element; a decode error likewise ends the epoch.
func decodeFrames(raw <-chan rawFrame, frames chan<- pipeReadResult, done <-chan struct{}) {
	decoder := xdr.NewBytesDecoder()
	for {
		var rf rawFrame
		select {
		case rf = <-raw:
		case <-done:
			return
		}

		result := pipeReadResult{err: rf.err}
		if rf.err == nil {
			if _, err := decoder.DecodeBytes(&result.lcm, rf.payload); err != nil {
				result = pipeReadResult{err: fmt.Errorf("decoding %d-byte frame: %w", len(rf.payload), err)}
			}
		}

		select {
		case frames <- result:
		case <-done:
			return
		}
		if result.err != nil {
			return
		}
	}
}

// appendLedger merges src's content into dst: transaction-set phases,
// transaction results, upgrades, and evicted keys are appended, and dst's
// header (already renumbered and stamped by the caller) stands for the merged
// ledger. Both sides must be V1/V2 with generalized transaction sets.
//
// Unlike the SDK's loadtest.MergeLedgers, this deliberately does NOT rewrite
// ledger-sequence references inside ledger entries (lastModifiedLedgerSeq,
// TTL liveUntilLedgerSeq, account seqLedger). Nothing downstream reads those
// fields, and the rewrite is a full marshal/parse/walk/re-marshal round trip
// over every frame — measured at ~5x the entire ledger-processing cost at
// full per-ledger volume, making it the stream's cadence bottleneck.
func appendLedger(dst *xdr.LedgerCloseMeta, src xdr.LedgerCloseMeta) error {
	if src.V != dst.V {
		return fmt.Errorf("source ledger version %d is incompatible with destination version %d", src.V, dst.V)
	}
	switch dst.V {
	case 1:
		srcTxSet, ok := src.V1.TxSet.GetV1TxSet()
		if !ok {
			return fmt.Errorf("source ledger txset version %d is not supported", src.V1.TxSet.V)
		}
		dst.V1.TxSet.V1TxSet.Phases = append(dst.V1.TxSet.V1TxSet.Phases, srcTxSet.Phases...)
		dst.V1.TxProcessing = append(dst.V1.TxProcessing, src.V1.TxProcessing...)
		dst.V1.UpgradesProcessing = append(dst.V1.UpgradesProcessing, src.V1.UpgradesProcessing...)
		dst.V1.EvictedKeys = append(dst.V1.EvictedKeys, src.V1.EvictedKeys...)
	case 2:
		srcTxSet, ok := src.V2.TxSet.GetV1TxSet()
		if !ok {
			return fmt.Errorf("source ledger txset version %d is not supported", src.V2.TxSet.V)
		}
		dst.V2.TxSet.V1TxSet.Phases = append(dst.V2.TxSet.V1TxSet.Phases, srcTxSet.Phases...)
		dst.V2.TxProcessing = append(dst.V2.TxProcessing, src.V2.TxProcessing...)
		dst.V2.UpgradesProcessing = append(dst.V2.UpgradesProcessing, src.V2.UpgradesProcessing...)
		dst.V2.EvictedKeys = append(dst.V2.EvictedKeys, src.V2.EvictedKeys...)
	default:
		return fmt.Errorf("ledger version %d is not supported", dst.V)
	}
	return nil
}

// setLedgerSeq rewrites the ledger header sequence. Only V1/V2 are accepted:
// that is what protocol-27+ cores emit, and merging requires generalized
// transaction sets, which V0 predates.
func setLedgerSeq(lcm *xdr.LedgerCloseMeta, sequence uint32) error {
	switch lcm.V {
	case 1:
		lcm.V1.LedgerHeader.Header.LedgerSeq = xdr.Uint32(sequence)
	case 2:
		lcm.V2.LedgerHeader.Header.LedgerSeq = xdr.Uint32(sequence)
	default:
		return fmt.Errorf("ledger version %d is not supported", lcm.V)
	}
	return nil
}

// stampCloseTime overwrites the header close time with the wall clock,
// clamped to be non-decreasing. apply-load emits closeTime 0 on every ledger;
// downstream storage partitions rows by this timestamp, so it must advance.
func (b *StreamingLoadtestLedgerBackend) stampCloseTime(lcm *xdr.LedgerCloseMeta) error {
	ct := xdr.TimePoint(time.Now().Unix())
	if ct < b.lastCloseTime {
		ct = b.lastCloseTime
	}
	switch lcm.V {
	case 1:
		lcm.V1.LedgerHeader.Header.ScpValue.CloseTime = ct
	case 2:
		lcm.V2.LedgerHeader.Header.ScpValue.CloseTime = ct
	default:
		return fmt.Errorf("ledger version %d is not supported", lcm.V)
	}
	b.lastCloseTime = ct
	return nil
}

// GetLatestLedgerSequence reports the last emitted ledger. There is no
// meaningful "network tip" for a paced synthetic stream, so consumers see a
// lag of at most one ledger; ingestion throughput (ledgers per second versus
// the configured pacing) is the signal to watch instead of lag.
func (b *StreamingLoadtestLedgerBackend) GetLatestLedgerSequence(ctx context.Context) (uint32, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if !b.prepared {
		return 0, fmt.Errorf("GetLatestLedgerSequence called before PrepareRange")
	}
	if b.latestEmitted != 0 {
		return b.latestEmitted, nil
	}
	return b.preparedFrom, nil
}

func (b *StreamingLoadtestLedgerBackend) IsPrepared(ctx context.Context, ledgerRange ledgerbackend.Range) (bool, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.prepared && !ledgerRange.Bounded() && ledgerRange.From() >= b.preparedFrom, nil
}

func (b *StreamingLoadtestLedgerBackend) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return nil
	}
	b.closed = true
	close(b.done)
	for _, p := range b.pipes {
		if p.file != nil {
			if err := p.file.Close(); err != nil && !errors.Is(err, os.ErrClosed) {
				log.Warnf("streaming-loadtest: closing pipe %s: %v", p.path, err)
			}
			p.file = nil
			p.frames = nil
		}
	}
	return nil
}
