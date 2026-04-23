package ingest

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/stellar/go-stellar-sdk/ingest/ledgerbackend"
	"github.com/stellar/go-stellar-sdk/support/log"
	"github.com/stellar/go-stellar-sdk/xdr"
)

// StreamingLoadtestBackendConfig configures the StreamingLoadtestLedgerBackend.
type StreamingLoadtestBackendConfig struct {
	// MetaPipePath is the filesystem path of a FIFO carrying stream-framed
	// XDR LedgerCloseMeta records, written by stellar-core apply-load via its
	// METADATA_OUTPUT_STREAM setting.
	MetaPipePath string
	// LedgerCloseDuration paces GetLedger emits. 0 = uncapped.
	LedgerCloseDuration time.Duration
	// NetworkPassphrase is recorded but not validated by this backend; apply-load
	// writes meta that is passphrase-independent at the framing layer.
	NetworkPassphrase string
	// ArchiveURL, when non-empty, makes the constructor open the FIFO and
	// drain frames until the history archive publishes its first checkpoint.
	// Required when co-located with stellar-core apply-load: apply-load
	// blocks on FIFO writes (buffered write + flush per ledger close) once
	// the 64KB pipe buffer fills, and can't publish a checkpoint to the
	// archive until someone drains the pipe. wallet-backend's startLiveIngestion
	// path requires a valid checkpoint from the archive before it reaches
	// PrepareRange, so the drain must happen here.
	ArchiveURL string
	// DrainTimeout is the hard cap on how long the constructor's drain waits
	// for the archive to publish. 0 = default (5 minutes).
	DrainTimeout time.Duration
}

// StreamingLoadtestLedgerBackend reads stream-framed XDR LedgerCloseMeta from a
// named pipe, typically produced by `stellar-core apply-load`. It implements
// ledgerbackend.LedgerBackend. It is dev-only and intended for load testing.
type StreamingLoadtestLedgerBackend struct {
	config StreamingLoadtestBackendConfig

	pipeFile  *os.File
	xdrStream *xdr.Stream

	mu            sync.RWMutex
	prepared      bool
	preparedFrom  uint32
	latestSeqSeen uint32
	lastEmitTime  time.Time
	done          bool

	// pendingFrames holds frames drained from the pipe during constructor
	// startup that GetLedger must replay before reading from the pipe again.
	//
	// The drain unsticks apply-load (see StreamingLoadtestBackendConfig.ArchiveURL)
	// and stops once it has consumed a frame with seq >= archive checkpoint.
	// Because the archive poll is asynchronous, the drain typically overshoots
	// the checkpoint by a few ledgers. Rather than discard those frames (the
	// ingest loop would start from the archive checkpoint and hit a sequence
	// mismatch on the pipe), we buffer every frame from the archive checkpoint
	// onward and replay them here.
	pendingFrames []xdr.LedgerCloseMeta
}

// Verify interface implementation at compile time.
var _ ledgerbackend.LedgerBackend = (*StreamingLoadtestLedgerBackend)(nil)

const defaultDrainTimeout = 5 * time.Minute

func NewStreamingLoadtestLedgerBackend(cfg StreamingLoadtestBackendConfig) (*StreamingLoadtestLedgerBackend, error) {
	if cfg.MetaPipePath == "" {
		return nil, fmt.Errorf("MetaPipePath is required")
	}
	b := &StreamingLoadtestLedgerBackend{config: cfg}

	if cfg.ArchiveURL != "" {
		timeout := cfg.DrainTimeout
		if timeout == 0 {
			timeout = defaultDrainTimeout
		}
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()
		if err := b.openPipe(ctx); err != nil {
			return nil, fmt.Errorf("opening meta pipe: %w", err)
		}
		if err := b.drainUntilArchiveReady(ctx); err != nil {
			if closeErr := b.pipeFile.Close(); closeErr != nil {
				log.Ctx(ctx).Warnf("closing pipe after drain failure: %v", closeErr)
			}
			return nil, fmt.Errorf("waiting for archive checkpoint: %w", err)
		}
		b.prepared = true
	}
	return b, nil
}

// openPipe opens the FIFO read-side. Blocks until apply-load opens the write side.
// Respects ctx cancellation.
func (b *StreamingLoadtestLedgerBackend) openPipe(ctx context.Context) error {
	openResult := make(chan struct {
		f   *os.File
		err error
	}, 1)
	go func() {
		f, err := os.OpenFile(b.config.MetaPipePath, os.O_RDONLY, 0)
		openResult <- struct {
			f   *os.File
			err error
		}{f, err}
	}()

	select {
	case <-ctx.Done():
		return fmt.Errorf("context cancelled waiting for pipe writer: %w", ctx.Err())
	case res := <-openResult:
		if res.err != nil {
			return fmt.Errorf("opening meta pipe %s: %w", b.config.MetaPipePath, res.err)
		}
		b.pipeFile = res.f
		b.xdrStream = xdr.NewStream(b.pipeFile)
		return nil
	}
}

// drainUntilArchiveReady reads and discards frames from the pipe until the
// history archive at cfg.ArchiveURL reports a non-zero currentLedger AND the
// drain has consumed a frame with seq >= that currentLedger. Blocks until done
// or until ctx expires.
//
// This unsticks apply-load (which blocks on FIFO writes after ~64KB of meta)
// long enough for it to close ledgers through the first checkpoint boundary
// and publish the history archive, which wallet-backend's
// PopulateAccountTokens path requires.
func (b *StreamingLoadtestLedgerBackend) drainUntilArchiveReady(ctx context.Context) error {
	archivePollInterval := 500 * time.Millisecond

	archiveReady := make(chan uint32, 1)

	pollCtx, cancelPoll := context.WithCancel(ctx)
	defer cancelPoll()
	go func() {
		for {
			curLedger, err := b.fetchArchiveCurrentLedger(pollCtx)
			if err == nil && curLedger > 0 {
				select {
				case archiveReady <- curLedger:
				case <-pollCtx.Done():
				}
				return
			}
			select {
			case <-pollCtx.Done():
				return
			case <-time.After(archivePollInterval):
			}
		}
	}()

	var archiveCheckpointLedger uint32
	// buffered holds every frame we have read, in order. Once the archive
	// publishes a checkpoint C, we trim buffered to start at C and hand the
	// remainder to GetLedger via pendingFrames.
	var buffered []xdr.LedgerCloseMeta
	for {
		if archiveCheckpointLedger == 0 {
			select {
			case v := <-archiveReady:
				archiveCheckpointLedger = v
				log.Ctx(ctx).Infof("streaming-loadtest: archive published checkpoint %d; draining until we consume that ledger from the pipe", v)
			default:
			}
		}

		if err := ctx.Err(); err != nil {
			return fmt.Errorf("drain cancelled: %w", err)
		}

		var lcm xdr.LedgerCloseMeta
		if err := b.xdrStream.ReadOne(&lcm); err != nil {
			if errors.Is(err, io.EOF) {
				return fmt.Errorf("meta stream ended during drain: %w", io.EOF)
			}
			return fmt.Errorf("reading meta frame during drain: %w", err)
		}
		seq := lcm.LedgerSequence()
		buffered = append(buffered, lcm)
		b.mu.Lock()
		if seq > b.latestSeqSeen {
			b.latestSeqSeen = seq
		}
		b.mu.Unlock()

		if archiveCheckpointLedger > 0 && seq >= archiveCheckpointLedger {
			// Trim buffered to start at the archive checkpoint so the caller
			// can replay from there. If the checkpoint ledger was already
			// discarded above (drain outran the archive poll), the earliest
			// retained frame is our effective replay start.
			start := 0
			for i, f := range buffered {
				if f.LedgerSequence() >= archiveCheckpointLedger {
					start = i
					break
				}
			}
			b.mu.Lock()
			b.pendingFrames = buffered[start:]
			b.mu.Unlock()
			log.Ctx(ctx).Infof("streaming-loadtest: drain complete at ledger %d; buffered %d frame(s) starting at ledger %d for replay", seq, len(buffered)-start, buffered[start].LedgerSequence())
			return nil
		}
	}
}

func (b *StreamingLoadtestLedgerBackend) fetchArchiveCurrentLedger(ctx context.Context) (uint32, error) {
	url := strings.TrimRight(b.config.ArchiveURL, "/") + "/.well-known/stellar-history.json"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return 0, fmt.Errorf("building archive request: %w", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, fmt.Errorf("fetching archive HAS: %w", err)
	}
	defer func() {
		if closeErr := resp.Body.Close(); closeErr != nil {
			log.Ctx(ctx).Warnf("closing archive response body: %v", closeErr)
		}
	}()
	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("archive returned %s", resp.Status)
	}
	var has struct {
		CurrentLedger uint32 `json:"currentLedger"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&has); err != nil {
		return 0, fmt.Errorf("decoding archive HAS: %w", err)
	}
	return has.CurrentLedger, nil
}

func (b *StreamingLoadtestLedgerBackend) PrepareRange(ctx context.Context, ledgerRange ledgerbackend.Range) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.prepared {
		return nil
	}
	if ledgerRange.Bounded() {
		return fmt.Errorf("streaming-loadtest backend only supports unbounded ranges")
	}

	// Fallback path for when the backend was constructed without ArchiveURL
	// (unit tests that bypass the drain). Open the pipe here.
	openResult := make(chan struct {
		f   *os.File
		err error
	}, 1)
	go func() {
		f, err := os.OpenFile(b.config.MetaPipePath, os.O_RDONLY, 0)
		openResult <- struct {
			f   *os.File
			err error
		}{f, err}
	}()

	select {
	case <-ctx.Done():
		return fmt.Errorf("context cancelled waiting for pipe writer: %w", ctx.Err())
	case res := <-openResult:
		if res.err != nil {
			return fmt.Errorf("opening meta pipe %s: %w", b.config.MetaPipePath, res.err)
		}
		b.pipeFile = res.f
	}

	b.xdrStream = xdr.NewStream(b.pipeFile)
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
	if b.done {
		return xdr.LedgerCloseMeta{}, fmt.Errorf("backend closed")
	}

	// Pace: sleep until closeDuration has elapsed since last emit.
	if b.config.LedgerCloseDuration > 0 && !b.lastEmitTime.IsZero() {
		nextEmit := b.lastEmitTime.Add(b.config.LedgerCloseDuration)
		wait := time.Until(nextEmit)
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

	var lcm xdr.LedgerCloseMeta
	if len(b.pendingFrames) > 0 {
		lcm = b.pendingFrames[0]
		b.pendingFrames = b.pendingFrames[1:]
	} else {
		if err := b.xdrStream.ReadOne(&lcm); err != nil {
			if errors.Is(err, io.EOF) {
				return xdr.LedgerCloseMeta{}, fmt.Errorf("meta stream ended: %w", io.EOF)
			}
			return xdr.LedgerCloseMeta{}, fmt.Errorf("reading meta frame: %w", err)
		}
	}

	gotSeq := lcm.LedgerSequence()
	if gotSeq != sequence {
		return xdr.LedgerCloseMeta{}, fmt.Errorf("stream sequence mismatch: expected %d, got %d", sequence, gotSeq)
	}
	if gotSeq > b.latestSeqSeen {
		b.latestSeqSeen = gotSeq
	}
	b.lastEmitTime = time.Now()
	return lcm, nil
}

func (b *StreamingLoadtestLedgerBackend) GetLatestLedgerSequence(ctx context.Context) (uint32, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	if !b.prepared {
		return 0, fmt.Errorf("GetLatestLedgerSequence called before PrepareRange")
	}
	return b.latestSeqSeen, nil
}

func (b *StreamingLoadtestLedgerBackend) IsPrepared(ctx context.Context, ledgerRange ledgerbackend.Range) (bool, error) {
	return b.prepared, nil
}

func (b *StreamingLoadtestLedgerBackend) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.done {
		return nil
	}
	b.done = true
	if b.pipeFile != nil {
		if err := b.pipeFile.Close(); err != nil {
			return fmt.Errorf("closing meta pipe: %w", err)
		}
	}
	return nil
}
