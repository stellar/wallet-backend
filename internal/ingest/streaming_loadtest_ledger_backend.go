package ingest

import (
	"context"
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	"github.com/stellar/go-stellar-sdk/ingest/ledgerbackend"
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
}

// Verify interface implementation at compile time.
var _ ledgerbackend.LedgerBackend = (*StreamingLoadtestLedgerBackend)(nil)

func NewStreamingLoadtestLedgerBackend(cfg StreamingLoadtestBackendConfig) (*StreamingLoadtestLedgerBackend, error) {
	if cfg.MetaPipePath == "" {
		return nil, fmt.Errorf("MetaPipePath is required")
	}
	return &StreamingLoadtestLedgerBackend{config: cfg}, nil
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

	// Open the read-side. This blocks until a writer opens the FIFO on the other end.
	// Honour context cancellation by running the open in a goroutine.
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

// GetLedger, GetLatestLedgerSequence, IsPrepared implemented in later tasks.
func (b *StreamingLoadtestLedgerBackend) GetLedger(ctx context.Context, sequence uint32) (xdr.LedgerCloseMeta, error) {
	return xdr.LedgerCloseMeta{}, fmt.Errorf("not implemented")
}

func (b *StreamingLoadtestLedgerBackend) GetLatestLedgerSequence(ctx context.Context) (uint32, error) {
	return 0, fmt.Errorf("not implemented")
}

func (b *StreamingLoadtestLedgerBackend) IsPrepared(ctx context.Context, ledgerRange ledgerbackend.Range) (bool, error) {
	return false, fmt.Errorf("not implemented")
}

func (b *StreamingLoadtestLedgerBackend) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.done {
		return nil
	}
	b.done = true
	if b.pipeFile != nil {
		return b.pipeFile.Close()
	}
	return nil
}

// Silence unused import complaints until GetLedger uses io.EOF.
var _ = io.EOF
