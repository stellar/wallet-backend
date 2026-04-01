// Package ingest provides ledger backend implementations optimized for wallet-backend's
// single-consumer access pattern.
//
// # Optimized RPC Backend
//
// optimizedRPCBackend is a fork of the SDK's RPCLedgerBackend, tailored for wallet-backend
// where exactly one goroutine consumes ledgers sequentially from each backend instance.
// This single-consumer contract enables several optimizations over the general-purpose SDK:
//
//   - No mutexes: The SDK uses an RWMutex (bufferLock) on every method and holds it across
//     2-second retry sleeps in GetLedger. With a single consumer, no synchronization is needed.
//
//   - No GetHealth call: The SDK calls GetHealth before every GetLedgers to check the retention
//     window. Since GetLedgersResponse.LatestLedger already reports the tip, we track it from
//     each response, saving one RPC round-trip per fetch.
//
//   - Buffer accumulation: The SDK clears the entire buffer on every fetch (initBuffer).
//     We accumulate across fetches and evict only entries below the consumer's position,
//     improving cache hit rates for sequential access.
//
//   - Plain fields: The SDK uses atomic.Uint32 for latestBufferLedger, chan+sync.Once for
//     close signaling. We use plain uint32 and bool since only one goroutine accesses them.
//
// Callers MUST NOT share an optimizedRPCBackend instance across goroutines.
package ingest

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/stellar/go-stellar-sdk/ingest/ledgerbackend"
	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"
	"github.com/stellar/go-stellar-sdk/xdr"
)

const (
	defaultRPCBufferSize uint32 = 10
	rpcRetryWaitInterval        = 2 * time.Second
)

// rpcClient defines the minimal RPC interface needed by optimizedRPCBackend.
// Satisfied by rpcclient.Client from the SDK.
type rpcClient interface {
	GetLedgers(ctx context.Context, req protocol.GetLedgersRequest) (protocol.GetLedgersResponse, error)
	GetHealth(ctx context.Context) (protocol.GetHealthResponse, error)
}

// optimizedRPCBackend fetches ledger data from a Stellar RPC server.
//
// It implements ledgerbackend.LedgerBackend with zero synchronization primitives,
// relying on the single-consumer contract for correctness.
// See package-level documentation for the full list of optimizations.
type optimizedRPCBackend struct {
	client     rpcClient
	bufferSize uint32

	// All fields below are accessed by exactly one goroutine — no mutex needed.
	buffer        map[uint32]xdr.LedgerCloseMeta
	preparedRange *ledgerbackend.Range
	nextLedger    uint32
	// latestLedger is updated from every GetLedgers response's LatestLedger field,
	// eliminating the separate GetHealth RPC call the SDK makes on each fetch.
	latestLedger uint32
	closed       bool
}

func newOptimizedRPCBackend(client rpcClient, bufferSize uint32) *optimizedRPCBackend {
	if bufferSize == 0 {
		bufferSize = defaultRPCBufferSize
	}
	return &optimizedRPCBackend{
		client:     client,
		bufferSize: bufferSize,
		buffer:     make(map[uint32]xdr.LedgerCloseMeta),
	}
}

// GetLatestLedgerSequence returns the latest ledger sequence observed from RPC responses.
//
// Optimization: This is a zero-cost plain field read. The SDK uses atomic.Uint32.Load()
// since multiple goroutines may access it; with a single consumer, no atomics are needed.
// The value is updated on every GetLedgers response in GetLedger/PrepareRange.
func (b *optimizedRPCBackend) GetLatestLedgerSequence(_ context.Context) (uint32, error) {
	if b.closed {
		return 0, fmt.Errorf("optimizedRPCBackend is closed")
	}
	if b.preparedRange == nil {
		return 0, fmt.Errorf("optimizedRPCBackend must be prepared before calling GetLatestLedgerSequence")
	}
	return b.latestLedger, nil
}

// GetLedger returns the ledger metadata for the given sequence number.
//
// Optimizations vs SDK RPCLedgerBackend.GetLedger:
//   - No lock held during retry sleep. The SDK holds a WLock across the entire method
//     including the 2-second retry wait, blocking all other methods. Here, no lock exists.
//   - GetHealth is not called. The SDK calls GetHealth before GetLedgers to check whether
//     the sequence is beyond the retention window. We use latestLedger (tracked from
//     GetLedgersResponse.LatestLedger) to make the same determination with zero RPC overhead.
//   - Buffer is accumulated, not cleared. The SDK calls initBuffer() (clearing the map) before
//     every fetch. We keep previously fetched ledgers and only evict entries below nextLedger,
//     so sequential access never re-fetches the same batch.
func (b *optimizedRPCBackend) GetLedger(ctx context.Context, sequence uint32) (xdr.LedgerCloseMeta, error) {
	if b.closed {
		return xdr.LedgerCloseMeta{}, fmt.Errorf("optimizedRPCBackend is closed")
	}
	if b.preparedRange == nil {
		return xdr.LedgerCloseMeta{}, fmt.Errorf("optimizedRPCBackend must be prepared before calling GetLedger")
	}

	r := *b.preparedRange
	if sequence < r.From() || (r.Bounded() && sequence > r.To()) {
		return xdr.LedgerCloseMeta{}, fmt.Errorf("requested ledger %d is outside prepared range %s", sequence, r)
	}
	if sequence != b.nextLedger {
		return xdr.LedgerCloseMeta{}, fmt.Errorf("requested ledger %d is not the expected ledger %d", sequence, b.nextLedger)
	}

	for {
		lcm, err := b.getBufferedLedger(ctx, sequence)
		if err == nil {
			b.nextLedger = sequence + 1
			b.evictOldEntries()
			return lcm, nil
		}

		if !isBeyondLatest(err) {
			return xdr.LedgerCloseMeta{}, err
		}

		// Ledger not available yet — wait and retry.
		// No lock is held during sleep (vs SDK which holds WLock for the entire duration).
		select {
		case <-ctx.Done():
			return xdr.LedgerCloseMeta{}, fmt.Errorf("waiting for ledger %d: %w", sequence, ctx.Err())
		case <-time.After(rpcRetryWaitInterval):
			continue
		}
	}
}

// PrepareRange validates the starting ledger availability and prepares the backend
// for sequential consumption from ledgerRange.From() onwards.
func (b *optimizedRPCBackend) PrepareRange(ctx context.Context, ledgerRange ledgerbackend.Range) error {
	if b.closed {
		return fmt.Errorf("optimizedRPCBackend is closed")
	}
	if b.preparedRange != nil {
		return fmt.Errorf("optimizedRPCBackend is already prepared with range %s", *b.preparedRange)
	}

	// Validate starting ledger availability via a GetLedgers call.
	// This also populates the buffer and latestLedger for the first GetLedger call.
	_, err := b.getBufferedLedger(ctx, ledgerRange.From())
	if err != nil && !isBeyondLatest(err) {
		return err
	}

	b.nextLedger = ledgerRange.From()
	b.preparedRange = &ledgerRange
	return nil
}

// IsPrepared reports whether the backend is prepared for the given range.
func (b *optimizedRPCBackend) IsPrepared(_ context.Context, ledgerRange ledgerbackend.Range) (bool, error) {
	if b.closed {
		return false, fmt.Errorf("optimizedRPCBackend is closed")
	}
	if b.preparedRange == nil {
		return false, nil
	}
	return b.preparedRange.Contains(ledgerRange), nil
}

// Close marks the backend as closed. Subsequent calls to any method will return an error.
func (b *optimizedRPCBackend) Close() error {
	b.closed = true
	return nil
}

// getBufferedLedger returns a ledger from the local buffer, fetching from RPC if not cached.
//
// Unlike the SDK, this method does NOT call GetHealth. It determines whether a ledger is
// beyond the RPC tip by comparing against latestLedger, which is updated from every
// GetLedgersResponse. This saves one RPC round-trip per fetch.
//
// Unlike the SDK, the buffer is NOT cleared before populating. Previously fetched ledgers
// remain available until evicted by evictOldEntries, improving cache locality for
// sequential access patterns.
func (b *optimizedRPCBackend) getBufferedLedger(ctx context.Context, sequence uint32) (xdr.LedgerCloseMeta, error) {
	if lcm, exists := b.buffer[sequence]; exists {
		return lcm, nil
	}

	// Fetch a batch starting from the requested sequence.
	resp, err := b.client.GetLedgers(ctx, protocol.GetLedgersRequest{
		StartLedger: sequence,
		Pagination: &protocol.LedgerPaginationOptions{
			Limit: uint(b.bufferSize),
		},
	})
	if err != nil {
		return xdr.LedgerCloseMeta{}, fmt.Errorf("fetching ledgers from RPC: %w", err)
	}

	// Track the RPC tip from every response — replaces the SDK's GetHealth call.
	b.latestLedger = resp.LatestLedger

	// Populate buffer (accumulate, don't clear).
	for _, info := range resp.Ledgers {
		var lcm xdr.LedgerCloseMeta
		if err := xdr.SafeUnmarshalBase64(info.LedgerMetadata, &lcm); err != nil {
			return xdr.LedgerCloseMeta{}, fmt.Errorf("unmarshalling ledger %d: %w", info.Sequence, err)
		}
		b.buffer[info.Sequence] = lcm
	}

	// Check if the requested sequence was in the response.
	if lcm, exists := b.buffer[sequence]; exists {
		return lcm, nil
	}

	// Sequence wasn't returned — either beyond the RPC tip or missing.
	if sequence > b.latestLedger {
		return xdr.LedgerCloseMeta{}, &beyondLatestError{sequence: sequence, latest: b.latestLedger}
	}
	return xdr.LedgerCloseMeta{}, &ledgerbackend.RPCLedgerMissingError{Sequence: sequence}
}

// evictOldEntries removes buffer entries that are below the current consumption position.
// The SDK clears the entire buffer on every fetch; we take the cheaper path of evicting
// only consumed entries, bounded at O(bufferSize) per call.
func (b *optimizedRPCBackend) evictOldEntries() {
	for seq := range b.buffer {
		if seq < b.nextLedger {
			delete(b.buffer, seq)
		}
	}
}

// beyondLatestError indicates the requested ledger is not yet available on the RPC server.
type beyondLatestError struct {
	sequence uint32
	latest   uint32
}

func (e *beyondLatestError) Error() string {
	return fmt.Sprintf("ledger %d is beyond latest RPC ledger %d", e.sequence, e.latest)
}

func isBeyondLatest(err error) bool {
	var target *beyondLatestError
	return errors.As(err, &target)
}
