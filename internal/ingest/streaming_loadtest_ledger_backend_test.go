package ingest

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/stellar/go-stellar-sdk/ingest/ledgerbackend"
	"github.com/stellar/go-stellar-sdk/xdr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// streamTestTimeout bounds every test: the backend blocks on FIFO reads, so a
// wiring mistake would otherwise hang the package.
const streamTestTimeout = 10 * time.Second

// reopenGrace is how long a test waits, after triggering a stream-epoch end,
// before attaching the replacement writer. A FIFO is a single shared object:
// a writer that attaches while the backend still holds the dead epoch's read
// descriptor writes into a buffer that is discarded when that descriptor
// closes, and the backend would then block forever on the reopened pipe.
const reopenGrace = 250 * time.Millisecond

// mkFIFOs creates n named pipes in one temp dir and returns their paths.
func mkFIFOs(t *testing.T, n int) []string {
	t.Helper()
	dir := t.TempDir()
	paths := make([]string, n)
	for i := range paths {
		paths[i] = filepath.Join(dir, fmt.Sprintf("meta-%d.pipe", i))
		require.NoError(t, syscall.Mkfifo(paths[i], 0o600))
	}
	return paths
}

func newStreamingBackend(t *testing.T, paths []string, pace time.Duration) *StreamingLoadtestLedgerBackend {
	t.Helper()
	backend, err := NewStreamingLoadtestLedgerBackend(StreamingLoadtestBackendConfig{
		MetaSources:         paths,
		LedgerCloseDuration: pace,
	})
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, backend.Close()) })
	return backend
}

// pipeFeeder writes stream-framed records onto one FIFO from a background
// goroutine. The goroutine is required: opening a FIFO for writing blocks
// until a reader attaches, and the backend opens its read side lazily inside
// GetLedger. Queued writes are delivered in order, so a send followed by a
// close reaches the reader as data-then-EOF.
type pipeFeeder struct {
	t         *testing.T
	path      string
	jobs      chan []byte
	closeOnce sync.Once
}

func newPipeFeeder(t *testing.T, path string) *pipeFeeder {
	t.Helper()
	f := &pipeFeeder{t: t, path: path, jobs: make(chan []byte, 256)}
	go f.run()
	t.Cleanup(f.close)
	return f
}

func (p *pipeFeeder) run() {
	file, err := os.OpenFile(p.path, os.O_WRONLY, 0)
	if err != nil {
		return
	}
	defer file.Close()
	for job := range p.jobs {
		if job == nil {
			return
		}
		if _, err := file.Write(job); err != nil {
			return // the backend closed its read side; nothing left to deliver
		}
	}
}

// send queues one stream frame per ledger. Marshalling happens on the calling
// goroutine so a malformed fixture fails the test directly.
func (p *pipeFeeder) send(ledgers ...xdr.LedgerCloseMeta) {
	p.t.Helper()
	for _, ledger := range ledgers {
		var buf bytes.Buffer
		require.NoError(p.t, xdr.MarshalFramed(&buf, ledger))
		p.jobs <- buf.Bytes()
	}
}

// sendRaw queues bytes verbatim, for fixtures that are not valid frames.
func (p *pipeFeeder) sendRaw(b []byte) {
	p.jobs <- b
}

// close flushes queued writes and then closes the write side, which the
// backend observes as end of stream.
func (p *pipeFeeder) close() {
	p.closeOnce.Do(func() { p.jobs <- nil })
}

// truncatedFrame is a stream frame whose header promises 64 payload bytes but
// which carries only 8, the way a killed writer leaves the pipe.
func truncatedFrame() []byte {
	var header [4]byte
	binary.BigEndian.PutUint32(header[:], 64|0x80000000)
	return append(header[:], make([]byte, 8)...)
}

// makeStreamLedger builds a marshalable V2 LedgerCloseMeta — the only version
// apply-load can emit (see mutableHeader) — with the given header sequence,
// txCount transactions, and one ledger entry per entrySeq value carried as an
// upgrade change. Close time is 0, matching what apply-load emits. The
// transaction set is a generalized V1 set with one phase, which is what
// appendLedger requires of both sides.
func makeStreamLedger(seq uint32, txCount int, entrySeqs ...uint32) xdr.LedgerCloseMeta {
	header := xdr.LedgerHeaderHistoryEntry{
		Header: xdr.LedgerHeader{
			LedgerSeq: xdr.Uint32(seq),
			ScpValue:  xdr.StellarValue{CloseTime: 0},
		},
	}
	txSet := xdr.GeneralizedTransactionSet{
		V: 1,
		V1TxSet: &xdr.TransactionSetV1{
			Phases: []xdr.TransactionPhase{{V: 0, V0Components: &[]xdr.TxSetComponent{}}},
		},
	}

	txProcessing := make([]xdr.TransactionResultMetaV1, txCount)
	for i := range txProcessing {
		txProcessing[i] = xdr.TransactionResultMetaV1{
			Result:            successfulTxResult(),
			TxApplyProcessing: xdr.TransactionMeta{V: 3, V3: &xdr.TransactionMetaV3{}},
		}
	}
	return xdr.LedgerCloseMeta{V: 2, V2: &xdr.LedgerCloseMetaV2{
		LedgerHeader:       header,
		TxSet:              txSet,
		TxProcessing:       txProcessing,
		UpgradesProcessing: entryUpgrades(entrySeqs),
	}}
}

// makeV0StreamLedger builds a V0 LedgerCloseMeta, the pre-generalized-txset
// shape the backend refuses to renumber or merge.
func makeV0StreamLedger(seq uint32) xdr.LedgerCloseMeta {
	return xdr.LedgerCloseMeta{V: 0, V0: &xdr.LedgerCloseMetaV0{
		LedgerHeader: xdr.LedgerHeaderHistoryEntry{
			Header: xdr.LedgerHeader{LedgerSeq: xdr.Uint32(seq)},
		},
	}}
}

// makeV1StreamLedger builds a V1 LedgerCloseMeta, the protocol-20-22 shape
// apply-load cannot produce and the backend therefore rejects.
func makeV1StreamLedger(seq uint32) xdr.LedgerCloseMeta {
	return xdr.LedgerCloseMeta{V: 1, V1: &xdr.LedgerCloseMetaV1{
		LedgerHeader: xdr.LedgerHeaderHistoryEntry{
			Header: xdr.LedgerHeader{LedgerSeq: xdr.Uint32(seq)},
		},
		TxSet: xdr.GeneralizedTransactionSet{
			V:       1,
			V1TxSet: &xdr.TransactionSetV1{},
		},
	}}
}

func successfulTxResult() xdr.TransactionResultPair {
	return xdr.TransactionResultPair{
		Result: xdr.TransactionResult{
			FeeCharged: 100,
			Result: xdr.TransactionResultResult{
				Code:    xdr.TransactionResultCodeTxSuccess,
				Results: &[]xdr.OperationResult{},
			},
		},
	}
}

// entryUpgrades parks one ledger entry per sequence in a base-fee upgrade's
// changes. Upgrade changes are a convenient carrier because appendLedger
// appends them in pipe order, so a merged ledger exposes each source stream's
// entries separately.
func entryUpgrades(entrySeqs []uint32) []xdr.UpgradeEntryMeta {
	if len(entrySeqs) == 0 {
		return nil
	}
	newBaseFee := xdr.Uint32(100)
	changes := make(xdr.LedgerEntryChanges, 0, len(entrySeqs))
	for _, seq := range entrySeqs {
		changes = append(changes, xdr.LedgerEntryChange{
			Type: xdr.LedgerEntryChangeTypeLedgerEntryState,
			State: &xdr.LedgerEntry{
				LastModifiedLedgerSeq: xdr.Uint32(seq),
				Data: xdr.LedgerEntryData{
					Type: xdr.LedgerEntryTypeTtl,
					Ttl:  &xdr.TtlEntry{},
				},
			},
		})
	}
	return []xdr.UpgradeEntryMeta{{
		Upgrade: xdr.LedgerUpgrade{
			Type:       xdr.LedgerUpgradeTypeLedgerUpgradeBaseFee,
			NewBaseFee: &newBaseFee,
		},
		Changes: changes,
	}}
}

// streamEntrySeqs returns the LastModifiedLedgerSeq of every entry the ledger
// carries, in merge order (first pipe's entries first).
func streamEntrySeqs(t *testing.T, lcm xdr.LedgerCloseMeta) []uint32 {
	t.Helper()
	require.Equal(t, int32(2), lcm.V, "unexpected ledger version")
	var seqs []uint32
	for _, upgrade := range lcm.V2.UpgradesProcessing {
		for _, change := range upgrade.Changes {
			require.NotNil(t, change.State)
			seqs = append(seqs, uint32(change.State.LastModifiedLedgerSeq))
		}
	}
	return seqs
}

func streamTxCount(t *testing.T, lcm xdr.LedgerCloseMeta) int {
	t.Helper()
	require.Equal(t, int32(2), lcm.V, "unexpected ledger version")
	return len(lcm.V2.TxProcessing)
}

func streamPhaseCount(t *testing.T, lcm xdr.LedgerCloseMeta) int {
	t.Helper()
	require.Equal(t, int32(2), lcm.V, "unexpected ledger version")
	return len(lcm.V2.TxSet.V1TxSet.Phases)
}

func streamCloseTime(t *testing.T, lcm xdr.LedgerCloseMeta) xdr.TimePoint {
	t.Helper()
	require.Equal(t, int32(2), lcm.V, "unexpected ledger version")
	return lcm.V2.LedgerHeader.Header.ScpValue.CloseTime
}

type getLedgerResult struct {
	lcm xdr.LedgerCloseMeta
	err error
}

// getLedgerAsync runs GetLedger on its own goroutine, for the cases where it
// must block until the test attaches a new writer.
func getLedgerAsync(backend *StreamingLoadtestLedgerBackend, ctx context.Context, sequence uint32) <-chan getLedgerResult {
	ch := make(chan getLedgerResult, 1)
	go func() {
		lcm, err := backend.GetLedger(ctx, sequence)
		ch <- getLedgerResult{lcm: lcm, err: err}
	}()
	return ch
}

func TestNewStreamingLoadtestLedgerBackend_RejectsBadConfig(t *testing.T) {
	_, err := NewStreamingLoadtestLedgerBackend(StreamingLoadtestBackendConfig{})
	require.ErrorContains(t, err, "MetaSources is required")

	_, err = NewStreamingLoadtestLedgerBackend(StreamingLoadtestBackendConfig{
		MetaSources: []string{"/tmp/a.pipe", ""},
	})
	require.ErrorContains(t, err, "MetaSources[1] is empty")

	_, err = NewStreamingLoadtestLedgerBackend(StreamingLoadtestBackendConfig{
		MetaSources: []string{"tcp-listen://127.0.0.1:notaport"},
	})
	require.ErrorContains(t, err, "MetaSources[0]: listening on")
}

func TestStreamingLoadtestBackend_RenumbersFirstEpoch(t *testing.T) {
	paths := mkFIFOs(t, 1)
	backend := newStreamingBackend(t, paths, 0)

	// apply-load starts each run at raw ledger 2 (genesis emits no meta) and
	// tags genesis-created entries with lastModified 1.
	feeder := newPipeFeeder(t, paths[0])
	for raw := uint32(2); raw <= 4; raw++ {
		feeder.send(makeStreamLedger(raw, 1, 1, raw+5))
	}

	ctx, cancel := context.WithTimeout(context.Background(), streamTestTimeout)
	defer cancel()
	require.NoError(t, backend.PrepareRange(ctx, ledgerbackend.UnboundedRange(1)))

	for _, want := range []uint32{1, 2, 3} {
		lcm, err := backend.GetLedger(ctx, want)
		require.NoError(t, err)
		assert.Equal(t, want, lcm.LedgerSequence())
		// Only the header is renumbered (raw 2..4 emitted as 1..3). Entry
		// ledger-sequence references keep their raw values — nothing
		// downstream reads them, and rewriting them costs a full XDR
		// round trip per frame. Raw here is want+1 (the epoch diff is -1).
		assert.Equal(t, []uint32{1, want + 1 + 5}, streamEntrySeqs(t, lcm))
	}
}

func TestStreamingLoadtestBackend_MergesEveryPipe(t *testing.T) {
	paths := mkFIFOs(t, 3)
	backend := newStreamingBackend(t, paths, 0)

	// Different raw starting sequences per pipe: nextFrame verifies every
	// frame against its pipe's own diff, so the merge succeeding at all
	// proves the diff is derived per pipe (a shared diff would trip the
	// sequence-mismatch error on two of the three pipes).
	rawStarts := []uint32{2, 100, 7}
	txCounts := []int{1, 2, 3}
	for i, path := range paths {
		feeder := newPipeFeeder(t, path)
		for n := uint32(0); n < 2; n++ {
			raw := rawStarts[i] + n
			feeder.send(makeStreamLedger(raw, txCounts[i], raw+5))
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), streamTestTimeout)
	defer cancel()
	require.NoError(t, backend.PrepareRange(ctx, ledgerbackend.UnboundedRange(1)))

	for _, want := range []uint32{1, 2} {
		lcm, err := backend.GetLedger(ctx, want)
		require.NoError(t, err)
		assert.Equal(t, want, lcm.LedgerSequence())
		assert.Equal(t, 6, streamTxCount(t, lcm), "merged ledger holds every pipe's transactions")
		assert.Equal(t, 3, streamPhaseCount(t, lcm), "merged ledger holds every pipe's txset phases")
		// Entries keep each stream's raw sequence references (raw start + n + 5).
		assert.Equal(t, []uint32{want + 6, want + 104, want + 11}, streamEntrySeqs(t, lcm))
	}
}

func TestStreamingLoadtestBackend_RestartsOnePipeOnly(t *testing.T) {
	paths := mkFIFOs(t, 3)
	backend := newStreamingBackend(t, paths, 0)

	// Pipes 0 and 2 run for the whole test. Pipe 1's writer exits after two
	// ledgers and a fresh one attaches, the way a restarted apply-load does.
	for _, i := range []int{0, 2} {
		feeder := newPipeFeeder(t, paths[i])
		for raw := uint32(2); raw <= 8; raw++ {
			feeder.send(makeStreamLedger(raw, 1))
		}
	}
	shortLived := newPipeFeeder(t, paths[1])
	shortLived.send(makeStreamLedger(2, 1), makeStreamLedger(3, 1))

	ctx, cancel := context.WithTimeout(context.Background(), streamTestTimeout)
	defer cancel()
	require.NoError(t, backend.PrepareRange(ctx, ledgerbackend.UnboundedRange(1)))

	for _, want := range []uint32{1, 2} {
		lcm, err := backend.GetLedger(ctx, want)
		require.NoError(t, err)
		require.Equal(t, want, lcm.LedgerSequence())
		require.Equal(t, 3, streamTxCount(t, lcm))
	}
	shortLived.close()

	// This call blocks: pipe 1 hits EOF, the backend drops that epoch and
	// waits in open(2) for a new writer while pipes 0 and 2 hold their peeked
	// frames.
	pending := getLedgerAsync(backend, ctx, 3)
	time.Sleep(reopenGrace)
	restarted := newPipeFeeder(t, paths[1])
	restarted.send(makeStreamLedger(2, 1), makeStreamLedger(3, 1))

	res := <-pending
	require.NoError(t, res.err)
	assert.Equal(t, uint32(3), res.lcm.LedgerSequence())
	assert.Equal(t, 3, streamTxCount(t, res.lcm), "the restarted pipe's frame is merged in")

	// The restarted pipe advances on its own diff (raw 3 -> 4) while the
	// untouched pipes stay in lockstep on theirs (raw 5 -> 4).
	lcm, err := backend.GetLedger(ctx, 4)
	require.NoError(t, err)
	assert.Equal(t, uint32(4), lcm.LedgerSequence())
	assert.Equal(t, 3, streamTxCount(t, lcm))
}

func TestStreamingLoadtestBackend_RecoversFromTruncatedFrame(t *testing.T) {
	paths := mkFIFOs(t, 1)
	backend := newStreamingBackend(t, paths, 0)

	killed := newPipeFeeder(t, paths[0])
	killed.send(makeStreamLedger(2, 1))
	killed.sendRaw(truncatedFrame())
	killed.close()

	ctx, cancel := context.WithTimeout(context.Background(), streamTestTimeout)
	defer cancel()
	require.NoError(t, backend.PrepareRange(ctx, ledgerbackend.UnboundedRange(1)))

	lcm, err := backend.GetLedger(ctx, 1)
	require.NoError(t, err)
	require.Equal(t, uint32(1), lcm.LedgerSequence())

	// The decode error must end the epoch and wait for a new writer, not
	// surface to the consumer.
	pending := getLedgerAsync(backend, ctx, 2)
	time.Sleep(reopenGrace)
	clean := newPipeFeeder(t, paths[0])
	clean.send(makeStreamLedger(2, 1), makeStreamLedger(3, 1))

	res := <-pending
	require.NoError(t, res.err)
	assert.Equal(t, uint32(2), res.lcm.LedgerSequence())

	lcm, err = backend.GetLedger(ctx, 3)
	require.NoError(t, err)
	assert.Equal(t, uint32(3), lcm.LedgerSequence())
}

func TestStreamingLoadtestBackend_StampsAdvancingCloseTime(t *testing.T) {
	paths := mkFIFOs(t, 1)
	backend := newStreamingBackend(t, paths, 0)

	feeder := newPipeFeeder(t, paths[0])
	for raw := uint32(2); raw <= 4; raw++ {
		feeder.send(makeStreamLedger(raw, 1))
	}

	ctx, cancel := context.WithTimeout(context.Background(), streamTestTimeout)
	defer cancel()
	require.NoError(t, backend.PrepareRange(ctx, ledgerbackend.UnboundedRange(1)))

	var previous xdr.TimePoint
	for _, want := range []uint32{1, 2, 3} {
		lcm, err := backend.GetLedger(ctx, want)
		require.NoError(t, err)
		closeTime := streamCloseTime(t, lcm)
		assert.Greater(t, uint64(closeTime), uint64(0), "apply-load's closeTime 0 must be replaced")
		assert.GreaterOrEqual(t, uint64(closeTime), uint64(previous))
		previous = closeTime
	}
}

func TestStreamingLoadtestBackend_RetryServesCachedLedger(t *testing.T) {
	paths := mkFIFOs(t, 1)
	backend := newStreamingBackend(t, paths, 0)

	// Exactly one frame per emitted ledger: a retry that consumed another
	// frame would leave nothing for the follow-up request.
	feeder := newPipeFeeder(t, paths[0])
	feeder.send(makeStreamLedger(2, 1), makeStreamLedger(3, 1))

	ctx, cancel := context.WithTimeout(context.Background(), streamTestTimeout)
	defer cancel()
	require.NoError(t, backend.PrepareRange(ctx, ledgerbackend.UnboundedRange(1)))

	_, err := backend.GetLedger(ctx, 1)
	require.NoError(t, err)
	first, err := backend.GetLedger(ctx, 2)
	require.NoError(t, err)

	retried, err := backend.GetLedger(ctx, 2)
	require.NoError(t, err)
	assert.Equal(t, first, retried, "a repeat request replays the cached ledger verbatim")

	feeder.send(makeStreamLedger(4, 1))
	lcm, err := backend.GetLedger(ctx, 3)
	require.NoError(t, err)
	assert.Equal(t, uint32(3), lcm.LedgerSequence(), "the retry left the stream position untouched")
}

func TestStreamingLoadtestBackend_PacesEmits(t *testing.T) {
	paths := mkFIFOs(t, 1)
	pace := 100 * time.Millisecond
	backend := newStreamingBackend(t, paths, pace)

	feeder := newPipeFeeder(t, paths[0])
	for raw := uint32(2); raw <= 4; raw++ {
		feeder.send(makeStreamLedger(raw, 1))
	}

	ctx, cancel := context.WithTimeout(context.Background(), streamTestTimeout)
	defer cancel()
	require.NoError(t, backend.PrepareRange(ctx, ledgerbackend.UnboundedRange(1)))

	start := time.Now()
	_, err := backend.GetLedger(ctx, 1)
	require.NoError(t, err)
	assert.Less(t, time.Since(start), pace, "the first emit has nothing to pace against")

	for _, want := range []uint32{2, 3} {
		start = time.Now()
		_, err = backend.GetLedger(ctx, want)
		require.NoError(t, err)
		assert.GreaterOrEqual(t, time.Since(start), pace-10*time.Millisecond,
			"emit %d should wait out the close duration", want)
	}
}

func TestStreamingLoadtestBackend_TracksLatestLedgerSequence(t *testing.T) {
	paths := mkFIFOs(t, 1)
	backend := newStreamingBackend(t, paths, 0)

	feeder := newPipeFeeder(t, paths[0])
	feeder.send(makeStreamLedger(2, 1), makeStreamLedger(3, 1))

	ctx, cancel := context.WithTimeout(context.Background(), streamTestTimeout)
	defer cancel()

	_, err := backend.GetLatestLedgerSequence(ctx)
	require.ErrorContains(t, err, "before PrepareRange")

	require.NoError(t, backend.PrepareRange(ctx, ledgerbackend.UnboundedRange(100)))
	seq, err := backend.GetLatestLedgerSequence(ctx)
	require.NoError(t, err)
	assert.Equal(t, uint32(100), seq, "before the first emit the range start is the tip")

	for _, want := range []uint32{100, 101} {
		_, err = backend.GetLedger(ctx, want)
		require.NoError(t, err)
		seq, err = backend.GetLatestLedgerSequence(ctx)
		require.NoError(t, err)
		assert.Equal(t, want, seq)
	}

	prepared, err := backend.IsPrepared(ctx, ledgerbackend.UnboundedRange(100))
	require.NoError(t, err)
	assert.True(t, prepared)

	prepared, err = backend.IsPrepared(ctx, ledgerbackend.BoundedRange(100, 101))
	require.NoError(t, err)
	assert.False(t, prepared, "the backend never serves a bounded range")
}

func TestStreamingLoadtestBackend_RejectsBadRequests(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), streamTestTimeout)
	defer cancel()

	t.Run("bounded range", func(t *testing.T) {
		backend := newStreamingBackend(t, mkFIFOs(t, 1), 0)
		err := backend.PrepareRange(ctx, ledgerbackend.BoundedRange(1, 5))
		require.ErrorContains(t, err, "only supports unbounded ranges")
	})

	t.Run("get before prepare", func(t *testing.T) {
		backend := newStreamingBackend(t, mkFIFOs(t, 1), 0)
		_, err := backend.GetLedger(ctx, 1)
		require.ErrorContains(t, err, "GetLedger called before PrepareRange")
	})

	t.Run("first request is not the range start", func(t *testing.T) {
		backend := newStreamingBackend(t, mkFIFOs(t, 1), 0)
		require.NoError(t, backend.PrepareRange(ctx, ledgerbackend.UnboundedRange(5)))
		_, err := backend.GetLedger(ctx, 6)
		require.ErrorContains(t, err, "non-sequential ledger request: expected 5, got 6")
	})

	t.Run("request skips ahead after an emit", func(t *testing.T) {
		paths := mkFIFOs(t, 1)
		backend := newStreamingBackend(t, paths, 0)
		feeder := newPipeFeeder(t, paths[0])
		feeder.send(makeStreamLedger(2, 1), makeStreamLedger(3, 1))

		require.NoError(t, backend.PrepareRange(ctx, ledgerbackend.UnboundedRange(1)))
		_, err := backend.GetLedger(ctx, 1)
		require.NoError(t, err)
		_, err = backend.GetLedger(ctx, 3)
		require.ErrorContains(t, err, "non-sequential ledger request: expected 2, got 3")
	})

	t.Run("unsupported ledger versions", func(t *testing.T) {
		// V0 predates generalized transaction sets; V1 (protocols 20-22)
		// cannot come out of apply-load, which always runs the core binary's
		// current protocol. The backend is V2-only and must reject both.
		for name, ledger := range map[string]xdr.LedgerCloseMeta{
			"v0": makeV0StreamLedger(2),
			"v1": makeV1StreamLedger(2),
		} {
			t.Run(name, func(t *testing.T) {
				paths := mkFIFOs(t, 1)
				backend := newStreamingBackend(t, paths, 0)
				feeder := newPipeFeeder(t, paths[0])
				feeder.send(ledger)

				require.NoError(t, backend.PrepareRange(ctx, ledgerbackend.UnboundedRange(1)))
				_, err := backend.GetLedger(ctx, 1)
				require.ErrorContains(t, err, "is not supported")
			})
		}
	})
}

func TestStreamingLoadtestBackend_RejectsSequenceGapWithinEpoch(t *testing.T) {
	paths := mkFIFOs(t, 1)
	backend := newStreamingBackend(t, paths, 0)

	// A gap inside one writer's lifetime means frames were lost, which is not
	// the same as the writer restarting and is not recoverable by reopening.
	feeder := newPipeFeeder(t, paths[0])
	feeder.send(makeStreamLedger(2, 1), makeStreamLedger(5, 1))

	ctx, cancel := context.WithTimeout(context.Background(), streamTestTimeout)
	defer cancel()
	require.NoError(t, backend.PrepareRange(ctx, ledgerbackend.UnboundedRange(1)))

	_, err := backend.GetLedger(ctx, 1)
	require.NoError(t, err)

	_, err = backend.GetLedger(ctx, 2)
	require.ErrorContains(t, err, "sequence mismatch within stream epoch")
}

func TestNewLedgerBackend_StreamingLoadtest(t *testing.T) {
	backend, err := NewLedgerBackend(context.Background(), Configs{
		LedgerBackendType:           LedgerBackendTypeStreamingLoadtest,
		LoadtestMetaSources:         mkFIFOs(t, 2),
		LoadtestLedgerCloseDuration: 500 * time.Millisecond,
	})
	require.NoError(t, err)
	require.NotNil(t, backend)
	streaming, ok := backend.(*StreamingLoadtestLedgerBackend)
	require.True(t, ok, "NewLedgerBackend should return a StreamingLoadtestLedgerBackend")
	assert.Len(t, streaming.sources, 2)
	assert.Equal(t, 500*time.Millisecond, streaming.config.LedgerCloseDuration)
	assert.NoError(t, backend.Close())
}

// newTCPStreamingBackend builds a backend with a single tcp-listen source on
// an ephemeral loopback port and returns it with the resolved listener
// address to dial. (Multi-source TCP coverage rides on the mixed-sources
// test.)
func newTCPStreamingBackend(t *testing.T) (*StreamingLoadtestLedgerBackend, string) {
	t.Helper()
	backend, err := NewStreamingLoadtestLedgerBackend(StreamingLoadtestBackendConfig{
		MetaSources: []string{"tcp-listen://127.0.0.1:0"},
	})
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, backend.Close()) })
	source := backend.sources[0]
	require.NotNil(t, source.listener, "tcp-listen source must hold a listener")
	return backend, source.listener.Addr().String()
}

// tcpFeeder writes stream-framed records over one dialed connection, the way
// an apply-load producer streams through the dialer shim's socket. Dialing
// succeeds as soon as the backend's listener exists (the constructor binds it
// eagerly), even before the backend accepts: the connection waits in the
// accept backlog.
type tcpFeeder struct {
	t    *testing.T
	conn net.Conn
}

func newTCPFeeder(t *testing.T, addr string) *tcpFeeder {
	t.Helper()
	conn, err := net.Dial("tcp", addr)
	require.NoError(t, err)
	f := &tcpFeeder{t: t, conn: conn}
	t.Cleanup(f.close)
	return f
}

func (f *tcpFeeder) send(ledgers ...xdr.LedgerCloseMeta) {
	f.t.Helper()
	for _, ledger := range ledgers {
		var buf bytes.Buffer
		require.NoError(f.t, xdr.MarshalFramed(&buf, ledger))
		_, err := f.conn.Write(buf.Bytes())
		require.NoError(f.t, err)
	}
}

// close closes the connection, which the backend observes as end of stream —
// the TCP equivalent of an apply-load process exiting.
func (f *tcpFeeder) close() {
	_ = f.conn.Close()
}

func TestStreamingLoadtestBackend_TCPStreamsFrames(t *testing.T) {
	backend, addr := newTCPStreamingBackend(t)

	feeder := newTCPFeeder(t, addr)
	for raw := uint32(2); raw <= 4; raw++ {
		feeder.send(makeStreamLedger(raw, 1, 1, raw+5))
	}

	ctx, cancel := context.WithTimeout(context.Background(), streamTestTimeout)
	defer cancel()
	require.NoError(t, backend.PrepareRange(ctx, ledgerbackend.UnboundedRange(1)))

	for _, want := range []uint32{1, 2, 3} {
		lcm, err := backend.GetLedger(ctx, want)
		require.NoError(t, err)
		assert.Equal(t, want, lcm.LedgerSequence())
		assert.Equal(t, []uint32{1, want + 1 + 5}, streamEntrySeqs(t, lcm))
	}
}

func TestStreamingLoadtestBackend_TCPReconnectStartsNewEpoch(t *testing.T) {
	backend, addr := newTCPStreamingBackend(t)

	first := newTCPFeeder(t, addr)
	first.send(makeStreamLedger(2, 1), makeStreamLedger(3, 1))

	ctx, cancel := context.WithTimeout(context.Background(), streamTestTimeout)
	defer cancel()
	require.NoError(t, backend.PrepareRange(ctx, ledgerbackend.UnboundedRange(1)))

	for _, want := range []uint32{1, 2} {
		lcm, err := backend.GetLedger(ctx, want)
		require.NoError(t, err)
		require.Equal(t, want, lcm.LedgerSequence())
	}
	first.close()

	// This call blocks: the connection's close ends the epoch and the backend
	// re-accepts on the same listener. Unlike a FIFO there is no shared-object
	// reopen hazard — the replacement dial is a distinct connection, so no
	// grace delay is needed before attaching it.
	pending := getLedgerAsync(backend, ctx, 3)
	restarted := newTCPFeeder(t, addr)
	restarted.send(makeStreamLedger(2, 1), makeStreamLedger(3, 1))

	res := <-pending
	require.NoError(t, res.err)
	assert.Equal(t, uint32(3), res.lcm.LedgerSequence(), "the reconnect's raw ledger 2 re-anchors onto the requested sequence")

	lcm, err := backend.GetLedger(ctx, 4)
	require.NoError(t, err)
	assert.Equal(t, uint32(4), lcm.LedgerSequence(), "the new epoch advances on its own diff")
}

func TestStreamingLoadtestBackend_MergesMixedSources(t *testing.T) {
	fifo := mkFIFOs(t, 1)[0]
	backend, err := NewStreamingLoadtestLedgerBackend(StreamingLoadtestBackendConfig{
		MetaSources: []string{fifo, "tcp-listen://127.0.0.1:0"},
	})
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, backend.Close()) })

	pipeSide := newPipeFeeder(t, fifo)
	pipeSide.send(makeStreamLedger(2, 1, 7), makeStreamLedger(3, 1, 8))
	tcpSide := newTCPFeeder(t, backend.sources[1].listener.Addr().String())
	tcpSide.send(makeStreamLedger(50, 2, 55), makeStreamLedger(51, 2, 56))

	ctx, cancel := context.WithTimeout(context.Background(), streamTestTimeout)
	defer cancel()
	require.NoError(t, backend.PrepareRange(ctx, ledgerbackend.UnboundedRange(1)))

	for _, want := range []uint32{1, 2} {
		lcm, err := backend.GetLedger(ctx, want)
		require.NoError(t, err)
		assert.Equal(t, want, lcm.LedgerSequence())
		assert.Equal(t, 3, streamTxCount(t, lcm), "merged ledger holds both sources' transactions")
		assert.Equal(t, []uint32{want + 6, want + 54}, streamEntrySeqs(t, lcm), "FIFO entries merge before TCP entries (source order)")
	}
}

func TestStreamingLoadtestBackend_TCPAdoptsPendingAcceptAfterCancel(t *testing.T) {
	backend, addr := newTCPStreamingBackend(t)

	prepCtx, prepCancel := context.WithTimeout(context.Background(), streamTestTimeout)
	defer prepCancel()
	require.NoError(t, backend.PrepareRange(prepCtx, ledgerbackend.UnboundedRange(1)))

	// No producer has dialed: GetLedger parks in the accept and the context
	// cancellation surfaces, leaving the accept pending on the source.
	shortCtx, shortCancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer shortCancel()
	_, err := backend.GetLedger(shortCtx, 1)
	require.ErrorContains(t, err, "waiting for stream writer")

	// The retried call adopts the pending accept instead of stacking another.
	feeder := newTCPFeeder(t, addr)
	feeder.send(makeStreamLedger(2, 1))
	lcm, err := backend.GetLedger(prepCtx, 1)
	require.NoError(t, err)
	assert.Equal(t, uint32(1), lcm.LedgerSequence())
}

func TestStreamingLoadtestBackend_CloseStopsListening(t *testing.T) {
	backend, addr := newTCPStreamingBackend(t)
	require.NoError(t, backend.Close())

	_, err := net.Dial("tcp", addr)
	require.Error(t, err, "a closed backend's listener refuses producer dials")
}
