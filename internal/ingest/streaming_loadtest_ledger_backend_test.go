package ingest

import (
	"bytes"
	"context"
	"encoding/binary"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/stellar/go-stellar-sdk/ingest/ledgerbackend"
	"github.com/stellar/go-stellar-sdk/xdr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mkFIFO creates a named pipe in a temp dir. Returns the path; t.Cleanup removes it.
func mkFIFO(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "meta.pipe")
	require.NoError(t, syscall.Mkfifo(path, 0o600))
	return path
}

func TestStreamingLoadtestBackend_PrepareRangeOpensPipe(t *testing.T) {
	pipePath := mkFIFO(t)

	backend, err := NewStreamingLoadtestLedgerBackend(StreamingLoadtestBackendConfig{
		MetaPipePath:        pipePath,
		LedgerCloseDuration: 0,
		NetworkPassphrase:   "Apply Load",
	})
	require.NoError(t, err)
	defer backend.Close()

	// A write-side opener must exist for the read-side open to proceed.
	writerOpened := make(chan struct{})
	go func() {
		f, err := os.OpenFile(pipePath, os.O_WRONLY, 0)
		if err == nil {
			close(writerOpened)
			t.Cleanup(func() { _ = f.Close() })
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	err = backend.PrepareRange(ctx, ledgerbackend.UnboundedRange(2))
	require.NoError(t, err)
	assert.True(t, backend.prepared, "PrepareRange should have set prepared=true")
}

// writeLedgerCloseMeta writes a single LedgerCloseMeta as a stream-framed XDR
// record to the given writer. Mimics what stellar-core apply-load produces.
func writeLedgerCloseMeta(t *testing.T, w *os.File, seq uint32) {
	t.Helper()
	lcm := xdr.LedgerCloseMeta{
		V: 0,
		V0: &xdr.LedgerCloseMetaV0{
			LedgerHeader: xdr.LedgerHeaderHistoryEntry{
				Header: xdr.LedgerHeader{
					LedgerSeq: xdr.Uint32(seq),
				},
			},
		},
	}
	var payload bytes.Buffer
	_, err := xdr.Marshal(&payload, &lcm)
	require.NoError(t, err)

	length := uint32(payload.Len()) | 0x80000000
	var header [4]byte
	binary.BigEndian.PutUint32(header[:], length)
	_, err = w.Write(header[:])
	require.NoError(t, err)
	_, err = w.Write(payload.Bytes())
	require.NoError(t, err)
}

func TestStreamingLoadtestBackend_GetLedgerReadsFrame(t *testing.T) {
	pipePath := mkFIFO(t)

	backend, err := NewStreamingLoadtestLedgerBackend(StreamingLoadtestBackendConfig{
		MetaPipePath:        pipePath,
		LedgerCloseDuration: 0,
		NetworkPassphrase:   "Apply Load",
	})
	require.NoError(t, err)
	defer backend.Close()

	writerDone := make(chan error, 1)
	go func() {
		f, err := os.OpenFile(pipePath, os.O_WRONLY, 0)
		if err != nil {
			writerDone <- err
			return
		}
		defer f.Close()
		writeLedgerCloseMeta(t, f, 42)
		writerDone <- nil
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	require.NoError(t, backend.PrepareRange(ctx, ledgerbackend.UnboundedRange(42)))

	got, err := backend.GetLedger(ctx, 42)
	require.NoError(t, err)
	assert.Equal(t, uint32(42), got.LedgerSequence())
	require.NoError(t, <-writerDone)
}

func TestStreamingLoadtestBackend_GetLedgerPaces(t *testing.T) {
	pipePath := mkFIFO(t)

	pace := 200 * time.Millisecond
	backend, err := NewStreamingLoadtestLedgerBackend(StreamingLoadtestBackendConfig{
		MetaPipePath:        pipePath,
		LedgerCloseDuration: pace,
		NetworkPassphrase:   "Apply Load",
	})
	require.NoError(t, err)
	defer backend.Close()

	writerDone := make(chan error, 1)
	go func() {
		f, err := os.OpenFile(pipePath, os.O_WRONLY, 0)
		if err != nil {
			writerDone <- err
			return
		}
		defer f.Close()
		writeLedgerCloseMeta(t, f, 1)
		writeLedgerCloseMeta(t, f, 2)
		writeLedgerCloseMeta(t, f, 3)
		writerDone <- nil
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, backend.PrepareRange(ctx, ledgerbackend.UnboundedRange(1)))

	start := time.Now()
	_, err = backend.GetLedger(ctx, 1)
	require.NoError(t, err)
	elapsedFirst := time.Since(start)
	assert.Less(t, elapsedFirst, 100*time.Millisecond, "first GetLedger should not sleep")

	start2 := time.Now()
	_, err = backend.GetLedger(ctx, 2)
	require.NoError(t, err)
	elapsedSecond := time.Since(start2)
	assert.GreaterOrEqual(t, elapsedSecond, pace-20*time.Millisecond,
		"second GetLedger should pace by at least %v", pace)

	start3 := time.Now()
	_, err = backend.GetLedger(ctx, 3)
	require.NoError(t, err)
	elapsedThird := time.Since(start3)
	assert.GreaterOrEqual(t, elapsedThird, pace-20*time.Millisecond,
		"third GetLedger should pace by at least %v", pace)

	require.NoError(t, <-writerDone)
}

func TestStreamingLoadtestBackend_GetLatestLedgerSequence(t *testing.T) {
	pipePath := mkFIFO(t)

	backend, err := NewStreamingLoadtestLedgerBackend(StreamingLoadtestBackendConfig{
		MetaPipePath: pipePath,
	})
	require.NoError(t, err)
	defer backend.Close()

	writerDone := make(chan error, 1)
	go func() {
		f, err := os.OpenFile(pipePath, os.O_WRONLY, 0)
		if err != nil {
			writerDone <- err
			return
		}
		defer f.Close()
		writeLedgerCloseMeta(t, f, 100)
		writeLedgerCloseMeta(t, f, 101)
		writerDone <- nil
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	require.NoError(t, backend.PrepareRange(ctx, ledgerbackend.UnboundedRange(100)))

	_, err = backend.GetLedger(ctx, 100)
	require.NoError(t, err)
	seq, err := backend.GetLatestLedgerSequence(ctx)
	require.NoError(t, err)
	assert.Equal(t, uint32(100), seq)

	_, err = backend.GetLedger(ctx, 101)
	require.NoError(t, err)
	seq, err = backend.GetLatestLedgerSequence(ctx)
	require.NoError(t, err)
	assert.Equal(t, uint32(101), seq)
	require.NoError(t, <-writerDone)
}

func TestStreamingLoadtestBackend_GetLedgerEOF(t *testing.T) {
	pipePath := mkFIFO(t)

	backend, err := NewStreamingLoadtestLedgerBackend(StreamingLoadtestBackendConfig{
		MetaPipePath: pipePath,
	})
	require.NoError(t, err)
	defer backend.Close()

	writerDone := make(chan error, 1)
	go func() {
		f, err := os.OpenFile(pipePath, os.O_WRONLY, 0)
		if err != nil {
			writerDone <- err
			return
		}
		writeLedgerCloseMeta(t, f, 5)
		f.Close()
		writerDone <- nil
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	require.NoError(t, backend.PrepareRange(ctx, ledgerbackend.UnboundedRange(5)))

	_, err = backend.GetLedger(ctx, 5)
	require.NoError(t, err)

	_, err = backend.GetLedger(ctx, 6)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "meta stream ended")
	require.NoError(t, <-writerDone)
}

func TestNewLedgerBackend_StreamingLoadtest(t *testing.T) {
	pipePath := mkFIFO(t)

	go func() {
		f, err := os.OpenFile(pipePath, os.O_WRONLY, 0)
		if err == nil {
			t.Cleanup(func() { _ = f.Close() })
		}
	}()

	cfg := Configs{
		LedgerBackendType:   LedgerBackendTypeStreamingLoadtest,
		MetaPipePath:        pipePath,
		LedgerCloseDuration: 500 * time.Millisecond,
		NetworkPassphrase:   "Apply Load",
	}
	backend, err := NewLedgerBackend(context.Background(), cfg)
	require.NoError(t, err)
	require.NotNil(t, backend)
	_, ok := backend.(*StreamingLoadtestLedgerBackend)
	assert.True(t, ok, "NewLedgerBackend should return a StreamingLoadtestLedgerBackend")
	assert.NoError(t, backend.Close())
}
