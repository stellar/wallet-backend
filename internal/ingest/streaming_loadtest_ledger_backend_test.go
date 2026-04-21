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
