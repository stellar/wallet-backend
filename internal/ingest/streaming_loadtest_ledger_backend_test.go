package ingest

import (
	"context"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/stellar/go-stellar-sdk/ingest/ledgerbackend"
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
