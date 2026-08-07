package ingest

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stellar/go-stellar-sdk/ingest/ledgerbackend"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/wallet-backend/internal/indexer"
)

// TestStreamingLoadtestBackendRealCorpus replays real `stellar-core apply-load`
// meta through the backend and the production transaction reader. It proves,
// against real core output rather than hand-built fixtures, that renumbered
// and merged ledgers still parse through the exact code path live ingestion
// uses (transaction-set-to-result pairing included).
//
// Opt-in: set STREAMING_LOADTEST_CORPUS to a comma-separated list of meta.xdr
// files (regular files work; EOF exercises the reopen path by replaying the
// file as a new stream epoch). Generate them by running
// `stellar-core apply-load` (BUILD_TESTS image) with METADATA_OUTPUT_STREAM
// pointed at a file, one run per transaction profile.
func TestStreamingLoadtestBackendRealCorpus(t *testing.T) {
	corpus := os.Getenv("STREAMING_LOADTEST_CORPUS")
	if corpus == "" {
		t.Skip("set STREAMING_LOADTEST_CORPUS=<meta.xdr>[,<meta.xdr>...] to run")
	}
	paths := strings.Split(corpus, ",")

	// apply-load hard-overrides its network passphrase to this value.
	const passphrase = "Apply Load"
	// Enough ledgers to cross at least one EOF/reopen boundary per file with
	// the reference smoke corpora (50 benchmark ledgers plus setup each).
	const ledgersToRead = 200

	backend, err := NewStreamingLoadtestLedgerBackend(StreamingLoadtestBackendConfig{
		MetaPipePaths: paths,
	})
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	require.NoError(t, backend.PrepareRange(ctx, ledgerbackend.UnboundedRange(1)))
	defer func() {
		cancel()
		require.NoError(t, backend.Close())
	}()

	var lastCloseTime int64
	totalTxs := 0
	for seq := uint32(1); seq <= ledgersToRead; seq++ {
		lcm, err := backend.GetLedger(ctx, seq)
		require.NoError(t, err, "ledger %d", seq)

		require.Equal(t, seq, lcm.LedgerSequence())
		ct := lcm.LedgerCloseTime()
		require.Positive(t, ct, "ledger %d close time", seq)
		require.GreaterOrEqual(t, ct, lastCloseTime, "ledger %d close time regressed", seq)
		lastCloseTime = ct

		// The production read path: this is what live ingestion runs on every
		// ledger, so a merged ledger it cannot parse would fail here first.
		txs, err := indexer.GetLedgerTransactions(ctx, passphrase, lcm)
		require.NoError(t, err, "reading transactions of ledger %d", seq)
		totalTxs += len(txs)
	}

	assert.Positive(t, totalTxs)
	t.Logf("read %d ledgers, %d transactions total from %d file(s)", ledgersToRead, totalTxs, len(paths))
}
