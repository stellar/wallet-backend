package datamigrations

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alitto/pond/v2"
	"github.com/stellar/go-stellar-sdk/ingest/ledgerbackend"
	"github.com/stellar/go-stellar-sdk/network"
	"github.com/stellar/go-stellar-sdk/xdr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// readAheadBackend serves its ledgers and records the highest one asked for.
type readAheadBackend struct {
	fakeBackend
	highest atomic.Uint32
}

func (b *readAheadBackend) GetLedger(ctx context.Context, seq uint32) (xdr.LedgerCloseMeta, error) {
	for {
		cur := b.highest.Load()
		if seq <= cur || b.highest.CompareAndSwap(cur, seq) {
			break
		}
	}
	return b.fakeBackend.GetLedger(ctx, seq)
}

func TestRunner_ReadsTheNextBatchWhileOneIsWritten(t *testing.T) {
	ctx := context.Background()
	withRunnerSizes(t, 6, 2) // one slice of three two-ledger batches
	closedAt := time.Date(2026, time.March, 1, 12, 0, 0, 0, time.UTC)
	backend := &readAheadBackend{fakeBackend: fakeBackend{ledgers: map[uint32]xdr.LedgerCloseMeta{}}}
	for seq := uint32(1); seq <= 6; seq++ {
		backend.ledgers[seq] = memoLedger(t, seq, closedAt.Add(time.Duration(seq)*time.Minute), xdr.MemoID(uint64(seq)))
	}
	workers := pond.NewPool(2)
	t.Cleanup(workers.StopAndWait)
	runner := &Runner{
		Deps:           Deps{NetworkPassphrase: network.TestNetworkPassphrase, Pool: workers},
		BackendFactory: func(context.Context) (ledgerbackend.LedgerBackend, error) { return backend, nil },
	}
	migration, ok := Find(memoMigration)
	require.True(t, ok)

	var (
		writing atomic.Int32
		sizes   []int
		lasts   []bool
	)
	flush := func(rows []any, last bool) error {
		if writing.Add(1) != 1 {
			return errors.New("two batches written at once")
		}
		defer writing.Add(-1)
		if len(sizes) == 0 {
			// The first batch holds ledgers 1 and 2. Ledger 3 belongs to the next batch,
			// and it must be read before this write returns.
			if !assert.Eventually(t, func() bool { return backend.highest.Load() >= 3 }, 5*time.Second, time.Millisecond) {
				return errors.New("the next batch was not read during the write")
			}
		}
		sizes = append(sizes, len(rows))
		lasts = append(lasts, last)
		return nil
	}

	loClosedAt, err := runner.readLedgers(ctx, migration, 1, 6, flush)
	require.NoError(t, err)
	assert.Equal(t, closedAt.Add(time.Minute), loClosedAt)
	assert.Equal(t, []int{2, 2, 2}, sizes, "one transaction per ledger, two ledgers per batch, in order")
	assert.Equal(t, []bool{false, false, true}, lasts)
}
