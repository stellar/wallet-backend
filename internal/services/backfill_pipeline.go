package services

import (
	"time"

	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/wallet-backend/internal/indexer"
)

// rawLedger is a decoded ledger handed from the fetcher to a process worker.
type rawLedger struct {
	seq  uint32
	meta xdr.LedgerCloseMeta
}

// processedLedger is the indexer output for one ledger, handed from a process worker to
// the flusher.
type processedLedger struct {
	seq       uint32
	closeTime time.Time
	buffer    *indexer.IndexerBuffer
}

// orderedBuffer turns out-of-order processed ledgers into contiguous ascending runs.
// Process workers finish in arbitrary order; orderedBuffer releases a ledger only once
// every earlier ledger has arrived, so the flusher always sees a gapless prefix.
type orderedBuffer struct {
	next    uint32
	pending map[uint32]processedLedger
}

func newOrderedBuffer(start uint32) *orderedBuffer {
	return &orderedBuffer{next: start, pending: make(map[uint32]processedLedger)}
}

// add records p and returns the contiguous run (possibly empty) now releasable.
func (o *orderedBuffer) add(p processedLedger) []processedLedger {
	o.pending[p.seq] = p
	var run []processedLedger
	for {
		next, ok := o.pending[o.next]
		if !ok {
			break
		}
		run = append(run, next)
		delete(o.pending, o.next)
		o.next++
	}
	return run
}
