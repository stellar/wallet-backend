package indexer

import (
	"context"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/alitto/pond/v2"

	"github.com/stellar/go-stellar-sdk/network"
)

// These benchmarks run against the real pubnet ledger fixtures under testdata/ (see
// loadLedgerFixture), so they exercise the actual transaction/operation/change shapes production
// ingests rather than synthetic data. Fixtures are pubnet, hence PublicNetworkPassphrase.

// benchIndexer builds an Indexer with the real processors. nil metrics is accepted (see
// TestIndexer_ProcessLedgerTransactions_FeePhaseBalances).
func benchIndexer(tb testing.TB) *Indexer {
	idx, err := NewIndexer(network.PublicNetworkPassphrase, pond.NewPool(runtime.NumCPU()), nil)
	if err != nil {
		tb.Fatal(err)
	}
	return idx
}

// BenchmarkProcessRealLedger measures a full per-ledger indexing pass: parallel workers build a
// per-tx TransactionResult, which are folded into one ledger buffer. One sub-benchmark per fixture.
func BenchmarkProcessRealLedger(b *testing.B) {
	ctx := context.Background()
	idx := benchIndexer(b)

	paths, err := filepath.Glob("testdata/*.xdr.gz")
	if err != nil {
		b.Fatal(err)
	}
	for _, path := range paths {
		lcm := loadLedgerFixture(b, path)
		txs, err := GetLedgerTransactions(ctx, network.PublicNetworkPassphrase, lcm)
		if err != nil {
			b.Fatal(err)
		}

		b.Run(filepath.Base(path), func(b *testing.B) {
			buf := NewIndexerBuffer()
			b.ReportAllocs()
			for b.Loop() {
				if _, err := idx.ProcessLedgerTransactions(ctx, txs, buf); err != nil {
					b.Fatal(err)
				}
				buf.Clear()
			}
		})
	}
}

// BenchmarkFoldRealLedger isolates the serial fold this change actually touches: it pre-builds each
// real ledger's per-tx results once (untimed) and then measures only IngestTransactionResult + the
// buffer Clear that mirrors backfill reuse. One sub-benchmark per fixture.
func BenchmarkFoldRealLedger(b *testing.B) {
	ctx := context.Background()
	idx := benchIndexer(b)

	paths, err := filepath.Glob("testdata/*.xdr.gz")
	if err != nil {
		b.Fatal(err)
	}
	for _, path := range paths {
		lcm := loadLedgerFixture(b, path)
		txs, err := GetLedgerTransactions(ctx, network.PublicNetworkPassphrase, lcm)
		if err != nil {
			b.Fatal(err)
		}

		results := make([]*TransactionResult, 0, len(txs))
		for _, tx := range txs {
			result, err := idx.processTransaction(ctx, tx)
			if err != nil {
				b.Fatal(err)
			}
			results = append(results, result)
		}

		b.Run(filepath.Base(path), func(b *testing.B) {
			buf := NewIndexerBuffer()
			b.ReportAllocs()
			for b.Loop() {
				for _, result := range results {
					buf.IngestTransactionResult(result)
				}
				buf.Clear()
			}
		})
	}
}
