package services

import (
	"context"
	"fmt"
	"math"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stellar/go-stellar-sdk/ingest/ledgerbackend"
	"github.com/stellar/go-stellar-sdk/support/log"
	"github.com/stellar/go-stellar-sdk/toid"

	"github.com/stellar/wallet-backend/internal/data"
	"github.com/stellar/wallet-backend/internal/db"
	"github.com/stellar/wallet-backend/internal/indexer"
	"github.com/stellar/wallet-backend/internal/utils"
)

// Memo backfill: fills transactions.memo_type/memo for rows ingested before the memo
// columns existed, by re-reading their ledgers from the ingester's ledger backend.
//
// Live ingestion writes memos for every ledger from its restart onward and never
// touches older rows, so the backfill needs no coordination with it beyond the
// cursor's starting point (latest_ingest_ledger+1 at the first run; see
// startLiveIngestion). It runs inside the live ingester, under its advisory lock,
// so there is exactly one writer.
//
// The walk is newest-first in slices so recent history gets memos first. Each slice
// reads one bounded range through a fresh backend (the SDK backends accept a single
// PrepareRange), flushes an UPDATE every memoBackfillFlushLedgers ledgers to bound
// memory, and commits the cursor with the slice's last flush. The UPDATE runs directly
// on compressed chunks (see TransactionModel.BatchUpdateMemo); a chunk is recompressed
// once the walk has passed it, so at most about one chunk's rows sit decompressed at a
// time. A crash redoes at most one slice; rewriting a row with the same memo is a no-op.
var (
	memoBackfillSliceLedgers uint32 = 10_000
	memoBackfillFlushLedgers uint32 = 1_000
	memoBackfillRetryWait           = 30 * time.Second
)

// runMemoBackfill walks the memo backfill to completion, retrying a failed slice after
// memoBackfillRetryWait. It returns when the cursor is at or below the oldest ingested
// ledger or when ctx is cancelled. It never fails the ingester: a deployment whose
// backend cannot serve old history (an RPC past its retention) keeps retrying, logging
// the reason, until the datastore backend is configured.
func (m *ingestService) runMemoBackfill(ctx context.Context) {
	for {
		err := m.memoBackfillPass(ctx)
		if err == nil || ctx.Err() != nil {
			return
		}
		m.appMetrics.Ingestion.MemoBackfillErrorsTotal.Inc()
		log.Ctx(ctx).Errorf("memo backfill: %v; retrying in %s (history older than the RPC retention window needs --ledger-backend-type=datastore)", err, memoBackfillRetryWait)
		select {
		case <-ctx.Done():
			return
		case <-time.After(memoBackfillRetryWait):
		}
	}
}

// memoBackfillPass walks slices from the cursor down to the oldest ingested ledger.
// It returns nil once done and an error on the first failed slice.
func (m *ingestService) memoBackfillPass(ctx context.Context) error {
	cursor, err := m.models.IngestStore.Get(ctx, data.MemoBackfillCursorName)
	if err != nil {
		return fmt.Errorf("reading memo backfill cursor: %w", err)
	}
	floor, err := m.models.IngestStore.Get(ctx, data.OldestLedgerCursorName)
	if err != nil {
		return fmt.Errorf("reading oldest ingested ledger: %w", err)
	}
	if cursor <= floor {
		return nil
	}

	// Chunks compressed before the walk; each is recompressed once the walk has passed its
	// start. The live chunk is never compressed, so it is never in this list.
	compressed, err := m.models.Transactions.ListCompressedChunks(ctx)
	if err != nil {
		return err
	}
	log.Ctx(ctx).Infof("memo backfill: filling ledgers [%d, %d] newest-first, %d compressed chunks to recompress", floor, cursor-1, len(compressed))

	for cursor > floor {
		hi := cursor - 1
		lo := floor
		if cursor-floor > memoBackfillSliceLedgers {
			lo = cursor - memoBackfillSliceLedgers
		}

		start := time.Now()
		loClosedAt, rows, err := m.backfillMemoSlice(ctx, lo, hi)
		if err != nil {
			return fmt.Errorf("backfilling memos for ledgers [%d, %d]: %w", lo, hi, err)
		}
		m.appMetrics.Ingestion.MemoBackfillCursor.Set(float64(lo))
		log.Ctx(ctx).Infof("memo backfill: ledgers [%d, %d] done, %d rows in %s", lo, hi, rows, time.Since(start).Round(time.Millisecond))

		for len(compressed) > 0 && !compressed[0].RangeStart.Before(loClosedAt) {
			if err := m.models.Transactions.CompressChunk(ctx, compressed[0].Name); err != nil {
				return err
			}
			compressed = compressed[1:]
		}

		cursor = lo
		floor, err = m.models.IngestStore.Get(ctx, data.OldestLedgerCursorName)
		if err != nil {
			return fmt.Errorf("reading oldest ingested ledger: %w", err)
		}
	}

	// The chunk holding the oldest ledger starts before any slice's lowest close time, so
	// it (and anything retention has not dropped yet) is recompressed here.
	for _, c := range compressed {
		if err := m.models.Transactions.CompressChunk(ctx, c.Name); err != nil {
			return err
		}
	}
	log.Ctx(ctx).Infof("memo backfill: complete down to ledger %d", cursor)
	return nil
}

// backfillMemoSlice reads ledgers [lo, hi] through a fresh bounded backend, flushing the
// memos every memoBackfillFlushLedgers ledgers and committing cursor=lo with the last
// flush. It returns lo's close time (the lowest ledger_created_at rewritten) and the row
// count.
func (m *ingestService) backfillMemoSlice(ctx context.Context, lo, hi uint32) (time.Time, int64, error) {
	// toid packs the ledger into the high 32 bits of a signed int64, so a ledger above
	// MaxInt32 would wrap negative and match no row. Mainnet is four orders of magnitude
	// below this; fail loudly instead of silently skipping.
	if hi > math.MaxInt32 {
		return time.Time{}, 0, fmt.Errorf("ledger %d exceeds the largest toid ledger %d", hi, int64(math.MaxInt32))
	}
	backend, err := m.ledgerBackendFactory(ctx)
	if err != nil {
		return time.Time{}, 0, fmt.Errorf("creating ledger backend: %w", err)
	}
	defer utils.DeferredClose(ctx, backend, "closing memo backfill ledger backend")
	if err := backend.PrepareRange(ctx, ledgerbackend.BoundedRange(lo, hi)); err != nil {
		return time.Time{}, 0, fmt.Errorf("preparing ledger range: %w", err)
	}

	var (
		loClosedAt time.Time
		total      int64
		rows       []data.TransactionMemo
		flushFrom  = lo
	)
	for seq := lo; seq <= hi && seq <= math.MaxInt32; seq++ {
		meta, err := backend.GetLedger(ctx, seq)
		if err != nil {
			return time.Time{}, 0, fmt.Errorf("fetching ledger %d: %w", seq, err)
		}
		closedAt := meta.ClosedAt()
		if seq == lo {
			loClosedAt = closedAt
		}
		txs, err := indexer.GetLedgerTransactions(ctx, m.networkPassphrase, meta, m.ledgerIndexerPool)
		if err != nil {
			return time.Time{}, 0, fmt.Errorf("reading ledger %d transactions: %w", seq, err)
		}
		for _, tx := range txs {
			memoType, memo := utils.EncodeMemo(tx.Envelope.Memo())
			rows = append(rows, data.TransactionMemo{
				ToID:            toid.New(int32(seq), int32(tx.Index), 0).ToInt64(),
				LedgerCreatedAt: closedAt,
				MemoType:        memoType,
				Memo:            memo,
			})
		}

		last := seq == hi
		if !last && seq-flushFrom+1 < memoBackfillFlushLedgers {
			continue
		}
		n, err := m.flushMemos(ctx, rows, lo, last)
		if err != nil {
			return time.Time{}, 0, fmt.Errorf("flushing memos for ledgers [%d, %d]: %w", flushFrom, seq, err)
		}
		total += n
		rows = rows[:0]
		flushFrom = seq + 1
	}
	return loClosedAt, total, nil
}

// flushMemos writes one batch of memos and, on the slice's last flush, the cursor, in
// one transaction. Interrupted mid-way, nothing of this flush is visible.
func (m *ingestService) flushMemos(ctx context.Context, rows []data.TransactionMemo, cursor uint32, last bool) (int64, error) {
	var updated int64
	err := db.RunInTransaction(ctx, m.models.DB, func(dbTx pgx.Tx) error {
		n, err := m.models.Transactions.BatchUpdateMemo(ctx, dbTx, rows)
		if err != nil {
			return err
		}
		updated = n
		if last {
			if err := m.models.IngestStore.Update(ctx, dbTx, data.MemoBackfillCursorName, cursor); err != nil {
				return fmt.Errorf("advancing memo backfill cursor to %d: %w", cursor, err)
			}
		}
		return nil
	})
	if err != nil {
		return 0, fmt.Errorf("memo flush transaction: %w", err)
	}
	return updated, nil
}
