package services

import "sync"

// backfillWatermark tracks which ledgers in a gap have been flushed to DB,
// and reports the highest contiguous flushed ledger for cursor updates.
// Thread-safe: multiple flush workers call MarkFlushed concurrently.
type backfillWatermark struct {
	mu       sync.Mutex
	flushed  []bool // indexed by (ledgerSeq - start)
	start    uint32
	end      uint32
	cursor   uint32 // highest contiguous flushed ledger (0 = none)
	scanFrom uint32 // resume scan from last cursor position
}

func newBackfillWatermark(start, end uint32) *backfillWatermark {
	return &backfillWatermark{
		flushed:  make([]bool, end-start+1),
		start:    start,
		end:      end,
		scanFrom: start,
	}
}

// MarkFlushed records ledger sequences as flushed and advances the cursor
// through any contiguous range. Returns true if the cursor advanced.
func (w *backfillWatermark) MarkFlushed(ledgers []uint32) bool {
	w.mu.Lock()
	defer w.mu.Unlock()

	for _, seq := range ledgers {
		if seq >= w.start && seq <= w.end {
			w.flushed[seq-w.start] = true
		}
	}

	oldCursor := w.cursor
	for w.scanFrom <= w.end && w.flushed[w.scanFrom-w.start] {
		w.cursor = w.scanFrom
		w.scanFrom++
	}

	return w.cursor != oldCursor
}

// Cursor returns the highest contiguous flushed ledger, or 0 if none.
func (w *backfillWatermark) Cursor() uint32 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.cursor
}

// Complete returns true if all ledgers in the gap have been flushed.
func (w *backfillWatermark) Complete() bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.cursor == w.end
}
