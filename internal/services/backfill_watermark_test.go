package services

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_backfillWatermark_MarkFlushed_sequential(t *testing.T) {
	w := newBackfillWatermark(100, 109) // 10 ledgers: 100-109

	// Flush ledgers 100-104
	advanced := w.MarkFlushed([]uint32{100, 101, 102, 103, 104})
	assert.True(t, advanced)
	assert.Equal(t, uint32(104), w.Cursor())

	// Flush ledgers 105-109
	advanced = w.MarkFlushed([]uint32{105, 106, 107, 108, 109})
	assert.True(t, advanced)
	assert.Equal(t, uint32(109), w.Cursor())
	assert.True(t, w.Complete())
}

func Test_backfillWatermark_MarkFlushed_outOfOrder(t *testing.T) {
	w := newBackfillWatermark(100, 109)

	// Flush ledgers 105-109 first — cursor can't advance
	advanced := w.MarkFlushed([]uint32{105, 106, 107, 108, 109})
	assert.False(t, advanced)
	assert.Equal(t, uint32(0), w.Cursor()) // no contiguous range from start

	// Flush ledgers 100-104 — cursor jumps to 109
	advanced = w.MarkFlushed([]uint32{100, 101, 102, 103, 104})
	assert.True(t, advanced)
	assert.Equal(t, uint32(109), w.Cursor())
	assert.True(t, w.Complete())
}

func Test_backfillWatermark_MarkFlushed_withGap(t *testing.T) {
	w := newBackfillWatermark(100, 109)

	// Flush 100-102
	w.MarkFlushed([]uint32{100, 101, 102})
	assert.Equal(t, uint32(102), w.Cursor())

	// Flush 105-107 (skip 103-104)
	advanced := w.MarkFlushed([]uint32{105, 106, 107})
	assert.False(t, advanced) // cursor stuck at 102

	// Flush 103-104 — cursor jumps to 107
	advanced = w.MarkFlushed([]uint32{103, 104})
	assert.True(t, advanced)
	assert.Equal(t, uint32(107), w.Cursor())
	assert.False(t, w.Complete()) // 108, 109 still missing
}

func Test_backfillWatermark_singleLedger(t *testing.T) {
	w := newBackfillWatermark(500, 500)

	advanced := w.MarkFlushed([]uint32{500})
	assert.True(t, advanced)
	assert.Equal(t, uint32(500), w.Cursor())
	assert.True(t, w.Complete())
}

func Test_backfillWatermark_emptyFlush(t *testing.T) {
	w := newBackfillWatermark(100, 109)

	advanced := w.MarkFlushed([]uint32{})
	assert.False(t, advanced)
	assert.Equal(t, uint32(0), w.Cursor())
}
