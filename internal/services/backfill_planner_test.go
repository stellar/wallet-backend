package services

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/wallet-backend/internal/data"
	"github.com/stellar/wallet-backend/internal/indexer"
)

func Test_alignToChunk(t *testing.T) {
	day := 24 * time.Hour
	testCases := []struct {
		name     string
		t        time.Time
		interval time.Duration
		want     time.Time
	}{
		{"midday aligns to UTC midnight", time.Date(2026, 10, 27, 13, 45, 1, 0, time.UTC), day, time.Date(2026, 10, 27, 0, 0, 0, 0, time.UTC)},
		{"midnight is its own boundary", time.Date(2026, 10, 27, 0, 0, 0, 0, time.UTC), day, time.Date(2026, 10, 27, 0, 0, 0, 0, time.UTC)},
		{"one microsecond before midnight stays in the previous day", time.Date(2026, 10, 27, 0, 0, 0, 0, time.UTC).Add(-time.Microsecond), day, time.Date(2026, 10, 26, 0, 0, 0, 0, time.UTC)},
		{"non-UTC input aligns on the UTC grid", time.Date(2026, 10, 27, 1, 0, 0, 0, time.FixedZone("plus2", 2*3600)), day, time.Date(2026, 10, 26, 0, 0, 0, 0, time.UTC)},
		{"seven-day interval counts from the epoch", time.Date(2026, 10, 27, 12, 0, 0, 0, time.UTC), 7 * day, time.Unix(1792627200, 0).UTC()},
		{"one-hour interval", time.Date(2026, 10, 27, 13, 45, 1, 0, time.UTC), time.Hour, time.Date(2026, 10, 27, 13, 0, 0, 0, time.UTC)},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got := alignToChunk(tc.t, tc.interval)
			assert.True(t, tc.want.Equal(got), "want %s, got %s", tc.want, got)
			// Idempotent and never after the input.
			assert.True(t, alignToChunk(got, tc.interval).Equal(got))
			assert.False(t, got.After(tc.t))
		})
	}
}

func Test_overlapsAnyChunk(t *testing.T) {
	day := 24 * time.Hour
	d27 := time.Date(2026, 10, 27, 0, 0, 0, 0, time.UTC)
	ranges := []chunkRange{
		{Table: "transactions", Start: d27, End: d27.Add(day)},
		{Table: "operations", Start: d27.Add(2 * day), End: d27.Add(3 * day)},
	}
	assert.True(t, overlapsAnyChunk(ranges, d27, d27.Add(day)), "same range")
	assert.True(t, overlapsAnyChunk(ranges, d27.Add(-12*time.Hour), d27.Add(12*time.Hour)), "straddles a start")
	assert.False(t, overlapsAnyChunk(ranges, d27.Add(day), d27.Add(2*day)), "the empty day between two chunks")
	assert.False(t, overlapsAnyChunk(ranges, d27.Add(-day), d27), "range ending exactly at a chunk start")
	assert.False(t, overlapsAnyChunk(nil, d27, d27.Add(day)), "no chunks")
}

func Test_maxWindowsInFlight(t *testing.T) {
	assert.Equal(t, 2, maxWindowsInFlight(1))
	assert.Equal(t, 2, maxWindowsInFlight(5))
	assert.Equal(t, 3, maxWindowsInFlight(6))
	assert.Equal(t, 3, maxWindowsInFlight(10))
	assert.Equal(t, 4, maxWindowsInFlight(15))
}

// ledgerClock is a fake close-time source: ledger seq closes at
// start + (seq-firstSeq)*cadence, except where overridden. It counts probes.
type ledgerClock struct {
	firstSeq  uint32
	start     time.Time
	cadence   time.Duration
	overrides map[uint32]time.Time
	probes    int
	failAt    uint32
}

func (c *ledgerClock) closeTime(seq uint32) time.Time {
	if t, ok := c.overrides[seq]; ok {
		return t
	}
	return c.start.Add(time.Duration(int64(seq)-int64(c.firstSeq)) * c.cadence)
}

func (c *ledgerClock) probe(_ context.Context, seq uint32) (time.Time, error) {
	c.probes++
	if c.failAt != 0 && seq == c.failAt {
		return time.Time{}, errors.New("ledger source unavailable")
	}
	return c.closeTime(seq), nil
}

func Test_findFirstLedgerAtOrAfter(t *testing.T) {
	// Pubnet-like: 5 s ledgers, 17280 per day. Ledger 1000 closes at the
	// first midnight; ledger 18280 at the next.
	midnight := time.Date(2026, 10, 27, 0, 0, 0, 0, time.UTC)
	newClock := func() *ledgerClock {
		return &ledgerClock{firstSeq: 1000, start: midnight, cadence: 5 * time.Second}
	}
	ctx := context.Background()

	t.Run("finds the first ledger at the boundary from a cadence guess", func(t *testing.T) {
		c := newClock()
		upper := uint32(18279) // last ledger of the day
		first, err := findFirstLedgerAtOrAfter(ctx, c.probe, midnight, 1, upper, c.closeTime(upper), 5*time.Second)
		require.NoError(t, err)
		assert.Equal(t, uint32(1000), first)
		assert.LessOrEqual(t, c.probes, 12, "a good cadence guess should need few probes")
	})

	t.Run("boundary falls between ledgers", func(t *testing.T) {
		c := newClock()
		target := midnight.Add(2 * time.Second) // between ledger 1000 (0 s) and 1001 (5 s)
		first, err := findFirstLedgerAtOrAfter(ctx, c.probe, target, 1, 18279, c.closeTime(18279), 5*time.Second)
		require.NoError(t, err)
		assert.Equal(t, uint32(1001), first)
	})

	t.Run("wrong cadence still converges", func(t *testing.T) {
		c := newClock()
		first, err := findFirstLedgerAtOrAfter(ctx, c.probe, midnight, 1, 18279, c.closeTime(18279), 50*time.Second)
		require.NoError(t, err)
		assert.Equal(t, uint32(1000), first)
		assert.Less(t, c.probes, maxBoundaryProbes)
	})

	t.Run("floor reached when every ledger closes after the boundary", func(t *testing.T) {
		c := newClock()
		// Ask for a boundary a day before the clock's first ledger: the
		// requested start ledger bounds the search.
		first, err := findFirstLedgerAtOrAfter(ctx, c.probe, midnight.Add(-24*time.Hour), 1200, 18279, c.closeTime(18279), 5*time.Second)
		require.NoError(t, err)
		assert.Equal(t, uint32(1200), first)
	})

	t.Run("floor equals upper", func(t *testing.T) {
		c := newClock()
		first, err := findFirstLedgerAtOrAfter(ctx, c.probe, midnight, 5000, 5000, c.closeTime(5000), 5*time.Second)
		require.NoError(t, err)
		assert.Equal(t, uint32(5000), first)
		assert.Zero(t, c.probes)
	})

	t.Run("upper before the boundary is an error", func(t *testing.T) {
		c := newClock()
		_, err := findFirstLedgerAtOrAfter(ctx, c.probe, midnight.Add(time.Hour), 1, 1100, c.closeTime(1100), 5*time.Second)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "before the boundary")
	})

	t.Run("probe errors propagate", func(t *testing.T) {
		c := newClock()
		c.failAt = 1000
		_, err := findFirstLedgerAtOrAfter(ctx, c.probe, midnight, 1, 18279, c.closeTime(18279), 5*time.Second)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "ledger source unavailable")
	})
}

func Test_gapWindows(t *testing.T) {
	day := 24 * time.Hour
	midnight := time.Date(2026, 10, 27, 0, 0, 0, 0, time.UTC)
	c := &ledgerClock{firstSeq: 1000, start: midnight, cadence: 5 * time.Second}
	ctx := context.Background()

	// Gap inside one day, and a gap that straddles midnight into the next day.
	gaps := []data.LedgerRange{{GapStart: 2000, GapEnd: 2100}, {GapStart: 18270, GapEnd: 18290}}
	windows, err := gapWindows(ctx, c.probe, gaps, day)
	require.NoError(t, err)
	require.Len(t, windows, 2)

	assert.Equal(t, uint32(2000), windows[0].FirstLedger)
	assert.Equal(t, uint32(2100), windows[0].LastLedger)
	assert.True(t, windows[0].RangeStart.Equal(midnight))
	assert.True(t, windows[0].RangeEnd.Equal(midnight.Add(day)))
	assert.False(t, windows[0].BelowCursor)

	assert.True(t, windows[1].RangeStart.Equal(midnight), "starts in the first day")
	assert.True(t, windows[1].RangeEnd.Equal(midnight.Add(2*day)), "ends after the second day")
}

func Test_concatCopyRows(t *testing.T) {
	a, b, c := indexer.NewIndexerBuffer(), indexer.NewIndexerBuffer(), indexer.NewIndexerBuffer()
	hash := func(b string) string { return strings.Repeat(b, 64) }
	txA, txB, txC := createTestTransaction(hash("a"), 1), createTestTransaction(hash("b"), 2), createTestTransaction(hash("c"), 3)
	a.PushTransaction("GBXGQJWVLWOYHFLVTKWV5FGHA3LNYY2JQKM7OAJAUEQFU6LPCSEFVXON", &txA)
	b.PushTransaction("GBXGQJWVLWOYHFLVTKWV5FGHA3LNYY2JQKM7OAJAUEQFU6LPCSEFVXON", &txB)
	b.PushTransaction("GBXGQJWVLWOYHFLVTKWV5FGHA3LNYY2JQKM7OAJAUEQFU6LPCSEFVXON", &txC)
	for _, buf := range []*indexer.IndexerBuffer{a, b, c} {
		require.NoError(t, buf.BuildCopyRows())
	}
	items := []processedLedger{{seq: 1, buffer: a}, {seq: 2, buffer: b}, {seq: 3, buffer: c}}
	rows := concatCopyRows(items, (*indexer.IndexerBuffer).GetTransactionCopyRows)
	assert.Len(t, rows, 3, "one row per transaction across the three ledgers, empty ledgers contribute none")
	assert.Equal(t, 3, cap(rows), "sized once")
	assert.Empty(t, concatCopyRows(nil, (*indexer.IndexerBuffer).GetTransactionCopyRows))
}
