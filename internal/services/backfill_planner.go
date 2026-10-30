package services

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stellar/go-stellar-sdk/ingest/ledgerbackend"
	"github.com/stellar/go-stellar-sdk/support/log"

	"github.com/stellar/wallet-backend/internal/data"
	"github.com/stellar/wallet-backend/internal/db"
)

// backfillWindow is the unit of backfill work: a close-time range
// [RangeStart, RangeEnd) and the ledgers whose close times fall inside it.
// A chunk window covers exactly one chunk of the bulk hypertables, so the
// window can become a chunk in one step (attach) and be compressed as a
// whole. A gap window fills a hole above the cursor and spans whatever
// chunks the hole's ledgers touch; it is never attached.
type backfillWindow struct {
	RangeStart  time.Time
	RangeEnd    time.Time
	FirstLedger uint32
	LastLedger  uint32
	// BelowCursor is true for the contiguous region under oldest_ingest_ledger:
	// closing such a window lowers the cursor to FirstLedger. Only such a
	// window can be attached as a chunk; a gap window is written in place.
	BelowCursor bool
}

func (w backfillWindow) ledgerCount() uint32 { return w.LastLedger - w.FirstLedger + 1 }

func (w backfillWindow) String() string {
	return fmt.Sprintf("window [%d-%d] %s..%s", w.FirstLedger, w.LastLedger,
		w.RangeStart.UTC().Format(time.RFC3339), w.RangeEnd.UTC().Format(time.RFC3339))
}

// chunkRange is one existing chunk of a time-partitioned hypertable.
type chunkRange struct {
	Table string
	Start time.Time
	End   time.Time
}

// readBulkChunkInterval reads the chunk interval the bulk hypertables share.
// The five tables are created with the same interval and live mode sets them
// together, so a mismatch means the schema was changed by hand; the backfill
// refuses to guess which one a window should follow.
func readBulkChunkInterval(ctx context.Context, pool db.Querier) (time.Duration, error) {
	rows, err := pool.Query(ctx,
		`SELECT hypertable_name, time_interval
		   FROM timescaledb_information.dimensions
		  WHERE hypertable_name = ANY($1) AND time_interval IS NOT NULL`,
		data.BulkCopyTableNames())
	if err != nil {
		return 0, fmt.Errorf("reading chunk intervals: %w", err)
	}
	defer rows.Close()

	var interval time.Duration
	seen := 0
	for rows.Next() {
		var table string
		var tableInterval time.Duration
		if err := rows.Scan(&table, &tableInterval); err != nil {
			return 0, fmt.Errorf("scanning chunk interval: %w", err)
		}
		if seen > 0 && tableInterval != interval {
			return 0, fmt.Errorf("bulk hypertables disagree on chunk interval: %s has %s, another has %s", table, tableInterval, interval)
		}
		interval = tableInterval
		seen++
	}
	if err := rows.Err(); err != nil {
		return 0, fmt.Errorf("reading chunk intervals: %w", err)
	}
	if seen != len(data.BulkCopyTableNames()) {
		return 0, fmt.Errorf("expected a time dimension on %d bulk hypertables, found %d", len(data.BulkCopyTableNames()), seen)
	}
	if interval <= 0 {
		return 0, fmt.Errorf("invalid chunk interval %s", interval)
	}
	return interval, nil
}

// alignToChunk returns the start of the chunk that contains t. TimescaleDB
// places chunk boundaries at multiples of the chunk interval counted from the
// Unix epoch, so a one-day interval gives UTC midnights.
func alignToChunk(t time.Time, interval time.Duration) time.Time {
	micros := t.UnixMicro()
	step := interval.Microseconds()
	aligned := micros - ((micros%step)+step)%step
	return time.UnixMicro(aligned).UTC()
}

// readChunkRanges lists the chunks of the given time-partitioned hypertables.
func readChunkRanges(ctx context.Context, pool db.Querier, tables []string) ([]chunkRange, error) {
	rows, err := pool.Query(ctx,
		`SELECT hypertable_name, range_start, range_end
		   FROM timescaledb_information.chunks
		  WHERE hypertable_name = ANY($1) AND range_start IS NOT NULL`, tables)
	if err != nil {
		return nil, fmt.Errorf("reading chunk ranges: %w", err)
	}
	ranges, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (chunkRange, error) {
		var r chunkRange
		err := row.Scan(&r.Table, &r.Start, &r.End)
		return r, err //nolint:wrapcheck // CollectRows wraps
	})
	if err != nil {
		return nil, fmt.Errorf("collecting chunk ranges: %w", err)
	}
	return ranges, nil
}

// overlapsAnyChunk reports whether [start, end) intersects an existing chunk.
func overlapsAnyChunk(ranges []chunkRange, start, end time.Time) bool {
	for _, r := range ranges {
		if r.Start.Before(end) && start.Before(r.End) {
			return true
		}
	}
	return false
}

// ledgerCloseTimeProbe returns the close time of one ledger. The backfill
// uses it to place window boundaries, which are close times, on the ledger
// axis, which is what the ledger source is addressed by.
type ledgerCloseTimeProbe func(ctx context.Context, seq uint32) (time.Time, error)

// newLedgerCloseTimeProbe builds a probe over one backend from the factory.
// Each call prepares a one-ledger range and reads it, so a probe costs one
// ledger download and decode. The returned close function releases the
// backend.
func newLedgerCloseTimeProbe(ctx context.Context, factory LedgerBackendFactory) (ledgerCloseTimeProbe, func(), error) {
	backend, err := factory(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("creating probe backend: %w", err)
	}
	probe := func(ctx context.Context, seq uint32) (time.Time, error) {
		if err := backend.PrepareRange(ctx, ledgerbackend.BoundedRange(seq, seq)); err != nil {
			return time.Time{}, fmt.Errorf("preparing ledger %d: %w", seq, err)
		}
		meta, err := backend.GetLedger(ctx, seq)
		if err != nil {
			return time.Time{}, fmt.Errorf("fetching ledger %d: %w", seq, err)
		}
		return time.Unix(meta.LedgerCloseTime(), 0).UTC(), nil
	}
	closeFn := func() {
		if err := backend.Close(); err != nil {
			log.Ctx(ctx).Warnf("closing probe backend: %v", err)
		}
	}
	return probe, closeFn, nil
}

// maxBoundaryProbes bounds the search for a window's first ledger. The
// estimate from the ledger cadence is usually within a few ledgers and the
// bracket then halves each step, so a search that runs this long means the
// ledger source is returning inconsistent close times.
const maxBoundaryProbes = 64

// errBoundaryNotFound reports a boundary search that ran out of probes.
var errBoundaryNotFound = errors.New("could not place a window boundary on the ledger axis")

// findFirstLedgerAtOrAfter returns the smallest ledger in [floor, upper] whose
// close time is at or after target, given that upper's close time is
// upperClose (at or after target). It returns floor when every ledger down to
// floor closes at or after target, which is how the oldest window stops at
// the requested start ledger. ledgerCadence is the recent average time between
// ledgers and only shapes the first guess.
func findFirstLedgerAtOrAfter(ctx context.Context, probe ledgerCloseTimeProbe, target time.Time, floor, upper uint32, upperClose time.Time, ledgerCadence time.Duration) (uint32, error) {
	if upperClose.Before(target) {
		return 0, fmt.Errorf("ledger %d closes at %s, before the boundary %s", upper, upperClose.UTC().Format(time.RFC3339), target.UTC().Format(time.RFC3339))
	}
	if floor >= upper {
		return floor, nil
	}
	if ledgerCadence <= 0 {
		ledgerCadence = 5 * time.Second
	}

	// hi always closes at or after target. lo, once found, closes before it.
	hi := upper
	var lo uint32

	// First guess from the cadence, then widen downwards until a ledger
	// before the boundary is found or the floor is reached.
	step := uint32(max(int64(1), int64(upperClose.Sub(target)/ledgerCadence)))
	guess := upper
	probes := 0
	for {
		if guess <= floor+step {
			guess = floor
		} else {
			guess -= step
		}
		closeTime, err := probe(ctx, guess)
		if err != nil {
			return 0, err
		}
		probes++
		if closeTime.Before(target) {
			lo = guess
			break
		}
		hi = guess
		if guess == floor {
			return floor, nil
		}
		step *= 2
		if probes >= maxBoundaryProbes {
			return 0, fmt.Errorf("%w: boundary %s between ledgers %d and %d", errBoundaryNotFound, target.UTC().Format(time.RFC3339), floor, upper)
		}
	}

	// Binary search the bracket (lo, hi].
	for hi-lo > 1 {
		mid := lo + (hi-lo)/2
		closeTime, err := probe(ctx, mid)
		if err != nil {
			return 0, err
		}
		probes++
		if closeTime.Before(target) {
			lo = mid
		} else {
			hi = mid
		}
		if probes >= maxBoundaryProbes {
			return 0, fmt.Errorf("%w: boundary %s between ledgers %d and %d", errBoundaryNotFound, target.UTC().Format(time.RFC3339), lo, hi)
		}
	}
	return hi, nil
}

// backfillPlanner yields windows newest first, one at a time, from the
// ledger just below the cursor down to the requested start ledger. Each
// window is placed only when it is reached, so a run never spends probes on
// windows it has not got to.
type backfillPlanner struct {
	probe      ledgerCloseTimeProbe
	interval   time.Duration
	startLeger uint32

	// The window above the next one: its first ledger and range start bound
	// the next window from above.
	nextLastLedger uint32
	nextRangeEnd   time.Time
	// close time of nextLastLedger, known from the previous window or probed
	nextLastClose time.Time
	// cadence measured over the previous window, used for the first guess
	cadence time.Duration
	done    bool
}

// newBackfillPlanner prepares the walk below the cursor. cursor is
// oldest_ingest_ledger; the first window ends at cursor-1.
func newBackfillPlanner(ctx context.Context, probe ledgerCloseTimeProbe, interval time.Duration, startLedger, cursor uint32) (*backfillPlanner, error) {
	if startLedger >= cursor {
		return &backfillPlanner{done: true}, nil
	}
	last := cursor - 1
	lastClose, err := probe(ctx, last)
	if err != nil {
		return nil, fmt.Errorf("placing the ledger below the cursor: %w", err)
	}
	return &backfillPlanner{
		probe:          probe,
		interval:       interval,
		startLeger:     startLedger,
		nextLastLedger: last,
		nextRangeEnd:   alignToChunk(lastClose, interval).Add(interval),
		nextLastClose:  lastClose,
		cadence:        5 * time.Second,
	}, nil
}

// Next returns the next window below the previous one, or ok=false once the
// start ledger has been reached.
func (p *backfillPlanner) Next(ctx context.Context) (backfillWindow, bool, error) {
	if p.done {
		return backfillWindow{}, false, nil
	}
	rangeEnd := p.nextRangeEnd
	rangeStart := rangeEnd.Add(-p.interval)

	first, err := findFirstLedgerAtOrAfter(ctx, p.probe, rangeStart, p.startLeger, p.nextLastLedger, p.nextLastClose, p.cadence)
	if err != nil {
		return backfillWindow{}, false, fmt.Errorf("placing window starting %s: %w", rangeStart.UTC().Format(time.RFC3339), err)
	}

	w := backfillWindow{
		RangeStart:  rangeStart,
		RangeEnd:    rangeEnd,
		FirstLedger: first,
		LastLedger:  p.nextLastLedger,
		BelowCursor: true,
	}

	if first <= p.startLeger {
		p.done = true
		return w, true, nil
	}
	// The ledger below this window's first ledger closes before rangeStart,
	// and the search established that; its exact close time is probed when
	// the next window is placed. Refresh the cadence from this window.
	if n := w.ledgerCount(); n > 1 {
		p.cadence = p.nextLastClose.Sub(rangeStart) / time.Duration(n)
		if p.cadence <= 0 {
			p.cadence = 5 * time.Second
		}
	}
	p.nextLastLedger = first - 1
	p.nextRangeEnd = rangeStart
	lastClose, err := p.probe(ctx, p.nextLastLedger)
	if err != nil {
		return backfillWindow{}, false, fmt.Errorf("placing ledger %d: %w", p.nextLastLedger, err)
	}
	p.nextLastClose = lastClose
	return w, true, nil
}

// gapWindows turns the holes above the cursor into plain windows. Each
// window's time range spans the chunks its ledgers touch, probed from its
// first and last ledger, so the pre-fill delete stays bounded to those chunks.
func gapWindows(ctx context.Context, probe ledgerCloseTimeProbe, gaps []data.LedgerRange, interval time.Duration) ([]backfillWindow, error) {
	windows := make([]backfillWindow, 0, len(gaps))
	for _, gap := range gaps {
		firstClose, err := probe(ctx, gap.GapStart)
		if err != nil {
			return nil, fmt.Errorf("placing gap [%d-%d]: %w", gap.GapStart, gap.GapEnd, err)
		}
		lastClose := firstClose
		if gap.GapEnd != gap.GapStart {
			if lastClose, err = probe(ctx, gap.GapEnd); err != nil {
				return nil, fmt.Errorf("placing gap [%d-%d]: %w", gap.GapStart, gap.GapEnd, err)
			}
		}
		windows = append(windows, backfillWindow{
			RangeStart:  alignToChunk(firstClose, interval),
			RangeEnd:    alignToChunk(lastClose, interval).Add(interval),
			FirstLedger: gap.GapStart,
			LastLedger:  gap.GapEnd,
			BelowCursor: false,
		})
	}
	return windows, nil
}
