package services

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/stellar/go-stellar-sdk/support/log"
)

// backfillCompressor compresses uncompressed TimescaleDB chunks as the flusher reports
// contiguous progress. The flusher sends the close time of the highest contiguously-
// persisted ledger ("safe end") on triggerCh; the compressor compresses any uncompressed
// chunk ending before it. The signal is monotonic, so no watermark or ordering state is
// needed.
type backfillCompressor struct {
	pool      *pgxpool.Pool
	tables    []string
	triggerCh chan time.Time
	done      chan struct{}
}

func newBackfillCompressor(ctx context.Context, pool *pgxpool.Pool, tables []string) *backfillCompressor {
	c := &backfillCompressor{
		pool:      pool,
		tables:    tables,
		triggerCh: make(chan time.Time, 256),
		done:      make(chan struct{}),
	}
	go c.run(ctx)
	return c
}

// trigger reports that every ledger with close time <= safeEnd is persisted. The send is
// intentionally blocking: it applies backpressure if compression falls behind, and it
// guarantees the final (highest) safe-end is delivered so the tail always compresses.
func (c *backfillCompressor) trigger(safeEnd time.Time) {
	c.triggerCh <- safeEnd
}

// Wait stops accepting triggers and blocks until the background goroutine drains.
func (c *backfillCompressor) Wait() {
	close(c.triggerCh)
	<-c.done
}

func (c *backfillCompressor) run(ctx context.Context) {
	defer close(c.done)
	total := 0
	for safeEnd := range c.triggerCh {
		safeEnd = drainLatest(c.triggerCh, safeEnd) // collapse a burst into one pass
		for _, table := range c.tables {
			total += c.compressTable(ctx, table, safeEnd)
		}
	}
	log.Ctx(ctx).Infof("Progressive compression complete: %d chunks compressed", total)
}

// drainLatest collapses all currently-queued safe-end values into the maximum, so a burst
// of flush signals triggers a single compression pass instead of one per flush.
func drainLatest(ch <-chan time.Time, end time.Time) time.Time {
	for {
		select {
		case next, ok := <-ch:
			if !ok {
				return end
			}
			if next.After(end) {
				end = next
			}
		default:
			return end
		}
	}
}

// compressTable compresses uncompressed chunks for one table whose range_end < safeEnd.
func (c *backfillCompressor) compressTable(ctx context.Context, table string, safeEnd time.Time) int {
	rows, err := c.pool.Query(ctx,
		`SELECT c.chunk_schema || '.' || c.chunk_name
		 FROM timescaledb_information.chunks c
		 WHERE c.hypertable_name = $1
		   AND NOT c.is_compressed
		   AND c.range_end < $2::timestamptz`,
		table, safeEnd)
	if err != nil {
		log.Ctx(ctx).Warnf("Failed to list chunks for %s: %v", table, err)
		return 0
	}
	var chunks []string
	for rows.Next() {
		var chunk string
		if scanErr := rows.Scan(&chunk); scanErr != nil {
			log.Ctx(ctx).Warnf("Failed to scan chunk row for %s: %v", table, scanErr)
			continue
		}
		chunks = append(chunks, chunk)
	}
	if err := rows.Err(); err != nil {
		log.Ctx(ctx).Warnf("Error iterating chunk rows for %s: %v", table, err)
		rows.Close()
		return 0
	}
	rows.Close()

	compressed := 0
	for _, chunk := range chunks {
		select {
		case <-ctx.Done():
			return compressed
		default:
		}
		if _, err := c.pool.Exec(ctx, `SELECT compress_chunk($1::regclass)`, chunk); err != nil {
			log.Ctx(ctx).Warnf("Failed to compress chunk %s: %v", chunk, err)
			continue
		}
		compressed++
	}
	if compressed > 0 {
		log.Ctx(ctx).Infof("Compressed %d chunks for %s (safe end: %s)", compressed, table, safeEnd.Format(time.RFC3339))
	}
	return compressed
}
