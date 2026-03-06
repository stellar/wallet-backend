package services

import (
	"context"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stellar/go-stellar-sdk/support/log"
)

// progressiveRecompressor compresses uncompressed TimescaleDB chunks as they become safe during backfill.
// Tracks batch completion via a watermark to determine when chunks are fully written.
type progressiveRecompressor struct {
	pool   *pgxpool.Pool
	tables []string
	ctx    context.Context

	mu           sync.Mutex
	completed    []bool
	endTimes     []time.Time
	watermarkIdx int       // index of highest contiguous completed batch (-1 = none)
	globalStart  time.Time // lower bound for chunk queries (batch 0's StartTime)
	globalEnd    time.Time // upper bound for verification (max EndTime across completed batches)

	triggerCh chan time.Time // safeEnd for recompression window
	done      chan struct{}
}

// newProgressiveRecompressor creates a compressor that progressively compresses uncompressed chunks
// as contiguous batches complete. Starts a background goroutine for compression work.
func newProgressiveRecompressor(ctx context.Context, pool *pgxpool.Pool, tables []string, totalBatches int) *progressiveRecompressor {
	r := &progressiveRecompressor{
		pool:         pool,
		tables:       tables,
		ctx:          ctx,
		completed:    make([]bool, totalBatches),
		endTimes:     make([]time.Time, totalBatches),
		watermarkIdx: -1,
		triggerCh:    make(chan time.Time, totalBatches),
		done:         make(chan struct{}),
	}
	go r.runCompression()
	return r
}

// MarkDone records a batch as complete and advances the watermark if possible.
// If the watermark advances, triggers recompression of chunks in the safe window.
func (r *progressiveRecompressor) MarkDone(batchIdx int, startTime, endTime time.Time) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.completed[batchIdx] = true
	r.endTimes[batchIdx] = endTime

	// Record global start from batch 0 (earliest time boundary for queries)
	if batchIdx == 0 {
		r.globalStart = startTime
	}

	// Track the maximum EndTime across all completed batches for verification scope
	if endTime.After(r.globalEnd) {
		r.globalEnd = endTime
	}

	// Advance watermark past contiguous completed batches
	oldWatermark := r.watermarkIdx
	for r.watermarkIdx+1 < len(r.completed) && r.completed[r.watermarkIdx+1] {
		r.watermarkIdx++
	}

	// Trigger recompression if watermark advanced
	if r.watermarkIdx > oldWatermark {
		r.triggerCh <- r.endTimes[r.watermarkIdx]
	}
}

// Wait closes the trigger channel and waits for background compression to finish.
func (r *progressiveRecompressor) Wait() {
	close(r.triggerCh)
	<-r.done
}

// runCompression processes compression triggers in the background.
// For each safe window, queries and compresses uncompressed chunks per table.
func (r *progressiveRecompressor) runCompression() {
	defer close(r.done)

	totalCompressed := 0
	for safeEnd := range r.triggerCh {
		for _, table := range r.tables {
			count := r.compressTableChunks(table, safeEnd)
			totalCompressed += count
		}
	}

	log.Ctx(r.ctx).Infof("Progressive compression complete: %d total chunks compressed", totalCompressed)
}

// compressTableChunks compresses uncompressed chunks for a single table within the safe window.
// Queries chunks where range_end falls within (globalStart, safeEnd) to catch the leading
// boundary chunk that overlaps globalStart.
func (r *progressiveRecompressor) compressTableChunks(table string, safeEnd time.Time) int {
	rows, err := r.pool.Query(r.ctx,
		`SELECT c.chunk_schema || '.' || c.chunk_name
		 FROM timescaledb_information.chunks c
		 WHERE c.hypertable_name = $1
		   AND NOT c.is_compressed
		   AND c.range_end < $2::timestamptz
		   AND c.range_end > $3::timestamptz`,
		table, safeEnd, r.globalStart)
	if err != nil {
		log.Ctx(r.ctx).Warnf("Failed to get chunks for %s: %v", table, err)
		return 0
	}
	defer rows.Close()

	var chunks []string
	for rows.Next() {
		var chunk string
		if err := rows.Scan(&chunk); err != nil {
			log.Ctx(r.ctx).Warnf("Failed to scan chunk row for table %s: %v", table, err)
			continue
		}
		chunks = append(chunks, chunk)
	}

	compressed := 0
	for _, chunk := range chunks {
		select {
		case <-r.ctx.Done():
			log.Ctx(r.ctx).Warnf("Compression cancelled for %s after %d chunks", table, compressed)
			return compressed
		default:
		}

		_, err := r.pool.Exec(r.ctx, `SELECT compress_chunk($1::regclass)`, chunk)
		if err != nil {
			log.Ctx(r.ctx).Warnf("Failed to compress chunk %s: %v", chunk, err)
			continue
		}
		compressed++
		log.Ctx(r.ctx).Debugf("Compressed chunk for %s: %s", table, chunk)
	}

	if compressed > 0 {
		log.Ctx(r.ctx).Infof("Compressed %d chunks for table %s (window end: %s)",
			compressed, table, safeEnd.Format(time.RFC3339))
	}

	return compressed
}
