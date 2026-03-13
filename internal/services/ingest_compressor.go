package services

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stellar/go-stellar-sdk/support/log"

	"github.com/stellar/wallet-backend/internal/db"
)

// progressiveCompressor compresses uncompressed TimescaleDB chunks as they become safe during backfill.
type progressiveCompressor struct {
	pool   *pgxpool.Pool
	chunksByBatch map[BackfillBatch][]*db.Chunk
	ctx    context.Context
	compressorCh <-chan *CompressBatch
	done      chan struct{}
}

// newProgressiveCompressor creates a compressor that progressively compresses uncompressed chunks
// as contiguous batches complete. Starts a background goroutine for compression work.
// globalStart is the chunk-boundary-aligned start timestamp for scoping chunk queries.
func newProgressiveCompressor(ctx context.Context, pool *pgxpool.Pool, chunksByBatch map[BackfillBatch][]*db.Chunk, compressorCh <-chan *CompressBatch) *progressiveCompressor {
	r := &progressiveCompressor{
		ctx:          ctx,
		pool:         pool,
		chunksByBatch: chunksByBatch,
		compressorCh: compressorCh,
		done: make(chan struct{}),
	}
	return r
}

// Wait closes the trigger channel and waits for background compression to finish.
func (r *progressiveCompressor) Wait() {
	<-r.done
}

// runCompression processes compression triggers in the background.
// For each safe window, queries and compresses uncompressed chunks per table.
func (r *progressiveCompressor) runCompression() {
	go func() {
		defer close(r.done)
		for item := range r.compressorCh {
			if r.ctx.Err() != nil {
				break
			}

			chunks := r.chunksByBatch[*item.batch]
			remaining := chunks[:0]
			for _, chunk := range chunks {
				if !chunk.End.After(item.ledgerCloseTime) {
					if chunk.NumWriters.Add(-1) == 0 {
						if err := r.compressChunk(r.ctx, chunk.Name); err != nil {
							log.Ctx(r.ctx).Warnf("Failed to compress %s, will retry next run: %v", chunk.Name, err)
						}
					}
				} else {
					remaining = append(remaining, chunk)
				}
			}
			r.chunksByBatch[*item.batch] = remaining
		}
	}()
}

func (r *progressiveCompressor) compressChunk(ctx context.Context, chunk string) error {
	if _, err := r.pool.Exec(ctx, `SELECT compress_chunk($1::regclass)`, chunk); err != nil {
		return fmt.Errorf("compressing chunk %s: %w", chunk, err)
	}
	if err := db.SetChunkLogged(ctx, r.pool, chunk); err != nil {
		return fmt.Errorf("setting chunk %s logged: %w", chunk, err)
	}
	log.Ctx(ctx).Infof("Compressed chunk %s", chunk)
	return nil
}
