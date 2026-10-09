package prices

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stellar/go-stellar-sdk/support/log"

	"github.com/stellar/wallet-backend/internal/metrics"
)

// SnapshotHolder keeps the latest price snapshot in memory for request handlers. Run reloads it
// on an interval; readers never touch the database.
type SnapshotHolder struct {
	db       *pgxpool.Pool
	interval time.Duration
	current  atomic.Pointer[Snapshot]
	metrics  *metrics.PricesMetrics
}

// NewSnapshotHolder builds a holder that reloads every interval. m may be nil.
func NewSnapshotHolder(db *pgxpool.Pool, interval time.Duration, m *metrics.PricesMetrics) *SnapshotHolder {
	return &SnapshotHolder{db: db, interval: interval, metrics: m}
}

// Run loads the snapshot immediately, then again every interval until ctx is done. A failed load
// keeps the previous snapshot and is logged; the first failure leaves handlers with no prices
// rather than stopping the server.
func (h *SnapshotHolder) Run(ctx context.Context) {
	h.reload(ctx)
	ticker := time.NewTicker(h.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			h.reload(ctx)
		}
	}
}

func (h *SnapshotHolder) reload(ctx context.Context) {
	start := time.Now()
	snap, err := LoadSnapshot(ctx, h.db, start)
	if err != nil {
		log.Ctx(ctx).Errorf("Loading price snapshot: %v", err)
		if h.metrics != nil {
			h.metrics.SnapshotReloadErrorsTotal.Inc()
		}
		return
	}
	h.current.Store(snap)
	if h.metrics != nil {
		h.metrics.SnapshotReloadDuration.Observe(time.Since(start).Seconds())
		h.metrics.SnapshotTokens.Set(float64(len(snap.Prices)))
	}
	log.Ctx(ctx).Debugf("Loaded price snapshot: %d tokens in %s", len(snap.Prices), time.Since(start))
}

// Current returns the latest snapshot, or nil before the first successful load.
func (h *SnapshotHolder) Current() *Snapshot {
	return h.current.Load()
}

// Set installs snap as the current snapshot. Run does this itself; tests use it to avoid a database.
func (h *SnapshotHolder) Set(snap *Snapshot) {
	h.current.Store(snap)
}
