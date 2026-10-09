// Package prices derives token prices from ingested fills and oracle anchor rates.
package prices

import (
	"sync/atomic"
	"time"
)

// AnchorRates are the USD rates of the two anchor tokens that price every fill, as last read from
// the oracle. AsOf is the oracle's own timestamp for the reading.
type AnchorRates struct {
	XLMUSD  float64
	USDCUSD float64
	AsOf    time.Time
}

// Anchor holds the current anchor rates for the persist path. The oracle poller replaces the
// value; readers get a consistent snapshot without locking.
type Anchor struct {
	rates atomic.Pointer[AnchorRates]
}

// Set publishes new rates.
func (a *Anchor) Set(r AnchorRates) {
	a.rates.Store(&r)
}

// Get returns the current rates; ok is false until the first Set.
func (a *Anchor) Get() (AnchorRates, bool) {
	p := a.rates.Load()
	if p == nil {
		return AnchorRates{}, false
	}
	return *p, true
}
