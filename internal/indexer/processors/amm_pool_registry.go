package processors

import (
	"sync"

	"github.com/stellar/wallet-backend/internal/indexer/types"
)

// AMMPoolRegistry is the in-memory set of trusted AMM pools, keyed by pool contract address. The
// pools processor adds to it as factories announce pairs and the trades processor reads it on
// every swap event; both run from parallel transaction workers.
type AMMPoolRegistry struct {
	mu    sync.RWMutex
	pools map[string]types.AMMPool
}

// NewAMMPoolRegistry builds a registry preloaded with pools.
func NewAMMPoolRegistry(pools []types.AMMPool) *AMMPoolRegistry {
	r := &AMMPoolRegistry{pools: make(map[string]types.AMMPool, len(pools))}
	for _, p := range pools {
		r.pools[p.Pool] = p
	}
	return r
}

// Get returns the pool registered at addr.
func (r *AMMPoolRegistry) Get(addr string) (types.AMMPool, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	p, ok := r.pools[addr]
	return p, ok
}

// Add registers a pool, keeping the first registration.
func (r *AMMPoolRegistry) Add(p types.AMMPool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.pools[p.Pool]; !exists {
		r.pools[p.Pool] = p
	}
}

// Len returns the number of registered pools.
func (r *AMMPoolRegistry) Len() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.pools)
}
