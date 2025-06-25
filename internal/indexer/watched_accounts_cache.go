package indexer

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/bits-and-blooms/bloom/v3"
	"github.com/stellar/go/support/log"
)

type ReloadFn func(ctx context.Context) ([]string, error)

type WatchedAccountsCache struct {
	bloomFilter    atomic.Value // holds *bloom.BloomFilter
	reloadInterval time.Duration
	reloadFn       ReloadFn
	quit           chan struct{}
}

func NewWatchedAccountsCache(ctx context.Context, reloadInterval time.Duration, reloadFn ReloadFn) *WatchedAccountsCache {
	wc := &WatchedAccountsCache{
		reloadInterval: reloadInterval,
		reloadFn:       reloadFn,
		quit:           make(chan struct{}),
	}
	go wc.periodicRebuild(ctx)
	return wc
}

// periodicRebuild is a goroutine that periodically reloads the watched accounts and updates the bloom filter.
// It also listens for signals to stop the goroutine.
// It is started by NewWatchedAccountsCache and can be stopped by Close().
func (wc *WatchedAccountsCache) periodicRebuild(ctx context.Context) {
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, syscall.SIGTERM, syscall.SIGINT, syscall.SIGQUIT)
	defer signal.Stop(signalChan)

	ticker := time.NewTicker(wc.reloadInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Ctx(ctx).Infof("watched accounts cache stopped due to context cancellation: %v", ctx.Err())
			return

		case sig := <-signalChan:
			log.Ctx(ctx).Warnf("watched accounts cache stopped due to signal %s", sig)
			return

		case <-wc.quit:
			log.Ctx(ctx).Infof("watched accounts cache stopped due to quit signal")
			return

		case <-ticker.C:
			wc.DoRebuild(ctx)
		}
	}
}

// DoRebuild reloads the watched accounts and updates the bloom filter.
// It is called by the periodicRebuild goroutine and uses the reloadFn to get the accounts.
func (wc *WatchedAccountsCache) DoRebuild(ctx context.Context) {
	accounts, err := wc.reloadFn(ctx)
	if err != nil {
		panic(fmt.Errorf("failed to reload watched accounts: %w", err))
	}

	newFilter := bloom.NewWithEstimates(uint(len(accounts)), 0.01)
	for _, account := range accounts {
		newFilter.AddString(account)
	}

	wc.bloomFilter.Store(newFilter)
}

// IsWatched checks if an account is watched by the bloom filter.
func (wc *WatchedAccountsCache) IsWatched(account string) bool {
	bf := wc.bloomFilter.Load()
	if bf == nil {
		return false
	}
	return bf.(*bloom.BloomFilter).TestString(account)
}

// Close stops the periodicRebuild goroutine.
func (wc *WatchedAccountsCache) Close() {
	close(wc.quit)
}
