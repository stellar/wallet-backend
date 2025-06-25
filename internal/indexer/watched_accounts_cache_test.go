package indexer

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_NewWatchedAccountsCache_periodicRebuild_doRebuild_IsWatched(t *testing.T) {
	ctx := context.Background()
	reloadInterval := 100 * time.Millisecond
	reloadFn := func(ctx context.Context) ([]string, error) {
		return []string{"watched1", "watched2"}, nil
	}
	cache := NewWatchedAccountsCache(ctx, reloadInterval, reloadFn)
	require.NotNil(t, cache)
	defer cache.Close()

	// Test that the cache is created correctly
	require.NotNil(t, cache)
	assert.Equal(t, reloadInterval, cache.reloadInterval)
	assert.NotNil(t, cache.reloadFn)
	assert.NotNil(t, cache.quit)

	// Test that the accounts are not being watched yet
	assert.False(t, cache.IsWatched("watched1"))
	assert.False(t, cache.IsWatched("watched2"))
	assert.False(t, cache.IsWatched("not_watched"))

	// Wait for initial rebuild and test that accounts are watched
	time.Sleep(150 * time.Millisecond)
	assert.True(t, cache.IsWatched("watched1"))
	assert.True(t, cache.IsWatched("watched2"))
	assert.False(t, cache.IsWatched("not_watched"))
}

func Test_WatchedAccountsCache_doRebuild_Error(t *testing.T) {
	ctx := context.Background()
	reloadFn := func(ctx context.Context) ([]string, error) {
		return nil, errors.New("reload failed")
	}

	cache := &WatchedAccountsCache{
		reloadInterval: 1 * time.Hour,
		reloadFn:       reloadFn,
		quit:           make(chan struct{}),
	}
	defer cache.Close()

	// Test that rebuild panics on error
	assert.PanicsWithError(t, "failed to reload watched accounts: reload failed", func() {
		cache.DoRebuild(ctx)
	})
}

func Test_WatchedAccountsCache_Stop(t *testing.T) {
	ctx := context.Background()
	reloadFn := func(ctx context.Context) ([]string, error) {
		return []string{"account1"}, nil
	}
	cache := NewWatchedAccountsCache(ctx, 1*time.Hour, reloadFn)

	// Stop the cache
	cache.Close()
	_, ok := <-cache.quit
	assert.False(t, ok, "quit channel should be closed after Close()")
}

func TestWatchedAccountsCache_largeAccountSet_concurrentAccess_falsePositiveRatio(t *testing.T) {
	ctx := context.Background()

	// Create a large set of accounts
	reloadFn := func(ctx context.Context) ([]string, error) {
		accounts := make([]string, 1000)
		for i := range make([]int, 1000) {
			accounts[i] = fmt.Sprintf("account_%d", i)
		}
		return accounts, nil
	}

	cache := &WatchedAccountsCache{
		reloadInterval: 1 * time.Hour,
		reloadFn:       reloadFn,
		quit:           make(chan struct{}),
	}
	defer cache.Close()

	// Test rebuild with large account set
	cache.DoRebuild(ctx)

	// Measure false positive ratio by testing accounts NOT in the filter, with concurrent access
	falsePositives := 0
	testCount := 10000
	wg := sync.WaitGroup{}
	mu := sync.Mutex{}
	wg.Add(testCount)
	for i := range make([]int, testCount) {
		// Test accounts >=1000 (not in the bloom filter)
		go func() {
			defer wg.Done()
			if cache.IsWatched(fmt.Sprintf("account_%d", i+1000)) {
				mu.Lock()
				defer mu.Unlock()
				falsePositives++
			}
		}()
	}
	wg.Wait()

	falsePositiveRatio := float64(falsePositives) / float64(testCount)
	t.Logf("False positives: %d/%d = %.4f (expected ~0.01)", falsePositives, testCount, falsePositiveRatio)
	assert.InDelta(t, 0.01, falsePositiveRatio, 0.005) // Should be between [0.005, 0.015]
}

func Test_WatchedAccountsCache_periodicRebuild_exitting(t *testing.T) {
	testCases := []struct {
		name       string
		exittingFn func(cancel context.CancelFunc)
	}{
		{
			name: "context_cancellation",
			exittingFn: func(cancel context.CancelFunc) {
				cancel()
			},
		},
		{
			name: "SIGTERM",
			exittingFn: func(cancel context.CancelFunc) {
				err := syscall.Kill(syscall.Getpid(), syscall.SIGTERM)
				require.NoError(t, err)
			},
		},
		{
			name: "SIGINT",
			exittingFn: func(cancel context.CancelFunc) {
				err := syscall.Kill(syscall.Getpid(), syscall.SIGINT)
				require.NoError(t, err)
			},
		},
		{
			name: "SIGQUIT",
			exittingFn: func(cancel context.CancelFunc) {
				err := syscall.Kill(syscall.Getpid(), syscall.SIGQUIT)
				require.NoError(t, err)
			},
		},
	}

	reloadFn := func(ctx context.Context) ([]string, error) {
		return []string{"account1"}, nil
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())

			cache := &WatchedAccountsCache{
				reloadInterval: 200 * time.Millisecond,
				reloadFn:       reloadFn,
				quit:           make(chan struct{}),
			}
			defer cache.Close()

			go func() {
				time.Sleep(100 * time.Millisecond)
				tc.exittingFn(cancel)
			}()
			cache.periodicRebuild(ctx)

			// Verify rebuild happened
			assert.False(t, cache.IsWatched("account1"), "the reload function should not be called because the context was cancelled")
		})
	}
}
