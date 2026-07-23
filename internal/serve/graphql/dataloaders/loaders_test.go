package dataloaders

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type fakeItem struct {
	pk  int
	val string
}

// TestNewOneToManyLoader_ConcurrentLoadsCoalesceIntoOneFetch is dataloadgen's whole reason to
// exist: N goroutines calling Load concurrently for same-shape keys must land in the same batch
// window and produce exactly one fetcher call, not N. A regression that widened the batch window
// wouldn't break this (it would still coalesce, just slower), but a regression that dropped
// batching entirely — e.g. capacity/wait misconfigured to fire per-key — would turn one fetch
// call into loadCount and this test catches that deterministically via the call counter, with no
// dependency on wall-clock timing.
func TestNewOneToManyLoader_ConcurrentLoadsCoalesceIntoOneFetch(t *testing.T) {
	type key struct{ id int }

	var fetchCalls int32
	loader := newOneToManyLoader(
		func(_ context.Context, keys []key) ([]fakeItem, error) {
			atomic.AddInt32(&fetchCalls, 1)
			items := make([]fakeItem, len(keys))
			for i, k := range keys {
				items[i] = fakeItem{pk: k.id, val: "v"}
			}
			return items, nil
		},
		func(item fakeItem) int { return item.pk },
		func(k key) int { return k.id },
		func(item fakeItem) fakeItem { return item },
		func(key) QueryShape { return QueryShape{} },
		"test",
		nil,
	)

	const loadCount = 20
	ctx := context.Background()
	results := make([][]*fakeItem, loadCount)
	errs := make([]error, loadCount)

	var wg sync.WaitGroup
	wg.Add(loadCount)
	for i := range loadCount {
		go func(i int) {
			defer wg.Done()
			results[i], errs[i] = loader.Load(ctx, key{id: i})
		}(i)
	}
	wg.Wait()

	for i := range loadCount {
		require.NoError(t, errs[i])
		require.Len(t, results[i], 1)
		assert.Equal(t, i, results[i][0].pk)
	}
	assert.Equal(t, int32(1), atomic.LoadInt32(&fetchCalls), "N concurrent same-shape Loads must coalesce into a single fetch call")
}
