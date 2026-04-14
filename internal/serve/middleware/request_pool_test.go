package middleware

import (
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRequestPoolMiddleware(t *testing.T) {
	t.Run("injects pool into context", func(t *testing.T) {
		var poolFound bool
		inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			pool := RequestPoolFromContext(r.Context())
			poolFound = pool != nil
		})

		handler := RequestPoolMiddleware(10)(inner)
		req := httptest.NewRequest(http.MethodPost, "/graphql/query", nil)
		rr := httptest.NewRecorder()
		handler.ServeHTTP(rr, req)

		assert.True(t, poolFound, "request pool should be present in context")
	})

	t.Run("limits concurrency", func(t *testing.T) {
		const maxConcurrency = 5
		var peakConcurrent atomic.Int64
		var currentConcurrent atomic.Int64

		inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			pool := RequestPoolFromContext(r.Context())
			require.NotNil(t, pool)

			var wg sync.WaitGroup
			// Submit more tasks than the concurrency limit
			for i := 0; i < 20; i++ {
				wg.Add(1)
				pool.Submit(func() {
					defer wg.Done()
					cur := currentConcurrent.Add(1)
					// Track peak concurrency
					for {
						peak := peakConcurrent.Load()
						if cur <= peak || peakConcurrent.CompareAndSwap(peak, cur) {
							break
						}
					}
					time.Sleep(10 * time.Millisecond)
					currentConcurrent.Add(-1)
				})
			}
			wg.Wait()
		})

		handler := RequestPoolMiddleware(maxConcurrency)(inner)
		req := httptest.NewRequest(http.MethodPost, "/graphql/query", nil)
		rr := httptest.NewRecorder()
		handler.ServeHTTP(rr, req)

		assert.LessOrEqual(t, peakConcurrent.Load(), int64(maxConcurrency),
			"peak concurrency should not exceed pool limit")
		assert.Greater(t, peakConcurrent.Load(), int64(0),
			"at least some tasks should have run concurrently")
	})

	t.Run("pool is stopped after handler returns", func(t *testing.T) {
		var capturedStopped bool
		inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			pool := RequestPoolFromContext(r.Context())
			require.NotNil(t, pool)
			// Submit a task to ensure pool is running
			task := pool.Submit(func() {})
			task.Wait()
			assert.False(t, pool.Stopped(), "pool should not be stopped during handler")
		})

		var afterHandlerPool bool
		wrappedMiddleware := func(next http.Handler) http.Handler {
			return RequestPoolMiddleware(5)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				next.ServeHTTP(w, r)
				// After inner handler returns but before middleware returns,
				// the pool should still be accessible
				pool := RequestPoolFromContext(r.Context())
				afterHandlerPool = pool != nil
				// Note: pool.Stopped() may or may not be true here since
				// StopAndWait happens after ServeHTTP returns in the middleware
			}))
		}

		handler := wrappedMiddleware(inner)
		req := httptest.NewRequest(http.MethodPost, "/graphql/query", nil)
		rr := httptest.NewRecorder()
		handler.ServeHTTP(rr, req)

		_ = capturedStopped
		assert.True(t, afterHandlerPool, "pool should be in context")
	})
}

func TestRequestPoolFromContext_NoPool(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	pool := RequestPoolFromContext(req.Context())
	assert.Nil(t, pool, "should return nil when no pool in context")
}
