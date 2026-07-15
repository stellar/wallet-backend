package middleware

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRequestTimeoutMiddleware(t *testing.T) {
	t.Run("request context carries a deadline", func(t *testing.T) {
		var sawDeadline bool
		var sawDuration time.Duration
		next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			deadline, ok := r.Context().Deadline()
			sawDeadline = ok
			if ok {
				sawDuration = time.Until(deadline)
			}
			w.WriteHeader(http.StatusOK)
		})

		h := RequestTimeoutMiddleware(30 * time.Second)(next)
		req := httptest.NewRequest(http.MethodGet, "/", nil)
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)

		require.True(t, sawDeadline, "request context must carry a deadline")
		assert.InDelta(t, 30*time.Second, sawDuration, float64(2*time.Second))
		assert.Equal(t, http.StatusOK, rec.Code)
	})

	t.Run("context is canceled once the timeout elapses", func(t *testing.T) {
		done := make(chan error, 1)
		next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			<-r.Context().Done()
			done <- r.Context().Err()
			w.WriteHeader(http.StatusOK)
		})

		h := RequestTimeoutMiddleware(10 * time.Millisecond)(next)
		req := httptest.NewRequest(http.MethodGet, "/", nil)
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)

		select {
		case err := <-done:
			assert.Equal(t, context.DeadlineExceeded, err)
		case <-time.After(time.Second):
			t.Fatal("context was never canceled")
		}
	})
}
