package middleware

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRateLimiter(t *testing.T) {
	okHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	t.Run("allows requests within limit", func(t *testing.T) {
		handler := RateLimiter(10, 5)(okHandler)

		for i := 0; i < 5; i++ {
			req := httptest.NewRequest(http.MethodPost, "/graphql/query", nil)
			req.RemoteAddr = "192.168.1.1:12345"
			rr := httptest.NewRecorder()
			handler.ServeHTTP(rr, req)
			assert.Equal(t, http.StatusOK, rr.Code, "request %d should succeed", i)
		}
	})

	t.Run("rejects requests exceeding burst", func(t *testing.T) {
		handler := RateLimiter(1, 2)(okHandler)

		// First 2 should succeed (burst)
		for i := 0; i < 2; i++ {
			req := httptest.NewRequest(http.MethodPost, "/graphql/query", nil)
			req.RemoteAddr = "10.0.0.1:12345"
			rr := httptest.NewRecorder()
			handler.ServeHTTP(rr, req)
			assert.Equal(t, http.StatusOK, rr.Code, "burst request %d should succeed", i)
		}

		// Third should be rate limited
		req := httptest.NewRequest(http.MethodPost, "/graphql/query", nil)
		req.RemoteAddr = "10.0.0.1:12345"
		rr := httptest.NewRecorder()
		handler.ServeHTTP(rr, req)
		assert.Equal(t, http.StatusTooManyRequests, rr.Code)
	})

	t.Run("tracks IPs independently", func(t *testing.T) {
		handler := RateLimiter(1, 1)(okHandler)

		// Exhaust limit for IP A
		req := httptest.NewRequest(http.MethodPost, "/graphql/query", nil)
		req.RemoteAddr = "10.0.0.1:12345"
		rr := httptest.NewRecorder()
		handler.ServeHTTP(rr, req)
		assert.Equal(t, http.StatusOK, rr.Code)

		// IP A should be limited
		rr = httptest.NewRecorder()
		handler.ServeHTTP(rr, req)
		assert.Equal(t, http.StatusTooManyRequests, rr.Code)

		// IP B should still work
		req2 := httptest.NewRequest(http.MethodPost, "/graphql/query", nil)
		req2.RemoteAddr = "10.0.0.2:12345"
		rr2 := httptest.NewRecorder()
		handler.ServeHTTP(rr2, req2)
		assert.Equal(t, http.StatusOK, rr2.Code)
	})
}

func TestExtractIP(t *testing.T) {
	tests := []struct {
		name       string
		remoteAddr string
		xForwarded string
		xRealIP    string
		expected   string
	}{
		{
			name:       "prefers X-Real-IP over RemoteAddr",
			remoteAddr: "10.0.0.1:1234",
			xRealIP:    "203.0.113.50",
			expected:   "203.0.113.50",
		},
		{
			name:       "ignores X-Forwarded-For (spoofable)",
			remoteAddr: "10.0.0.1:1234",
			xForwarded: "203.0.113.50, 70.41.3.18",
			expected:   "10.0.0.1",
		},
		{
			name:       "X-Real-IP takes priority over X-Forwarded-For",
			remoteAddr: "10.0.0.1:1234",
			xForwarded: "1.2.3.4",
			xRealIP:    "203.0.113.50",
			expected:   "203.0.113.50",
		},
		{
			name:       "falls back to RemoteAddr",
			remoteAddr: "10.0.0.1:1234",
			expected:   "10.0.0.1",
		},
		{
			name:       "handles RemoteAddr without port",
			remoteAddr: "10.0.0.1",
			expected:   "10.0.0.1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/", nil)
			req.RemoteAddr = tt.remoteAddr
			if tt.xForwarded != "" {
				req.Header.Set("X-Forwarded-For", tt.xForwarded)
			}
			if tt.xRealIP != "" {
				req.Header.Set("X-Real-IP", tt.xRealIP)
			}
			got := extractIP(req)
			require.Equal(t, tt.expected, got)
		})
	}
}
