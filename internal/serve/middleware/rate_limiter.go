package middleware

import (
	"net"
	"net/http"
	"sync"
	"time"

	"golang.org/x/time/rate"
)

const staleEntryTTL = 5 * time.Minute

// ipLimiter tracks per-IP rate limiters with opportunistic cleanup of stale entries.
type ipLimiter struct {
	mu          sync.Mutex
	limiters    map[string]*visitorEntry
	rate        rate.Limit
	burst       int
	lastCleanup time.Time
}

type visitorEntry struct {
	limiter  *rate.Limiter
	lastSeen time.Time
}

func newIPLimiter(r rate.Limit, burst int) *ipLimiter {
	return &ipLimiter{
		limiters:    make(map[string]*visitorEntry),
		rate:        r,
		burst:       burst,
		lastCleanup: time.Now(),
	}
}

func (ipl *ipLimiter) getLimiter(ip string) *rate.Limiter {
	ipl.mu.Lock()
	defer ipl.mu.Unlock()

	now := time.Now()

	// Opportunistic cleanup: run at most once per minute while holding the lock.
	if now.Sub(ipl.lastCleanup) > time.Minute {
		for k, v := range ipl.limiters {
			if now.Sub(v.lastSeen) > staleEntryTTL {
				delete(ipl.limiters, k)
			}
		}
		ipl.lastCleanup = now
	}

	v, exists := ipl.limiters[ip]
	if !exists {
		limiter := rate.NewLimiter(ipl.rate, ipl.burst)
		ipl.limiters[ip] = &visitorEntry{limiter: limiter, lastSeen: now}
		return limiter
	}
	v.lastSeen = now
	return v.limiter
}

// RateLimiter returns HTTP middleware that limits requests per IP address.
// requestsPerSecond controls the steady-state rate; burst controls the maximum
// number of requests allowed in a single burst.
func RateLimiter(requestsPerSecond float64, burst int) func(http.Handler) http.Handler {
	limiter := newIPLimiter(rate.Limit(requestsPerSecond), burst)

	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ip := extractIP(r)
			if !limiter.getLimiter(ip).Allow() {
				http.Error(w, http.StatusText(http.StatusTooManyRequests), http.StatusTooManyRequests)
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}

// extractIP returns the client IP for rate-limiting purposes.
func extractIP(r *http.Request) string {
	if xri := r.Header.Get("X-Real-IP"); xri != "" {
		if net.ParseIP(xri) != nil {
			return xri
		}
	}

	ip, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return ip
}
