package middleware

import (
	"net"
	"net/http"
	"sync"
	"time"

	"golang.org/x/time/rate"
)

// ipLimiter tracks per-IP rate limiters with periodic cleanup of stale entries.
type ipLimiter struct {
	mu       sync.Mutex
	limiters map[string]*visitorEntry
	rate     rate.Limit
	burst    int
}

type visitorEntry struct {
	limiter  *rate.Limiter
	lastSeen time.Time
}

func newIPLimiter(r rate.Limit, burst int) *ipLimiter {
	ipl := &ipLimiter{
		limiters: make(map[string]*visitorEntry),
		rate:     r,
		burst:    burst,
	}
	go ipl.cleanup()
	return ipl
}

func (ipl *ipLimiter) getLimiter(ip string) *rate.Limiter {
	ipl.mu.Lock()
	defer ipl.mu.Unlock()

	v, exists := ipl.limiters[ip]
	if !exists {
		limiter := rate.NewLimiter(ipl.rate, ipl.burst)
		ipl.limiters[ip] = &visitorEntry{limiter: limiter, lastSeen: time.Now()}
		return limiter
	}
	v.lastSeen = time.Now()
	return v.limiter
}

// cleanup removes entries not seen in the last 5 minutes, runs every minute.
func (ipl *ipLimiter) cleanup() {
	for {
		time.Sleep(time.Minute)
		ipl.mu.Lock()
		for ip, v := range ipl.limiters {
			if time.Since(v.lastSeen) > 5*time.Minute {
				delete(ipl.limiters, ip)
			}
		}
		ipl.mu.Unlock()
	}
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
//
// Header trust order:
//  1. X-Real-IP — set by the NGINX ingress controller to the real client IP
//     (derived from $remote_addr, which is trustworthy because the AWS NLB
//     preserves the client IP at L4). NGINX overwrites any client-supplied
//     X-Real-IP, so this value cannot be spoofed.
//  2. RemoteAddr — direct connection peer; useful when there is no reverse proxy.
//
// X-Forwarded-For is intentionally NOT used. With use-forwarded-headers: "true",
// NGINX appends the real IP but preserves attacker-controlled leftmost values.
// An attacker can rotate the first XFF entry per request to bypass per-IP limits.
func extractIP(r *http.Request) string {
	if xri := r.Header.Get("X-Real-IP"); xri != "" {
		return xri
	}

	ip, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return ip
}
