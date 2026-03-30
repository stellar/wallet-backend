package metrics

import "github.com/prometheus/client_golang/prometheus"

// AuthMetrics holds Prometheus collectors for authentication operations.
type AuthMetrics struct {
	ExpiredSignaturesTotal prometheus.Counter
}

func newAuthMetrics(reg prometheus.Registerer) *AuthMetrics {
	m := &AuthMetrics{
		ExpiredSignaturesTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "wallet_auth_expired_signatures_total",
			Help: "Total number of signature verifications that failed due to expiration.",
		}),
	}
	reg.MustRegister(m.ExpiredSignaturesTotal)
	return m
}
