package middleware

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/go-chi/chi"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/wallet-backend/internal/metrics"
)

// TestMetricsMiddleware_BoundedLabels pins the property that no
// client-controlled string reaches a label. Hundreds of distinct paths and method
// tokens must collapse into a handful of series.
func TestMetricsMiddleware_BoundedLabels(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := metrics.NewMetrics(reg).HTTP

	ok := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	mux := chi.NewRouter()
	mux.Use(MetricsMiddleware(m))
	mux.Get("/health", ok)
	mux.Route("/graphql", func(r chi.Router) {
		r.Handle("/query", ok)
	})

	do := func(method, path string) {
		mux.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(method, path, nil))
	}
	for i := range 200 {
		do(http.MethodGet, fmt.Sprintf("/does-not-exist/%d", i)) // 404, nothing matched
		do(fmt.Sprintf("VERB%d", i), "/graphql/query")           // 405, unknown method token
		do(http.MethodGet, fmt.Sprintf("/graphql/query/%d", i))  // 404 under a mounted prefix
	}
	do(http.MethodGet, "/health")
	do(http.MethodPost, "/graphql/query")

	// Every endpoint label must be a route pattern or the fixed fallback; every method
	// label must be a known verb or the fixed fallback.
	allowedEndpoints := map[string]bool{unmatchedEndpoint: true, "/graphql/*": true, "/health": true, "/graphql/query": true}
	allowedMethods := map[string]bool{"GET": true, "POST": true, otherMethod: true}
	families, err := reg.Gather()
	require.NoError(t, err)
	for _, family := range families {
		if !strings.HasPrefix(family.GetName(), "wallet_http_") {
			continue // the registry also holds DB, GraphQL and auth collectors
		}
		for _, metric := range family.GetMetric() {
			for _, label := range metric.GetLabel() {
				switch label.GetName() {
				case "endpoint":
					assert.True(t, allowedEndpoints[label.GetValue()], "unbounded endpoint label %q", label.GetValue())
				case "method":
					assert.True(t, allowedMethods[label.GetValue()], "unbounded method label %q", label.GetValue())
				}
			}
		}
	}

	// 600 distinct requests, at most 5 series: unmatched/GET/404, unmatched/OTHER/405,
	// the mounted-prefix 404, and the two routed requests.
	assert.LessOrEqual(t, testutil.CollectAndCount(m.RequestsTotal), 5)
	assert.LessOrEqual(t, testutil.CollectAndCount(m.RequestsDuration), 5)
	assert.Equal(t, 1.0, testutil.ToFloat64(m.RequestsTotal.WithLabelValues("/health", "GET", "200")))
	assert.Equal(t, 1.0, testutil.ToFloat64(m.RequestsTotal.WithLabelValues("/graphql/query", "POST", "200")))
	assert.Equal(t, 200.0, testutil.ToFloat64(m.RequestsTotal.WithLabelValues(unmatchedEndpoint, otherMethod, "405")))
}
