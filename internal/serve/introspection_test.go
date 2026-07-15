package serve

import (
	"net/http"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/wallet-backend/internal/data"
	"github.com/stellar/wallet-backend/internal/metrics"
)

const introspectionQuery = `{ __schema { queryType { name } } }`

// newIntrospectionTestHandler mirrors newGraphQLTestHandler but also lets the test control the
// introspection flag (GQL-06), which newGraphQLTestHandler doesn't expose since every other test
// relies on its default (disabled).
func newIntrospectionTestHandler(t *testing.T, introspectionEnabled bool) http.Handler {
	t.Helper()

	registry := prometheus.NewRegistry()
	m := metrics.NewMetrics(registry)
	models, err := data.NewModels(complexityTestPool, m.DB)
	require.NoError(t, err)

	return handler(handlerDeps{
		Models:                      models,
		Metrics:                     m,
		GraphQLComplexityLimit:      1000,
		GraphQLIntrospectionEnabled: introspectionEnabled,
	})
}

func TestGraphQLIntrospectionGating(t *testing.T) {
	t.Run("disabled by default: introspection query is rejected", func(t *testing.T) {
		h := newIntrospectionTestHandler(t, false)
		resp := performGraphQLRequest(t, h, introspectionQuery)
		require.NotEmpty(t, resp.Errors, "introspection must be rejected when the extension is not registered")
	})

	t.Run("enabled: introspection query succeeds", func(t *testing.T) {
		h := newIntrospectionTestHandler(t, true)
		resp := performGraphQLRequest(t, h, introspectionQuery)
		require.Empty(t, resp.Errors)
		assert.Contains(t, string(resp.Data), "Query")
	})
}
