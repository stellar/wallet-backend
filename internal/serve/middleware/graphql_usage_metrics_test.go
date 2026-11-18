package middleware

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/99designs/gqlgen/graphql"
	"github.com/99designs/gqlgen/graphql/executor"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/wallet-backend/internal/metrics"
	generated "github.com/stellar/wallet-backend/internal/serve/graphql/generated"
)

var usageTestNow = time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)

// newUsageTestExecutor builds a gqlgen executor over the real schema with the usage extension
// registered. CreateOperationContext parses, validates, coerces variables, and then runs the
// extension, which is the same path a real request takes. No resolvers run.
func newUsageTestExecutor(t *testing.T) (*executor.Executor, *metrics.GraphQLMetrics) {
	t.Helper()
	m := metrics.NewGraphQLMetrics(prometheus.NewRegistry())
	ext := NewGraphQLUsageMetrics(m)
	ext.now = func() time.Time { return usageTestNow }

	exec := executor.New(generated.NewExecutableSchema(generated.Config{}))
	exec.Use(ext)
	return exec, m
}

// usageTestContext returns a context with the operation start time that gqlgen's HTTP transports set.
func usageTestContext() context.Context {
	return graphql.StartOperationTrace(context.Background())
}

func runUsageQuery(t *testing.T, exec *executor.Executor, query string, vars map[string]any) {
	t.Helper()
	_, errs := exec.CreateOperationContext(usageTestContext(), &graphql.RawParams{Query: query, Variables: vars})
	require.Empty(t, errs)
}

func fieldCount(m *metrics.GraphQLMetrics, path string) float64 {
	return testutil.ToFloat64(m.FieldUsageTotal.WithLabelValues(path))
}

func argCount(m *metrics.GraphQLMetrics, path, arg, value string) float64 {
	return testutil.ToFloat64(m.ArgumentUsageTotal.WithLabelValues(path, arg, value))
}

func TestGraphQLUsageMetrics_NestedFields(t *testing.T) {
	exec, m := newUsageTestExecutor(t)
	runUsageQuery(t, exec, `{
		accountByAddress(address: "G1") {
			address
			stateChanges { edges { node { reason } } }
		}
	}`, nil)

	for _, path := range []string{
		"accountByAddress",
		"accountByAddress.address",
		"accountByAddress.stateChanges",
		"accountByAddress.stateChanges.edges",
		"accountByAddress.stateChanges.edges.node",
		"accountByAddress.stateChanges.edges.node.reason",
	} {
		assert.Equal(t, 1.0, fieldCount(m, path), path)
	}
	assert.Equal(t, 6, testutil.CollectAndCount(m.FieldUsageTotal))
}

func TestGraphQLUsageMetrics_MultipleRootFields(t *testing.T) {
	exec, m := newUsageTestExecutor(t)
	runUsageQuery(t, exec, `{
		accountByAddress(address: "G1") { address }
		transactionByHash(hash: "abc") { hash }
	}`, nil)

	assert.Equal(t, 1.0, fieldCount(m, "accountByAddress.address"))
	assert.Equal(t, 1.0, fieldCount(m, "transactionByHash.hash"))
	assert.Equal(t, 1.0, argCount(m, "accountByAddress", "address", argValueSet))
	assert.Equal(t, 1.0, argCount(m, "transactionByHash", "hash", argValueSet))
}

func TestGraphQLUsageMetrics_Aliases(t *testing.T) {
	exec, m := newUsageTestExecutor(t)
	runUsageQuery(t, exec, `{
		a: accountByAddress(address: "G1") { addr: address }
		b: accountByAddress(address: "G2") { address }
	}`, nil)

	assert.Equal(t, 1.0, fieldCount(m, "accountByAddress"))
	assert.Equal(t, 1.0, fieldCount(m, "accountByAddress.address"))
	assert.Equal(t, 1.0, argCount(m, "accountByAddress", "address", argValueSet))
	assert.Equal(t, 2, testutil.CollectAndCount(m.FieldUsageTotal))
}

func TestGraphQLUsageMetrics_Fragments(t *testing.T) {
	exec, m := newUsageTestExecutor(t)
	runUsageQuery(t, exec, `
		query {
			accountByAddress(address: "G1") {
				...AccountFields
				stateChanges(first: 5) {
					edges { node { ... on BalanceChange { reason } } }
				}
			}
		}
		fragment AccountFields on Account {
			address
			transactions(last: 20) { edges { node { hash } } }
		}`, nil)

	assert.Equal(t, 1.0, fieldCount(m, "accountByAddress.address"))
	assert.Equal(t, 1.0, fieldCount(m, "accountByAddress.transactions.edges.node.hash"))
	assert.Equal(t, 1.0, fieldCount(m, "accountByAddress.stateChanges.edges.node.reason"))
	assert.Equal(t, 1.0, argCount(m, "accountByAddress.transactions", "last", "<=25"))
	assert.Equal(t, 1.0, argCount(m, "accountByAddress.stateChanges", "first", "<=10"))
}

func TestGraphQLUsageMetrics_SkipAndInclude(t *testing.T) {
	exec, m := newUsageTestExecutor(t)
	runUsageQuery(t, exec, `query($skip: Boolean!) {
		accountByAddress(address: "G1") {
			address @skip(if: $skip)
			transactions @include(if: false) { edges { cursor } }
		}
	}`, map[string]any{"skip": true})

	assert.Equal(t, 1, testutil.CollectAndCount(m.FieldUsageTotal))
	assert.Equal(t, 1.0, fieldCount(m, "accountByAddress"))
}

func TestGraphQLUsageMetrics_Variables(t *testing.T) {
	exec, m := newUsageTestExecutor(t)
	runUsageQuery(t, exec, `query($addr: String!, $first: Int, $cat: StateChangeCategory, $unused: Int) {
		accountByAddress(address: $addr) {
			stateChanges(first: $first, last: $unused, filter: { category: $cat }) { edges { cursor } }
		}
	}`, map[string]any{"addr": "G1", "first": json.Number("75"), "cat": "BALANCE"})

	assert.Equal(t, 1.0, argCount(m, "accountByAddress", "address", argValueSet))
	assert.Equal(t, 1.0, argCount(m, "accountByAddress.stateChanges", "first", "<=100"))
	assert.Equal(t, 1.0, argCount(m, "accountByAddress.stateChanges", "filter.category", "BALANCE"))
	// $unused was not supplied, so "last" is not counted.
	assert.Equal(t, 3, testutil.CollectAndCount(m.ArgumentUsageTotal))
}

func TestGraphQLUsageMetrics_InputObjectSubFields(t *testing.T) {
	exec, m := newUsageTestExecutor(t)
	runUsageQuery(t, exec, `{
		accountByAddress(address: "G1") {
			stateChanges(filter: {
				category: BALANCE, reason: CREDIT, transactionHash: "abc", operationId: 12
			}) { edges { cursor } }
		}
	}`, nil)

	path := "accountByAddress.stateChanges"
	assert.Equal(t, 1.0, argCount(m, path, "filter.category", "BALANCE"))
	assert.Equal(t, 1.0, argCount(m, path, "filter.reason", "CREDIT"))
	assert.Equal(t, 1.0, argCount(m, path, "filter.transactionHash", argValueSet))
	assert.Equal(t, 1.0, argCount(m, path, "filter.operationId", argValueSet))
	assert.Equal(t, 5, testutil.CollectAndCount(m.ArgumentUsageTotal))
}

func TestGraphQLUsageMetrics_OmittedArgumentsNotCounted(t *testing.T) {
	exec, m := newUsageTestExecutor(t)
	runUsageQuery(t, exec, `{
		accountByAddress(address: "G1") {
			stateChanges(filter: { category: null }) { edges { cursor } }
			transactions { edges { cursor } }
		}
	}`, nil)

	// Only the address argument was supplied with a value.
	assert.Equal(t, 1, testutil.CollectAndCount(m.ArgumentUsageTotal))
}

func TestGraphQLUsageMetrics_PageSizeBuckets(t *testing.T) {
	for _, tc := range []struct {
		first int
		want  string
	}{
		{1, "<=10"},
		{10, "<=10"},
		{11, "<=25"},
		{25, "<=25"},
		{26, "<=50"},
		{50, "<=50"},
		{51, "<=100"},
		{100, "<=100"},
		{101, ">100"},
	} {
		exec, m := newUsageTestExecutor(t)
		runUsageQuery(t, exec, `query($n: Int) { blendPools(first: $n) { edges { cursor } } }`,
			map[string]any{"n": tc.first})
		assert.Equal(t, 1.0, argCount(m, "blendPools", "first", tc.want), "first=%d", tc.first)
	}
}

func TestGraphQLUsageMetrics_TimeRangeBuckets(t *testing.T) {
	ts := func(d time.Duration) string { return usageTestNow.Add(-d).Format(time.RFC3339) }
	const day = 24 * time.Hour

	for _, tc := range []struct {
		name     string
		vars     map[string]any
		argument string
		want     string
	}{
		{"since only, 1d", map[string]any{"since": ts(day)}, "since", "<=1d"},
		{"since only, 7d", map[string]any{"since": ts(7 * day)}, "since", "<=7d"},
		{"since only, 30d", map[string]any{"since": ts(30 * day)}, "since", "<=30d"},
		{"since only, 365d", map[string]any{"since": ts(365 * day)}, "since", "<=365d"},
		{"since only, >365d", map[string]any{"since": ts(366 * day)}, "since", ">365d"},
		{"both set", map[string]any{"since": ts(40 * day), "until": ts(35 * day)}, "since", "<=7d"},
		{"until before since", map[string]any{"since": ts(day), "until": ts(2 * day)}, "since", "<=1d"},
		{"until only", map[string]any{"until": ts(day)}, "until", argValueOpenStart},
		{"unparseable", map[string]any{"since": "yesterday"}, "since", argValueInvalid},
		{"until only, unparseable", map[string]any{"until": "yesterday"}, "until", argValueInvalid},
	} {
		t.Run(tc.name, func(t *testing.T) {
			exec, m := newUsageTestExecutor(t)
			runUsageQuery(t, exec, `query($since: Time, $until: Time) {
				accountByAddress(address: "G1") {
					operations(since: $since, until: $until) { edges { cursor } }
				}
			}`, tc.vars)

			assert.Equal(t, 1.0, argCount(m, "accountByAddress.operations", tc.argument, tc.want))
			// The range is recorded once, on a single argument.
			assert.Equal(t, 2, testutil.CollectAndCount(m.ArgumentUsageTotal))
		})
	}
}

func TestGraphQLUsageMetrics_OncePerOperation(t *testing.T) {
	exec, m := newUsageTestExecutor(t)
	query := `{
		accountByAddress(address: "G1") {
			transactions(first: 50) { edges { node { hash } } }
			more: transactions(first: 40) { edges { node { hash } } }
		}
	}`

	runUsageQuery(t, exec, query, nil)
	assert.Equal(t, 1.0, fieldCount(m, "accountByAddress.transactions.edges.node.hash"))
	assert.Equal(t, 1.0, argCount(m, "accountByAddress.transactions", "first", "<=50"))

	runUsageQuery(t, exec, query, nil)
	assert.Equal(t, 2.0, fieldCount(m, "accountByAddress.transactions.edges.node.hash"))
	assert.Equal(t, 2.0, argCount(m, "accountByAddress.transactions", "first", "<=50"))
}

func TestGraphQLUsageMetrics_InvalidOperationsNotCounted(t *testing.T) {
	exec, m := newUsageTestExecutor(t)

	for _, query := range []string{
		`{ accountByAddress(address: "G1") { address `,        // parse error
		`{ accountByAddress(address: "G1") { noSuchField } }`, // validation error
	} {
		_, errs := exec.CreateOperationContext(usageTestContext(), &graphql.RawParams{Query: query})
		require.NotEmpty(t, errs)
	}

	// Variable coercion error: $n must be an Int.
	_, errs := exec.CreateOperationContext(usageTestContext(), &graphql.RawParams{
		Query:     `query($n: Int) { blendPools(first: $n) { edges { cursor } } }`,
		Variables: map[string]any{"n": "abc"},
	})
	require.NotEmpty(t, errs)

	assert.Equal(t, 0, testutil.CollectAndCount(m.FieldUsageTotal))
	assert.Equal(t, 0, testutil.CollectAndCount(m.ArgumentUsageTotal))
}
