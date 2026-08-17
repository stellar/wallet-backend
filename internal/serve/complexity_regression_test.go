package serve

import (
	"context"
	"testing"

	"github.com/99designs/gqlgen/complexity"
	"github.com/99designs/gqlgen/graphql"
	"github.com/stretchr/testify/require"
	"github.com/vektah/gqlparser/v2"

	"github.com/stellar/wallet-backend/cmd/utils"
	generated "github.com/stellar/wallet-backend/internal/serve/graphql/generated"
	resolvers "github.com/stellar/wallet-backend/internal/serve/graphql/resolvers"
	"github.com/stellar/wallet-backend/pkg/wbclient"
)

// The queries priced below are the documents pkg/wbclient itself builds (wbclient.Queries()),
// not copies of them, so any fragment or field added to the SDK is priced here the moment it
// lands: a change that pushes a shipped query past the deployment default fails this test
// instead of failing a client at runtime.

// maxRequestedPageSize is the largest page the account- and transaction-scoped resolvers accept
// (maxAccountPageLimit and maxBalancePageLimit in internal/serve/graphql/resolvers, both 100).
// Every paginated query the SDK builds takes its page size as a $first variable, so binding it
// here prices each query at the worst case a client can ask for.
const maxRequestedPageSize = 100

// maxRequestedBlendPoolPageSize is the largest blendPools page the resolver accepts
// (maxBlendPoolPageLimit in internal/serve/graphql/resolvers). It is half maxRequestedPageSize
// because every pool node fans out ×30 reserves, so pricing blendPools at 100 would budget for a
// page the resolver rejects outright.
const maxRequestedBlendPoolPageSize = 50

// requestedPageSizes maps each SDK query to the page size it is priced at: the largest page its
// own resolver will serve. Queries absent from the map are priced at maxRequestedPageSize.
var requestedPageSizes = map[string]int{
	"BlendPools": maxRequestedBlendPoolPageSize,
}

// requestedPageSize reports the page size queryName is priced at.
func requestedPageSize(queryName string) int {
	if size, ok := requestedPageSizes[queryName]; ok {
		return size
	}
	return maxRequestedPageSize
}

// substantialQueryFloors pins a lower bound on the query shapes the complexity limit is sized
// around. Without a floor the upper-bound assertion would also pass on a near-zero measurement,
// which is what a silently dropped selection set looks like. Queries not listed here only have
// to be non-zero.
var substantialQueryFloors = map[string]int{
	"AccountBalances":                           1000,
	"AccountStateChanges":                       1000,
	"AccountTransactionsWithOpsAndStateChanges": 1000,
	"BlendPools":                                1000,
	"AccountBlendPositions":                     1000,
}

// newComplexityCalculationSchema builds an ExecutableSchema wired with the production complexity
// config, with no DB or server behind it: complexity.Calculate only walks the query AST against the
// registered Complexity funcs and never invokes a resolver, so a zero-value Resolver is sufficient.
func newComplexityCalculationSchema(t *testing.T) graphql.ExecutableSchema {
	t.Helper()

	cfg := generated.Config{Resolvers: &resolvers.Resolver{}}
	addComplexityCalculation(&cfg)
	return generated.NewExecutableSchema(cfg)
}

// defaultComplexityLimit reports the GRAPHQL_COMPLEXITY_LIMIT a deployment runs with when it does
// not override the flag, read from the option itself so the budget below tracks the shipped
// default rather than a second copy of the number.
func defaultComplexityLimit(t *testing.T) int {
	t.Helper()

	var sink int
	limit, ok := utils.GraphQLComplexityLimitOption(&sink).FlagDefault.(int)
	require.True(t, ok, "graphql-complexity-limit FlagDefault should be an int")
	return limit
}

// TestSDKQueriesFitDefaultComplexityLimit locks in that every query pkg/wbclient sends is
// servable by a wallet-backend running the built-in GRAPHQL_COMPLEXITY_LIMIT, at the largest page
// a resolver will accept.
//
// Measured complexities for the queries that dominate the budget (gqlgen sums mutually exclusive
// inline fragments, so the exhaustive state-change and balance selections over-count relative to
// what any one row resolves): BlendPools=26,550 at first:50 — 50 pages of a 531-cost connection
// selection, itself the 523-cost pool node plus its cursor, the edges wrapper, and the 5-cost
// pageInfo block. At first:100: AccountTransactionsWithOpsAndStateChanges=10,101,
// Account/Transaction/OperationStateChanges=8,401, AccountBlendPositions=7,595,
// AccountBalances=3,901.
func TestSDKQueriesFitDefaultComplexityLimit(t *testing.T) {
	es := newComplexityCalculationSchema(t)
	limit := defaultComplexityLimit(t)

	for name, query := range wbclient.Queries() {
		t.Run(name, func(t *testing.T) {
			doc, gerr := gqlparser.LoadQueryWithRules(es.Schema(), query, nil)
			require.Empty(t, gerr)

			pageSize := requestedPageSize(name)
			c := complexity.Calculate(context.Background(), es, doc.Operations[0], map[string]any{"first": pageSize})
			t.Logf("%s: computed complexity at first:%d = %d", name, pageSize, c)

			require.Positive(t, c, "a zero complexity means LoadQueryWithRules silently dropped the selection set")
			if floor, ok := substantialQueryFloors[name]; ok {
				require.Greater(t, c, floor, "query should be substantial enough to be a meaningful worst case")
			}
			require.LessOrEqual(t, c, limit, "every query the SDK ships must be servable under the default complexity limit (%d)", limit)
		})
	}
}

// TestAccountTransactionEdgeOperationsAndStateChangesHaveNoComplexityMultiplier guards the
// invariant documented in serve.go: AccountTransactionEdge.operations/stateChanges must never get a
// complexity multiplier, or freighter's full-detail account-history query breaks the complexity limit.
func TestAccountTransactionEdgeOperationsAndStateChangesHaveNoComplexityMultiplier(t *testing.T) {
	es := newComplexityCalculationSchema(t)
	ctx := context.Background()

	_, ok := es.Complexity(ctx, "AccountTransactionEdge", "operations", 100, map[string]any{})
	require.False(t, ok, "AccountTransactionEdge.operations must have NO complexity multiplier registered")

	_, ok = es.Complexity(ctx, "AccountTransactionEdge", "stateChanges", 100, map[string]any{})
	require.False(t, ok, "AccountTransactionEdge.stateChanges must have NO complexity multiplier registered")

	// Positive control: Transaction.operations (a different type's field of the same name) SHOULD
	// have a multiplier. If this fails, the two assertions above are checking the wrong thing.
	_, ok = es.Complexity(ctx, "Transaction", "operations", 100, map[string]any{})
	require.True(t, ok, "Transaction.operations should have a complexity multiplier; if not, this guard test cannot distinguish 'no multiplier' from 'field does not exist'")

	// Break-detection: prove the assertions above actually discriminate. Register the naive x50
	// multiplier a well-meaning future edit might add (matching the pattern every other paginated
	// field in addComplexityCalculation uses) and confirm `ok` flips to true, and that the resulting
	// schema pushes the SDK's full-detail account-history query over the default limit.
	regressedCfg := generated.Config{Resolvers: &resolvers.Resolver{}}
	addComplexityCalculation(&regressedCfg)
	regressedCfg.Complexity.AccountTransactionEdge.Operations = func(childComplexity int) int { return childComplexity * 50 }
	regressedCfg.Complexity.AccountTransactionEdge.StateChanges = func(childComplexity int) int { return childComplexity * 50 }
	regressedES := generated.NewExecutableSchema(regressedCfg)

	_, ok = regressedES.Complexity(ctx, "AccountTransactionEdge", "operations", 100, map[string]any{})
	require.True(t, ok, "sanity check on the break-detection config: the multiplier should be registered")

	fullDetailQuery := wbclient.Queries()["AccountTransactionsWithOpsAndStateChanges"]
	require.NotEmpty(t, fullDetailQuery)
	doc, gerr := gqlparser.LoadQueryWithRules(regressedES.Schema(), fullDetailQuery, nil)
	require.Empty(t, gerr)
	regressed := complexity.Calculate(ctx, regressedES, doc.Operations[0], map[string]any{"first": maxRequestedPageSize})
	t.Logf("full-detail account-history query complexity with a hypothetical AccountTransactionEdge multiplier = %d", regressed)
	require.Greater(t, regressed, defaultComplexityLimit(t), "this confirms the guard above is load-bearing: without it, the full-detail account-history query blows the complexity limit")
}
