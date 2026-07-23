package serve

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// buildRecursiveAccountsQuery builds a query that nests repetitions levels deep by chaining
// Transaction.accounts -> Account.transactions -> edges -> node (a Transaction again), which lets
// an otherwise-shallow schema be nested arbitrarily deep. first: 1 on each transactions connection
// keeps complexity from also blowing up, so the test isolates the depth limit specifically.
func buildRecursiveAccountsQuery(repetitions int, txHash string) string {
	inner := "hash"
	for i := 0; i < repetitions; i++ {
		inner = fmt.Sprintf(`accounts { transactions(first: 1) { edges { node { %s } } } }`, inner)
	}
	return fmt.Sprintf(`query { transactionByHash(hash: "%s") { %s } }`, txHash, inner)
}

func TestGraphQLDepthLimitRejectsDeeplyNestedQueries(t *testing.T) {
	// A very high complexity limit isolates the depth check: DepthLimit is registered before
	// FixedComplexityLimit in handler(), so a query this deep is rejected on depth before
	// complexity is even evaluated.
	h := newGraphQLTestHandler(t, 1_000_000_000)

	t.Run("shallow query passes", func(t *testing.T) {
		query := buildRecursiveAccountsQuery(1, complexityTestTxHash)
		resp := performGraphQLRequest(t, h, query)
		require.Empty(t, resp.Errors)
	})

	t.Run("deeply nested query is rejected with QUERY_TOO_DEEP, not masked", func(t *testing.T) {
		// repetitions=5 adds 4 levels each (accounts, transactions, edges, node) on top of
		// transactionByHash(1) and the leaf hash(1): 1 + 5*4 + 1 = 22, over MaxQueryDepth(15).
		query := buildRecursiveAccountsQuery(5, complexityTestTxHash)
		resp := performGraphQLRequest(t, h, query)

		require.Len(t, resp.Errors, 1)
		assert.Equal(t, "QUERY_TOO_DEEP", resp.Errors[0].Extensions["code"])
		assert.Contains(t, resp.Errors[0].Message, "exceeds the limit of 15")
		assert.Equal(t, "null", string(resp.Data))
	})
}
