package serve

import (
	"context"
	"testing"

	"github.com/99designs/gqlgen/complexity"
	"github.com/99designs/gqlgen/graphql"
	"github.com/stretchr/testify/require"
	"github.com/vektah/gqlparser/v2"

	generated "github.com/stellar/wallet-backend/internal/serve/graphql/generated"
	resolvers "github.com/stellar/wallet-backend/internal/serve/graphql/resolvers"
)

// The queries below mirror the shapes pkg/wbclient/queries.go builds for freighter's
// account-detail views (buildAccountBalancesQuery / buildAccountTransactionsWithOpsAndStateChangesQuery):
// max page size (first: 100, the resolver-enforced cap, see graphqlutils.DefaultPageLimit and the
// resolvers' page-size clamping) with every field of every concrete implementer selected. A single
// query combining both at first:100 is not representative: freighter issues them as separate
// requests, and combined they total ~9500, over budget on their own. Each is tested independently
// as its own worst case.

const freighterAccountBalancesQuery = `
	query {
		accountByAddress(address: "GAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAWHF") {
			balances(first: 100) {
				edges {
					node {
						balance
						tokenId
						tokenType
						... on NativeBalance {
							minimumBalance
							buyingLiabilities
							sellingLiabilities
							numSubentries
							lastModifiedLedger
						}
						... on TrustlineBalance {
							code
							issuer
							type
							limit
							buyingLiabilities
							sellingLiabilities
							lastModifiedLedger
							isAuthorized
							isAuthorizedToMaintainLiabilities
						}
						... on SACBalance {
							code
							issuer
							decimals
							isAuthorized
							isClawbackEnabled
						}
						... on SEP41Balance {
							name
							symbol
							decimals
							lastModifiedLedger
						}
						... on LiquidityPoolBalance {
							liquidityPoolId
							reserves { asset amount }
							lastModifiedLedger
						}
					}
					cursor
				}
				pageInfo { startCursor endCursor hasNextPage hasPreviousPage }
			}
		}
	}
`

// freighterAccountTransactionsQuery is the "full-detail account-history query" referenced in the
// comments above addComplexityCalculation in serve.go: it selects a full set of Transaction scalar
// fields, plus the AccountTransactionEdge-only operations/stateChanges fields (plain lists, no
// first/last args) with a broad field selection across every BaseStateChange implementer. This is
// the query the AccountTransactionEdge no-multiplier design in addComplexityCalculation exists to protect.
const freighterAccountTransactionsQuery = `
	query {
		accountByAddress(address: "GAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAWHF") {
			transactions(first: 100) {
				edges {
					node {
						hash
						feeCharged
						resultCode
						ledgerNumber
						ledgerCreatedAt
						isFeeBump
						ingestedAt
					}
					cursor
					operations {
						id
						operationType
						operationXdr
						resultCode
						successful
						ledgerNumber
						ledgerCreatedAt
						ingestedAt
					}
					stateChanges {
						type
						reason
						ingestedAt
						ledgerCreatedAt
						ledgerNumber
						... on StandardBalanceChange {
							standardBalanceTokenId: tokenId
							amount
							toMuxedId
						}
						... on AccountChange {
							funderAddress
							deployerAddress
							destinationAddress
						}
						... on SignerChange {
							signerAddress
							signerWeights
						}
						... on SignerThresholdsChange {
							thresholds
						}
						... on MetadataChange {
							metadataKeyValue: keyValue
						}
						... on FlagsChange {
							flags
						}
						... on TrustlineChange {
							trustlineTokenId: tokenId
							limit
							trustlineLiquidityPoolId: liquidityPoolId
						}
						... on ReservesChange {
							sponsoredAddress
							sponsorAddress
							sponsoredData
							sponsoredTrustline
							claimableBalanceId
							liquidityPoolId
						}
						... on BalanceAuthorizationChange {
							balanceAuthTokenId: tokenId
							balanceAuthLiquidityPoolId: liquidityPoolId
							flags
						}
					}
				}
				pageInfo { startCursor endCursor hasNextPage hasPreviousPage }
			}
		}
	}
`

// newComplexityCalculationSchema builds an ExecutableSchema wired with the production complexity
// config, with no DB or server behind it: complexity.Calculate only walks the query AST against the
// registered Complexity funcs and never invokes a resolver, so a zero-value Resolver is sufficient.
func newComplexityCalculationSchema(t *testing.T) graphql.ExecutableSchema {
	t.Helper()

	cfg := generated.Config{Resolvers: &resolvers.Resolver{}}
	addComplexityCalculation(&cfg)
	return generated.NewExecutableSchema(cfg)
}

// TestFreighterFullDetailQueriesStayUnderComplexityLimit locks in that freighter's two heaviest
// account-detail queries stay under the prod GRAPHQL_COMPLEXITY_LIMIT=6000. This only holds because
// AccountTransactionEdge.operations/stateChanges have no complexity multiplier registered (see
// TestAccountTransactionEdgeOperationsAndStateChangesHaveNoComplexityMultiplier below); if that ever
// changes, this test starts failing too.
func TestFreighterFullDetailQueriesStayUnderComplexityLimit(t *testing.T) {
	es := newComplexityCalculationSchema(t)

	testCases := []struct {
		name  string
		query string
		// computed complexity on record at time of writing, for both cases well under the
		// GRAPHQL_COMPLEXITY_LIMIT=6000 production limit: balances=3901, transactions=5601.
		floor int
	}{
		{name: "account balances, first:100, every Balance implementer field", query: freighterAccountBalancesQuery, floor: 1000},
		{name: "account transactions, first:100, embedded operations+stateChanges per edge", query: freighterAccountTransactionsQuery, floor: 1000},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			doc, gerr := gqlparser.LoadQueryWithRules(es.Schema(), tc.query, nil)
			require.Empty(t, gerr)

			c := complexity.Calculate(context.Background(), es, doc.Operations[0], nil)
			t.Logf("%s: computed complexity = %d", tc.name, c)

			require.Greater(t, c, tc.floor, "query should be substantial enough to be a meaningful worst case; a near-zero value likely means LoadQuery silently dropped selections")
			require.Less(t, c, 6000, "freighter's full-detail query must stay under the prod complexity limit (GRAPHQL_COMPLEXITY_LIMIT=6000)")
		})
	}
}

// TestAccountTransactionEdgeOperationsAndStateChangesHaveNoComplexityMultiplier guards the
// invariant documented in serve.go: AccountTransactionEdge.operations/stateChanges must never get a
// complexity multiplier, or freighter's full-detail account-history query breaks the prod limit.
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
	// schema pushes freighter's transactions query over the prod limit.
	regressedCfg := generated.Config{Resolvers: &resolvers.Resolver{}}
	addComplexityCalculation(&regressedCfg)
	regressedCfg.Complexity.AccountTransactionEdge.Operations = func(childComplexity int) int { return childComplexity * 50 }
	regressedCfg.Complexity.AccountTransactionEdge.StateChanges = func(childComplexity int) int { return childComplexity * 50 }
	regressedES := generated.NewExecutableSchema(regressedCfg)

	_, ok = regressedES.Complexity(ctx, "AccountTransactionEdge", "operations", 100, map[string]any{})
	require.True(t, ok, "sanity check on the break-detection config: the multiplier should be registered")

	doc, gerr := gqlparser.LoadQueryWithRules(regressedES.Schema(), freighterAccountTransactionsQuery, nil)
	require.Empty(t, gerr)
	regressed := complexity.Calculate(ctx, regressedES, doc.Operations[0], nil)
	t.Logf("freighter transactions query complexity with a hypothetical AccountTransactionEdge multiplier = %d", regressed)
	require.Greater(t, regressed, 6000, "this confirms the guard above is load-bearing: without it, the freighter transactions query blows the prod complexity limit")
}
