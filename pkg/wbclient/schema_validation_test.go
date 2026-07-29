package wbclient

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/vektah/gqlparser/v2"
	"github.com/vektah/gqlparser/v2/ast"
)

// TestClientQueriesValidateAgainstSchema validates every query this client builds against the
// server's SDL, catching drift between pkg/wbclient and internal/serve/graphql/schema (field
// renames, fragment/type mismatches, SameResponseShape violations) without a running server.
func TestClientQueriesValidateAgainstSchema(t *testing.T) {
	schemaDir := filepath.Join("..", "..", "internal", "serve", "graphql", "schema")
	entries, err := os.ReadDir(schemaDir)
	require.NoError(t, err)

	var sources []*ast.Source
	for _, e := range entries {
		if filepath.Ext(e.Name()) != ".graphqls" {
			continue
		}
		b, rerr := os.ReadFile(filepath.Join(schemaDir, e.Name()))
		require.NoError(t, rerr)
		sources = append(sources, &ast.Source{Name: e.Name(), Input: string(b)})
	}

	schema, gerr := gqlparser.LoadSchema(sources...)
	require.Nil(t, gerr, "schema load: %v", gerr)

	// Every query builder in queries.go, invoked with its default field set (nil = defaults),
	// exactly as the client methods do.
	queries := map[string]string{
		"TransactionByHash":                         buildTransactionByHashQuery(nil),
		"AccountByAddress":                          buildAccountByAddressQuery(nil),
		"OperationByID":                             buildOperationByIDQuery(nil),
		"AccountTransactions":                       buildAccountTransactionsQuery(nil),
		"AccountOperations":                         buildAccountOperationsQuery(nil),
		"TransactionOperations":                     buildTransactionOperationsQuery(nil),
		"AccountBalances":                           buildAccountBalancesQuery(),
		"AccountStateChanges":                       buildAccountStateChangesQuery(),
		"TransactionStateChanges":                   buildTransactionStateChangesQuery(),
		"OperationStateChanges":                     buildOperationStateChangesQuery(),
		"AccountTransactionsWithOpsAndStateChanges": buildAccountTransactionsWithOpsAndStateChangesQuery(),
	}
	for name, q := range queries {
		t.Run(name, func(t *testing.T) {
			_, verr := gqlparser.LoadQueryWithRules(schema, q, nil)
			require.Empty(t, verr, "query %s failed validation", name)
		})
	}
}
