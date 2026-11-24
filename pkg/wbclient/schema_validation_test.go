package wbclient

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/vektah/gqlparser/v2"
	"github.com/vektah/gqlparser/v2/ast"

	"github.com/stellar/wallet-backend/pkg/wbclient/types"
)

// TestClientQueriesValidateAgainstSchema validates every query this client builds against the
// server's SDL, catching drift between pkg/wbclient and internal/serve/graphql/schema (field
// renames, fragment/type mismatches, SameResponseShape violations) without a running server.
func TestClientQueriesValidateAgainstSchema(t *testing.T) {
	schema := loadServerSchema(t)

	for name, q := range Queries() {
		t.Run(name, func(t *testing.T) {
			_, verr := gqlparser.LoadQueryWithRules(schema, q, nil)
			require.Empty(t, verr, "query %s failed validation", name)
		})
	}
}

// TestSimulatedStateChangeTypesMatchSchema fails when the server gains a
// simulated state change type this client does not select or decode.
func TestSimulatedStateChangeTypesMatchSchema(t *testing.T) {
	schema := loadServerSchema(t)

	implementations := schema.PossibleTypes[schema.Types["BaseSimulatedStateChange"].Name]
	require.NotEmpty(t, implementations)
	for _, def := range implementations {
		t.Run(def.Name, func(t *testing.T) {
			inlineFragmentBlock(t, simulatedStateChangeFragments, def.Name)
			_, err := types.UnmarshalSimulatedStateChangeNode([]byte(`{"__typename":"` + def.Name + `"}`))
			require.NoError(t, err)
		})
	}
}

func loadServerSchema(t *testing.T) *ast.Schema {
	t.Helper()
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
	return schema
}
