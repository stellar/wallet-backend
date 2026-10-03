package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vektah/gqlparser/v2"
	"github.com/vektah/gqlparser/v2/ast"
)

const testSchema = `
directive @goField(forceResolver: Boolean) on FIELD_DEFINITION

"""Root queries."""
type Query {
    """Look up a thing."""
    thing(id: ID!, limit: Int = 10): Thing
}

"""Something with a name."""
interface Named { name: String! }

"""A thing.
Spans two lines."""
type Thing implements Named {
    name: String!
    """Tags | pipes."""
    tags(first: Int): [Tag!]! @goField(forceResolver: true)
    old: String @deprecated(reason: "Use name.")
}

enum Tag {
    """First tag."""
    A
    B @deprecated(reason: "Gone.")
}

input ThingFilter { tag: Tag = A }
`

func TestRender(t *testing.T) {
	schema, gqlErr := gqlparser.LoadSchema(&ast.Source{Name: "test.graphqls", Input: testSchema})
	require.NoError(t, gqlErr)

	out := render(schema)

	for _, want := range []string{
		"# GraphQL schema reference\n",
		"## Queries\n\nRoot queries.\n\n### thing\n\nLook up a thing.\n\nReturns: [Thing](#thing)\n",
		"| `id` | `ID`! |  |  |\n",
		"| `limit` | `Int` | `10` |  |\n",
		"### Thing\n\nA thing. Spans two lines.\n\nImplements: [Named](#named)\n",
		"| `tags` | \\[[Tag](#tag)!\\]! | Tags \\| pipes. |\n",
		"| `old` | `String` | Deprecated: Use name. |\n",
		"Arguments of `tags`:\n",
		"Implemented by: [Thing](#thing)\n",
		"| `B` | Deprecated: Gone. |\n",
		"| `tag` | [Tag](#tag) | Default: `A`. |\n",
	} {
		assert.Contains(t, out, want)
	}

	assert.NotContains(t, out, "__schema")
	assert.NotContains(t, out, "goField")
	assert.NotContains(t, out, "### Query\n")
	assert.NotContains(t, out, "## Directives")
}
