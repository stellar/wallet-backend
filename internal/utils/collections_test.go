package utils

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBuildMap_Success(t *testing.T) {
	type item struct {
		id   string
		name string
	}
	items := []item{
		{id: "a", name: "Alice"},
		{id: "b", name: "Bob"},
	}
	result, err := BuildMap(items, func(i item) string { return i.id })
	require.NoError(t, err)
	assert.Len(t, result, 2)
	assert.Equal(t, "Alice", result["a"].name)
	assert.Equal(t, "Bob", result["b"].name)
}

func TestBuildMap_EmptySlice(t *testing.T) {
	result, err := BuildMap([]string{}, func(s string) string { return s })
	require.NoError(t, err)
	assert.Empty(t, result)
}

func TestBuildMap_DuplicateKey(t *testing.T) {
	items := []string{"x", "y", "x"}
	_, err := BuildMap(items, func(s string) string { return s })
	require.Error(t, err)
	assert.Contains(t, err.Error(), `duplicate key "x"`)
}
