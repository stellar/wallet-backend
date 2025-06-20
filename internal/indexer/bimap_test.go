package indexer

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_NewBiMap(t *testing.T) {
	t.Run("🟢creates_empty_bimap", func(t *testing.T) {
		bimap := NewBiMap[string, int]()

		require.NotNil(t, bimap)
		assert.NotNil(t, bimap.forward)
		assert.NotNil(t, bimap.backward)
		assert.Empty(t, bimap.forward)
		assert.Empty(t, bimap.backward)
	})

	t.Run("🟢creates_bimap_with_different_types", func(t *testing.T) {
		bimap := NewBiMap[int, bool]()

		require.NotNil(t, bimap)
		assert.NotNil(t, bimap.forward)
		assert.NotNil(t, bimap.backward)
		assert.Empty(t, bimap.forward)
		assert.Empty(t, bimap.backward)
	})
}

func TestBiMap_Add(t *testing.T) {
	t.Run("🟢adds_single_key_value_pair", func(t *testing.T) {
		bimap := NewBiMap[string, int]()
		bimap.Add("key1", 100)

		// Check forward mapping
		forwardSet := bimap.GetForward("key1")
		assert.ElementsMatch(t, []int{100}, forwardSet.ToSlice())

		// Check backward mapping
		backwardSet := bimap.GetBackward(100)
		assert.ElementsMatch(t, []string{"key1"}, backwardSet.ToSlice())
	})

	t.Run("🟢adds_multiple_values_for_same_key", func(t *testing.T) {
		bimap := NewBiMap[string, int]()

		bimap.Add("key1", 100)
		bimap.Add("key1", 200)
		bimap.Add("key1", 300)

		// Check forward mapping
		assert.ElementsMatch(t, []int{100, 200, 300}, bimap.GetForward("key1").ToSlice())

		// Check backward mappings
		assert.ElementsMatch(t, []string{"key1"}, bimap.GetBackward(100).ToSlice())
		assert.ElementsMatch(t, []string{"key1"}, bimap.GetBackward(200).ToSlice())
		assert.ElementsMatch(t, []string{"key1"}, bimap.GetBackward(300).ToSlice())
	})

	t.Run("🟢adds_same_value_for_multiple_keys", func(t *testing.T) {
		bimap := NewBiMap[string, int]()

		bimap.Add("key1", 100)
		bimap.Add("key2", 100)
		bimap.Add("key3", 100)

		// Check forward mappings
		assert.ElementsMatch(t, []int{100}, bimap.GetForward("key1").ToSlice())
		assert.ElementsMatch(t, []int{100}, bimap.GetForward("key2").ToSlice())
		assert.ElementsMatch(t, []int{100}, bimap.GetForward("key3").ToSlice())

		// Check backward mapping
		backwardSet := bimap.GetBackward(100)
		assert.ElementsMatch(t, []string{"key1", "key2", "key3"}, backwardSet.ToSlice())
	})

	t.Run("🟢handles_duplicate_additions", func(t *testing.T) {
		bimap := NewBiMap[string, bool]()

		bimap.Add("key1", true)
		bimap.Add("key1", true) // Duplicate

		// Check forward mapping
		assert.ElementsMatch(t, []bool{true}, bimap.GetForward("key1").ToSlice())

		// Check backward mapping
		assert.ElementsMatch(t, []string{"key1"}, bimap.GetBackward(true).ToSlice())
	})
}

func Test_BiMap_ManyToMany(t *testing.T) {
	t.Run("🟢handles_complex_many_to_many_relationships", func(t *testing.T) {
		bimap := NewBiMap[string, int]()

		// Create a complex mapping:
		// key1 -> [100, 200, 300]
		// key2 -> [200, 300, 400]
		// key3 -> [100, 400, 500]
		// key4 -> [0]
		bimap.Add("key1", 100)
		bimap.Add("key1", 200)
		bimap.Add("key1", 300)
		bimap.Add("key2", 200)
		bimap.Add("key2", 300)
		bimap.Add("key2", 400)
		bimap.Add("key3", 100)
		bimap.Add("key3", 400)
		bimap.Add("key3", 500)
		bimap.Add("key4", 0)

		// Verify forward mappings
		assert.ElementsMatch(t, []int{100, 200, 300}, bimap.GetForward("key1").ToSlice())
		assert.ElementsMatch(t, []int{200, 300, 400}, bimap.GetForward("key2").ToSlice())
		assert.ElementsMatch(t, []int{100, 400, 500}, bimap.GetForward("key3").ToSlice())
		assert.ElementsMatch(t, []int{0}, bimap.GetForward("key4").ToSlice())

		// Verify backward mappings
		assert.ElementsMatch(t, []string{"key1", "key3"}, bimap.GetBackward(100).ToSlice())
		assert.ElementsMatch(t, []string{"key1", "key2"}, bimap.GetBackward(200).ToSlice())
		assert.ElementsMatch(t, []string{"key1", "key2"}, bimap.GetBackward(300).ToSlice())
		assert.ElementsMatch(t, []string{"key2", "key3"}, bimap.GetBackward(400).ToSlice())
		assert.ElementsMatch(t, []string{"key4"}, bimap.GetBackward(0).ToSlice())
	})
}
