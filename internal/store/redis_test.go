// Package store provides data access layer for Redis operations.
package store

import (
	"context"
	"strconv"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// setupTestRedis creates a miniredis instance and returns a RedisStore connected to it
func setupTestRedis(t *testing.T) (*RedisStore, *miniredis.Miniredis) {
	mr := miniredis.RunT(t)

	port, err := strconv.Atoi(mr.Port())
	require.NoError(t, err)

	store := NewRedisStore(mr.Host(), port, "")

	return store, mr
}

func TestRedisStore_SAdd(t *testing.T) {
	ctx := context.Background()

	t.Run("successfully adds single member to set", func(t *testing.T) {
		store, mr := setupTestRedis(t)
		defer mr.Close()

		err := store.SAdd(ctx, "test:set", "member1")
		require.NoError(t, err)

		// Verify member was added
		members, err := mr.Members("test:set")
		require.NoError(t, err)
		assert.Equal(t, []string{"member1"}, members)
	})

	t.Run("successfully adds multiple members to set", func(t *testing.T) {
		store, mr := setupTestRedis(t)
		defer mr.Close()

		err := store.SAdd(ctx, "test:set", "member1", "member2", "member3")
		require.NoError(t, err)

		// Verify members were added
		members, err := mr.Members("test:set")
		require.NoError(t, err)
		assert.ElementsMatch(t, []string{"member1", "member2", "member3"}, members)
	})

	t.Run("handles error when Redis is unavailable", func(t *testing.T) {
		store, mr := setupTestRedis(t)
		mr.Close() // Close to simulate connection error

		err := store.SAdd(ctx, "test:set", "member1")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "adding members to set test:set")
	})
}

func TestRedisStore_SMembers(t *testing.T) {
	ctx := context.Background()

	t.Run("successfully retrieves all set members", func(t *testing.T) {
		store, mr := setupTestRedis(t)
		defer mr.Close()

		// Add members directly to miniredis
		_, err := mr.SetAdd("test:set", "member1", "member2", "member3")
		require.NoError(t, err)

		members, err := store.SMembers(ctx, "test:set")
		require.NoError(t, err)
		assert.ElementsMatch(t, []string{"member1", "member2", "member3"}, members)
	})

	t.Run("returns empty slice for non-existent set", func(t *testing.T) {
		store, mr := setupTestRedis(t)
		defer mr.Close()

		members, err := store.SMembers(ctx, "non:existent:set")
		require.NoError(t, err)
		assert.Empty(t, members)
	})

	t.Run("handles error when Redis is unavailable", func(t *testing.T) {
		store, mr := setupTestRedis(t)
		mr.Close() // Close to simulate connection error

		members, err := store.SMembers(ctx, "test:set")
		assert.Error(t, err)
		assert.Nil(t, members)
		assert.Contains(t, err.Error(), "getting members of set test:set")
	})
}

func TestRedisStore_Get(t *testing.T) {
	ctx := context.Background()

	t.Run("successfully retrieves existing key", func(t *testing.T) {
		store, mr := setupTestRedis(t)
		defer mr.Close()

		// Set value directly in miniredis
		err := mr.Set("test:key", "test:value")
		require.NoError(t, err)

		value, err := store.Get(ctx, "test:key")
		require.NoError(t, err)
		assert.Equal(t, "test:value", value)
	})

	t.Run("returns empty string for non-existent key", func(t *testing.T) {
		store, mr := setupTestRedis(t)
		defer mr.Close()

		value, err := store.Get(ctx, "non:existent:key")
		require.NoError(t, err)
		assert.Equal(t, "", value)
	})

	t.Run("handles error when Redis is unavailable", func(t *testing.T) {
		store, mr := setupTestRedis(t)
		mr.Close() // Close to simulate connection error

		value, err := store.Get(ctx, "test:key")
		assert.Error(t, err)
		assert.Equal(t, "", value)
		assert.Contains(t, err.Error(), "getting key test:key")
	})
}

func TestRedisStore_HGet(t *testing.T) {
	ctx := context.Background()

	t.Run("successfully retrieves existing hash field", func(t *testing.T) {
		store, mr := setupTestRedis(t)
		defer mr.Close()

		// Set hash field directly in miniredis
		mr.HSet("test:hash", "field1", "value1")

		value, err := store.HGet(ctx, "test:hash", "field1")
		require.NoError(t, err)
		assert.Equal(t, "value1", value)
	})

	t.Run("returns empty string for non-existent field", func(t *testing.T) {
		store, mr := setupTestRedis(t)
		defer mr.Close()

		value, err := store.HGet(ctx, "test:hash", "nonexistent")
		require.NoError(t, err)
		assert.Equal(t, "", value)
	})

	t.Run("returns empty string for non-existent key", func(t *testing.T) {
		store, mr := setupTestRedis(t)
		defer mr.Close()

		value, err := store.HGet(ctx, "nonexistent:hash", "field1")
		require.NoError(t, err)
		assert.Equal(t, "", value)
	})

	t.Run("handles error when Redis is unavailable", func(t *testing.T) {
		store, mr := setupTestRedis(t)
		mr.Close() // Close to simulate connection error

		value, err := store.HGet(ctx, "test:hash", "field1")
		assert.Error(t, err)
		assert.Equal(t, "", value)
		assert.Contains(t, err.Error(), "getting key test:hash")
	})
}

func TestRedisStore_HMGet(t *testing.T) {
	ctx := context.Background()

	t.Run("successfully retrieves multiple hash fields", func(t *testing.T) {
		store, mr := setupTestRedis(t)
		defer mr.Close()

		// Set hash fields directly in miniredis
		mr.HSet("test:hash", "field1", "value1")
		mr.HSet("test:hash", "field2", "value2")
		mr.HSet("test:hash", "field3", "value3")

		result, err := store.HMGet(ctx, "test:hash", "field1", "field2", "field3")
		require.NoError(t, err)
		assert.Len(t, result, 3)
		assert.Equal(t, "value1", result["field1"])
		assert.Equal(t, "value2", result["field2"])
		assert.Equal(t, "value3", result["field3"])
	})

	t.Run("skips non-existent fields", func(t *testing.T) {
		store, mr := setupTestRedis(t)
		defer mr.Close()

		// Set only some fields
		mr.HSet("test:hash", "field1", "value1")
		mr.HSet("test:hash", "field3", "value3")

		result, err := store.HMGet(ctx, "test:hash", "field1", "field2", "field3")
		require.NoError(t, err)
		assert.Len(t, result, 2)
		assert.Equal(t, "value1", result["field1"])
		assert.Equal(t, "value3", result["field3"])
		_, exists := result["field2"]
		assert.False(t, exists, "field2 should not exist in result")
	})

	t.Run("returns empty map for non-existent key", func(t *testing.T) {
		store, mr := setupTestRedis(t)
		defer mr.Close()

		result, err := store.HMGet(ctx, "nonexistent:hash", "field1", "field2")
		require.NoError(t, err)
		assert.Empty(t, result)
	})

	t.Run("returns empty map when all fields are non-existent", func(t *testing.T) {
		store, mr := setupTestRedis(t)
		defer mr.Close()

		// Create a hash with different fields
		mr.HSet("test:hash", "other_field", "other_value")

		result, err := store.HMGet(ctx, "test:hash", "field1", "field2")
		require.NoError(t, err)
		assert.Empty(t, result)
	})

	t.Run("handles single field request", func(t *testing.T) {
		store, mr := setupTestRedis(t)
		defer mr.Close()

		mr.HSet("test:hash", "field1", "value1")

		result, err := store.HMGet(ctx, "test:hash", "field1")
		require.NoError(t, err)
		assert.Len(t, result, 1)
		assert.Equal(t, "value1", result["field1"])
	})

	t.Run("handles error when Redis is unavailable", func(t *testing.T) {
		store, mr := setupTestRedis(t)
		mr.Close() // Close to simulate connection error

		result, err := store.HMGet(ctx, "test:hash", "field1")
		assert.Error(t, err)
		assert.Nil(t, result)
		assert.Contains(t, err.Error(), "getting key test:hash")
	})
}

func TestRedisStore_HSet(t *testing.T) {
	ctx := context.Background()

	t.Run("successfully stores hash field", func(t *testing.T) {
		store, mr := setupTestRedis(t)
		defer mr.Close()

		err := store.HSet(ctx, "test:hash", "field1", "value1")
		require.NoError(t, err)

		// Verify value was stored
		value := mr.HGet("test:hash", "field1")
		assert.Equal(t, "value1", value)
	})

	t.Run("successfully overwrites existing field", func(t *testing.T) {
		store, mr := setupTestRedis(t)
		defer mr.Close()

		// Set initial value
		mr.HSet("test:hash", "field1", "old_value")

		// Overwrite with new value
		err := store.HSet(ctx, "test:hash", "field1", "new_value")
		require.NoError(t, err)

		// Verify value was overwritten
		value := mr.HGet("test:hash", "field1")
		assert.Equal(t, "new_value", value)
	})

	t.Run("handles error when Redis is unavailable", func(t *testing.T) {
		store, mr := setupTestRedis(t)
		mr.Close() // Close to simulate connection error

		err := store.HSet(ctx, "test:hash", "field1", "value1")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "setting key test:hash")
	})
}

func TestRedisStore_Set(t *testing.T) {
	ctx := context.Background()

	t.Run("successfully stores key-value pair", func(t *testing.T) {
		store, mr := setupTestRedis(t)
		defer mr.Close()

		err := store.Set(ctx, "test:key", "test:value")
		require.NoError(t, err)

		// Verify value was stored
		value, err := mr.Get("test:key")
		require.NoError(t, err)
		assert.Equal(t, "test:value", value)
	})

	t.Run("successfully overwrites existing value", func(t *testing.T) {
		store, mr := setupTestRedis(t)
		defer mr.Close()

		// Set initial value
		err := mr.Set("test:key", "old:value")
		require.NoError(t, err)

		// Overwrite with new value
		err = store.Set(ctx, "test:key", "new:value")
		require.NoError(t, err)

		// Verify value was overwritten
		value, err := mr.Get("test:key")
		require.NoError(t, err)
		assert.Equal(t, "new:value", value)
	})

	t.Run("handles error when Redis is unavailable", func(t *testing.T) {
		store, mr := setupTestRedis(t)
		mr.Close() // Close to simulate connection error

		err := store.Set(ctx, "test:key", "test:value")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "setting key test:key")
	})
}

func TestRedisStore_ExecutePipeline(t *testing.T) {
	ctx := context.Background()

	t.Run("successfully executes empty operations", func(t *testing.T) {
		store, mr := setupTestRedis(t)
		defer mr.Close()

		err := store.ExecutePipeline(ctx, []RedisPipelineOperation{})
		assert.NoError(t, err)
	})

	t.Run("successfully executes SetOpAdd operations", func(t *testing.T) {
		store, mr := setupTestRedis(t)
		defer mr.Close()

		operations := []RedisPipelineOperation{
			{
				Op:      SetOpAdd,
				Key:     "set1",
				Members: []string{"member1", "member2"},
			},
			{
				Op:      SetOpAdd,
				Key:     "set2",
				Members: []string{"member3", "member4"},
			},
		}

		err := store.ExecutePipeline(ctx, operations)
		require.NoError(t, err)

		// Verify members were added to both sets
		members1, err := mr.Members("set1")
		require.NoError(t, err)
		assert.ElementsMatch(t, []string{"member1", "member2"}, members1)

		members2, err := mr.Members("set2")
		require.NoError(t, err)
		assert.ElementsMatch(t, []string{"member3", "member4"}, members2)
	})

	t.Run("successfully executes SetOpRemove operations", func(t *testing.T) {
		store, mr := setupTestRedis(t)
		defer mr.Close()

		// Setup initial set members
		_, err := mr.SetAdd("set1", "member1", "member2", "member3")
		require.NoError(t, err)

		operations := []RedisPipelineOperation{
			{
				Op:      SetOpRemove,
				Key:     "set1",
				Members: []string{"member2"},
			},
		}

		err = store.ExecutePipeline(ctx, operations)
		require.NoError(t, err)

		// Verify member was removed
		members, err := mr.Members("set1")
		require.NoError(t, err)
		assert.ElementsMatch(t, []string{"member1", "member3"}, members)
	})

	t.Run("successfully executes SET operations", func(t *testing.T) {
		store, mr := setupTestRedis(t)
		defer mr.Close()

		operations := []RedisPipelineOperation{
			{
				Op:    OpSet,
				Key:   "key1",
				Value: "value1",
			},
			{
				Op:    OpSet,
				Key:   "key2",
				Value: "value2",
			},
		}

		err := store.ExecutePipeline(ctx, operations)
		require.NoError(t, err)

		// Verify values were set
		value1, err := mr.Get("key1")
		require.NoError(t, err)
		assert.Equal(t, "value1", value1)

		value2, err := mr.Get("key2")
		require.NoError(t, err)
		assert.Equal(t, "value2", value2)
	})

	t.Run("successfully executes mixed operations", func(t *testing.T) {
		store, mr := setupTestRedis(t)
		defer mr.Close()

		// Setup initial data
		_, err := mr.SetAdd("set1", "member1", "member2")
		require.NoError(t, err)

		operations := []RedisPipelineOperation{
			{
				Op:      SetOpAdd,
				Key:     "set2",
				Members: []string{"newMember"},
			},
			{
				Op:      SetOpRemove,
				Key:     "set1",
				Members: []string{"member1"},
			},
			{
				Op:    OpSet,
				Key:   "key1",
				Value: "value1",
			},
		}

		err = store.ExecutePipeline(ctx, operations)
		require.NoError(t, err)

		// Verify all operations were executed
		members1, err := mr.Members("set1")
		require.NoError(t, err)
		assert.ElementsMatch(t, []string{"member2"}, members1)

		members2, err := mr.Members("set2")
		require.NoError(t, err)
		assert.ElementsMatch(t, []string{"newMember"}, members2)

		value, err := mr.Get("key1")
		require.NoError(t, err)
		assert.Equal(t, "value1", value)
	})

	t.Run("returns error for unsupported operation", func(t *testing.T) {
		store, mr := setupTestRedis(t)
		defer mr.Close()

		operations := []RedisPipelineOperation{
			{
				Op:  RedisOperation("INVALID_OP"),
				Key: "test:key",
			},
		}

		err := store.ExecutePipeline(ctx, operations)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "unsupported set operation: INVALID_OP")
	})

	t.Run("handles error when Redis is unavailable", func(t *testing.T) {
		store, mr := setupTestRedis(t)
		mr.Close() // Close to simulate connection error

		operations := []RedisPipelineOperation{
			{
				Op:    OpSet,
				Key:   "key1",
				Value: "value1",
			},
		}

		err := store.ExecutePipeline(ctx, operations)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "executing pipeline")
	})

	t.Run("reports partial failure when one operation has wrong type", func(t *testing.T) {
		store, mr := setupTestRedis(t)
		defer mr.Close()

		// Set a key as a string type - this will cause WRONGTYPE error when we try HSET on it
		err := mr.Set("key1", "string_value")
		require.NoError(t, err)

		operations := []RedisPipelineOperation{
			{
				Op:    OpSet,
				Key:   "key0",
				Value: "value0",
			},
			{
				Op:    OpHSet, // This will fail with WRONGTYPE because key1 is a string, not a hash
				Key:   "key1",
				Field: "field1",
				Value: "value1",
			},
			{
				Op:    OpSet,
				Key:   "key2",
				Value: "value2",
			},
		}

		err = store.ExecutePipeline(ctx, operations)
		// Pipeline should return an error due to the WRONGTYPE failure
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "WRONGTYPE")

		// Despite the error, other operations in the pipeline still executed
		// This demonstrates partial failure - Redis executes ALL commands in MULTI/EXEC
		value0, err := mr.Get("key0")
		require.NoError(t, err)
		assert.Equal(t, "value0", value0)

		value2, err := mr.Get("key2")
		require.NoError(t, err)
		assert.Equal(t, "value2", value2)

		// The failed operation didn't modify key1 - it's still a string
		value1, err := mr.Get("key1")
		require.NoError(t, err)
		assert.Equal(t, "string_value", value1)
	})
}

func TestRedisStore_Close(t *testing.T) {
	t.Run("successfully closes connection", func(t *testing.T) {
		store, mr := setupTestRedis(t)
		defer mr.Close()

		// Close should succeed
		err := store.Close()
		assert.NoError(t, err)

		// Subsequent operations should fail after Close
		ctx := context.Background()
		_, err = store.Get(ctx, "test:key")
		assert.Error(t, err)
	})

	t.Run("close is idempotent", func(t *testing.T) {
		store, mr := setupTestRedis(t)
		defer mr.Close()

		// First close should succeed
		err := store.Close()
		assert.NoError(t, err)

		// Second close returns error but doesn't panic (go-redis returns "client is closed")
		err = store.Close()
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "closing redis connection")
	})
}
