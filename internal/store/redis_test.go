// Package store provides data access layer for Redis operations.
package store

import (
	"context"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// setupTestRedis creates a miniredis instance and returns a RedisStore connected to it
func setupTestRedis(t *testing.T) (*RedisStore, *miniredis.Miniredis) {
	mr := miniredis.RunT(t)

	store := &RedisStore{
		client: redis.NewClient(&redis.Options{
			Addr: mr.Addr(),
		}),
	}

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
		assert.Contains(t, err.Error(), "executing set pipeline")
	})
}
