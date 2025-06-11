package store

import (
	"context"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/wallet-backend/internal/entities"
)

// setupTestRedis creates a new miniredis instance and returns a RedisStore connected to it
func setupTestRedis(t *testing.T) (*RedisStore, *miniredis.Miniredis) {
	mr := miniredis.RunT(t)

	// Extract host and port from the address
	addr := mr.Addr()
	parts := strings.Split(addr, ":")
	host := parts[0]
	port, err := strconv.Atoi(parts[1])
	if err != nil {
		t.Fatalf("Failed to parse port: %v", err)
	}

	store := NewRedisStore(host, port)

	return store, mr
}

func TestRedisStore_GetHealth(t *testing.T) {
	t.Run("healthy_redis", func(t *testing.T) {
		store, _ := setupTestRedis(t)
		defer store.Close()

		ctx := context.Background()
		health, err := store.GetHealth(ctx)
		require.NoError(t, err)
		assert.Equal(t, entities.HealthResponse{Status: entities.Healthy}, health)
	})

	t.Run("redis_down", func(t *testing.T) {
		store, mr := setupTestRedis(t)
		mr.Close() // Close miniredis to simulate it being down

		ctx := context.Background()
		health, err := store.GetHealth(ctx)
		assert.Error(t, err)
		assert.Equal(t, entities.HealthResponse{Status: entities.Error}, health)

		store.Close()
	})
}

func TestRedisStore_HSet_HGet(t *testing.T) {
	store, mr := setupTestRedis(t)
	defer store.Close()

	ctx := context.Background()

	t.Run("set_and_get_success", func(t *testing.T) {
		key := "test:key"
		field := "field1"
		value := "value1"

		// Set without expiration
		err := store.HSet(ctx, key, field, value, 0)
		require.NoError(t, err)

		// Get the value
		got, err := store.HGet(ctx, key, field)
		require.NoError(t, err)
		assert.Equal(t, value, got)
	})

	t.Run("set_with_expiration", func(t *testing.T) {
		key := "test:key:exp"
		field := "field1"
		value := "value1"
		expiration := 1 * time.Second

		// Set with expiration
		err := store.HSet(ctx, key, field, value, expiration)
		require.NoError(t, err)

		// Verify it exists
		got, err := store.HGet(ctx, key, field)
		require.NoError(t, err)
		assert.Equal(t, value, got)

		// Fast forward time to expire the key
		mr.FastForward(2 * time.Second)

		// Verify it's expired
		_, err = store.HGet(ctx, key, field)
		assert.Error(t, err)
	})

	t.Run("get_non_existent_field", func(t *testing.T) {
		_, err := store.HGet(ctx, "non:existent", "field")
		assert.Error(t, err)
	})
}

func TestRedisStore_Delete(t *testing.T) {
	store, _ := setupTestRedis(t)
	defer store.Close()

	ctx := context.Background()

	t.Run("delete_single_key", func(t *testing.T) {
		key := "test:delete"

		// Set a value
		err := store.HSet(ctx, key, "field", "value", 0)
		require.NoError(t, err)

		// Verify it exists
		exists, err := store.Exists(ctx, key)
		require.NoError(t, err)
		assert.Equal(t, int64(1), exists)

		// Delete it
		err = store.Delete(ctx, key)
		require.NoError(t, err)

		// Verify it's gone
		exists, err = store.Exists(ctx, key)
		require.NoError(t, err)
		assert.Equal(t, int64(0), exists)
	})

	t.Run("delete_multiple_keys", func(t *testing.T) {
		keys := []string{"test:key1", "test:key2", "test:key3"}

		// Set values for all keys
		for _, key := range keys {
			err := store.HSet(ctx, key, "field", "value", 0)
			require.NoError(t, err)
		}

		// Delete all at once
		err := store.Delete(ctx, keys...)
		require.NoError(t, err)

		// Verify all are gone
		exists, err := store.Exists(ctx, keys...)
		require.NoError(t, err)
		assert.Equal(t, int64(0), exists)
	})

	t.Run("delete_non_existent_key", func(t *testing.T) {
		// Deleting non-existent keys should not error
		err := store.Delete(ctx, "non:existent:key")
		assert.NoError(t, err)
	})
}

func TestRedisStore_Exists(t *testing.T) {
	store, _ := setupTestRedis(t)
	defer store.Close()

	ctx := context.Background()

	t.Run("single_key_exists", func(t *testing.T) {
		key := "test:exists"

		// Set a value
		err := store.HSet(ctx, key, "field", "value", 0)
		require.NoError(t, err)

		// Check existence
		count, err := store.Exists(ctx, key)
		require.NoError(t, err)
		assert.Equal(t, int64(1), count)
	})

	t.Run("single_key_not_exists", func(t *testing.T) {
		count, err := store.Exists(ctx, "non:existent")
		require.NoError(t, err)
		assert.Equal(t, int64(0), count)
	})

	t.Run("multiple_keys_mixed", func(t *testing.T) {
		// Create some keys
		existingKeys := []string{"test:exists1", "test:exists2"}
		for _, key := range existingKeys {
			err := store.HSet(ctx, key, "field", "value", 0)
			require.NoError(t, err)
		}

		// Check mix of existing and non-existing keys
		checkKeys := []string{"test:exists1", "test:exists2", "test:not:exists1", "test:not:exists2"}
		count, err := store.Exists(ctx, checkKeys...)
		require.NoError(t, err)
		assert.Equal(t, int64(2), count) // Only 2 keys exist
	})
}
