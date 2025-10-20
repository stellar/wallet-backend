// Package store provides data access layer for Redis operations.
package store

import (
	"context"
	"fmt"

	"github.com/redis/go-redis/v9"
)

// RedisStore provides generic Redis operations for caching.
type RedisStore struct {
	client *redis.Client
}

// NewRedisStore creates a new Redis store with the given connection parameters.
func NewRedisStore(host string, port int, password string) *RedisStore {
	addr := fmt.Sprintf("%s:%d", host, port)
	return &RedisStore{
		client: redis.NewClient(&redis.Options{
			Addr:     addr,
			Password: password,
		}),
	}
}

// SAdd adds one or more members to a set stored at key.
func (r *RedisStore) SAdd(ctx context.Context, key string, members ...string) error {
	if err := r.client.SAdd(ctx, key, members).Err(); err != nil {
		return fmt.Errorf("adding members to set %s: %w", key, err)
	}
	return nil
}

// SMembers returns all members of the set stored at key.
func (r *RedisStore) SMembers(ctx context.Context, key string) ([]string, error) {
	members, err := r.client.SMembers(ctx, key).Result()
	if err != nil {
		return nil, fmt.Errorf("getting members of set %s: %w", key, err)
	}
	return members, nil
}

// SRem removes one or more members from the set stored at key.
func (r *RedisStore) SRem(ctx context.Context, key string, members ...string) error {
	if err := r.client.SRem(ctx, key, members).Err(); err != nil {
		return fmt.Errorf("removing members from set %s: %w", key, err)
	}
	return nil
}

// SIsMember checks if member is a member of the set stored at key.
func (r *RedisStore) SIsMember(ctx context.Context, key string, member string) (bool, error) {
	isMember, err := r.client.SIsMember(ctx, key, member).Result()
	if err != nil {
		return false, fmt.Errorf("checking membership in set %s: %w", key, err)
	}
	return isMember, nil
}

// Del deletes one or more keys.
func (r *RedisStore) Del(ctx context.Context, keys ...string) error {
	if err := r.client.Del(ctx, keys...).Err(); err != nil {
		return fmt.Errorf("deleting keys: %w", err)
	}
	return nil
}

// Exists checks if one or more keys exist.
func (r *RedisStore) Exists(ctx context.Context, keys ...string) (int64, error) {
	count, err := r.client.Exists(ctx, keys...).Result()
	if err != nil {
		return 0, fmt.Errorf("checking key existence: %w", err)
	}
	return count, nil
}
