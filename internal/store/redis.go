package store

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/stellar/wallet-backend/internal/entities"
)

type RedisStore struct {
	redis *redis.Client
}

func NewRedisStore(host string, port int) *RedisStore {
	addr := fmt.Sprintf("%s:%d", host, port)

	return &RedisStore{
		redis: redis.NewClient(&redis.Options{
			Addr: addr,
		}),
	}
}

func (r *RedisStore) GetHealth(ctx context.Context) (entities.HealthResponse, error) {
	_, err := r.redis.Ping(ctx).Result()
	if err != nil {
		return entities.HealthResponse{Status: entities.Error}, fmt.Errorf("pinging redis: %w", err)
	}

	return entities.HealthResponse{
		Status: entities.Healthy,
	}, nil
}

func (r *RedisStore) HGet(ctx context.Context, key string, field string) (string, error) {
	val, err := r.redis.HGet(ctx, key, field).Result()
	if err != nil {
		return "", fmt.Errorf("getting hash field %s from key %s: %w", field, key, err)
	}
	return val, nil
}

func (r *RedisStore) HSet(ctx context.Context, key string, field string, value interface{}, expiration time.Duration) error {
	err := r.redis.HSet(ctx, key, field, value).Err()
	if err != nil {
		return fmt.Errorf("setting hash field %s in key %s: %w", field, key, err)
	}
	if expiration > 0 {
		if err := r.redis.Expire(ctx, key, expiration).Err(); err != nil {
			return fmt.Errorf("setting expiration on key %s: %w", key, err)
		}
	}
	return nil
}

func (r *RedisStore) HGetAll(ctx context.Context, key string) (map[string]string, error) {
	val, err := r.redis.HGetAll(ctx, key).Result()
	if err != nil {
		return nil, fmt.Errorf("getting all hash fields from key %s: %w", key, err)
	}
	return val, nil
}

func (r *RedisStore) Delete(ctx context.Context, keys ...string) error {
	if err := r.redis.Del(ctx, keys...).Err(); err != nil {
		return fmt.Errorf("deleting keys: %w", err)
	}
	return nil
}

func (r *RedisStore) Exists(ctx context.Context, keys ...string) (int64, error) {
	count, err := r.redis.Exists(ctx, keys...).Result()
	if err != nil {
		return 0, fmt.Errorf("checking key existence: %w", err)
	}
	return count, nil
}

func (r *RedisStore) Close() error {
	if err := r.redis.Close(); err != nil {
		return fmt.Errorf("closing redis connection: %w", err)
	}
	return nil
}
