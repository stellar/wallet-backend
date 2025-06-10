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

func NewRedisStore(host string, port int, password string) *RedisStore {
	addr := fmt.Sprintf("%s:%d", host, port)

	return &RedisStore{
		redis: redis.NewClient(&redis.Options{
			Addr:     addr,
			Password: password,
		}),
	}
}

func (r *RedisStore) GetHealth(ctx context.Context) (entities.HealthResponse, error) {
	_, err := r.redis.Ping(ctx).Result()
	if err != nil {
		return entities.HealthResponse{Status: entities.Error}, err
	}

	return entities.HealthResponse{
		Status: entities.Healthy,
	}, nil
}

func (r *RedisStore) HGet(ctx context.Context, key string, field string) (string, error) {
	val, err := r.redis.HGet(ctx, key, field).Result()
	if err != nil {
		return "", err
	}
	return val, nil
}

func (r *RedisStore) HSet(ctx context.Context, key string, field string, value interface{}, expiration time.Duration) error {
	err := r.redis.HSet(ctx, key, field, value).Err()
	if err != nil {
		return err
	}
	if expiration > 0 {
		return r.redis.Expire(ctx, key, expiration).Err()
	}
	return nil
}

func (r *RedisStore) HGetAll(ctx context.Context, key string) (map[string]string, error) {
	val, err := r.redis.HGetAll(ctx, key).Result()
	if err != nil {
		return nil, err
	}
	return val, nil
}

func (r *RedisStore) Delete(ctx context.Context, keys ...string) error {
	return r.redis.Del(ctx, keys...).Err()
}

func (r *RedisStore) Exists(ctx context.Context, keys ...string) (int64, error) {
	return r.redis.Exists(ctx, keys...).Result()
}

func (r *RedisStore) Close() error {
	return r.redis.Close()
}
