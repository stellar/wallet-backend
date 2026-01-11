// Package store provides data access layer for Redis operations.
package store

import (
	"context"
	"errors"
	"fmt"

	"github.com/redis/go-redis/v9"
)

// RedisOperation represents a single Redis set operation type.
type RedisOperation string

const (
	SetOpAdd    RedisOperation = "SADD"
	SetOpRemove RedisOperation = "SREM"
	OpSet       RedisOperation = "SET"
	OpHSet      RedisOperation = "HSET"
	OpHDel      RedisOperation = "HDEL"
)

// RedisPipelineOperation represents a single set operation to be executed in a pipeline.
type RedisPipelineOperation struct {
	Op      RedisOperation
	Key     string
	Members []string
	Value   string
	Field   string
}

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

// Get retrieves the value of a key.
func (r *RedisStore) Get(ctx context.Context, key string) (string, error) {
	val, err := r.client.Get(ctx, key).Result()
	if errors.Is(err, redis.Nil) {
		return "", nil // Key doesn't exist
	}
	if err != nil {
		return "", fmt.Errorf("getting key %s: %w", key, err)
	}
	return val, nil
}

// Set stores a string value at a key.
func (r *RedisStore) Set(ctx context.Context, key, value string) error {
	if err := r.client.Set(ctx, key, value, 0).Err(); err != nil {
		return fmt.Errorf("setting key %s: %w", key, err)
	}
	return nil
}

// HSet stores a string value at a key.
func (r *RedisStore) HSet(ctx context.Context, key, field, value string) error {
	if err := r.client.HSet(ctx, key, field, value).Err(); err != nil {
		return fmt.Errorf("setting key %s: %w", key, err)
	}
	return nil
}

// HGet retrieves the value of a field in a hash stored at key.
func (r *RedisStore) HGet(ctx context.Context, key string, field string) (string, error) {
	val, err := r.client.HGet(ctx, key, field).Result()
	if errors.Is(err, redis.Nil) {
		return "", nil // Key doesn't exist
	}
	if err != nil {
		return "", fmt.Errorf("getting key %s: %w", key, err)
	}
	return val, nil
}

// HMGet retrieves the values of multiple fields and their values in a hash stored at key.
func (r *RedisStore) HMGet(ctx context.Context, key string, fields ...string) (map[string]string, error) {
	vals, err := r.client.HMGet(ctx, key, fields...).Result()
	if err != nil {
		return nil, fmt.Errorf("getting key %s: %w", key, err)
	}

	result := make(map[string]string)
	for i, field := range fields {
		if vals[i] == nil {
			// Field doesn't exist in Redis - this is normal for new accounts
			continue
		}
		str, ok := vals[i].(string)
		if !ok {
			return nil, fmt.Errorf("getting field %s from key %s: %w", field, key, err)
		}
		result[field] = str
	}
	return result, nil
}

// ExecutePipeline executes multiple operations (SADD/SREM/SET) in a single pipeline.
// This reduces network round trips and improves performance when processing many operations.
// Returns an error if any operation in the pipeline fails.
func (r *RedisStore) ExecutePipeline(ctx context.Context, operations []RedisPipelineOperation) error {
	if len(operations) == 0 {
		return nil
	}

	pipe := r.client.TxPipeline()
	for _, op := range operations {
		switch op.Op {
		case SetOpAdd:
			pipe.SAdd(ctx, op.Key, op.Members)
		case SetOpRemove:
			pipe.SRem(ctx, op.Key, op.Members)
		case OpSet:
			pipe.Set(ctx, op.Key, op.Value, 0)
		case OpHSet:
			pipe.HSet(ctx, op.Key, op.Field, op.Value)
		case OpHDel:
			pipe.HDel(ctx, op.Key, op.Field)
		default:
			return fmt.Errorf("unsupported set operation: %s", op.Op)
		}
	}

	cmds, err := pipe.Exec(ctx)
	if err != nil && !errors.Is(err, redis.Nil) {
		// Collect individual failures for diagnostics
		if cmds != nil && len(cmds) == len(operations) {
			var failures []string
			for i, cmd := range cmds {
				if cmdErr := cmd.Err(); cmdErr != nil && !errors.Is(cmdErr, redis.Nil) {
					failures = append(failures, fmt.Sprintf(
						"op[%d] %s key=%s: %v",
						i, operations[i].Op, operations[i].Key, cmdErr,
					))
				}
			}
			if len(failures) > 0 {
				return fmt.Errorf("pipeline execution failed with %d errors: %v", len(failures), failures)
			}
		}
		return fmt.Errorf("executing pipeline: %w", err)
	}

	return nil
}
