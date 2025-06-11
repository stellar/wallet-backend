package store

import (
	"context"
	"time"
)

// RedisClient defines the interface for Redis operations
type RedisClient interface {
	HGet(ctx context.Context, key string, field string) (string, error)
	HSet(ctx context.Context, key string, field string, value interface{}, expiration time.Duration) error
	Delete(ctx context.Context, keys ...string) error
	Exists(ctx context.Context, keys ...string) (int64, error)
}

// ContractStore defines the interface for contract-related operations
type ContractStore interface {
	Set(ctx context.Context, contractID string, name string, symbol string) error
	Name(ctx context.Context, contractID string) (string, error)
	Symbol(ctx context.Context, contractID string) (string, error)
	Exists(ctx context.Context, contractID string) (bool, error)
}
