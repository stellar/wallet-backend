package store

import (
	"context"
	"time"
)

const (
	contractNameKey = "name"
	contractSymbolKey = "symbol"
	defaultExpiration = 24 * time.Hour
)

type ContractStore interface {
	Set(ctx context.Context, contractID string, name string, symbol string) error
	Name(ctx context.Context, contractID string) (string, error)
	Symbol(ctx context.Context, contractID string) (string, error)
	Exists(ctx context.Context, contractID string) (bool, error)
}

type contractStore struct {
	redis *RedisStore
}

func NewContractStore(redis *RedisStore) ContractStore {
	return &contractStore{
		redis: redis,
	}
}

func (s *contractStore) Set(ctx context.Context, contractID string, name string, symbol string) error {
	err := s.redis.HSet(ctx, contractID, contractNameKey, name, defaultExpiration)
	if err != nil {
		return err
	}
	err = s.redis.HSet(ctx, contractID, contractSymbolKey, symbol, defaultExpiration)
	if err != nil {
		return err
	}
	return nil
}

func (s *contractStore) Name(ctx context.Context, contractID string) (string, error) {
	return s.redis.HGet(ctx, contractID, contractNameKey)
}

func (s *contractStore) Symbol(ctx context.Context, contractID string) (string, error) {
	return s.redis.HGet(ctx, contractID, contractSymbolKey)
}

func (s *contractStore) Exists(ctx context.Context, contractID string) (bool, error) {
	count, err := s.redis.Exists(ctx, contractID)
	if err != nil {
		return false, err
	}
	return count > 0, nil
}
