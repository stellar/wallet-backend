package store

import (
	"context"
	"fmt"
	"time"
)

const (
	contractNameKey   = "name"
	contractSymbolKey = "symbol"
	defaultExpiration = 24 * time.Hour
)

type contractStore struct {
	redis RedisClient
}

func NewContractStore(redis RedisClient) ContractStore {
	return &contractStore{
		redis: redis,
	}
}

func (s *contractStore) Set(ctx context.Context, contractID string, name string, symbol string) error {
	err := s.redis.HSet(ctx, contractID, contractNameKey, name, defaultExpiration)
	if err != nil {
		return fmt.Errorf("setting contract name: %w", err)
	}
	err = s.redis.HSet(ctx, contractID, contractSymbolKey, symbol, defaultExpiration)
	if err != nil {
		return fmt.Errorf("setting contract symbol: %w", err)
	}
	return nil
}

func (s *contractStore) Name(ctx context.Context, contractID string) (string, error) {
	name, err := s.redis.HGet(ctx, contractID, contractNameKey)
	if err != nil {
		return "", fmt.Errorf("getting contract name: %w", err)
	}
	return name, nil
}

func (s *contractStore) Symbol(ctx context.Context, contractID string) (string, error) {
	symbol, err := s.redis.HGet(ctx, contractID, contractSymbolKey)
	if err != nil {
		return "", fmt.Errorf("getting contract symbol: %w", err)
	}
	return symbol, nil
}

func (s *contractStore) Exists(ctx context.Context, contractID string) (bool, error) {
	count, err := s.redis.Exists(ctx, contractID)
	if err != nil {
		return false, fmt.Errorf("checking contract existence: %w", err)
	}
	return count > 0, nil
}
