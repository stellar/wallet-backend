package store

import (
	"context"
	"time"

	"github.com/stretchr/testify/mock"
)

type MockRedisClient struct {
	mock.Mock
}

func (m *MockRedisClient) HGet(ctx context.Context, key string, field string) (string, error) {
	args := m.Called(ctx, key, field)
	return args.String(0), args.Error(1)
}

func (m *MockRedisClient) HSet(ctx context.Context, key string, field string, value interface{}, expiration time.Duration) error {
	args := m.Called(ctx, key, field, value, expiration)
	return args.Error(0)
}

func (m *MockRedisClient) HGetAll(ctx context.Context, key string) (map[string]string, error) {
	args := m.Called(ctx, key)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(map[string]string), args.Error(1)
}

func (m *MockRedisClient) Delete(ctx context.Context, keys ...string) error {
	args := m.Called(ctx, keys)
	return args.Error(0)
}

func (m *MockRedisClient) Exists(ctx context.Context, keys ...string) (int64, error) {
	args := m.Called(ctx, keys)
	return args.Get(0).(int64), args.Error(1)
}

type MockContractStore struct {
	mock.Mock
}

func (m *MockContractStore) Set(ctx context.Context, contractID string, name string, symbol string) error {
	args := m.Called(ctx, contractID, name, symbol)
	return args.Error(0)
}

func (m *MockContractStore) Name(ctx context.Context, contractID string) (string, error) {
	args := m.Called(ctx, contractID)
	return args.String(0), args.Error(1)
}

func (m *MockContractStore) Symbol(ctx context.Context, contractID string) (string, error) {
	args := m.Called(ctx, contractID)
	return args.String(0), args.Error(1)
}

func (m *MockContractStore) Exists(ctx context.Context, contractID string) (bool, error) {
	args := m.Called(ctx, contractID)
	return args.Get(0).(bool), args.Error(1)
}
