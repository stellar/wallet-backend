package store

import (
	"context"

	"github.com/stretchr/testify/mock"
)

type MockContractStore struct {
	mock.Mock
}

func (m *MockContractStore) UpsertWithTx(ctx context.Context, contractID string, name string, symbol string) error {
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

func (m *MockContractStore) Exists(ctx context.Context, contractID string) bool {
	args := m.Called(ctx, contractID)
	return args.Get(0).(bool)
}
