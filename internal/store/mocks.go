package store

import (
	"context"

	"github.com/stretchr/testify/mock"
	"github.com/stellar/wallet-backend/internal/db"
)

type MockContractStore struct {
	mock.Mock
}

func (m *MockContractStore) InsertWithTx(ctx context.Context, tx db.Transaction, contractID string, name string, symbol string) error {
	args := m.Called(ctx, tx, contractID, name, symbol)
	return args.Error(0)
}

func (m *MockContractStore) UpdateWithTx(ctx context.Context, tx db.Transaction, contractID string, name string, symbol string) error {
	args := m.Called(ctx, tx, contractID, name, symbol)
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
