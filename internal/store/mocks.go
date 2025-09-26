package store

import (
	"context"

	"github.com/stretchr/testify/mock"
)

type MockTokenContractStore struct {
	mock.Mock
}

func (m *MockTokenContractStore) UpsertWithTx(ctx context.Context, contractID string, name string, symbol string) error {
	args := m.Called(ctx, contractID, name, symbol)
	return args.Error(0)
}

func (m *MockTokenContractStore) Name(ctx context.Context, contractID string) (string, error) {
	args := m.Called(ctx, contractID)
	return args.String(0), args.Error(1)
}

func (m *MockTokenContractStore) Symbol(ctx context.Context, contractID string) (string, error) {
	args := m.Called(ctx, contractID)
	return args.String(0), args.Error(1)
}

func (m *MockTokenContractStore) Exists(ctx context.Context, contractID string) bool {
	args := m.Called(ctx, contractID)
	return args.Get(0).(bool)
}

type MockAccountsStore struct {
	mock.Mock
}

func (m *MockAccountsStore) Add(accountID string) {
	m.Called(accountID)
}

func (m *MockAccountsStore) Remove(accountID string) {
	m.Called(accountID)
}

func (m *MockAccountsStore) Exists(accountID string) bool {
	args := m.Called(accountID)
	return args.Get(0).(bool)
}
