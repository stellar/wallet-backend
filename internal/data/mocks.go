package data

import (
	"context"

	"github.com/stretchr/testify/mock"
)

// Ensure MockAccountModel implements all AccountModel methods
var _ interface {
	GetAll(ctx context.Context) ([]string, error)
	Insert(ctx context.Context, address string) error
	Delete(ctx context.Context, address string) error
	BatchGetByIDs(ctx context.Context, accountIDs []string) ([]string, error)
	IsAccountFeeBumpEligible(ctx context.Context, address string) (bool, error)
} = (*MockAccountModel)(nil)

// MockAccountModel is a mock implementation of AccountModel
type MockAccountModel struct {
	mock.Mock
}

func (m *MockAccountModel) GetAll(ctx context.Context) ([]string, error) {
	args := m.Called(ctx)
	return args.Get(0).([]string), args.Error(1)
}

func (m *MockAccountModel) Insert(ctx context.Context, address string) error {
	args := m.Called(ctx, address)
	return args.Error(0)
}

func (m *MockAccountModel) Delete(ctx context.Context, address string) error {
	args := m.Called(ctx, address)
	return args.Error(0)
}

func (m *MockAccountModel) BatchGetByIDs(ctx context.Context, accountIDs []string) ([]string, error) {
	args := m.Called(ctx, accountIDs)
	return args.Get(0).([]string), args.Error(1)
}

func (m *MockAccountModel) IsAccountFeeBumpEligible(ctx context.Context, address string) (bool, error) {
	args := m.Called(ctx, address)
	return args.Bool(0), args.Error(1)
}