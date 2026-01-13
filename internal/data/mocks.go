// Mock implementations for data layer models used in testing.
// These mocks are used by service tests to isolate business logic from database operations.
package data

import (
	"context"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/mock"
)

// ContractModelMock is a mock implementation of ContractModelInterface.
type ContractModelMock struct {
	mock.Mock
}

var _ ContractModelInterface = (*ContractModelMock)(nil)

// NewContractModelMock creates a new instance of ContractModelMock.
func NewContractModelMock(t interface {
	mock.TestingT
	Cleanup(func())
},
) *ContractModelMock {
	mockContract := &ContractModelMock{}
	mockContract.Mock.Test(t)

	t.Cleanup(func() { mockContract.AssertExpectations(t) })

	return mockContract
}

func (m *ContractModelMock) GetByContractID(ctx context.Context, contractID string) (*Contract, error) {
	args := m.Called(ctx, contractID)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*Contract), args.Error(1)
}

func (m *ContractModelMock) GetAllContractIDs(ctx context.Context) ([]string, error) {
	args := m.Called(ctx)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]string), args.Error(1)
}

func (m *ContractModelMock) BatchGetByIDs(ctx context.Context, ids []int64) ([]*Contract, error) {
	args := m.Called(ctx, ids)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]*Contract), args.Error(1)
}

func (m *ContractModelMock) BatchInsert(ctx context.Context, dbTx pgx.Tx, contracts []*Contract) error {
	args := m.Called(ctx, dbTx, contracts)
	return args.Error(0)
}
