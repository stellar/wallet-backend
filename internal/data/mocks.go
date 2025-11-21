// Mock implementations for data layer models used in testing
package data

import (
	"context"

	"github.com/stretchr/testify/mock"

	"github.com/stellar/wallet-backend/internal/db"
)

type ContractModelMock struct {
	mock.Mock
}

var _ ContractModelInterface = (*ContractModelMock)(nil)

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

func (m *ContractModelMock) GetByID(ctx context.Context, contractID string) (*Contract, error) {
	args := m.Called(ctx, contractID)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*Contract), args.Error(1)
}

func (m *ContractModelMock) BatchGetByIDs(ctx context.Context, contractIDs []string) ([]*Contract, error) {
	args := m.Called(ctx, contractIDs)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]*Contract), args.Error(1)
}

func (m *ContractModelMock) BatchInsert(ctx context.Context, sqlExecuter db.SQLExecuter, contracts []*Contract) ([]string, error) {
	args := m.Called(ctx, sqlExecuter, contracts)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]string), args.Error(1)
}
