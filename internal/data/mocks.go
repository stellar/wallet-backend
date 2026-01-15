// Mock implementations for data layer models used in testing.
// These mocks are used by service tests to isolate business logic from database operations.
package data

import (
	"context"

	"github.com/google/uuid"
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

func (m *ContractModelMock) BatchGetByIDs(ctx context.Context, ids []uuid.UUID) ([]*Contract, error) {
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

// TrustlineBalanceModelMock is a mock implementation of TrustlineBalanceModelInterface.
type TrustlineBalanceModelMock struct {
	mock.Mock
}

var _ TrustlineBalanceModelInterface = (*TrustlineBalanceModelMock)(nil)

// NewTrustlineBalanceModelMock creates a new instance of TrustlineBalanceModelMock.
func NewTrustlineBalanceModelMock(t interface {
	mock.TestingT
	Cleanup(func())
},
) *TrustlineBalanceModelMock {
	mockModel := &TrustlineBalanceModelMock{}
	mockModel.Mock.Test(t)

	t.Cleanup(func() { mockModel.AssertExpectations(t) })

	return mockModel
}

func (m *TrustlineBalanceModelMock) GetByAccount(ctx context.Context, accountAddress string) ([]TrustlineBalance, error) {
	args := m.Called(ctx, accountAddress)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]TrustlineBalance), args.Error(1)
}

func (m *TrustlineBalanceModelMock) BatchUpsert(ctx context.Context, dbTx pgx.Tx, upserts []TrustlineBalance, deletes []TrustlineBalance) error {
	args := m.Called(ctx, dbTx, upserts, deletes)
	return args.Error(0)
}

func (m *TrustlineBalanceModelMock) BatchCopy(ctx context.Context, dbTx pgx.Tx, balances []TrustlineBalance) error {
	args := m.Called(ctx, dbTx, balances)
	return args.Error(0)
}

// AccountContractTokensModelMock is a mock implementation of AccountContractTokensModelInterface.
type AccountContractTokensModelMock struct {
	mock.Mock
}

var _ AccountContractTokensModelInterface = (*AccountContractTokensModelMock)(nil)

// NewAccountContractTokensModelMock creates a new instance of AccountContractTokensModelMock.
func NewAccountContractTokensModelMock(t interface {
	mock.TestingT
	Cleanup(func())
},
) *AccountContractTokensModelMock {
	mockModel := &AccountContractTokensModelMock{}
	mockModel.Mock.Test(t)

	t.Cleanup(func() { mockModel.AssertExpectations(t) })

	return mockModel
}

func (m *AccountContractTokensModelMock) GetByAccount(ctx context.Context, accountAddress string) ([]*Contract, error) {
	args := m.Called(ctx, accountAddress)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]*Contract), args.Error(1)
}

func (m *AccountContractTokensModelMock) BatchInsert(ctx context.Context, dbTx pgx.Tx, contractsByAccount map[string][]uuid.UUID) error {
	args := m.Called(ctx, dbTx, contractsByAccount)
	return args.Error(0)
}
