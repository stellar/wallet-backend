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

// AccountTokensModelMock is a mock implementation of AccountTokensModelInterface.
type AccountTokensModelMock struct {
	mock.Mock
}

var _ AccountTokensModelInterface = (*AccountTokensModelMock)(nil)

// NewAccountTokensModelMock creates a new instance of AccountTokensModelMock.
func NewAccountTokensModelMock(t interface {
	mock.TestingT
	Cleanup(func())
},
) *AccountTokensModelMock {
	mockModel := &AccountTokensModelMock{}
	mockModel.Mock.Test(t)

	t.Cleanup(func() { mockModel.AssertExpectations(t) })

	return mockModel
}

func (m *AccountTokensModelMock) GetTrustlines(ctx context.Context, accountAddress string) ([]Trustline, error) {
	args := m.Called(ctx, accountAddress)
	return args.Get(0).([]Trustline), args.Error(1)
}

func (m *AccountTokensModelMock) GetContracts(ctx context.Context, accountAddress string) ([]*Contract, error) {
	args := m.Called(ctx, accountAddress)
	return args.Get(0).([]*Contract), args.Error(1)
}

func (m *AccountTokensModelMock) BatchUpsertTrustlines(ctx context.Context, dbTx pgx.Tx, upserts []Trustline, deletes []Trustline) error {
	args := m.Called(ctx, dbTx, upserts, deletes)
	return args.Error(0)
}

func (m *AccountTokensModelMock) BatchInsertTrustlines(ctx context.Context, dbTx pgx.Tx, trustlines []Trustline) error {
	args := m.Called(ctx, dbTx, trustlines)
	return args.Error(0)
}

func (m *AccountTokensModelMock) BatchInsertContractTokens(ctx context.Context, dbTx pgx.Tx, contractsByAccount map[string][]uuid.UUID) error {
	args := m.Called(ctx, dbTx, contractsByAccount)
	return args.Error(0)
}
