// Mock implementations for data layer models used in testing
package data

import (
	"context"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/mock"
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

func (m *ContractModelMock) BatchInsert(ctx context.Context, dbTx pgx.Tx, contracts []*Contract) ([]string, error) {
	args := m.Called(ctx, dbTx, contracts)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]string), args.Error(1)
}

func (m *ContractModelMock) GetAllIDs(ctx context.Context) ([]string, error) {
	args := m.Called(ctx)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]string), args.Error(1)
}

// TrustlineAssetModelMock is a mock implementation of TrustlineAssetModelInterface.
type TrustlineAssetModelMock struct {
	mock.Mock
}

var _ TrustlineAssetModelInterface = (*TrustlineAssetModelMock)(nil)

func NewTrustlineAssetModelMock(t interface {
	mock.TestingT
	Cleanup(func())
},
) *TrustlineAssetModelMock {
	mockAsset := &TrustlineAssetModelMock{}
	mockAsset.Mock.Test(t)

	t.Cleanup(func() { mockAsset.AssertExpectations(t) })

	return mockAsset
}

func (m *TrustlineAssetModelMock) BatchGetOrInsert(ctx context.Context, dbTx pgx.Tx, assets []TrustlineAsset) (map[string]int64, error) {
	args := m.Called(ctx, dbTx, assets)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(map[string]int64), args.Error(1)
}

func (m *TrustlineAssetModelMock) BatchInsert(ctx context.Context, dbTx pgx.Tx, assets []TrustlineAsset) (map[string]int64, error) {
	args := m.Called(ctx, dbTx, assets)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(map[string]int64), args.Error(1)
}

func (m *TrustlineAssetModelMock) BatchGetByIDs(ctx context.Context, ids []int64) ([]*TrustlineAsset, error) {
	args := m.Called(ctx, ids)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]*TrustlineAsset), args.Error(1)
}
