// Mock implementations for data layer models used in testing.
// These mocks are used by service tests to isolate business logic from database operations.
package data

import (
	"context"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/mock"

	"github.com/stellar/wallet-backend/internal/indexer/types"
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

func (m *ContractModelMock) GetExisting(ctx context.Context, dbTx pgx.Tx, contractIDs []string) ([]string, error) {
	args := m.Called(ctx, dbTx, contractIDs)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]string), args.Error(1)
}

func (m *ContractModelMock) BatchInsert(ctx context.Context, dbTx pgx.Tx, contracts []*Contract) error {
	args := m.Called(ctx, dbTx, contracts)
	return args.Error(0)
}

// TrustlineAssetModelMock is a mock implementation of TrustlineAssetModelInterface.
type TrustlineAssetModelMock struct {
	mock.Mock
}

var _ TrustlineAssetModelInterface = (*TrustlineAssetModelMock)(nil)

// NewTrustlineAssetModelMock creates a new instance of TrustlineAssetModelMock.
func NewTrustlineAssetModelMock(t interface {
	mock.TestingT
	Cleanup(func())
},
) *TrustlineAssetModelMock {
	mockModel := &TrustlineAssetModelMock{}
	mockModel.Mock.Test(t)

	t.Cleanup(func() { mockModel.AssertExpectations(t) })

	return mockModel
}

func (m *TrustlineAssetModelMock) BatchInsert(ctx context.Context, dbTx pgx.Tx, assets []TrustlineAsset) error {
	args := m.Called(ctx, dbTx, assets)
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

// NativeBalanceModelMock is a mock implementation of NativeBalanceModelInterface.
type NativeBalanceModelMock struct {
	mock.Mock
}

var _ NativeBalanceModelInterface = (*NativeBalanceModelMock)(nil)

// NewNativeBalanceModelMock creates a new instance of NativeBalanceModelMock.
func NewNativeBalanceModelMock(t interface {
	mock.TestingT
	Cleanup(func())
},
) *NativeBalanceModelMock {
	mockModel := &NativeBalanceModelMock{}
	mockModel.Mock.Test(t)

	t.Cleanup(func() { mockModel.AssertExpectations(t) })

	return mockModel
}

func (m *NativeBalanceModelMock) GetByAccount(ctx context.Context, accountAddress string) (*NativeBalance, error) {
	args := m.Called(ctx, accountAddress)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*NativeBalance), args.Error(1)
}

func (m *NativeBalanceModelMock) BatchUpsert(ctx context.Context, dbTx pgx.Tx, upserts []NativeBalance, deletes []string) error {
	args := m.Called(ctx, dbTx, upserts, deletes)
	return args.Error(0)
}

func (m *NativeBalanceModelMock) BatchCopy(ctx context.Context, dbTx pgx.Tx, balances []NativeBalance) error {
	args := m.Called(ctx, dbTx, balances)
	return args.Error(0)
}

// SACBalanceModelMock is a mock implementation of SACBalanceModelInterface.
type SACBalanceModelMock struct {
	mock.Mock
}

var _ SACBalanceModelInterface = (*SACBalanceModelMock)(nil)

// NewSACBalanceModelMock creates a new instance of SACBalanceModelMock.
func NewSACBalanceModelMock(t interface {
	mock.TestingT
	Cleanup(func())
},
) *SACBalanceModelMock {
	mockModel := &SACBalanceModelMock{}
	mockModel.Mock.Test(t)

	t.Cleanup(func() { mockModel.AssertExpectations(t) })

	return mockModel
}

func (m *SACBalanceModelMock) GetByAccount(ctx context.Context, accountAddress string) ([]SACBalance, error) {
	args := m.Called(ctx, accountAddress)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]SACBalance), args.Error(1)
}

func (m *SACBalanceModelMock) BatchUpsert(ctx context.Context, dbTx pgx.Tx, upserts []SACBalance, deletes []SACBalance) error {
	args := m.Called(ctx, dbTx, upserts, deletes)
	return args.Error(0)
}

func (m *SACBalanceModelMock) BatchCopy(ctx context.Context, dbTx pgx.Tx, balances []SACBalance) error {
	args := m.Called(ctx, dbTx, balances)
	return args.Error(0)
}

// ProtocolWasmsModelMock is a mock implementation of ProtocolWasmsModelInterface.
type ProtocolWasmsModelMock struct {
	mock.Mock
}

var _ ProtocolWasmsModelInterface = (*ProtocolWasmsModelMock)(nil)

// NewProtocolWasmsModelMock creates a new instance of ProtocolWasmsModelMock.
func NewProtocolWasmsModelMock(t interface {
	mock.TestingT
	Cleanup(func())
},
) *ProtocolWasmsModelMock {
	mockModel := &ProtocolWasmsModelMock{}
	mockModel.Mock.Test(t)

	t.Cleanup(func() { mockModel.AssertExpectations(t) })

	return mockModel
}

func (m *ProtocolWasmsModelMock) BatchInsert(ctx context.Context, dbTx pgx.Tx, wasms []ProtocolWasms) error {
	args := m.Called(ctx, dbTx, wasms)
	return args.Error(0)
}

func (m *ProtocolWasmsModelMock) GetUnclassified(ctx context.Context) ([]ProtocolWasms, error) {
	args := m.Called(ctx)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]ProtocolWasms), args.Error(1)
}

func (m *ProtocolWasmsModelMock) BatchUpdateProtocolID(ctx context.Context, dbTx pgx.Tx, wasmHashes []types.HashBytea, protocolID string) error {
	args := m.Called(ctx, dbTx, wasmHashes, protocolID)
	return args.Error(0)
}

// ProtocolsModelMock is a mock implementation of ProtocolsModelInterface.
type ProtocolsModelMock struct {
	mock.Mock
}

var _ ProtocolsModelInterface = (*ProtocolsModelMock)(nil)

// NewProtocolsModelMock creates a new instance of ProtocolsModelMock.
func NewProtocolsModelMock(t interface {
	mock.TestingT
	Cleanup(func())
},
) *ProtocolsModelMock {
	mockModel := &ProtocolsModelMock{}
	mockModel.Mock.Test(t)

	t.Cleanup(func() { mockModel.AssertExpectations(t) })

	return mockModel
}

func (m *ProtocolsModelMock) UpdateClassificationStatus(ctx context.Context, dbTx pgx.Tx, protocolIDs []string, status string) error {
	args := m.Called(ctx, dbTx, protocolIDs, status)
	return args.Error(0)
}

func (m *ProtocolsModelMock) GetByIDs(ctx context.Context, protocolIDs []string) ([]Protocols, error) {
	args := m.Called(ctx, protocolIDs)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]Protocols), args.Error(1)
}

func (m *ProtocolsModelMock) GetClassified(ctx context.Context) ([]Protocols, error) {
	args := m.Called(ctx)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]Protocols), args.Error(1)
}

func (m *ProtocolsModelMock) InsertIfNotExists(ctx context.Context, dbTx pgx.Tx, protocolID string) error {
	args := m.Called(ctx, dbTx, protocolID)
	return args.Error(0)
}

// ProtocolContractsModelMock is a mock implementation of ProtocolContractsModelInterface.
type ProtocolContractsModelMock struct {
	mock.Mock
}

var _ ProtocolContractsModelInterface = (*ProtocolContractsModelMock)(nil)

// NewProtocolContractsModelMock creates a new instance of ProtocolContractsModelMock.
func NewProtocolContractsModelMock(t interface {
	mock.TestingT
	Cleanup(func())
},
) *ProtocolContractsModelMock {
	mockModel := &ProtocolContractsModelMock{}
	mockModel.Mock.Test(t)

	t.Cleanup(func() { mockModel.AssertExpectations(t) })

	return mockModel
}

func (m *ProtocolContractsModelMock) BatchInsert(ctx context.Context, dbTx pgx.Tx, contracts []ProtocolContracts) error {
	args := m.Called(ctx, dbTx, contracts)
	return args.Error(0)
}

// ProtocolWasmModelMock is a mock implementation of ProtocolWasmModelInterface.
type ProtocolWasmModelMock struct {
	mock.Mock
}

var _ ProtocolWasmModelInterface = (*ProtocolWasmModelMock)(nil)

// NewProtocolWasmModelMock creates a new instance of ProtocolWasmModelMock.
func NewProtocolWasmModelMock(t interface {
	mock.TestingT
	Cleanup(func())
},
) *ProtocolWasmModelMock {
	mockModel := &ProtocolWasmModelMock{}
	mockModel.Mock.Test(t)

	t.Cleanup(func() { mockModel.AssertExpectations(t) })

	return mockModel
}

func (m *ProtocolWasmModelMock) BatchInsert(ctx context.Context, dbTx pgx.Tx, wasms []ProtocolWasm) error {
	args := m.Called(ctx, dbTx, wasms)
	return args.Error(0)
}

// ProtocolContractsModelMock is a mock implementation of ProtocolContractsModelInterface.
type ProtocolContractsModelMock struct {
	mock.Mock
}

var _ ProtocolContractsModelInterface = (*ProtocolContractsModelMock)(nil)

// NewProtocolContractsModelMock creates a new instance of ProtocolContractsModelMock.
func NewProtocolContractsModelMock(t interface {
	mock.TestingT
	Cleanup(func())
},
) *ProtocolContractsModelMock {
	mockModel := &ProtocolContractsModelMock{}
	mockModel.Mock.Test(t)

	t.Cleanup(func() { mockModel.AssertExpectations(t) })

	return mockModel
}

func (m *ProtocolContractsModelMock) BatchInsert(ctx context.Context, dbTx pgx.Tx, contracts []ProtocolContracts) error {
	args := m.Called(ctx, dbTx, contracts)
	return args.Error(0)
}
