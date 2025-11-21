package services

import (
	"context"

	"github.com/stellar/go/ingest/ledgerbackend"
	"github.com/stellar/go/xdr"
	"github.com/stretchr/testify/mock"

	"github.com/stellar/wallet-backend/internal/entities"
	"github.com/stellar/wallet-backend/internal/indexer/types"
)

// LedgerBackendMock is a mock implementation of the ledgerbackend.LedgerBackend interface
type LedgerBackendMock struct {
	mock.Mock
}

var _ ledgerbackend.LedgerBackend = (*LedgerBackendMock)(nil)

func (l *LedgerBackendMock) GetLatestLedgerSequence(ctx context.Context) (uint32, error) {
	args := l.Called(ctx)
	return args.Get(0).(uint32), args.Error(1)
}

func (l *LedgerBackendMock) GetLedger(ctx context.Context, sequence uint32) (xdr.LedgerCloseMeta, error) {
	args := l.Called(ctx, sequence)
	return args.Get(0).(xdr.LedgerCloseMeta), args.Error(1)
}

func (l *LedgerBackendMock) PrepareRange(ctx context.Context, ledgerRange ledgerbackend.Range) error {
	args := l.Called(ctx, ledgerRange)
	return args.Error(0)
}

func (l *LedgerBackendMock) IsPrepared(ctx context.Context, ledgerRange ledgerbackend.Range) (bool, error) {
	args := l.Called(ctx, ledgerRange)
	return args.Bool(0), args.Error(1)
}

func (l *LedgerBackendMock) Close() error {
	args := l.Called()
	return args.Error(0)
}

// NewLedgerBackendMock creates a new instance of LedgerBackendMock
func NewLedgerBackendMock(t interface {
	mock.TestingT
	Cleanup(func())
},
) *LedgerBackendMock {
	mockBackend := &LedgerBackendMock{}
	mockBackend.Mock.Test(t)

	t.Cleanup(func() { mockBackend.AssertExpectations(t) })

	return mockBackend
}

type RPCServiceMock struct {
	mock.Mock
}

var _ RPCService = (*RPCServiceMock)(nil)

func (r *RPCServiceMock) TrackRPCServiceHealth(ctx context.Context, triggerHeartbeat <-chan any) error {
	args := r.Called(ctx, triggerHeartbeat)
	return args.Error(0)
}

func (r *RPCServiceMock) GetHeartbeatChannel() chan entities.RPCGetHealthResult {
	args := r.Called()
	return args.Get(0).(chan entities.RPCGetHealthResult)
}

func (r *RPCServiceMock) SendTransaction(transactionXdr string) (entities.RPCSendTransactionResult, error) {
	args := r.Called(transactionXdr)
	return args.Get(0).(entities.RPCSendTransactionResult), args.Error(1)
}

func (r *RPCServiceMock) GetTransaction(transactionHash string) (entities.RPCGetTransactionResult, error) {
	args := r.Called(transactionHash)
	return args.Get(0).(entities.RPCGetTransactionResult), args.Error(1)
}

func (r *RPCServiceMock) GetTransactions(startLedger int64, startCursor string, limit int) (entities.RPCGetTransactionsResult, error) {
	args := r.Called(startLedger, startCursor, limit)
	return args.Get(0).(entities.RPCGetTransactionsResult), args.Error(1)
}

func (r *RPCServiceMock) GetHealth() (entities.RPCGetHealthResult, error) {
	args := r.Called()
	return args.Get(0).(entities.RPCGetHealthResult), args.Error(1)
}

func (r *RPCServiceMock) GetLedgers(startLedger uint32, limit uint32) (GetLedgersResponse, error) {
	args := r.Called(startLedger, limit)
	return args.Get(0).(GetLedgersResponse), args.Error(1)
}

func (r *RPCServiceMock) GetLedgerEntries(keys []string) (entities.RPCGetLedgerEntriesResult, error) {
	args := r.Called(keys)
	return args.Get(0).(entities.RPCGetLedgerEntriesResult), args.Error(1)
}

func (r *RPCServiceMock) GetAccountLedgerSequence(address string) (int64, error) {
	args := r.Called(address)
	return args.Get(0).(int64), args.Error(1)
}

func (r *RPCServiceMock) SimulateTransaction(transactionXDR string, resourceConfig entities.RPCResourceConfig) (entities.RPCSimulateTransactionResult, error) {
	args := r.Called(transactionXDR, resourceConfig)
	return args.Get(0).(entities.RPCSimulateTransactionResult), args.Error(1)
}

func (r *RPCServiceMock) NetworkPassphrase() string {
	args := r.Called()
	return args.String(0)
}

// NewRPCServiceMock creates a new instance of RPCServiceMock. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
// The first argument is typically a *testing.T value.
func NewRPCServiceMock(t interface {
	mock.TestingT
	Cleanup(func())
},
) *RPCServiceMock {
	mock := &RPCServiceMock{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}

type AccountTokenServiceMock struct {
	mock.Mock
}

var _ AccountTokenService = (*AccountTokenServiceMock)(nil)

func (a *AccountTokenServiceMock) GetCheckpointLedger() uint32 {
	args := a.Called()
	return args.Get(0).(uint32)
}

func (a *AccountTokenServiceMock) PopulateAccountTokens(ctx context.Context) error {
	args := a.Called(ctx)
	return args.Error(0)
}

func (a *AccountTokenServiceMock) AddTrustlines(ctx context.Context, accountAddress string, assets []string) error {
	args := a.Called(ctx, accountAddress, assets)
	return args.Error(0)
}

func (a *AccountTokenServiceMock) AddContracts(ctx context.Context, accountAddress string, contractIDs []string) error {
	args := a.Called(ctx, accountAddress, contractIDs)
	return args.Error(0)
}

func (a *AccountTokenServiceMock) GetAccountTrustlines(ctx context.Context, accountAddress string) ([]string, error) {
	args := a.Called(ctx, accountAddress)
	return args.Get(0).([]string), args.Error(1)
}

func (a *AccountTokenServiceMock) GetAccountContracts(ctx context.Context, accountAddress string) ([]string, error) {
	args := a.Called(ctx, accountAddress)
	return args.Get(0).([]string), args.Error(1)
}

func (a *AccountTokenServiceMock) ProcessTokenChanges(ctx context.Context, trustlineChanges []types.TrustlineChange, contractChanges []types.ContractChange) error {
	args := a.Called(ctx, trustlineChanges, contractChanges)
	return args.Error(0)
}

// NewAccountTokenServiceMock creates a new instance of AccountTokenServiceMock. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
// The first argument is typically a *testing.T value.
func NewAccountTokenServiceMock(t interface {
	mock.TestingT
	Cleanup(func())
},
) *AccountTokenServiceMock {
	mock := &AccountTokenServiceMock{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}
