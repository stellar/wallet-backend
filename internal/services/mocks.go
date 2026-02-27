package services

import (
	"context"

	"github.com/jackc/pgx/v5"
	"github.com/stellar/go-stellar-sdk/historyarchive"
	"github.com/stellar/go-stellar-sdk/ingest"
	"github.com/stellar/go-stellar-sdk/ingest/ledgerbackend"
	"github.com/stellar/go-stellar-sdk/xdr"
	"github.com/stretchr/testify/mock"

	"github.com/stellar/wallet-backend/internal/data"
	"github.com/stellar/wallet-backend/internal/entities"
	"github.com/stellar/wallet-backend/internal/indexer"
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

// TokenIngestionServiceMock is a mock implementation of the TokenIngestionService interface
type TokenIngestionServiceMock struct {
	mock.Mock
}

var _ TokenIngestionService = (*TokenIngestionServiceMock)(nil)

func (m *TokenIngestionServiceMock) NewTokenProcessor(dbTx pgx.Tx, checkpointLedger uint32, contractValidator ContractValidator) TokenProcessor {
	args := m.Called(dbTx, checkpointLedger, contractValidator)
	return args.Get(0).(TokenProcessor)
}

func (m *TokenIngestionServiceMock) ProcessTokenChanges(ctx context.Context, dbTx pgx.Tx, trustlineChangesByTrustlineKey map[indexer.TrustlineChangeKey]types.TrustlineChange, contractChanges []types.ContractChange, accountChangesByAccountID map[string]types.AccountChange, sacBalanceChangesByKey map[indexer.SACBalanceChangeKey]types.SACBalanceChange) error {
	args := m.Called(ctx, dbTx, trustlineChangesByTrustlineKey, contractChanges, accountChangesByAccountID, sacBalanceChangesByKey)
	return args.Error(0)
}

// NewTokenIngestionServiceMock creates a new instance of TokenIngestionServiceMock.
func NewTokenIngestionServiceMock(t interface {
	mock.TestingT
	Cleanup(func())
},
) *TokenIngestionServiceMock {
	mock := &TokenIngestionServiceMock{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}

// TokenProcessorMock is a mock implementation of the TokenProcessor interface
type TokenProcessorMock struct {
	mock.Mock
}

var _ TokenProcessor = (*TokenProcessorMock)(nil)

func (m *TokenProcessorMock) ProcessEntry(ctx context.Context, change ingest.Change) error {
	args := m.Called(ctx, change)
	return args.Error(0)
}

func (m *TokenProcessorMock) ProcessContractCode(ctx context.Context, wasmHash xdr.Hash, wasmCode []byte) error {
	args := m.Called(ctx, wasmHash, wasmCode)
	return args.Error(0)
}

func (m *TokenProcessorMock) FlushBatchIfNeeded(ctx context.Context) error {
	args := m.Called(ctx)
	return args.Error(0)
}

func (m *TokenProcessorMock) FlushRemainingBatch(ctx context.Context) error {
	args := m.Called(ctx)
	return args.Error(0)
}

func (m *TokenProcessorMock) Finalize(ctx context.Context, dbTx pgx.Tx) error {
	args := m.Called(ctx, dbTx)
	return args.Error(0)
}

// NewTokenProcessorMock creates a new instance of TokenProcessorMock.
func NewTokenProcessorMock(t interface {
	mock.TestingT
	Cleanup(func())
},
) *TokenProcessorMock {
	mock := &TokenProcessorMock{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}

// WasmIngestionServiceMock is a mock implementation of the WasmIngestionService interface
type WasmIngestionServiceMock struct {
	mock.Mock
}

var _ WasmIngestionService = (*WasmIngestionServiceMock)(nil)

func (m *WasmIngestionServiceMock) ProcessContractCode(ctx context.Context, wasmHash xdr.Hash, wasmCode []byte) error {
	args := m.Called(ctx, wasmHash, wasmCode)
	return args.Error(0)
}

func (m *WasmIngestionServiceMock) PersistProtocolWasms(ctx context.Context, dbTx pgx.Tx) error {
	args := m.Called(ctx, dbTx)
	return args.Error(0)
}

// NewWasmIngestionServiceMock creates a new instance of WasmIngestionServiceMock.
func NewWasmIngestionServiceMock(t interface {
	mock.TestingT
	Cleanup(func())
},
) *WasmIngestionServiceMock {
	mock := &WasmIngestionServiceMock{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}

// HistoryArchiveMock is a mock implementation of the historyarchive.ArchiveInterface
type HistoryArchiveMock struct {
	mock.Mock
}

var _ historyarchive.ArchiveInterface = (*HistoryArchiveMock)(nil)

func (m *HistoryArchiveMock) GetPathHAS(path string) (historyarchive.HistoryArchiveState, error) {
	args := m.Called(path)
	return args.Get(0).(historyarchive.HistoryArchiveState), args.Error(1)
}

func (m *HistoryArchiveMock) PutPathHAS(path string, has historyarchive.HistoryArchiveState, opts *historyarchive.CommandOptions) error {
	args := m.Called(path, has, opts)
	return args.Error(0)
}

func (m *HistoryArchiveMock) BucketExists(bucket historyarchive.Hash) (bool, error) {
	args := m.Called(bucket)
	return args.Bool(0), args.Error(1)
}

func (m *HistoryArchiveMock) BucketSize(bucket historyarchive.Hash) (int64, error) {
	args := m.Called(bucket)
	return args.Get(0).(int64), args.Error(1)
}

func (m *HistoryArchiveMock) CategoryCheckpointExists(cat string, chk uint32) (bool, error) {
	args := m.Called(cat, chk)
	return args.Bool(0), args.Error(1)
}

func (m *HistoryArchiveMock) GetLedgerHeader(chk uint32) (xdr.LedgerHeaderHistoryEntry, error) {
	args := m.Called(chk)
	return args.Get(0).(xdr.LedgerHeaderHistoryEntry), args.Error(1)
}

func (m *HistoryArchiveMock) GetRootHAS() (historyarchive.HistoryArchiveState, error) {
	args := m.Called()
	return args.Get(0).(historyarchive.HistoryArchiveState), args.Error(1)
}

func (m *HistoryArchiveMock) GetLedgers(start, end uint32) (map[uint32]*historyarchive.Ledger, error) {
	args := m.Called(start, end)
	return args.Get(0).(map[uint32]*historyarchive.Ledger), args.Error(1)
}

func (m *HistoryArchiveMock) GetLatestLedgerSequence() (uint32, error) {
	args := m.Called()
	return args.Get(0).(uint32), args.Error(1)
}

func (m *HistoryArchiveMock) GetCheckpointHAS(chk uint32) (historyarchive.HistoryArchiveState, error) {
	args := m.Called(chk)
	return args.Get(0).(historyarchive.HistoryArchiveState), args.Error(1)
}

func (m *HistoryArchiveMock) PutCheckpointHAS(chk uint32, has historyarchive.HistoryArchiveState, opts *historyarchive.CommandOptions) error {
	args := m.Called(chk, has, opts)
	return args.Error(0)
}

func (m *HistoryArchiveMock) PutRootHAS(has historyarchive.HistoryArchiveState, opts *historyarchive.CommandOptions) error {
	args := m.Called(has, opts)
	return args.Error(0)
}

func (m *HistoryArchiveMock) ListBucket(dp historyarchive.DirPrefix) (chan string, chan error) {
	args := m.Called(dp)
	return args.Get(0).(chan string), args.Get(1).(chan error)
}

func (m *HistoryArchiveMock) ListAllBuckets() (chan string, chan error) {
	args := m.Called()
	return args.Get(0).(chan string), args.Get(1).(chan error)
}

func (m *HistoryArchiveMock) ListAllBucketHashes() (chan historyarchive.Hash, chan error) {
	args := m.Called()
	return args.Get(0).(chan historyarchive.Hash), args.Get(1).(chan error)
}

func (m *HistoryArchiveMock) ListCategoryCheckpoints(cat string, pth string) (chan uint32, chan error) {
	args := m.Called(cat, pth)
	return args.Get(0).(chan uint32), args.Get(1).(chan error)
}

func (m *HistoryArchiveMock) GetXdrStreamForHash(hash historyarchive.Hash) (*xdr.Stream, error) {
	args := m.Called(hash)
	return args.Get(0).(*xdr.Stream), args.Error(1)
}

func (m *HistoryArchiveMock) GetXdrStream(pth string) (*xdr.Stream, error) {
	args := m.Called(pth)
	return args.Get(0).(*xdr.Stream), args.Error(1)
}

func (m *HistoryArchiveMock) GetCheckpointManager() historyarchive.CheckpointManager {
	args := m.Called()
	return args.Get(0).(historyarchive.CheckpointManager)
}

func (m *HistoryArchiveMock) GetStats() []historyarchive.ArchiveStats {
	args := m.Called()
	return args.Get(0).([]historyarchive.ArchiveStats)
}

// ContractValidatorMock is a mock implementation of the ContractValidator interface
type ContractValidatorMock struct {
	mock.Mock
}

var _ ContractValidator = (*ContractValidatorMock)(nil)

func (m *ContractValidatorMock) ValidateFromContractCode(ctx context.Context, wasmCode []byte) (types.ContractType, error) {
	args := m.Called(ctx, wasmCode)
	return args.Get(0).(types.ContractType), args.Error(1)
}

func (m *ContractValidatorMock) Close(ctx context.Context) error {
	args := m.Called(ctx)
	return args.Error(0)
}

// NewContractValidatorMock creates a new instance of ContractValidatorMock.
func NewContractValidatorMock(t interface {
	mock.TestingT
	Cleanup(func())
},
) *ContractValidatorMock {
	mock := &ContractValidatorMock{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}

// ProtocolValidatorMock is a mock implementation of the ProtocolValidator interface
type ProtocolValidatorMock struct {
	mock.Mock
}

var _ ProtocolValidator = (*ProtocolValidatorMock)(nil)

func (m *ProtocolValidatorMock) ProtocolID() string {
	args := m.Called()
	return args.String(0)
}

func (m *ProtocolValidatorMock) Validate(ctx context.Context, wasmCode []byte) (bool, error) {
	args := m.Called(ctx, wasmCode)
	return args.Bool(0), args.Error(1)
}

// NewProtocolValidatorMock creates a new instance of ProtocolValidatorMock.
func NewProtocolValidatorMock(t interface {
	mock.TestingT
	Cleanup(func())
},
) *ProtocolValidatorMock {
	mock := &ProtocolValidatorMock{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}

// ChangeReaderMock is a mock implementation of the ChangeReader interface
type ChangeReaderMock struct {
	mock.Mock
}

var _ ingest.ChangeReader = (*ChangeReaderMock)(nil)

func (m *ChangeReaderMock) Read() (ingest.Change, error) {
	args := m.Called()
	return args.Get(0).(ingest.Change), args.Error(1)
}

func (m *ChangeReaderMock) Close() error {
	args := m.Called()
	return args.Error(0)
}

// NewChangeReaderMock creates a new instance of ChangeReaderMock.
func NewChangeReaderMock(t interface {
	mock.TestingT
	Cleanup(func())
},
) *ChangeReaderMock {
	mock := &ChangeReaderMock{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}

// ContractMetadataServiceMock is a mock implementation of ContractMetadataService
type ContractMetadataServiceMock struct {
	mock.Mock
}

var _ ContractMetadataService = (*ContractMetadataServiceMock)(nil)

func (c *ContractMetadataServiceMock) FetchSep41Metadata(ctx context.Context, contractIDs []string) ([]*data.Contract, error) {
	args := c.Called(ctx, contractIDs)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]*data.Contract), args.Error(1)
}

func (c *ContractMetadataServiceMock) FetchSACMetadata(ctx context.Context, contractIDs []string) ([]*data.Contract, error) {
	args := c.Called(ctx, contractIDs)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]*data.Contract), args.Error(1)
}

func (c *ContractMetadataServiceMock) FetchSingleField(ctx context.Context, contractAddress, functionName string, funcArgs ...xdr.ScVal) (xdr.ScVal, error) {
	args := c.Called(ctx, contractAddress, functionName, funcArgs)
	return args.Get(0).(xdr.ScVal), args.Error(1)
}

func (c *ContractMetadataServiceMock) FetchMetadata(ctx context.Context, contractTypesByID map[string]types.ContractType) ([]*data.Contract, error) {
	args := c.Called(ctx, contractTypesByID)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]*data.Contract), args.Error(1)
}

// NewContractMetadataServiceMock creates a new instance of ContractMetadataServiceMock.
func NewContractMetadataServiceMock(t interface {
	mock.TestingT
	Cleanup(func())
},
) *ContractMetadataServiceMock {
	mock := &ContractMetadataServiceMock{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}
