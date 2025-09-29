package indexer

import (
	"context"

	set "github.com/deckarep/golang-set/v2"
	"github.com/stellar/go/ingest"
	operation_processor "github.com/stellar/go/processors/operation"
	"github.com/stretchr/testify/mock"

	"github.com/stellar/wallet-backend/internal/indexer/processors"
	"github.com/stellar/wallet-backend/internal/indexer/types"
)

// Mock implementations for testing
type MockParticipantsProcessor struct {
	mock.Mock
}

func (m *MockParticipantsProcessor) GetTransactionParticipants(transaction ingest.LedgerTransaction) (set.Set[string], error) {
	args := m.Called(transaction)
	return args.Get(0).(set.Set[string]), args.Error(1)
}

func (m *MockParticipantsProcessor) GetOperationsParticipants(transaction ingest.LedgerTransaction) (map[int64]processors.OperationParticipants, error) {
	args := m.Called(transaction)
	return args.Get(0).(map[int64]processors.OperationParticipants), args.Error(1)
}

type MockTokenTransferProcessor struct {
	mock.Mock
}

func (m *MockTokenTransferProcessor) ProcessTransaction(ctx context.Context, tx ingest.LedgerTransaction) ([]types.StateChange, error) {
	args := m.Called(ctx, tx)
	return args.Get(0).([]types.StateChange), args.Error(1)
}

type MockOperationProcessor struct {
	mock.Mock
}

func (m *MockOperationProcessor) ProcessOperation(ctx context.Context, opWrapper *operation_processor.TransactionOperationWrapper) ([]types.StateChange, error) {
	args := m.Called(ctx, opWrapper)
	return args.Get(0).([]types.StateChange), args.Error(1)
}

func (m *MockOperationProcessor) Name() string {
	args := m.Called()
	return args.String(0)
}

type MockIndexerBuffer struct {
	mock.Mock
}

func (m *MockIndexerBuffer) PushParticipantTransaction(participant string, tx types.Transaction) {
	m.Called(participant, tx)
}

func (m *MockIndexerBuffer) PushParticipantOperation(participant string, op types.Operation, tx types.Transaction) {
	m.Called(participant, op, tx)
}

func (m *MockIndexerBuffer) PushStateChange(tx types.Transaction, op types.Operation, stateChange types.StateChange) {
	m.Called(tx, op, stateChange)
}

func (m *MockIndexerBuffer) GetParticipantTransactions(participant string) []types.Transaction {
	args := m.Called(participant)
	return args.Get(0).([]types.Transaction)
}

func (m *MockIndexerBuffer) GetParticipantOperations(participant string) map[int64]types.Operation {
	args := m.Called(participant)
	return args.Get(0).(map[int64]types.Operation)
}

func (m *MockIndexerBuffer) GetParticipants() set.Set[string] {
	args := m.Called()
	return args.Get(0).(set.Set[string])
}

func (m *MockIndexerBuffer) GetNumberOfTransactions() int {
	args := m.Called()
	return args.Get(0).(int)
}

func (m *MockIndexerBuffer) GetAllTransactions() []types.Transaction {
	args := m.Called()
	return args.Get(0).([]types.Transaction)
}

func (m *MockIndexerBuffer) GetAllOperations() []types.Operation {
	args := m.Called()
	return args.Get(0).([]types.Operation)
}

func (m *MockIndexerBuffer) GetAllStateChanges() []types.StateChange {
	args := m.Called()
	return args.Get(0).([]types.StateChange)
}

func (m *MockIndexerBuffer) MergeBuffer(other IndexerBufferInterface) {
	m.Called(other)
}

var (
	_ IndexerBufferInterface          = &MockIndexerBuffer{}
	_ ParticipantsProcessorInterface  = &MockParticipantsProcessor{}
	_ TokenTransferProcessorInterface = &MockTokenTransferProcessor{}
	_ OperationProcessorInterface     = &MockOperationProcessor{}
)
