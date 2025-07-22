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

type OperationProcessorInterface interface {
	ProcessOperation(ctx context.Context, opWrapper *operation_processor.TransactionOperationWrapper) ([]types.StateChange, error)
}

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

type MockEffectsProcessor struct {
	mock.Mock
}

func (m *MockEffectsProcessor) ProcessOperation(ctx context.Context, opWrapper *operation_processor.TransactionOperationWrapper) ([]types.StateChange, error) {
	args := m.Called(ctx, opWrapper)
	return args.Get(0).([]types.StateChange), args.Error(1)
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

func (m *MockIndexerBuffer) PushStateChanges(stateChanges []types.StateChange) {
	m.Called(stateChanges)
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

func (m *MockIndexerBuffer) GetAllStateChanges() []types.StateChange {
	args := m.Called()
	return args.Get(0).([]types.StateChange)
}

var (
	_ IndexerBufferInterface          = &MockIndexerBuffer{}
	_ ParticipantsProcessorInterface  = &MockParticipantsProcessor{}
	_ TokenTransferProcessorInterface = &MockTokenTransferProcessor{}
	_ OperationProcessorInterface     = &MockEffectsProcessor{}
)
