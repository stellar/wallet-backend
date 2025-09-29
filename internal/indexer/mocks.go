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

var (
	_ ParticipantsProcessorInterface  = &MockParticipantsProcessor{}
	_ TokenTransferProcessorInterface = &MockTokenTransferProcessor{}
	_ OperationProcessorInterface     = &MockOperationProcessor{}
)
