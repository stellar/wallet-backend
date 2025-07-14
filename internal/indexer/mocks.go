package indexer

import (
	"context"

	"github.com/stretchr/testify/mock"

	"github.com/stellar/go/ingest"
	"github.com/stellar/go/xdr"
	"github.com/stellar/wallet-backend/internal/processors"
	"github.com/stellar/wallet-backend/internal/indexer/types"

	set "github.com/deckarep/golang-set/v2"
)

// Test interfaces for mocking
type ParticipantsProcessorInterface interface {
	GetTransactionParticipants(transaction ingest.LedgerTransaction) (set.Set[string], error)
	GetOperationsParticipants(transaction ingest.LedgerTransaction) (map[int64]processors.OperationParticipants, error)
}

type TokenTransferProcessorInterface interface {
	ProcessTransaction(ctx context.Context, tx ingest.LedgerTransaction) ([]types.StateChange, error)
}

type EffectsProcessorInterface interface {
	ProcessOperation(ctx context.Context, tx ingest.LedgerTransaction, op xdr.Operation, opIdx uint32) ([]types.StateChange, error)
}

type IndexerBufferInterface interface {
	PushParticipantTransaction(participant string, tx types.Transaction)
	PushParticipantOperation(participant string, op types.Operation, tx types.Transaction)
	PushStateChanges(stateChanges []types.StateChange)
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

func (m *MockEffectsProcessor) ProcessOperation(ctx context.Context, tx ingest.LedgerTransaction, op xdr.Operation, opIdx uint32) ([]types.StateChange, error) {
	args := m.Called(ctx, tx, op, opIdx)
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