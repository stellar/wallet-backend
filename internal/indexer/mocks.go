package indexer

import (
	"context"

	set "github.com/deckarep/golang-set/v2"
	"github.com/stellar/go-stellar-sdk/ingest"
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

func (m *MockOperationProcessor) ProcessOperation(ctx context.Context, opWrapper *processors.TransactionOperationWrapper) ([]types.StateChange, error) {
	args := m.Called(ctx, opWrapper)
	return args.Get(0).([]types.StateChange), args.Error(1)
}

func (m *MockOperationProcessor) Name() string {
	args := m.Called()
	return args.String(0)
}

type MockTrustlinesProcessor struct {
	mock.Mock
}

func (m *MockTrustlinesProcessor) ProcessOperation(ctx context.Context, opWrapper *processors.TransactionOperationWrapper) ([]types.TrustlineChange, error) {
	args := m.Called(ctx, opWrapper)
	return args.Get(0).([]types.TrustlineChange), args.Error(1)
}

func (m *MockTrustlinesProcessor) Name() string {
	args := m.Called()
	return args.String(0)
}

type MockAccountsProcessor struct {
	mock.Mock
}

func (m *MockAccountsProcessor) ProcessOperation(ctx context.Context, opWrapper *processors.TransactionOperationWrapper) ([]types.AccountChange, error) {
	args := m.Called(ctx, opWrapper)
	return args.Get(0).([]types.AccountChange), args.Error(1)
}

func (m *MockAccountsProcessor) Name() string {
	args := m.Called()
	return args.String(0)
}

type MockSACBalancesProcessor struct {
	mock.Mock
}

func (m *MockSACBalancesProcessor) ProcessOperation(ctx context.Context, opWrapper *processors.TransactionOperationWrapper) ([]types.SACBalanceChange, error) {
	args := m.Called(ctx, opWrapper)
	return args.Get(0).([]types.SACBalanceChange), args.Error(1)
}

func (m *MockSACBalancesProcessor) Name() string {
	args := m.Called()
	return args.String(0)
}

var (
	_ ParticipantsProcessorInterface                = &MockParticipantsProcessor{}
	_ TokenTransferProcessorInterface               = &MockTokenTransferProcessor{}
	_ OperationProcessorInterface                   = &MockOperationProcessor{}
	_ LedgerChangeProcessor[types.TrustlineChange]  = &MockTrustlinesProcessor{}
	_ LedgerChangeProcessor[types.AccountChange]    = &MockAccountsProcessor{}
	_ LedgerChangeProcessor[types.SACBalanceChange] = &MockSACBalancesProcessor{}
)
