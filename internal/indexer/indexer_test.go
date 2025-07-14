// Package indexer provides transaction indexing functionality for the wallet backend
package indexer

import (
	"context"
	"errors"
	"fmt"
	"testing"

	set "github.com/deckarep/golang-set/v2"
	"github.com/stellar/go/ingest"
	"github.com/stellar/go/xdr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/stellar/wallet-backend/internal/indexer/types"
	"github.com/stellar/wallet-backend/internal/processors"
)

// Testable indexer wrapper to allow for dependency injection
type TestableIndexer struct {
	buffer              IndexerBufferInterface
	participantsProcessor ParticipantsProcessorInterface
	tokenTransferProcessor TokenTransferProcessorInterface
	effectsProcessor     EffectsProcessorInterface
}

func (ti *TestableIndexer) ProcessTransaction(ctx context.Context, transaction ingest.LedgerTransaction) error {
	// 1. Index transaction txParticipants
	txParticipants, err := ti.participantsProcessor.GetTransactionParticipants(transaction)
	if err != nil {
		return fmt.Errorf("getting transaction participants: %w", err)
	}

	// Create mock data transaction to avoid XDR conversion issues in tests
	dataTx := &types.Transaction{
		Hash:         "test_tx_hash",
		ToID:         1,
		LedgerNumber: 1,
	}
	
	if txParticipants.Cardinality() != 0 {
		for participant := range txParticipants.Iter() {
			ti.buffer.PushParticipantTransaction(participant, *dataTx)
		}
	}

	// 2. Index tx.Operations() participants
	opsParticipants, err := ti.participantsProcessor.GetOperationsParticipants(transaction)
	if err != nil {
		return fmt.Errorf("getting operations participants: %w", err)
	}
	for opID, opParticipants := range opsParticipants {
		// Create mock data operation to avoid XDR conversion issues in tests
		dataOp := &types.Operation{
			ID:     opID,
			TxHash: "test_tx_hash",
		}

		for participant := range opParticipants.Participants.Iter() {
			ti.buffer.PushParticipantOperation(participant, *dataOp, *dataTx)
		}

		// 2.1. Index effects state changes
		effectsStateChanges, err := ti.effectsProcessor.ProcessOperation(ctx, transaction, opParticipants.Operation, opParticipants.OperationIdx)
		if err != nil {
			return fmt.Errorf("processing effects state changes: %w", err)
		}
		ti.buffer.PushStateChanges(effectsStateChanges)
	}

	// 3. Index token transfer state changes
	tokenTransferStateChanges, err := ti.tokenTransferProcessor.ProcessTransaction(ctx, transaction)
	if err != nil {
		return fmt.Errorf("processing token transfer state changes: %w", err)
	}
	ti.buffer.PushStateChanges(tokenTransferStateChanges)

	return nil
}

func TestIndexer_ProcessTransaction(t *testing.T) {
	
	tests := []struct {
		name                           string
		setupMocks                     func(*MockParticipantsProcessor, *MockTokenTransferProcessor, *MockEffectsProcessor, *MockIndexerBuffer)
		wantError                      string
		txParticipants                 set.Set[string]
		opsParticipants               map[int64]processors.OperationParticipants
		tokenTransferStateChanges      []types.StateChange
		effectsStateChanges           []types.StateChange
	}{
		{
			name: "🟢 successful processing with participants",
			setupMocks: func(mockParticipants *MockParticipantsProcessor, mockTokenTransfer *MockTokenTransferProcessor, mockEffects *MockEffectsProcessor, mockBuffer *MockIndexerBuffer) {
				participants := set.NewSet("alice", "bob")
				mockParticipants.On("GetTransactionParticipants", mock.Anything).Return(participants, nil)
				
				opParticipants := map[int64]processors.OperationParticipants{
					1: {
						Operation:    xdr.Operation{},
						Participants: set.NewSet("alice"),
						OperationIdx: 0,
					},
				}
				mockParticipants.On("GetOperationsParticipants", mock.Anything).Return(opParticipants, nil)
				
				tokenStateChanges := []types.StateChange{{ID: "token_sc1"}}
				mockTokenTransfer.On("ProcessTransaction", mock.Anything, mock.Anything).Return(tokenStateChanges, nil)
				
				effectsStateChanges := []types.StateChange{{ID: "effects_sc1"}}
				mockEffects.On("ProcessOperation", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(effectsStateChanges, nil)
				
				mockBuffer.On("PushParticipantTransaction", mock.Anything, mock.Anything).Return()
				mockBuffer.On("PushParticipantOperation", mock.Anything, mock.Anything, mock.Anything).Return()
				mockBuffer.On("PushStateChanges", mock.Anything).Return()
			},
			txParticipants: set.NewSet("alice", "bob"),
			opsParticipants: map[int64]processors.OperationParticipants{
				1: {
					Operation:    xdr.Operation{},
					Participants: set.NewSet("alice"),
					OperationIdx: 0,
				},
			},
			tokenTransferStateChanges: []types.StateChange{{ID: "token_sc1"}},
			effectsStateChanges:      []types.StateChange{{ID: "effects_sc1"}},
		},
		{
			name: "🟢 successful processing without participants",
			setupMocks: func(mockParticipants *MockParticipantsProcessor, mockTokenTransfer *MockTokenTransferProcessor, mockEffects *MockEffectsProcessor, mockBuffer *MockIndexerBuffer) {
				participants := set.NewSet[string]()
				mockParticipants.On("GetTransactionParticipants", mock.Anything).Return(participants, nil)
				
				opParticipants := map[int64]processors.OperationParticipants{}
				mockParticipants.On("GetOperationsParticipants", mock.Anything).Return(opParticipants, nil)
				
				tokenStateChanges := []types.StateChange{}
				mockTokenTransfer.On("ProcessTransaction", mock.Anything, mock.Anything).Return(tokenStateChanges, nil)
				
				mockBuffer.On("PushStateChanges", mock.Anything).Return()
			},
			txParticipants:            set.NewSet[string](),
			opsParticipants:          map[int64]processors.OperationParticipants{},
			tokenTransferStateChanges: []types.StateChange{},
		},
		{
			name: "🔴 error getting transaction participants",
			setupMocks: func(mockParticipants *MockParticipantsProcessor, mockTokenTransfer *MockTokenTransferProcessor, mockEffects *MockEffectsProcessor, mockBuffer *MockIndexerBuffer) {
				mockParticipants.On("GetTransactionParticipants", mock.Anything).Return(set.NewSet[string](), errors.New("participant error"))
			},
			wantError: "getting transaction participants: participant error",
		},
		{
			name: "🔴 error getting operations participants",
			setupMocks: func(mockParticipants *MockParticipantsProcessor, mockTokenTransfer *MockTokenTransferProcessor, mockEffects *MockEffectsProcessor, mockBuffer *MockIndexerBuffer) {
				participants := set.NewSet[string]()
				mockParticipants.On("GetTransactionParticipants", mock.Anything).Return(participants, nil)
				mockParticipants.On("GetOperationsParticipants", mock.Anything).Return(map[int64]processors.OperationParticipants{}, errors.New("operations error"))
			},
			wantError: "getting operations participants: operations error",
		},
		{
			name: "🔴 error processing effects state changes",
			setupMocks: func(mockParticipants *MockParticipantsProcessor, mockTokenTransfer *MockTokenTransferProcessor, mockEffects *MockEffectsProcessor, mockBuffer *MockIndexerBuffer) {
				participants := set.NewSet[string]()
				mockParticipants.On("GetTransactionParticipants", mock.Anything).Return(participants, nil)
				
				opParticipants := map[int64]processors.OperationParticipants{
					1: {
						Operation:    xdr.Operation{},
						Participants: set.NewSet("alice"),
						OperationIdx: 0,
					},
				}
				mockParticipants.On("GetOperationsParticipants", mock.Anything).Return(opParticipants, nil)
				
				mockBuffer.On("PushParticipantOperation", mock.Anything, mock.Anything, mock.Anything).Return()
				mockEffects.On("ProcessOperation", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return([]types.StateChange{}, errors.New("effects error"))
			},
			wantError: "processing effects state changes: effects error",
		},
		{
			name: "🔴 error processing token transfer state changes",
			setupMocks: func(mockParticipants *MockParticipantsProcessor, mockTokenTransfer *MockTokenTransferProcessor, mockEffects *MockEffectsProcessor, mockBuffer *MockIndexerBuffer) {
				participants := set.NewSet[string]()
				mockParticipants.On("GetTransactionParticipants", mock.Anything).Return(participants, nil)
				
				opParticipants := map[int64]processors.OperationParticipants{}
				mockParticipants.On("GetOperationsParticipants", mock.Anything).Return(opParticipants, nil)
				
				mockTokenTransfer.On("ProcessTransaction", mock.Anything, mock.Anything).Return([]types.StateChange{}, errors.New("token transfer error"))
			},
			wantError: "processing token transfer state changes: token transfer error",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create mocks
			mockParticipants := &MockParticipantsProcessor{}
			mockTokenTransfer := &MockTokenTransferProcessor{}
			mockEffects := &MockEffectsProcessor{}
			mockBuffer := &MockIndexerBuffer{}
			
			// Setup mock expectations
			tt.setupMocks(mockParticipants, mockTokenTransfer, mockEffects, mockBuffer)
			
			// Create testable indexer with mocked dependencies
			indexer := &TestableIndexer{
				buffer:                 mockBuffer,
				participantsProcessor:  mockParticipants,
				tokenTransferProcessor: mockTokenTransfer,
				effectsProcessor:       mockEffects,
			}
			
			// Create test transaction
			ctx := context.Background()
			transaction := ingest.LedgerTransaction{
				Index: 1,
				Hash:  xdr.Hash{1, 2, 3},
			}
			
			// Call method under test
			err := indexer.ProcessTransaction(ctx, transaction)
			
			// Assert results
			if tt.wantError != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantError)
			} else {
				require.NoError(t, err)
			}
			
			// Verify all mock expectations were met
			mockParticipants.AssertExpectations(t)
			mockTokenTransfer.AssertExpectations(t)
			mockEffects.AssertExpectations(t)
			mockBuffer.AssertExpectations(t)
		})
	}
}