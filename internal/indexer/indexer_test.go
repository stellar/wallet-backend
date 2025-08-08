// Package indexer provides transaction indexing functionality for the wallet backend
package indexer

import (
	"context"
	"errors"
	"testing"

	set "github.com/deckarep/golang-set/v2"
	"github.com/stellar/go/ingest"
	"github.com/stellar/go/network"
	operation_processor "github.com/stellar/go/processors/operation"
	"github.com/stellar/go/xdr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/stellar/wallet-backend/internal/indexer/processors"
	"github.com/stellar/wallet-backend/internal/indexer/types"
)

var (
	accountA = xdr.MustMuxedAddress("GBXGQJWVLWOYHFLVTKWV5FGHA3LNYY2JQKM7OAJAUEQFU6LPCSEFVXON")
	accountB = xdr.MustMuxedAddress("GCCOBXW2XQNUSL467IEILE6MMCNRR66SSVL4YQADUNYYNUVREF3FIV2Z")
	oneUnit  = xdr.Int64(1e7)

	testLcm = xdr.LedgerCloseMeta{
		V: int32(0),
		V0: &xdr.LedgerCloseMetaV0{
			LedgerHeader: xdr.LedgerHeaderHistoryEntry{
				Header: xdr.LedgerHeader{
					LedgerVersion: 20,
					LedgerSeq:     xdr.Uint32(12345),
					ScpValue:      xdr.StellarValue{CloseTime: xdr.TimePoint(12345 * 100)},
				},
			},
			TxSet:              xdr.TransactionSet{},
			TxProcessing:       nil,
			UpgradesProcessing: nil,
			ScpInfo:            nil,
		},
		V1: nil,
	}

	testTx = ingest.LedgerTransaction{
		Index:  1,
		Ledger: testLcm,
		Hash:   xdr.Hash{1, 2, 3},
		Envelope: xdr.TransactionEnvelope{
			Type: xdr.EnvelopeTypeEnvelopeTypeTx,
			V1: &xdr.TransactionV1Envelope{
				Tx: xdr.Transaction{
					SourceAccount: xdr.MustMuxedAddress("GBF3XFXGBGNQDN3HOSZ7NVRF6TJ2JOD5U6ELIWJOOEI6T5WKMQT2YSXQ"),
					SeqNum:        xdr.SequenceNumber(54321),
				},
			},
		},
		Result: xdr.TransactionResultPair{
			TransactionHash: xdr.Hash{1, 2, 3},
			Result: xdr.TransactionResult{
				FeeCharged: xdr.Int64(100),
				Result: xdr.TransactionResultResult{
					Code:    xdr.TransactionResultCodeTxSuccess,
					Results: &[]xdr.OperationResult{},
				},
			},
		},
		UnsafeMeta: xdr.TransactionMeta{
			V: 3,
			V3: &xdr.TransactionMetaV3{
				Operations: []xdr.OperationMeta{{}},
			},
		},
	}

	createAccountOp = xdr.Operation{
		SourceAccount: &accountA,
		Body: xdr.OperationBody{
			Type: xdr.OperationTypeCreateAccount,
			CreateAccountOp: &xdr.CreateAccountOp{
				Destination:     accountB.ToAccountId(),
				StartingBalance: 100 * oneUnit,
			},
		},
	}
)

func TestIndexer_ProcessTransaction(t *testing.T) {
	tests := []struct {
		name            string
		setupMocks      func(*MockParticipantsProcessor, *MockTokenTransferProcessor, *MockEffectsProcessor, *MockIndexerBuffer)
		wantError       string
		txParticipants  set.Set[string]
		opsParticipants map[int64]processors.OperationParticipants
	}{
		{
			name: "🟢 successful processing with participants",
			setupMocks: func(mockParticipants *MockParticipantsProcessor, mockTokenTransfer *MockTokenTransferProcessor, mockEffects *MockEffectsProcessor, mockBuffer *MockIndexerBuffer) {
				participants := set.NewSet("alice", "bob")
				mockParticipants.On("GetTransactionParticipants", mock.Anything).Return(participants, nil)

				opParticipants := map[int64]processors.OperationParticipants{
					1: {
						OpWrapper: &operation_processor.TransactionOperationWrapper{
							Index:          0,
							Operation:      createAccountOp,
							Network:        network.TestNetworkPassphrase,
							LedgerSequence: 12345,
						},
						Participants: set.NewSet("alice"),
					},
				}
				mockParticipants.On("GetOperationsParticipants", mock.Anything).Return(opParticipants, nil)

				tokenStateChanges := []types.StateChange{{ToID: 1, StateChangeOrder: 1}}
				mockTokenTransfer.On("ProcessTransaction", mock.Anything, mock.Anything).Return(tokenStateChanges, nil)

				effectsStateChanges := []types.StateChange{{ToID: 1, StateChangeOrder: 1}}
				mockEffects.On("ProcessOperation", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(effectsStateChanges, nil)

				// Verify transaction was pushed to buffer with correct participants
				// PushParticipantTransaction is called once for each participant
				mockBuffer.On("PushParticipantTransaction", "alice",
					mock.MatchedBy(func(tx types.Transaction) bool {
						return tx.Hash == "0102030000000000000000000000000000000000000000000000000000000000"
					})).Return()
				mockBuffer.On("PushParticipantTransaction", "bob",
					mock.MatchedBy(func(tx types.Transaction) bool {
						return tx.Hash == "0102030000000000000000000000000000000000000000000000000000000000"
					})).Return()
				mockBuffer.On("CalculateStateChangeOrder").Return()

				// Verify operation was pushed to buffer with correct data
				// PushParticipantOperation is called once for each participant of each operation
				mockBuffer.On("PushParticipantOperation", "alice",
					mock.MatchedBy(func(op types.Operation) bool {
						return op.OperationType == types.OperationTypeCreateAccount
					}),
					mock.MatchedBy(func(tx types.Transaction) bool {
						return tx.Hash == "0102030000000000000000000000000000000000000000000000000000000000"
					})).Return()

				// Verify state changes were pushed to buffer
				// PushStateChanges is called separately for effects and token transfer state changes
				mockBuffer.On("PushStateChanges",
					mock.MatchedBy(func(stateChanges []types.StateChange) bool {
						return len(stateChanges) == 1 && stateChanges[0].ToID == 1 && stateChanges[0].StateChangeOrder == 1
					})).Return()
				mockBuffer.On("PushStateChanges",
					mock.MatchedBy(func(stateChanges []types.StateChange) bool {
						return len(stateChanges) == 1 && stateChanges[0].ToID == 1 && stateChanges[0].StateChangeOrder == 1
					})).Return()
			},
			txParticipants: set.NewSet("alice", "bob"),
			opsParticipants: map[int64]processors.OperationParticipants{
				1: {
					OpWrapper: &operation_processor.TransactionOperationWrapper{
						Index:          0,
						Operation:      createAccountOp,
						Network:        network.TestNetworkPassphrase,
						LedgerSequence: 12345,
					},
					Participants: set.NewSet("alice"),
				},
			},
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

				// Verify only empty state changes are pushed (no participant operations/transactions)
				// PushStateChanges is still called for token transfer state changes (even if empty)
				mockBuffer.On("PushStateChanges",
					mock.MatchedBy(func(stateChanges []types.StateChange) bool {
						return len(stateChanges) == 0
					})).Return()
				mockBuffer.On("CalculateStateChangeOrder").Return()
			},
			txParticipants:  set.NewSet[string](),
			opsParticipants: map[int64]processors.OperationParticipants{},
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
						OpWrapper: &operation_processor.TransactionOperationWrapper{
							Index:          0,
							Operation:      createAccountOp,
							Network:        network.TestNetworkPassphrase,
							LedgerSequence: 12345,
						},
						Participants: set.NewSet("alice"),
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
			indexer := &Indexer{
				Buffer:                 mockBuffer,
				participantsProcessor:  mockParticipants,
				tokenTransferProcessor: mockTokenTransfer,
				effectsProcessor:       mockEffects,
			}

			err := indexer.ProcessTransaction(context.Background(), testTx)

			// Assert results
			if tt.wantError != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantError)
			} else {
				require.NoError(t, err)

				// Additional assertions for successful cases
				if len(tt.txParticipants.ToSlice()) > 0 {
					// Verify PushParticipantTransaction was called when there are participants
					mockBuffer.AssertCalled(t, "PushParticipantTransaction", mock.Anything, mock.Anything)
				} else {
					// Verify PushParticipantTransaction was NOT called when there are no participants
					mockBuffer.AssertNotCalled(t, "PushParticipantTransaction", mock.Anything, mock.Anything)
				}

				if len(tt.opsParticipants) > 0 {
					// Verify PushParticipantOperation was called when there are operation participants
					mockBuffer.AssertCalled(t, "PushParticipantOperation", mock.Anything, mock.Anything, mock.Anything)
				} else {
					// Verify PushParticipantOperation was NOT called when there are no operation participants
					mockBuffer.AssertNotCalled(t, "PushParticipantOperation", mock.Anything, mock.Anything, mock.Anything)
				}

				// PushStateChanges should always be called
				mockBuffer.AssertCalled(t, "PushStateChanges", mock.Anything)
				mockBuffer.AssertCalled(t, "CalculateStateChangeOrder")
			}

			// Verify all mock expectations were met
			mockParticipants.AssertExpectations(t)
			mockTokenTransfer.AssertExpectations(t)
			mockEffects.AssertExpectations(t)
			mockBuffer.AssertExpectations(t)
		})
	}
}
