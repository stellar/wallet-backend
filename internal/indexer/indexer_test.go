// Package indexer provides transaction indexing functionality for the wallet backend
package indexer

import (
	"context"
	"errors"
	"runtime"
	"testing"

	"github.com/alitto/pond/v2"
	set "github.com/deckarep/golang-set/v2"
	"github.com/stellar/go/ingest"
	"github.com/stellar/go/network"
	operation_processor "github.com/stellar/go/processors/operation"
	"github.com/stellar/go/xdr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/stellar/wallet-backend/internal/entities"
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

	testTx2 = ingest.LedgerTransaction{
		Index:  2,
		Ledger: testLcm,
		Hash:   xdr.Hash{4, 5, 6},
		Envelope: xdr.TransactionEnvelope{
			Type: xdr.EnvelopeTypeEnvelopeTypeTx,
			V1: &xdr.TransactionV1Envelope{
				Tx: xdr.Transaction{
					SourceAccount: xdr.MustMuxedAddress("GBXGQJWVLWOYHFLVTKWV5FGHA3LNYY2JQKM7OAJAUEQFU6LPCSEFVXON"),
					SeqNum:        xdr.SequenceNumber(54322),
				},
			},
		},
		Result: xdr.TransactionResultPair{
			TransactionHash: xdr.Hash{4, 5, 6},
			Result: xdr.TransactionResult{
				FeeCharged: xdr.Int64(200),
				Result: xdr.TransactionResultResult{
					Code:    xdr.TransactionResultCodeTxSuccess,
					Results: &[]xdr.OperationResult{},
				},
			},
		},
		UnsafeMeta: xdr.TransactionMeta{
			V: 3,
			V3: &xdr.TransactionMetaV3{
				Operations: []xdr.OperationMeta{{}, {}},
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

	paymentOp = xdr.Operation{
		SourceAccount: &accountB,
		Body: xdr.OperationBody{
			Type: xdr.OperationTypePayment,
			PaymentOp: &xdr.PaymentOp{
				Destination: accountA,
				Asset:       xdr.Asset{Type: xdr.AssetTypeAssetTypeNative},
				Amount:      50 * oneUnit,
			},
		},
	}
)

// MockLedgerEntryProvider is a mock for LedgerEntryProvider
type MockLedgerEntryProvider struct {
	mock.Mock
}

func (m *MockLedgerEntryProvider) GetLedgerEntries(keys []string) (entities.RPCGetLedgerEntriesResult, error) {
	args := m.Called(keys)
	return args.Get(0).(entities.RPCGetLedgerEntriesResult), args.Error(1)
}

func TestIndexer_NewIndexer(t *testing.T) {
	networkPassphrase := network.TestNetworkPassphrase
	mockLedgerEntryProvider := &MockLedgerEntryProvider{}
	pool := pond.NewPool(runtime.NumCPU())

	indexer := NewIndexer(networkPassphrase, mockLedgerEntryProvider, pool)

	require.NotNil(t, indexer)
	assert.NotNil(t, indexer.participantsProcessor)
	assert.NotNil(t, indexer.tokenTransferProcessor)
	assert.NotNil(t, indexer.processors)
	assert.NotNil(t, indexer.pool)
	assert.Len(t, indexer.processors, 3) // effects, contract deploy, SAC events
}

func TestIndexer_CollectAllTransactionData(t *testing.T) {
	tests := []struct {
		name                 string
		transactions         []ingest.LedgerTransaction
		setupMocks           func(*MockParticipantsProcessor, *MockTokenTransferProcessor, *MockOperationProcessor, *MockOperationProcessor, *MockOperationProcessor)
		expectedParticipants set.Set[string]
		expectedTxDataCount  int
		expectedError        string
	}{
		{
			name:         "🟢 single transaction with participants",
			transactions: []ingest.LedgerTransaction{testTx},
			setupMocks: func(mockParticipants *MockParticipantsProcessor, mockTokenTransfer *MockTokenTransferProcessor, mockEffects *MockOperationProcessor, mockContractDeploy *MockOperationProcessor, mockSACEvents *MockOperationProcessor) {
				// Transaction participants
				txParticipants := set.NewSet("alice", "bob")
				mockParticipants.On("GetTransactionParticipants", testTx).Return(txParticipants, nil)

				// Operation participants
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
				mockParticipants.On("GetOperationsParticipants", testTx).Return(opParticipants, nil)

				// State changes from processors
				mockEffects.On("ProcessOperation", mock.Anything, mock.Anything).Return([]types.StateChange{
					{ToID: 1, AccountID: "alice", OperationID: 1, SortKey: "1-1"},
				}, nil)
				mockContractDeploy.On("ProcessOperation", mock.Anything, mock.Anything).Return([]types.StateChange{
					{ToID: 2, AccountID: "charlie", OperationID: 1, SortKey: "1-2"},
				}, nil)
				mockSACEvents.On("ProcessOperation", mock.Anything, mock.Anything).Return([]types.StateChange{}, nil)

				// Token transfer state changes
				mockTokenTransfer.On("ProcessTransaction", mock.Anything, testTx).Return([]types.StateChange{
					{ToID: 3, AccountID: "dave", OperationID: 0, SortKey: "0-1"},
				}, nil)
			},
			expectedParticipants: set.NewSet("alice", "bob", "charlie", "dave"),
			expectedTxDataCount:  1,
		},
		{
			name:         "🟢 multiple transactions with overlapping participants",
			transactions: []ingest.LedgerTransaction{testTx, testTx2},
			setupMocks: func(mockParticipants *MockParticipantsProcessor, mockTokenTransfer *MockTokenTransferProcessor, mockEffects *MockOperationProcessor, mockContractDeploy *MockOperationProcessor, mockSACEvents *MockOperationProcessor) {
				// First transaction
				txParticipants1 := set.NewSet("alice", "bob")
				mockParticipants.On("GetTransactionParticipants", testTx).Return(txParticipants1, nil)
				opParticipants1 := map[int64]processors.OperationParticipants{
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
				mockParticipants.On("GetOperationsParticipants", testTx).Return(opParticipants1, nil)

				// Second transaction with overlapping participants
				txParticipants2 := set.NewSet("bob", "charlie")
				mockParticipants.On("GetTransactionParticipants", testTx2).Return(txParticipants2, nil)
				opParticipants2 := map[int64]processors.OperationParticipants{
					2: {
						OpWrapper: &operation_processor.TransactionOperationWrapper{
							Index:          0,
							Operation:      paymentOp,
							Network:        network.TestNetworkPassphrase,
							LedgerSequence: 12345,
						},
						Participants: set.NewSet("charlie"),
					},
				}
				mockParticipants.On("GetOperationsParticipants", testTx2).Return(opParticipants2, nil)

				// Mock processors for both transactions
				mockEffects.On("ProcessOperation", mock.Anything, mock.Anything).Return([]types.StateChange{}, nil)
				mockContractDeploy.On("ProcessOperation", mock.Anything, mock.Anything).Return([]types.StateChange{}, nil)
				mockSACEvents.On("ProcessOperation", mock.Anything, mock.Anything).Return([]types.StateChange{}, nil)

				mockTokenTransfer.On("ProcessTransaction", mock.Anything, testTx).Return([]types.StateChange{}, nil)
				mockTokenTransfer.On("ProcessTransaction", mock.Anything, testTx2).Return([]types.StateChange{}, nil)
			},
			expectedParticipants: set.NewSet("alice", "bob", "charlie"),
			expectedTxDataCount:  2,
		},
		{
			name:         "🟢 empty transaction list",
			transactions: []ingest.LedgerTransaction{},
			setupMocks: func(mockParticipants *MockParticipantsProcessor, mockTokenTransfer *MockTokenTransferProcessor, mockEffects *MockOperationProcessor, mockContractDeploy *MockOperationProcessor, mockSACEvents *MockOperationProcessor) {
				// No mock expectations for empty list
			},
			expectedParticipants: set.NewSet[string](),
			expectedTxDataCount:  0,
		},
		{
			name:         "🟢 transaction with no participants",
			transactions: []ingest.LedgerTransaction{testTx},
			setupMocks: func(mockParticipants *MockParticipantsProcessor, mockTokenTransfer *MockTokenTransferProcessor, mockEffects *MockOperationProcessor, mockContractDeploy *MockOperationProcessor, mockSACEvents *MockOperationProcessor) {
				// Empty participants
				mockParticipants.On("GetTransactionParticipants", testTx).Return(set.NewSet[string](), nil)
				mockParticipants.On("GetOperationsParticipants", testTx).Return(map[int64]processors.OperationParticipants{}, nil)
				mockTokenTransfer.On("ProcessTransaction", mock.Anything, testTx).Return([]types.StateChange{}, nil)
			},
			expectedParticipants: set.NewSet[string](),
			expectedTxDataCount:  1,
		},
		{
			name:         "🔴 error getting transaction participants",
			transactions: []ingest.LedgerTransaction{testTx},
			setupMocks: func(mockParticipants *MockParticipantsProcessor, mockTokenTransfer *MockTokenTransferProcessor, mockEffects *MockOperationProcessor, mockContractDeploy *MockOperationProcessor, mockSACEvents *MockOperationProcessor) {
				mockParticipants.On("GetTransactionParticipants", testTx).Return(set.NewSet[string](), errors.New("participant error"))
			},
			expectedError: "getting transaction participants: participant error",
		},
		{
			name:         "🔴 error getting operations participants",
			transactions: []ingest.LedgerTransaction{testTx},
			setupMocks: func(mockParticipants *MockParticipantsProcessor, mockTokenTransfer *MockTokenTransferProcessor, mockEffects *MockOperationProcessor, mockContractDeploy *MockOperationProcessor, mockSACEvents *MockOperationProcessor) {
				mockParticipants.On("GetTransactionParticipants", testTx).Return(set.NewSet[string](), nil)
				mockParticipants.On("GetOperationsParticipants", testTx).Return(map[int64]processors.OperationParticipants{}, errors.New("operations error"))
			},
			expectedError: "getting operations participants: operations error",
		},
		{
			name:         "🔴 error in token transfer processor",
			transactions: []ingest.LedgerTransaction{testTx},
			setupMocks: func(mockParticipants *MockParticipantsProcessor, mockTokenTransfer *MockTokenTransferProcessor, mockEffects *MockOperationProcessor, mockContractDeploy *MockOperationProcessor, mockSACEvents *MockOperationProcessor) {
				mockParticipants.On("GetTransactionParticipants", testTx).Return(set.NewSet[string](), nil)
				mockParticipants.On("GetOperationsParticipants", testTx).Return(map[int64]processors.OperationParticipants{}, nil)
				mockTokenTransfer.On("ProcessTransaction", mock.Anything, testTx).Return([]types.StateChange{}, errors.New("token transfer error"))
			},
			expectedError: "processing token transfer state changes: token transfer error",
		},
		{
			name:         "🔴 error in operation processor",
			transactions: []ingest.LedgerTransaction{testTx},
			setupMocks: func(mockParticipants *MockParticipantsProcessor, mockTokenTransfer *MockTokenTransferProcessor, mockEffects *MockOperationProcessor, mockContractDeploy *MockOperationProcessor, mockSACEvents *MockOperationProcessor) {
				mockParticipants.On("GetTransactionParticipants", testTx).Return(set.NewSet[string](), nil)
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
				mockParticipants.On("GetOperationsParticipants", testTx).Return(opParticipants, nil)
				mockEffects.On("ProcessOperation", mock.Anything, mock.Anything).Return([]types.StateChange{}, errors.New("effects error"))
				mockEffects.On("Name").Return("effects")
			},
			expectedError: "processing effects state changes: effects error",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create mocks
			mockParticipants := &MockParticipantsProcessor{}
			mockTokenTransfer := &MockTokenTransferProcessor{}
			mockEffects := &MockOperationProcessor{}
			mockContractDeploy := &MockOperationProcessor{}
			mockSACEvents := &MockOperationProcessor{}

			// Setup mock expectations
			tt.setupMocks(mockParticipants, mockTokenTransfer, mockEffects, mockContractDeploy, mockSACEvents)

			// Create indexer with mocked dependencies
			indexer := &Indexer{
				participantsProcessor:  mockParticipants,
				tokenTransferProcessor: mockTokenTransfer,
				processors:             []OperationProcessorInterface{mockEffects, mockContractDeploy, mockSACEvents},
				pool:                   pond.NewPool(runtime.NumCPU()),
			}

			// Test CollectAllTransactionData
			precomputedData, allParticipants, err := indexer.CollectAllTransactionData(context.Background(), tt.transactions)

			// Assert results
			if tt.expectedError != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.expectedError)
				assert.Nil(t, precomputedData)
				assert.Nil(t, allParticipants)
			} else {
				require.NoError(t, err)
				assert.Len(t, precomputedData, tt.expectedTxDataCount)
				assert.Equal(t, tt.expectedParticipants, allParticipants)

				// Verify each PrecomputedTransactionData structure
				for i, txData := range precomputedData {
					// Since the processing is done in parallel, the order may not match
					// Instead, verify that each transaction exists in the expected set
					txFound := false
					for _, expectedTx := range tt.transactions {
						if txData.Transaction.Index == expectedTx.Index &&
							txData.Transaction.Hash == expectedTx.Hash {
							txFound = true
							break
						}
					}
					assert.True(t, txFound, "Transaction at index %d should be found in expected transactions", i)
					assert.NotNil(t, txData.TxParticipants)
					assert.NotNil(t, txData.OpsParticipants)
					assert.NotNil(t, txData.StateChanges)
					assert.NotNil(t, txData.AllParticipants)

					// Additional detailed assertions based on transaction
					if txData.Transaction.Index == 1 && tt.name == "🟢 single transaction with participants" {
						// Verify exact transaction participants
						assert.Equal(t, 2, txData.TxParticipants.Cardinality())
						assert.True(t, txData.TxParticipants.Contains("alice"))
						assert.True(t, txData.TxParticipants.Contains("bob"))

						// Verify operations participants
						assert.Contains(t, txData.OpsParticipants, int64(1))
						opParticipants := txData.OpsParticipants[int64(1)]
						assert.Equal(t, 1, opParticipants.Participants.Cardinality())
						assert.True(t, opParticipants.Participants.Contains("alice"))

						// Verify state changes
						assert.Len(t, txData.StateChanges, 3)
						// Find each state change and verify fields
						for _, sc := range txData.StateChanges {
							switch sc.AccountID {
							case "alice":
								assert.Equal(t, int64(1), sc.ToID)
								assert.Equal(t, int64(1), sc.OperationID)
								assert.Equal(t, "1-1", sc.SortKey)
							case "charlie":
								assert.Equal(t, int64(2), sc.ToID)
								assert.Equal(t, int64(1), sc.OperationID)
								assert.Equal(t, "1-2", sc.SortKey)
							case "dave":
								assert.Equal(t, int64(3), sc.ToID)
								assert.Equal(t, int64(0), sc.OperationID)
								assert.Equal(t, "0-1", sc.SortKey)
							}
						}

						// Verify AllParticipants is union of all
						assert.Equal(t, 4, txData.AllParticipants.Cardinality())
						assert.True(t, txData.AllParticipants.Contains("alice"))
						assert.True(t, txData.AllParticipants.Contains("bob"))
						assert.True(t, txData.AllParticipants.Contains("charlie"))
						assert.True(t, txData.AllParticipants.Contains("dave"))
					}

					if tt.name == "🟢 multiple transactions with overlapping participants" {
						// Verify first transaction
						if txData.Transaction.Index == 1 {
							assert.Equal(t, 2, txData.TxParticipants.Cardinality())
							assert.True(t, txData.TxParticipants.Contains("alice"))
							assert.True(t, txData.TxParticipants.Contains("bob"))
							assert.Contains(t, txData.OpsParticipants, int64(1))
						}
						// Verify second transaction
						if txData.Transaction.Index == 2 {
							assert.Equal(t, 2, txData.TxParticipants.Cardinality())
							assert.True(t, txData.TxParticipants.Contains("bob"))
							assert.True(t, txData.TxParticipants.Contains("charlie"))
							assert.Contains(t, txData.OpsParticipants, int64(2))
						}
					}

					if tt.name == "🟢 transaction with no participants" {
						assert.Equal(t, 0, txData.TxParticipants.Cardinality())
						assert.Equal(t, 0, len(txData.OpsParticipants))
						assert.Equal(t, 0, txData.AllParticipants.Cardinality())
					}
				}
			}

			// Verify all mock expectations
			mockParticipants.AssertExpectations(t)
			mockTokenTransfer.AssertExpectations(t)
			mockEffects.AssertExpectations(t)
			mockContractDeploy.AssertExpectations(t)
			mockSACEvents.AssertExpectations(t)
		})
	}
}

func TestIndexer_ProcessTransactions(t *testing.T) {
	tests := []struct {
		name             string
		precomputedData  []PrecomputedTransactionData
		existingAccounts set.Set[string]
		verifyBuffer     func(*testing.T, *IndexerBuffer)
		expectedError    string
	}{
		{
			name: "🟢 process with all existing accounts",
			precomputedData: []PrecomputedTransactionData{
				{
					Transaction:    testTx,
					TxParticipants: set.NewSet("alice", "bob"),
					OpsParticipants: map[int64]processors.OperationParticipants{
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
					StateChanges: []types.StateChange{
						{ToID: 1, AccountID: "alice", OperationID: 1, SortKey: "1-1"},
					},
					AllParticipants: set.NewSet("alice", "bob"),
				},
			},
			existingAccounts: set.NewSet("alice", "bob"),
			verifyBuffer: func(t *testing.T, buffer *IndexerBuffer) {
				// Verify participants were added correctly
				assert.Equal(t, 2, buffer.GetParticipants().Cardinality(), "should have 2 participants")
				assert.True(t, buffer.GetParticipants().Contains("alice"), "alice should be a participant")
				assert.True(t, buffer.GetParticipants().Contains("bob"), "bob should be a participant")

				// Verify transactions for alice
				aliceTxs := buffer.GetParticipantTransactions("alice")
				require.Len(t, aliceTxs, 1, "alice should have 1 transaction")
				assert.Equal(t, "0102030000000000000000000000000000000000000000000000000000000000", aliceTxs[0].Hash)
				assert.Equal(t, uint32(12345), aliceTxs[0].LedgerNumber)

				// Verify transactions for bob
				bobTxs := buffer.GetParticipantTransactions("bob")
				require.Len(t, bobTxs, 1, "bob should have 1 transaction")
				assert.Equal(t, "0102030000000000000000000000000000000000000000000000000000000000", bobTxs[0].Hash)

				// Verify operations for alice (only alice is in operation participants)
				aliceOps := buffer.GetParticipantOperations("alice")
				require.Len(t, aliceOps, 1, "alice should have 1 operation")
				assert.Equal(t, int64(1), aliceOps[1].ID)
				assert.Equal(t, "0102030000000000000000000000000000000000000000000000000000000000", aliceOps[1].TxHash)

				// Verify bob has no operations (not in operation participants)
				bobOps := buffer.GetParticipantOperations("bob")
				assert.Nil(t, bobOps, "bob should have no operations")

				// Verify state changes
				stateChanges := buffer.GetAllStateChanges()
				require.Len(t, stateChanges, 1, "should have 1 state change")
				assert.Equal(t, "alice", stateChanges[0].AccountID)
				assert.Equal(t, int64(1), stateChanges[0].ToID)
				assert.Equal(t, int64(1), stateChanges[0].OperationID)
				assert.Equal(t, int64(1), stateChanges[0].StateChangeOrder, "first state change should have order 1")
			},
		},
		{
			name: "🟢 process with mixed existing/non-existing accounts",
			precomputedData: []PrecomputedTransactionData{
				{
					Transaction:     testTx,
					TxParticipants:  set.NewSet("alice", "bob", "charlie"),
					OpsParticipants: map[int64]processors.OperationParticipants{},
					StateChanges: []types.StateChange{
						{ToID: 1, AccountID: "alice", OperationID: 0, SortKey: "0-1"},
						{ToID: 2, AccountID: "charlie", OperationID: 0, SortKey: "0-2"},
					},
					AllParticipants: set.NewSet("alice", "bob", "charlie"),
				},
			},
			existingAccounts: set.NewSet("alice", "bob"), // charlie doesn't exist
			verifyBuffer: func(t *testing.T, buffer *IndexerBuffer) {
				// Verify only existing accounts are processed (alice and bob, NOT charlie)
				assert.Equal(t, 2, buffer.GetParticipants().Cardinality(), "should have 2 participants (not 3)")
				assert.True(t, buffer.GetParticipants().Contains("alice"), "alice should be a participant")
				assert.True(t, buffer.GetParticipants().Contains("bob"), "bob should be a participant")
				assert.False(t, buffer.GetParticipants().Contains("charlie"), "charlie should NOT be a participant (doesn't exist)")

				// Verify transactions for alice
				aliceTxs := buffer.GetParticipantTransactions("alice")
				require.Len(t, aliceTxs, 1, "alice should have 1 transaction")
				assert.Equal(t, "0102030000000000000000000000000000000000000000000000000000000000", aliceTxs[0].Hash)

				// Verify transactions for bob
				bobTxs := buffer.GetParticipantTransactions("bob")
				require.Len(t, bobTxs, 1, "bob should have 1 transaction")

				// Verify charlie has no transactions (doesn't exist)
				charlieTxs := buffer.GetParticipantTransactions("charlie")
				assert.Nil(t, charlieTxs, "charlie should have no transactions (doesn't exist)")

				// Verify state changes - only alice should have one (charlie's should be skipped)
				stateChanges := buffer.GetAllStateChanges()
				require.Len(t, stateChanges, 1, "should have 1 state change (alice only, charlie skipped)")
				assert.Equal(t, "alice", stateChanges[0].AccountID, "only alice's state change should be present")
				assert.Equal(t, int64(1), stateChanges[0].ToID)
				assert.Equal(t, int64(0), stateChanges[0].OperationID, "fee state change (OperationID=0)")
				assert.Equal(t, int64(1), stateChanges[0].StateChangeOrder, "fee state changes get order 1")
			},
		},
		{
			name: "🟢 process with no existing accounts",
			precomputedData: []PrecomputedTransactionData{
				{
					Transaction:     testTx,
					TxParticipants:  set.NewSet("alice", "bob"),
					OpsParticipants: map[int64]processors.OperationParticipants{},
					StateChanges:    []types.StateChange{},
					AllParticipants: set.NewSet("alice", "bob"),
				},
			},
			existingAccounts: set.NewSet[string](), // No existing accounts
			verifyBuffer: func(t *testing.T, buffer *IndexerBuffer) {
				// Verify no accounts are processed since none exist
				assert.Equal(t, 0, buffer.GetParticipants().Cardinality(), "should have 0 participants (no existing accounts)")

				// Verify no transactions
				assert.Equal(t, 0, buffer.GetNumberOfTransactions(), "should have 0 transactions")

				// Verify no state changes
				stateChanges := buffer.GetAllStateChanges()
				assert.Len(t, stateChanges, 0, "should have 0 state changes")
			},
		},
		{
			name:             "🟢 empty precomputed data",
			precomputedData:  []PrecomputedTransactionData{},
			existingAccounts: set.NewSet("alice"),
			verifyBuffer: func(t *testing.T, buffer *IndexerBuffer) {
				// Verify buffer is empty with no precomputed data
				assert.Equal(t, 0, buffer.GetParticipants().Cardinality(), "should have 0 participants (no data)")
				assert.Equal(t, 0, buffer.GetNumberOfTransactions(), "should have 0 transactions")
				stateChanges := buffer.GetAllStateChanges()
				assert.Len(t, stateChanges, 0, "should have 0 state changes")
			},
		},
		{
			name: "🟢 multiple state changes per operation verify ordering",
			precomputedData: []PrecomputedTransactionData{
				{
					Transaction:    testTx,
					TxParticipants: set.NewSet("alice"),
					OpsParticipants: map[int64]processors.OperationParticipants{
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
					StateChanges: []types.StateChange{
						{ToID: 1, AccountID: "alice", OperationID: 1, SortKey: "1-1"},
						{ToID: 2, AccountID: "alice", OperationID: 1, SortKey: "1-2"},
						{ToID: 3, AccountID: "alice", OperationID: 1, SortKey: "1-3"},
					},
					AllParticipants: set.NewSet("alice"),
				},
			},
			existingAccounts: set.NewSet("alice"),
			verifyBuffer: func(t *testing.T, buffer *IndexerBuffer) {
				// Verify participant
				assert.Equal(t, 1, buffer.GetParticipants().Cardinality(), "should have 1 participant")
				assert.True(t, buffer.GetParticipants().Contains("alice"), "alice should be a participant")

				// Verify transaction
				aliceTxs := buffer.GetParticipantTransactions("alice")
				require.Len(t, aliceTxs, 1, "alice should have 1 transaction")

				// Verify operation
				aliceOps := buffer.GetParticipantOperations("alice")
				require.Len(t, aliceOps, 1, "alice should have 1 operation")
				assert.Equal(t, int64(1), aliceOps[1].ID)

				// Verify state changes with correct ordering
				stateChanges := buffer.GetAllStateChanges()
				require.Len(t, stateChanges, 3, "should have 3 state changes")

				// Verify first state change
				assert.Equal(t, "alice", stateChanges[0].AccountID)
				assert.Equal(t, int64(1), stateChanges[0].ToID)
				assert.Equal(t, int64(1), stateChanges[0].OperationID)
				assert.Equal(t, int64(1), stateChanges[0].StateChangeOrder, "first state change should have order 1")

				// Verify second state change
				assert.Equal(t, "alice", stateChanges[1].AccountID)
				assert.Equal(t, int64(2), stateChanges[1].ToID)
				assert.Equal(t, int64(1), stateChanges[1].OperationID)
				assert.Equal(t, int64(2), stateChanges[1].StateChangeOrder, "second state change should have order 2")

				// Verify third state change
				assert.Equal(t, "alice", stateChanges[2].AccountID)
				assert.Equal(t, int64(3), stateChanges[2].ToID)
				assert.Equal(t, int64(1), stateChanges[2].OperationID)
				assert.Equal(t, int64(3), stateChanges[2].StateChangeOrder, "third state change should have order 3")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create real buffer for actual testing
			realBuffer := NewIndexerBuffer()

			// Create indexer
			indexer := &Indexer{
				participantsProcessor:  &MockParticipantsProcessor{},
				tokenTransferProcessor: &MockTokenTransferProcessor{},
				processors:             []OperationProcessorInterface{},
				pool:                   pond.NewPool(runtime.NumCPU()),
			}

			// Test ProcessTransactions
			err := indexer.ProcessTransactions(context.Background(), tt.precomputedData, tt.existingAccounts, realBuffer)

			// Assert results
			if tt.expectedError != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.expectedError)
			} else {
				require.NoError(t, err)
				// Verify buffer contents after successful processing
				if tt.verifyBuffer != nil {
					tt.verifyBuffer(t, realBuffer)
				}
			}
		})
	}
}

func TestIndexer_getTransactionStateChanges(t *testing.T) {
	tests := []struct {
		name                 string
		transaction          ingest.LedgerTransaction
		opsParticipants      map[int64]processors.OperationParticipants
		setupMocks           func(*MockTokenTransferProcessor, *MockOperationProcessor, *MockOperationProcessor, *MockOperationProcessor)
		expectedStateChanges int
		expectedError        string
	}{
		{
			name:        "🟢 state changes from all processors",
			transaction: testTx,
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
			setupMocks: func(mockTokenTransfer *MockTokenTransferProcessor, mockEffects *MockOperationProcessor, mockContractDeploy *MockOperationProcessor, mockSACEvents *MockOperationProcessor) {
				// Operation processors
				mockEffects.On("ProcessOperation", mock.Anything, mock.Anything).Return([]types.StateChange{
					{ToID: 1, AccountID: "alice", OperationID: 1},
				}, nil)
				mockContractDeploy.On("ProcessOperation", mock.Anything, mock.Anything).Return([]types.StateChange{
					{ToID: 2, AccountID: "bob", OperationID: 1},
				}, nil)
				mockSACEvents.On("ProcessOperation", mock.Anything, mock.Anything).Return([]types.StateChange{}, nil)

				// Token transfer processor
				mockTokenTransfer.On("ProcessTransaction", mock.Anything, testTx).Return([]types.StateChange{
					{ToID: 3, AccountID: "charlie", OperationID: 0},
				}, nil)
			},
			expectedStateChanges: 3,
		},
		{
			name:            "🟢 no operations",
			transaction:     testTx,
			opsParticipants: map[int64]processors.OperationParticipants{},
			setupMocks: func(mockTokenTransfer *MockTokenTransferProcessor, mockEffects *MockOperationProcessor, mockContractDeploy *MockOperationProcessor, mockSACEvents *MockOperationProcessor) {
				mockTokenTransfer.On("ProcessTransaction", mock.Anything, testTx).Return([]types.StateChange{}, nil)
			},
			expectedStateChanges: 0,
		},
		{
			name:        "🔴 error in operation processor",
			transaction: testTx,
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
			setupMocks: func(mockTokenTransfer *MockTokenTransferProcessor, mockEffects *MockOperationProcessor, mockContractDeploy *MockOperationProcessor, mockSACEvents *MockOperationProcessor) {
				mockEffects.On("ProcessOperation", mock.Anything, mock.Anything).Return([]types.StateChange{}, errors.New("processor error"))
				mockEffects.On("Name").Return("effects")
			},
			expectedError: "processing effects state changes: processor error",
		},
		{
			name:            "🔴 error in token transfer processor",
			transaction:     testTx,
			opsParticipants: map[int64]processors.OperationParticipants{},
			setupMocks: func(mockTokenTransfer *MockTokenTransferProcessor, mockEffects *MockOperationProcessor, mockContractDeploy *MockOperationProcessor, mockSACEvents *MockOperationProcessor) {
				mockTokenTransfer.On("ProcessTransaction", mock.Anything, testTx).Return([]types.StateChange{}, errors.New("token transfer error"))
			},
			expectedError: "processing token transfer state changes: token transfer error",
		},
		{
			name:        "🟢 ErrInvalidOpType is ignored",
			transaction: testTx,
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
			setupMocks: func(mockTokenTransfer *MockTokenTransferProcessor, mockEffects *MockOperationProcessor, mockContractDeploy *MockOperationProcessor, mockSACEvents *MockOperationProcessor) {
				// One processor returns ErrInvalidOpType (should be ignored)
				mockEffects.On("ProcessOperation", mock.Anything, mock.Anything).Return([]types.StateChange{}, processors.ErrInvalidOpType)
				// Other processors work normally
				mockContractDeploy.On("ProcessOperation", mock.Anything, mock.Anything).Return([]types.StateChange{
					{ToID: 1, AccountID: "alice", OperationID: 1},
				}, nil)
				mockSACEvents.On("ProcessOperation", mock.Anything, mock.Anything).Return([]types.StateChange{}, nil)

				mockTokenTransfer.On("ProcessTransaction", mock.Anything, testTx).Return([]types.StateChange{}, nil)
			},
			expectedStateChanges: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create mocks
			mockTokenTransfer := &MockTokenTransferProcessor{}
			mockEffects := &MockOperationProcessor{}
			mockContractDeploy := &MockOperationProcessor{}
			mockSACEvents := &MockOperationProcessor{}

			// Setup mock expectations
			tt.setupMocks(mockTokenTransfer, mockEffects, mockContractDeploy, mockSACEvents)

			// Create indexer
			indexer := &Indexer{
				participantsProcessor:  &MockParticipantsProcessor{},
				tokenTransferProcessor: mockTokenTransfer,
				processors:             []OperationProcessorInterface{mockEffects, mockContractDeploy, mockSACEvents},
				pool:                   pond.NewPool(runtime.NumCPU()),
			}

			// Test getTransactionStateChanges
			stateChanges, err := indexer.getTransactionStateChanges(context.Background(), tt.transaction, tt.opsParticipants)

			// Assert results
			if tt.expectedError != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.expectedError)
			} else {
				require.NoError(t, err)
				assert.Len(t, stateChanges, tt.expectedStateChanges)

				// Add detailed assertions for specific test cases
				if tt.name == "🟢 state changes from all processors" {
					// Verify we got exactly 3 state changes
					assert.Len(t, stateChanges, 3)

					// Verify specific state changes by AccountID
					foundAlice := false
					foundBob := false
					foundCharlie := false
					for _, sc := range stateChanges {
						switch sc.AccountID {
						case "alice":
							assert.Equal(t, int64(1), sc.ToID)
							assert.Equal(t, int64(1), sc.OperationID)
							foundAlice = true
						case "bob":
							assert.Equal(t, int64(2), sc.ToID)
							assert.Equal(t, int64(1), sc.OperationID)
							foundBob = true
						case "charlie":
							assert.Equal(t, int64(3), sc.ToID)
							assert.Equal(t, int64(0), sc.OperationID)
							foundCharlie = true
						}
					}
					assert.True(t, foundAlice, "Should have state change for alice")
					assert.True(t, foundBob, "Should have state change for bob")
					assert.True(t, foundCharlie, "Should have state change for charlie")
				}

				if tt.name == "🟢 ErrInvalidOpType is ignored" {
					// Verify we got exactly 1 state change (from contract deploy only)
					assert.Len(t, stateChanges, 1)

					// Verify it's the correct state change
					sc := stateChanges[0]
					assert.Equal(t, "alice", sc.AccountID)
					assert.Equal(t, int64(1), sc.ToID)
					assert.Equal(t, int64(1), sc.OperationID)
				}
			}

			// Verify mock expectations
			mockTokenTransfer.AssertExpectations(t)
			mockEffects.AssertExpectations(t)
			mockContractDeploy.AssertExpectations(t)
			mockSACEvents.AssertExpectations(t)
		})
	}
}
