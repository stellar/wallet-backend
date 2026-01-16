// Package indexer provides transaction indexing functionality for the wallet backend
package indexer

import (
	"context"
	"errors"
	"runtime"
	"testing"

	"github.com/alitto/pond/v2"
	set "github.com/deckarep/golang-set/v2"
	"github.com/stellar/go-stellar-sdk/ingest"
	"github.com/stellar/go-stellar-sdk/network"
	"github.com/stellar/go-stellar-sdk/xdr"
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

func TestIndexer_NewIndexer(t *testing.T) {
	networkPassphrase := network.TestNetworkPassphrase
	pool := pond.NewPool(runtime.NumCPU())

	indexer := NewIndexer(networkPassphrase, pool, nil, false, false)

	require.NotNil(t, indexer)
	assert.NotNil(t, indexer.participantsProcessor)
	assert.NotNil(t, indexer.tokenTransferProcessor)
	assert.NotNil(t, indexer.processors)
	assert.NotNil(t, indexer.pool)
	assert.Len(t, indexer.processors, 3) // effects, contract deploy, SAC events
	assert.False(t, indexer.skipTxMeta)
	assert.False(t, indexer.skipTxEnvelope)
}

func TestIndexer_ProcessLedgerTransactions(t *testing.T) {
	t.Run("🟢 single transaction with participants", func(t *testing.T) {
		// Create mocks
		mockParticipants := &MockParticipantsProcessor{}
		mockTokenTransfer := &MockTokenTransferProcessor{}
		mockEffects := &MockOperationProcessor{}
		mockContractDeploy := &MockOperationProcessor{}
		mockSACEvents := &MockOperationProcessor{}
		mockTrustlines := &MockTrustlinesProcessor{}
		mockAccounts := &MockAccountsProcessor{}
		mockSACBalances := &MockSACBalancesProcessor{}

		// Setup mock expectations
		txParticipants := set.NewSet("alice", "bob")
		mockParticipants.On("GetTransactionParticipants", testTx).Return(txParticipants, nil)

		opParticipants := map[int64]processors.OperationParticipants{
			1: {
				OpWrapper: &processors.TransactionOperationWrapper{
					Index:          0,
					Operation:      createAccountOp,
					Network:        network.TestNetworkPassphrase,
					LedgerSequence: 12345,
				},
				Participants: set.NewSet("alice"),
			},
		}
		mockParticipants.On("GetOperationsParticipants", testTx).Return(opParticipants, nil)

		mockEffects.On("ProcessOperation", mock.Anything, mock.Anything).Return([]types.StateChange{
			{ToID: 1, AccountID: "alice", OperationID: 1, SortKey: "1-1"},
		}, nil)
		mockContractDeploy.On("ProcessOperation", mock.Anything, mock.Anything).Return([]types.StateChange{
			{ToID: 2, AccountID: "charlie", OperationID: 1, SortKey: "1-2"},
		}, nil)
		mockSACEvents.On("ProcessOperation", mock.Anything, mock.Anything).Return([]types.StateChange{}, nil)

		mockTokenTransfer.On("ProcessTransaction", mock.Anything, testTx).Return([]types.StateChange{
			{ToID: 3, AccountID: "dave", OperationID: 0, SortKey: "0-1"},
		}, nil)

		mockTrustlines.On("ProcessOperation", mock.Anything, mock.Anything).Return([]types.TrustlineChange{}, nil)
		mockAccounts.On("ProcessOperation", mock.Anything, mock.Anything).Return([]types.AccountChange{}, nil)
		mockSACBalances.On("ProcessOperation", mock.Anything, mock.Anything).Return([]types.SACBalanceChange{}, nil)

		// Create indexer
		indexer := &Indexer{
			participantsProcessor:  mockParticipants,
			tokenTransferProcessor: mockTokenTransfer,
			trustlinesProcessor:    mockTrustlines,
			accountsProcessor:      mockAccounts,
			sacBalancesProcessor:   mockSACBalances,
			processors:             []OperationProcessorInterface{mockEffects, mockContractDeploy, mockSACEvents},
			pool:                   pond.NewPool(runtime.NumCPU()),
			networkPassphrase:      network.TestNetworkPassphrase,
		}

		// Test ProcessLedgerTransactions
		buffer := NewIndexerBuffer()
		participantCount, err := indexer.ProcessLedgerTransactions(context.Background(), []ingest.LedgerTransaction{testTx}, buffer)

		// Assert results
		require.NoError(t, err)
		assert.Equal(t, 4, participantCount) // alice, bob, charlie, dave

		// Verify transactions
		allTxs := buffer.GetTransactions()
		require.Len(t, allTxs, 1, "should have 1 transaction")

		// Verify transaction participants
		txParticipantsMap := buffer.GetTransactionsParticipants()
		txHash := "0102030000000000000000000000000000000000000000000000000000000000"
		assert.True(t, txParticipantsMap[txHash].Contains("alice"), "alice should be in tx participants")
		assert.True(t, txParticipantsMap[txHash].Contains("bob"), "bob should be in tx participants")

		// Verify operations
		allOps := buffer.GetOperations()
		require.Len(t, allOps, 1, "should have 1 operation")

		// Verify state changes
		stateChanges := buffer.GetStateChanges()
		assert.Len(t, stateChanges, 3, "should have 3 state changes")

		// Verify mock expectations
		mockParticipants.AssertExpectations(t)
		mockTokenTransfer.AssertExpectations(t)
		mockEffects.AssertExpectations(t)
		mockContractDeploy.AssertExpectations(t)
		mockSACEvents.AssertExpectations(t)
		mockTrustlines.AssertExpectations(t)
		mockAccounts.AssertExpectations(t)
	})

	t.Run("🟢 multiple transactions with overlapping participants", func(t *testing.T) {
		// Create mocks
		mockParticipants := &MockParticipantsProcessor{}
		mockTokenTransfer := &MockTokenTransferProcessor{}
		mockEffects := &MockOperationProcessor{}
		mockContractDeploy := &MockOperationProcessor{}
		mockSACEvents := &MockOperationProcessor{}
		mockTrustlines := &MockTrustlinesProcessor{}
		mockAccounts := &MockAccountsProcessor{}
		mockSACBalances := &MockSACBalancesProcessor{}

		// Setup mocks for first transaction
		txParticipants1 := set.NewSet("alice", "bob")
		mockParticipants.On("GetTransactionParticipants", testTx).Return(txParticipants1, nil)
		opParticipants1 := map[int64]processors.OperationParticipants{
			1: {
				OpWrapper: &processors.TransactionOperationWrapper{
					Index:          0,
					Operation:      createAccountOp,
					Network:        network.TestNetworkPassphrase,
					LedgerSequence: 12345,
				},
				Participants: set.NewSet("alice"),
			},
		}
		mockParticipants.On("GetOperationsParticipants", testTx).Return(opParticipants1, nil)

		// Setup mocks for second transaction
		txParticipants2 := set.NewSet("bob", "charlie")
		mockParticipants.On("GetTransactionParticipants", testTx2).Return(txParticipants2, nil)
		opParticipants2 := map[int64]processors.OperationParticipants{
			2: {
				OpWrapper: &processors.TransactionOperationWrapper{
					Index:          0,
					Operation:      paymentOp,
					Network:        network.TestNetworkPassphrase,
					LedgerSequence: 12345,
				},
				Participants: set.NewSet("charlie"),
			},
		}
		mockParticipants.On("GetOperationsParticipants", testTx2).Return(opParticipants2, nil)

		mockEffects.On("ProcessOperation", mock.Anything, mock.Anything).Return([]types.StateChange{}, nil)
		mockContractDeploy.On("ProcessOperation", mock.Anything, mock.Anything).Return([]types.StateChange{}, nil)
		mockSACEvents.On("ProcessOperation", mock.Anything, mock.Anything).Return([]types.StateChange{}, nil)

		mockTokenTransfer.On("ProcessTransaction", mock.Anything, testTx).Return([]types.StateChange{}, nil)
		mockTokenTransfer.On("ProcessTransaction", mock.Anything, testTx2).Return([]types.StateChange{}, nil)

		mockTrustlines.On("ProcessOperation", mock.Anything, mock.Anything).Return([]types.TrustlineChange{}, nil)
		mockAccounts.On("ProcessOperation", mock.Anything, mock.Anything).Return([]types.AccountChange{}, nil)
		mockSACBalances.On("ProcessOperation", mock.Anything, mock.Anything).Return([]types.SACBalanceChange{}, nil)

		// Create indexer
		indexer := &Indexer{
			participantsProcessor:  mockParticipants,
			tokenTransferProcessor: mockTokenTransfer,
			trustlinesProcessor:    mockTrustlines,
			accountsProcessor:      mockAccounts,
			sacBalancesProcessor:   mockSACBalances,
			processors:             []OperationProcessorInterface{mockEffects, mockContractDeploy, mockSACEvents},
			pool:                   pond.NewPool(runtime.NumCPU()),
			networkPassphrase:      network.TestNetworkPassphrase,
		}

		// Test ProcessLedgerTransactions
		buffer := NewIndexerBuffer()
		participantCount, err := indexer.ProcessLedgerTransactions(context.Background(), []ingest.LedgerTransaction{testTx, testTx2}, buffer)

		// Assert results
		require.NoError(t, err)
		// alice, bob from tx1 + bob, charlie from tx2 = 2+2=4 (bob counted twice since per-tx)
		assert.Equal(t, 4, participantCount)

		// Verify transactions
		allTxs := buffer.GetTransactions()
		require.Len(t, allTxs, 2, "should have 2 transactions")

		// Verify mock expectations
		mockParticipants.AssertExpectations(t)
		mockTokenTransfer.AssertExpectations(t)
		mockEffects.AssertExpectations(t)
		mockContractDeploy.AssertExpectations(t)
		mockSACEvents.AssertExpectations(t)
		mockTrustlines.AssertExpectations(t)
		mockAccounts.AssertExpectations(t)
	})

	t.Run("🟢 empty transaction list", func(t *testing.T) {
		// Create mocks
		mockParticipants := &MockParticipantsProcessor{}
		mockTokenTransfer := &MockTokenTransferProcessor{}
		mockEffects := &MockOperationProcessor{}
		mockContractDeploy := &MockOperationProcessor{}
		mockSACEvents := &MockOperationProcessor{}

		// Create indexer
		indexer := &Indexer{
			participantsProcessor:  mockParticipants,
			tokenTransferProcessor: mockTokenTransfer,
			processors:             []OperationProcessorInterface{mockEffects, mockContractDeploy, mockSACEvents},
			pool:                   pond.NewPool(runtime.NumCPU()),
			networkPassphrase:      network.TestNetworkPassphrase,
		}

		// Test ProcessLedgerTransactions
		buffer := NewIndexerBuffer()
		participantCount, err := indexer.ProcessLedgerTransactions(context.Background(), []ingest.LedgerTransaction{}, buffer)

		// Assert results
		require.NoError(t, err)
		assert.Equal(t, 0, participantCount)
		assert.Equal(t, 0, buffer.GetNumberOfTransactions())

		// Verify mock expectations
		mockParticipants.AssertExpectations(t)
		mockTokenTransfer.AssertExpectations(t)
		mockEffects.AssertExpectations(t)
		mockContractDeploy.AssertExpectations(t)
		mockSACEvents.AssertExpectations(t)
	})

	t.Run("🟢 transaction with no participants", func(t *testing.T) {
		// Create mocks
		mockParticipants := &MockParticipantsProcessor{}
		mockTokenTransfer := &MockTokenTransferProcessor{}
		mockEffects := &MockOperationProcessor{}
		mockContractDeploy := &MockOperationProcessor{}
		mockSACEvents := &MockOperationProcessor{}

		// Setup mocks
		mockParticipants.On("GetTransactionParticipants", testTx).Return(set.NewSet[string](), nil)
		mockParticipants.On("GetOperationsParticipants", testTx).Return(map[int64]processors.OperationParticipants{}, nil)
		mockTokenTransfer.On("ProcessTransaction", mock.Anything, testTx).Return([]types.StateChange{}, nil)

		// Create indexer
		indexer := &Indexer{
			participantsProcessor:  mockParticipants,
			tokenTransferProcessor: mockTokenTransfer,
			processors:             []OperationProcessorInterface{mockEffects, mockContractDeploy, mockSACEvents},
			pool:                   pond.NewPool(runtime.NumCPU()),
			networkPassphrase:      network.TestNetworkPassphrase,
		}

		// Test ProcessLedgerTransactions
		buffer := NewIndexerBuffer()
		participantCount, err := indexer.ProcessLedgerTransactions(context.Background(), []ingest.LedgerTransaction{testTx}, buffer)

		// Assert results
		require.NoError(t, err)
		assert.Equal(t, 0, participantCount)

		// Verify mock expectations
		mockParticipants.AssertExpectations(t)
		mockTokenTransfer.AssertExpectations(t)
		mockEffects.AssertExpectations(t)
		mockContractDeploy.AssertExpectations(t)
		mockSACEvents.AssertExpectations(t)
	})

	t.Run("🔴 error getting transaction participants", func(t *testing.T) {
		// Create mocks
		mockParticipants := &MockParticipantsProcessor{}
		mockTokenTransfer := &MockTokenTransferProcessor{}
		mockEffects := &MockOperationProcessor{}
		mockContractDeploy := &MockOperationProcessor{}
		mockSACEvents := &MockOperationProcessor{}

		// Setup mocks
		mockParticipants.On("GetTransactionParticipants", testTx).Return(set.NewSet[string](), errors.New("participant error"))

		// Create indexer
		indexer := &Indexer{
			participantsProcessor:  mockParticipants,
			tokenTransferProcessor: mockTokenTransfer,
			processors:             []OperationProcessorInterface{mockEffects, mockContractDeploy, mockSACEvents},
			pool:                   pond.NewPool(runtime.NumCPU()),
			networkPassphrase:      network.TestNetworkPassphrase,
		}

		// Test ProcessLedgerTransactions
		buffer := NewIndexerBuffer()
		participantCount, err := indexer.ProcessLedgerTransactions(context.Background(), []ingest.LedgerTransaction{testTx}, buffer)

		// Assert results
		require.Error(t, err)
		assert.Contains(t, err.Error(), "getting transaction participants: participant error")
		assert.Equal(t, 0, participantCount)

		// Verify mock expectations
		mockParticipants.AssertExpectations(t)
		mockTokenTransfer.AssertExpectations(t)
		mockEffects.AssertExpectations(t)
		mockContractDeploy.AssertExpectations(t)
		mockSACEvents.AssertExpectations(t)
	})

	t.Run("🔴 error getting operations participants", func(t *testing.T) {
		// Create mocks
		mockParticipants := &MockParticipantsProcessor{}
		mockTokenTransfer := &MockTokenTransferProcessor{}
		mockEffects := &MockOperationProcessor{}
		mockContractDeploy := &MockOperationProcessor{}
		mockSACEvents := &MockOperationProcessor{}

		// Setup mocks
		mockParticipants.On("GetTransactionParticipants", testTx).Return(set.NewSet[string](), nil)
		mockParticipants.On("GetOperationsParticipants", testTx).Return(map[int64]processors.OperationParticipants{}, errors.New("operations error"))

		// Create indexer
		indexer := &Indexer{
			participantsProcessor:  mockParticipants,
			tokenTransferProcessor: mockTokenTransfer,
			processors:             []OperationProcessorInterface{mockEffects, mockContractDeploy, mockSACEvents},
			pool:                   pond.NewPool(runtime.NumCPU()),
			networkPassphrase:      network.TestNetworkPassphrase,
		}

		// Test ProcessLedgerTransactions
		buffer := NewIndexerBuffer()
		participantCount, err := indexer.ProcessLedgerTransactions(context.Background(), []ingest.LedgerTransaction{testTx}, buffer)

		// Assert results
		require.Error(t, err)
		assert.Contains(t, err.Error(), "getting operations participants: operations error")
		assert.Equal(t, 0, participantCount)

		// Verify mock expectations
		mockParticipants.AssertExpectations(t)
		mockTokenTransfer.AssertExpectations(t)
		mockEffects.AssertExpectations(t)
		mockContractDeploy.AssertExpectations(t)
		mockSACEvents.AssertExpectations(t)
	})

	t.Run("🔴 error in token transfer processor", func(t *testing.T) {
		// Create mocks
		mockParticipants := &MockParticipantsProcessor{}
		mockTokenTransfer := &MockTokenTransferProcessor{}
		mockEffects := &MockOperationProcessor{}
		mockContractDeploy := &MockOperationProcessor{}
		mockSACEvents := &MockOperationProcessor{}

		// Setup mocks
		mockParticipants.On("GetTransactionParticipants", testTx).Return(set.NewSet[string](), nil)
		mockParticipants.On("GetOperationsParticipants", testTx).Return(map[int64]processors.OperationParticipants{}, nil)
		mockTokenTransfer.On("ProcessTransaction", mock.Anything, testTx).Return([]types.StateChange{}, errors.New("token transfer error"))

		// Create indexer
		indexer := &Indexer{
			participantsProcessor:  mockParticipants,
			tokenTransferProcessor: mockTokenTransfer,
			processors:             []OperationProcessorInterface{mockEffects, mockContractDeploy, mockSACEvents},
			pool:                   pond.NewPool(runtime.NumCPU()),
			networkPassphrase:      network.TestNetworkPassphrase,
		}

		// Test ProcessLedgerTransactions
		buffer := NewIndexerBuffer()
		participantCount, err := indexer.ProcessLedgerTransactions(context.Background(), []ingest.LedgerTransaction{testTx}, buffer)

		// Assert results
		require.Error(t, err)
		assert.Contains(t, err.Error(), "processing token transfer state changes: token transfer error")
		assert.Equal(t, 0, participantCount)

		// Verify mock expectations
		mockParticipants.AssertExpectations(t)
		mockTokenTransfer.AssertExpectations(t)
		mockEffects.AssertExpectations(t)
		mockContractDeploy.AssertExpectations(t)
		mockSACEvents.AssertExpectations(t)
	})

	t.Run("🔴 error in operation processor", func(t *testing.T) {
		// Create mocks
		mockParticipants := &MockParticipantsProcessor{}
		mockTokenTransfer := &MockTokenTransferProcessor{}
		mockEffects := &MockOperationProcessor{}
		mockContractDeploy := &MockOperationProcessor{}
		mockSACEvents := &MockOperationProcessor{}

		// Setup mocks
		mockParticipants.On("GetTransactionParticipants", testTx).Return(set.NewSet[string](), nil)
		opParticipants := map[int64]processors.OperationParticipants{
			1: {
				OpWrapper: &processors.TransactionOperationWrapper{
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

		// Create indexer
		indexer := &Indexer{
			participantsProcessor:  mockParticipants,
			tokenTransferProcessor: mockTokenTransfer,
			processors:             []OperationProcessorInterface{mockEffects, mockContractDeploy, mockSACEvents},
			pool:                   pond.NewPool(runtime.NumCPU()),
			networkPassphrase:      network.TestNetworkPassphrase,
		}

		// Test ProcessLedgerTransactions
		buffer := NewIndexerBuffer()
		participantCount, err := indexer.ProcessLedgerTransactions(context.Background(), []ingest.LedgerTransaction{testTx}, buffer)

		// Assert results
		require.Error(t, err)
		assert.Contains(t, err.Error(), "processing effects state changes: effects error")
		assert.Equal(t, 0, participantCount)

		// Verify mock expectations
		mockParticipants.AssertExpectations(t)
		mockTokenTransfer.AssertExpectations(t)
		mockEffects.AssertExpectations(t)
		mockContractDeploy.AssertExpectations(t)
		mockSACEvents.AssertExpectations(t)
	})

	t.Run("🟢 multiple state changes per operation verify ordering", func(t *testing.T) {
		// Create mocks
		mockParticipants := &MockParticipantsProcessor{}
		mockTokenTransfer := &MockTokenTransferProcessor{}
		mockEffects := &MockOperationProcessor{}
		mockContractDeploy := &MockOperationProcessor{}
		mockSACEvents := &MockOperationProcessor{}
		mockTrustlines := &MockTrustlinesProcessor{}
		mockAccounts := &MockAccountsProcessor{}
		mockSACBalances := &MockSACBalancesProcessor{}

		// Setup mock expectations
		txParticipants := set.NewSet("alice")
		mockParticipants.On("GetTransactionParticipants", testTx).Return(txParticipants, nil)

		opParticipants := map[int64]processors.OperationParticipants{
			1: {
				OpWrapper: &processors.TransactionOperationWrapper{
					Index:          0,
					Operation:      createAccountOp,
					Network:        network.TestNetworkPassphrase,
					LedgerSequence: 12345,
				},
				Participants: set.NewSet("alice"),
			},
		}
		mockParticipants.On("GetOperationsParticipants", testTx).Return(opParticipants, nil)

		mockEffects.On("ProcessOperation", mock.Anything, mock.Anything).Return([]types.StateChange{
			{ToID: 1, AccountID: "alice", OperationID: 1, SortKey: "1-1"},
			{ToID: 2, AccountID: "alice", OperationID: 1, SortKey: "1-2"},
			{ToID: 3, AccountID: "alice", OperationID: 1, SortKey: "1-3"},
		}, nil)
		mockContractDeploy.On("ProcessOperation", mock.Anything, mock.Anything).Return([]types.StateChange{}, nil)
		mockSACEvents.On("ProcessOperation", mock.Anything, mock.Anything).Return([]types.StateChange{}, nil)

		mockTokenTransfer.On("ProcessTransaction", mock.Anything, testTx).Return([]types.StateChange{}, nil)

		mockTrustlines.On("ProcessOperation", mock.Anything, mock.Anything).Return([]types.TrustlineChange{}, nil)
		mockAccounts.On("ProcessOperation", mock.Anything, mock.Anything).Return([]types.AccountChange{}, nil)
		mockSACBalances.On("ProcessOperation", mock.Anything, mock.Anything).Return([]types.SACBalanceChange{}, nil)

		// Create indexer
		indexer := &Indexer{
			participantsProcessor:  mockParticipants,
			tokenTransferProcessor: mockTokenTransfer,
			trustlinesProcessor:    mockTrustlines,
			accountsProcessor:      mockAccounts,
			sacBalancesProcessor:   mockSACBalances,
			processors:             []OperationProcessorInterface{mockEffects, mockContractDeploy, mockSACEvents},
			pool:                   pond.NewPool(runtime.NumCPU()),
			networkPassphrase:      network.TestNetworkPassphrase,
		}

		// Test ProcessLedgerTransactions
		buffer := NewIndexerBuffer()
		participantCount, err := indexer.ProcessLedgerTransactions(context.Background(), []ingest.LedgerTransaction{testTx}, buffer)

		// Assert
		require.NoError(t, err)
		assert.Equal(t, 1, participantCount)

		// Verify state changes with correct ordering
		stateChanges := buffer.GetStateChanges()
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

		// Verify mock expectations
		mockParticipants.AssertExpectations(t)
		mockTokenTransfer.AssertExpectations(t)
		mockEffects.AssertExpectations(t)
		mockContractDeploy.AssertExpectations(t)
		mockSACEvents.AssertExpectations(t)
		mockTrustlines.AssertExpectations(t)
		mockAccounts.AssertExpectations(t)
	})
}

func TestIndexer_getTransactionStateChanges(t *testing.T) {
	t.Run("🟢 state changes from all processors", func(t *testing.T) {
		// Create mocks
		mockTokenTransfer := &MockTokenTransferProcessor{}
		mockEffects := &MockOperationProcessor{}
		mockContractDeploy := &MockOperationProcessor{}
		mockSACEvents := &MockOperationProcessor{}

		// Setup mocks
		opsParticipants := map[int64]processors.OperationParticipants{
			1: {
				OpWrapper: &processors.TransactionOperationWrapper{
					Index:          0,
					Operation:      createAccountOp,
					Network:        network.TestNetworkPassphrase,
					LedgerSequence: 12345,
				},
				Participants: set.NewSet("alice"),
			},
		}

		mockEffects.On("ProcessOperation", mock.Anything, mock.Anything).Return([]types.StateChange{
			{ToID: 1, AccountID: "alice", OperationID: 1},
		}, nil)
		mockContractDeploy.On("ProcessOperation", mock.Anything, mock.Anything).Return([]types.StateChange{
			{ToID: 2, AccountID: "bob", OperationID: 1},
		}, nil)
		mockSACEvents.On("ProcessOperation", mock.Anything, mock.Anything).Return([]types.StateChange{}, nil)

		mockTokenTransfer.On("ProcessTransaction", mock.Anything, testTx).Return([]types.StateChange{
			{ToID: 3, AccountID: "charlie", OperationID: 0},
		}, nil)

		// Create indexer
		indexer := &Indexer{
			participantsProcessor:  &MockParticipantsProcessor{},
			tokenTransferProcessor: mockTokenTransfer,
			processors:             []OperationProcessorInterface{mockEffects, mockContractDeploy, mockSACEvents},
			pool:                   pond.NewPool(runtime.NumCPU()),
		}

		// Test getTransactionStateChanges
		stateChanges, err := indexer.getTransactionStateChanges(context.Background(), testTx, opsParticipants)

		// Assert
		require.NoError(t, err)
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

		// Verify mock expectations
		mockTokenTransfer.AssertExpectations(t)
		mockEffects.AssertExpectations(t)
		mockContractDeploy.AssertExpectations(t)
		mockSACEvents.AssertExpectations(t)
	})

	t.Run("🟢 no operations", func(t *testing.T) {
		// Create mocks
		mockTokenTransfer := &MockTokenTransferProcessor{}
		mockEffects := &MockOperationProcessor{}
		mockContractDeploy := &MockOperationProcessor{}
		mockSACEvents := &MockOperationProcessor{}

		// Setup mocks
		opsParticipants := map[int64]processors.OperationParticipants{}
		mockTokenTransfer.On("ProcessTransaction", mock.Anything, testTx).Return([]types.StateChange{}, nil)

		// Create indexer
		indexer := &Indexer{
			participantsProcessor:  &MockParticipantsProcessor{},
			tokenTransferProcessor: mockTokenTransfer,
			processors:             []OperationProcessorInterface{mockEffects, mockContractDeploy, mockSACEvents},
			pool:                   pond.NewPool(runtime.NumCPU()),
		}

		// Test getTransactionStateChanges
		stateChanges, err := indexer.getTransactionStateChanges(context.Background(), testTx, opsParticipants)

		// Assert
		require.NoError(t, err)
		assert.Len(t, stateChanges, 0)

		// Verify mock expectations
		mockTokenTransfer.AssertExpectations(t)
		mockEffects.AssertExpectations(t)
		mockContractDeploy.AssertExpectations(t)
		mockSACEvents.AssertExpectations(t)
	})

	t.Run("🔴 error in operation processor", func(t *testing.T) {
		// Create mocks
		mockTokenTransfer := &MockTokenTransferProcessor{}
		mockEffects := &MockOperationProcessor{}
		mockContractDeploy := &MockOperationProcessor{}
		mockSACEvents := &MockOperationProcessor{}

		// Setup mocks
		opsParticipants := map[int64]processors.OperationParticipants{
			1: {
				OpWrapper: &processors.TransactionOperationWrapper{
					Index:          0,
					Operation:      createAccountOp,
					Network:        network.TestNetworkPassphrase,
					LedgerSequence: 12345,
				},
				Participants: set.NewSet("alice"),
			},
		}

		mockEffects.On("ProcessOperation", mock.Anything, mock.Anything).Return([]types.StateChange{}, errors.New("processor error"))
		mockEffects.On("Name").Return("effects")

		// Create indexer
		indexer := &Indexer{
			participantsProcessor:  &MockParticipantsProcessor{},
			tokenTransferProcessor: mockTokenTransfer,
			processors:             []OperationProcessorInterface{mockEffects, mockContractDeploy, mockSACEvents},
			pool:                   pond.NewPool(runtime.NumCPU()),
		}

		// Test getTransactionStateChanges
		stateChanges, err := indexer.getTransactionStateChanges(context.Background(), testTx, opsParticipants)

		// Assert
		require.Error(t, err)
		assert.Contains(t, err.Error(), "processing effects state changes: processor error")
		assert.Nil(t, stateChanges)

		// Verify mock expectations
		mockTokenTransfer.AssertExpectations(t)
		mockEffects.AssertExpectations(t)
		mockContractDeploy.AssertExpectations(t)
		mockSACEvents.AssertExpectations(t)
	})

	t.Run("🔴 error in token transfer processor", func(t *testing.T) {
		// Create mocks
		mockTokenTransfer := &MockTokenTransferProcessor{}
		mockEffects := &MockOperationProcessor{}
		mockContractDeploy := &MockOperationProcessor{}
		mockSACEvents := &MockOperationProcessor{}

		// Setup mocks
		opsParticipants := map[int64]processors.OperationParticipants{}
		mockTokenTransfer.On("ProcessTransaction", mock.Anything, testTx).Return([]types.StateChange{}, errors.New("token transfer error"))

		// Create indexer
		indexer := &Indexer{
			participantsProcessor:  &MockParticipantsProcessor{},
			tokenTransferProcessor: mockTokenTransfer,
			processors:             []OperationProcessorInterface{mockEffects, mockContractDeploy, mockSACEvents},
			pool:                   pond.NewPool(runtime.NumCPU()),
		}

		// Test getTransactionStateChanges
		stateChanges, err := indexer.getTransactionStateChanges(context.Background(), testTx, opsParticipants)

		// Assert
		require.Error(t, err)
		assert.Contains(t, err.Error(), "processing token transfer state changes: token transfer error")
		assert.Nil(t, stateChanges)

		// Verify mock expectations
		mockTokenTransfer.AssertExpectations(t)
		mockEffects.AssertExpectations(t)
		mockContractDeploy.AssertExpectations(t)
		mockSACEvents.AssertExpectations(t)
	})

	t.Run("🟢 ErrInvalidOpType is ignored", func(t *testing.T) {
		// Create mocks
		mockTokenTransfer := &MockTokenTransferProcessor{}
		mockEffects := &MockOperationProcessor{}
		mockContractDeploy := &MockOperationProcessor{}
		mockSACEvents := &MockOperationProcessor{}

		// Setup mocks
		opsParticipants := map[int64]processors.OperationParticipants{
			1: {
				OpWrapper: &processors.TransactionOperationWrapper{
					Index:          0,
					Operation:      createAccountOp,
					Network:        network.TestNetworkPassphrase,
					LedgerSequence: 12345,
				},
				Participants: set.NewSet("alice"),
			},
		}

		// One processor returns ErrInvalidOpType (should be ignored)
		mockEffects.On("ProcessOperation", mock.Anything, mock.Anything).Return([]types.StateChange{}, processors.ErrInvalidOpType)
		// Other processors work normally
		mockContractDeploy.On("ProcessOperation", mock.Anything, mock.Anything).Return([]types.StateChange{
			{ToID: 1, AccountID: "alice", OperationID: 1},
		}, nil)
		mockSACEvents.On("ProcessOperation", mock.Anything, mock.Anything).Return([]types.StateChange{}, nil)

		mockTokenTransfer.On("ProcessTransaction", mock.Anything, testTx).Return([]types.StateChange{}, nil)

		// Create indexer
		indexer := &Indexer{
			participantsProcessor:  &MockParticipantsProcessor{},
			tokenTransferProcessor: mockTokenTransfer,
			processors:             []OperationProcessorInterface{mockEffects, mockContractDeploy, mockSACEvents},
			pool:                   pond.NewPool(runtime.NumCPU()),
		}

		// Test getTransactionStateChanges
		stateChanges, err := indexer.getTransactionStateChanges(context.Background(), testTx, opsParticipants)

		// Assert
		require.NoError(t, err)
		assert.Len(t, stateChanges, 1)

		// Verify it's the correct state change
		sc := stateChanges[0]
		assert.Equal(t, "alice", sc.AccountID)
		assert.Equal(t, int64(1), sc.ToID)
		assert.Equal(t, int64(1), sc.OperationID)

		// Verify mock expectations
		mockTokenTransfer.AssertExpectations(t)
		mockEffects.AssertExpectations(t)
		mockContractDeploy.AssertExpectations(t)
		mockSACEvents.AssertExpectations(t)
	})
}
